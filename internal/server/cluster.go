package server

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"

	"kestreldb/internal/proto"
)

const ClusterSlotCount = 16384

// ClusterSlotRange is an inclusive range of Redis-compatible hash slots.
type ClusterSlotRange struct {
	Start uint16 `json:"start"`
	End   uint16 `json:"end"`
}

// ClusterNode describes one node and the slots it currently owns.
type ClusterNode struct {
	ID      string             `json:"id"`
	Address string             `json:"address"`
	Slots   []ClusterSlotRange `json:"slots"`
}

// ClusterTopology is an atomically replaceable view of cluster ownership.
// SetClusterTopology may be called while the server is running.
type ClusterTopology struct {
	LocalID string        `json:"local_id"`
	Nodes   []ClusterNode `json:"nodes"`
}

type clusterState struct {
	topology ClusterTopology
	nodes    map[string]ClusterNode
	owners   [ClusterSlotCount]string
}

type codedError struct {
	code    string
	message string
}

func (e *codedError) Error() string { return e.message }
func (e *codedError) Code() string  { return e.code }

// SetClusterTopology validates and atomically installs a topology. Unassigned
// slots are allowed during topology changes and return CLUSTERDOWN until a
// subsequent topology assigns them.
func (s *Server) SetClusterTopology(topology ClusterTopology) error {
	state, err := buildClusterState(topology)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.cluster = state
	s.mu.Unlock()
	return nil
}

// ClearClusterTopology returns the server to standalone routing mode.
func (s *Server) ClearClusterTopology() {
	s.mu.Lock()
	s.cluster = nil
	s.mu.Unlock()
}

// ClusterTopology returns a detached snapshot of the installed topology.
func (s *Server) ClusterTopology() (ClusterTopology, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cluster == nil {
		return ClusterTopology{}, false
	}
	return cloneClusterTopology(s.cluster.topology), true
}

func buildClusterState(topology ClusterTopology) (*clusterState, error) {
	if topology.LocalID == "" {
		return nil, errors.New("cluster local_id is required")
	}
	state := &clusterState{
		topology: cloneClusterTopology(topology),
		nodes:    make(map[string]ClusterNode, len(topology.Nodes)),
	}
	addresses := make(map[string]struct{}, len(topology.Nodes))
	for _, node := range topology.Nodes {
		if node.ID == "" || node.Address == "" {
			return nil, errors.New("cluster node id and address are required")
		}
		if _, exists := state.nodes[node.ID]; exists {
			return nil, fmt.Errorf("duplicate cluster node id %q", node.ID)
		}
		if _, exists := addresses[node.Address]; exists {
			return nil, fmt.Errorf("duplicate cluster node address %q", node.Address)
		}
		if _, _, err := net.SplitHostPort(node.Address); err != nil {
			return nil, fmt.Errorf("invalid cluster node address %q: %w", node.Address, err)
		}
		addresses[node.Address] = struct{}{}
		state.nodes[node.ID] = node
		for _, slots := range node.Slots {
			if slots.Start > slots.End || int(slots.End) >= ClusterSlotCount {
				return nil, fmt.Errorf("invalid slot range %d-%d", slots.Start, slots.End)
			}
			for slot := int(slots.Start); slot <= int(slots.End); slot++ {
				if owner := state.owners[slot]; owner != "" {
					return nil, fmt.Errorf("slot %d is assigned to both %q and %q", slot, owner, node.ID)
				}
				state.owners[slot] = node.ID
			}
		}
	}
	if _, exists := state.nodes[topology.LocalID]; !exists {
		return nil, fmt.Errorf("local cluster node %q is not present in topology", topology.LocalID)
	}
	return state, nil
}

func cloneClusterTopology(topology ClusterTopology) ClusterTopology {
	cloned := ClusterTopology{LocalID: topology.LocalID, Nodes: make([]ClusterNode, len(topology.Nodes))}
	for index, node := range topology.Nodes {
		cloned.Nodes[index] = node
		cloned.Nodes[index].Slots = append([]ClusterSlotRange(nil), node.Slots...)
	}
	return cloned
}

func (s *Server) routeCommand(name string, args []string) error {
	keys := commandKeys(name, args)
	if len(keys) == 0 {
		return nil
	}
	s.mu.Lock()
	state := s.cluster
	s.mu.Unlock()
	if state == nil {
		return nil
	}
	slot := ClusterSlot(keys[0])
	for _, key := range keys[1:] {
		if ClusterSlot(key) != slot {
			return &codedError{code: "CROSSSLOT", message: "Keys in request don't hash to the same slot"}
		}
	}
	ownerID := state.owners[slot]
	if ownerID == "" {
		return &codedError{code: "CLUSTERDOWN", message: "Hash slot not served"}
	}
	if ownerID != state.topology.LocalID {
		return &codedError{code: "MOVED", message: fmt.Sprintf("%d %s", slot, state.nodes[ownerID].Address)}
	}
	return nil
}

// ClusterSlot implements the Redis Cluster CRC16/XMODEM slot algorithm,
// including the first non-empty {...} hash tag in a key.
func ClusterSlot(key string) uint16 {
	value := key
	if open := strings.IndexByte(key, '{'); open >= 0 {
		if closeAt := strings.IndexByte(key[open+1:], '}'); closeAt > 0 {
			value = key[open+1 : open+1+closeAt]
		}
	}
	var crc uint16
	for index := 0; index < len(value); index++ {
		crc ^= uint16(value[index]) << 8
		for bit := 0; bit < 8; bit++ {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc % ClusterSlotCount
}

func (s *Server) writeClusterCommand(w *proto.Writer, args []string) error {
	if len(args) == 0 {
		return errors.New("wrong number of arguments for 'cluster' command")
	}
	s.mu.Lock()
	state := s.cluster
	s.mu.Unlock()
	if state == nil {
		return errors.New("cluster mode is disabled")
	}
	switch strings.ToUpper(args[0]) {
	case "MYID":
		if len(args) != 1 {
			return errors.New("wrong number of arguments for 'cluster|myid' command")
		}
		return w.WriteBlobString([]byte(state.topology.LocalID))
	case "KEYSLOT":
		if len(args) != 2 {
			return errors.New("wrong number of arguments for 'cluster|keyslot' command")
		}
		return w.WriteInt(int(ClusterSlot(args[1])))
	case "INFO":
		if len(args) != 1 {
			return errors.New("wrong number of arguments for 'cluster|info' command")
		}
		assigned := 0
		for _, owner := range state.owners {
			if owner != "" {
				assigned++
			}
		}
		clusterState := "fail"
		if assigned == ClusterSlotCount {
			clusterState = "ok"
		}
		value := fmt.Sprintf("cluster_state:%s\r\ncluster_slots_assigned:%d\r\ncluster_known_nodes:%d\r\n", clusterState, assigned, len(state.nodes))
		return w.WriteBlobString([]byte(value))
	case "SLOTS":
		if len(args) != 1 {
			return errors.New("wrong number of arguments for 'cluster|slots' command")
		}
		return writeClusterSlots(w, state)
	default:
		return fmt.Errorf("unsupported CLUSTER subcommand %q", args[0])
	}
}

type ownedSlotRange struct {
	rangeValue ClusterSlotRange
	node       ClusterNode
}

func writeClusterSlots(w *proto.Writer, state *clusterState) error {
	ranges := make([]ownedSlotRange, 0)
	for _, node := range state.topology.Nodes {
		for _, slots := range node.Slots {
			ranges = append(ranges, ownedSlotRange{rangeValue: slots, node: node})
		}
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].rangeValue.Start < ranges[j].rangeValue.Start })
	if err := w.WriteArrayHeader(len(ranges)); err != nil {
		return err
	}
	for _, owned := range ranges {
		host, portValue, err := net.SplitHostPort(owned.node.Address)
		if err != nil {
			return err
		}
		port, err := strconv.Atoi(portValue)
		if err != nil {
			return err
		}
		if err := w.WriteArrayHeader(3); err != nil {
			return err
		}
		if err := w.WriteInt(int(owned.rangeValue.Start)); err != nil {
			return err
		}
		if err := w.WriteInt(int(owned.rangeValue.End)); err != nil {
			return err
		}
		if err := w.WriteArrayHeader(3); err != nil {
			return err
		}
		if err := w.WriteBlobString([]byte(host)); err != nil {
			return err
		}
		if err := w.WriteInt(port); err != nil {
			return err
		}
		if err := w.WriteBlobString([]byte(owned.node.ID)); err != nil {
			return err
		}
	}
	return nil
}
