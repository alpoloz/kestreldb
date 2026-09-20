package engine

import "math/rand"

const (
	slMaxLevel = 16
	slP        = 0.25
)

type slLevel struct {
	next *slNode
	span int // number of level-0 nodes between this node and next at this level
}

type slNode struct {
	member   string
	score    float64
	backward *slNode
	levels   []slLevel
}

type slItem struct {
	Member string
	Score  float64
}

type skipList struct {
	head  *slNode
	tail  *slNode
	len   int
	level int
}

func newSLNode(level int, score float64, member string) *slNode {
	return &slNode{
		score:  score,
		member: member,
		levels: make([]slLevel, level),
	}
}

func newSkipList() *skipList {
	return &skipList{
		head:  newSLNode(slMaxLevel, 0, ""),
		level: 1,
	}
}

// slLess orders by (score ASC, member ASC) — member breaks ties deterministically.
func slLess(aScore float64, aMember string, bScore float64, bMember string) bool {
	if aScore != bScore {
		return aScore < bScore
	}
	return aMember < bMember
}

func (sl *skipList) randomLevel() int {
	level := 1
	for level < slMaxLevel && rand.Float64() < slP {
		level++
	}
	return level
}

func (sl *skipList) insert(score float64, member string) {
	update := make([]*slNode, slMaxLevel)
	rank := make([]int, slMaxLevel)

	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for x.levels[i].next != nil && slLess(x.levels[i].next.score, x.levels[i].next.member, score, member) {
			rank[i] += x.levels[i].span
			x = x.levels[i].next
		}
		update[i] = x
	}

	level := sl.randomLevel()
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			rank[i] = 0
			update[i] = sl.head
			update[i].levels[i].span = sl.len
		}
		sl.level = level
	}

	x = newSLNode(level, score, member)
	for i := 0; i < level; i++ {
		x.levels[i].next = update[i].levels[i].next
		update[i].levels[i].next = x
		// span of x at level i = old span of update[i] minus nodes now covered by update[i]
		x.levels[i].span = update[i].levels[i].span - (rank[0] - rank[i])
		update[i].levels[i].span = rank[0] - rank[i] + 1
	}
	// levels above the new node just gained one more node beneath them
	for i := level; i < sl.level; i++ {
		update[i].levels[i].span++
	}

	if update[0] == sl.head {
		x.backward = nil
	} else {
		x.backward = update[0]
	}
	if x.levels[0].next != nil {
		x.levels[0].next.backward = x
	} else {
		sl.tail = x
	}
	sl.len++
}

func (sl *skipList) delete(score float64, member string) bool {
	update := make([]*slNode, slMaxLevel)
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.levels[i].next != nil && slLess(x.levels[i].next.score, x.levels[i].next.member, score, member) {
			x = x.levels[i].next
		}
		update[i] = x
	}

	x = x.levels[0].next
	if x == nil || x.score != score || x.member != member {
		return false
	}

	for i := 0; i < sl.level; i++ {
		if update[i].levels[i].next == x {
			update[i].levels[i].span += x.levels[i].span - 1
			update[i].levels[i].next = x.levels[i].next
		} else {
			update[i].levels[i].span--
		}
	}

	if x.levels[0].next != nil {
		x.levels[0].next.backward = x.backward
	} else {
		sl.tail = x.backward
	}

	for sl.level > 1 && sl.head.levels[sl.level-1].next == nil {
		sl.level--
	}
	sl.len--
	return true
}

// nodeByRank returns the node at the given 1-indexed rank.
func (sl *skipList) nodeByRank(targetRank int) *slNode {
	rank := 0
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.levels[i].next != nil && rank+x.levels[i].span <= targetRank {
			rank += x.levels[i].span
			x = x.levels[i].next
		}
		if rank == targetRank {
			return x
		}
	}
	return nil
}

// rankOf returns the 0-indexed rank of the member with the given score.
func (sl *skipList) rankOf(score float64, member string) (int, bool) {
	rank := 0
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.levels[i].next != nil && slLess(x.levels[i].next.score, x.levels[i].next.member, score, member) {
			rank += x.levels[i].span
			x = x.levels[i].next
		}
		if x.levels[i].next != nil && x.levels[i].next.score == score && x.levels[i].next.member == member {
			return rank + x.levels[i].span - 1, true
		}
	}
	return 0, false
}

// rangeByRank returns items in the inclusive [start, stop] range (0-indexed, negatives wrap).
func (sl *skipList) rangeByRank(start, stop int) []slItem {
	start, stop, ok := normalizeRange(start, stop, sl.len)
	if !ok {
		return nil
	}
	x := sl.nodeByRank(start + 1) // nodeByRank is 1-indexed
	if x == nil {
		return nil
	}
	count := stop - start + 1
	result := make([]slItem, 0, count)
	for i := 0; i < count && x != nil; i++ {
		result = append(result, slItem{Member: x.member, Score: x.score})
		x = x.levels[0].next
	}
	return result
}

func normalizeRange(start, stop, length int) (int, int, bool) {
	if length == 0 {
		return 0, 0, false
	}
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop || stop < 0 {
		return 0, 0, false
	}
	return start, stop, true
}
