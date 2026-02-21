package engine

import (
	"math/rand"
	"time"
)

const (
	skipListMaxLevel = 16
	skipListP        = 0.25
)

type skipListLevel struct {
	forward *skipListNode
}

type skipListNode struct {
	member   string
	score    float64
	backward *skipListNode
	level    []skipListLevel
}

type SkipListItem struct {
	Member string
	Score  float64
}

type SkipList struct {
	header *skipListNode
	tail   *skipListNode
	level  int
	length int
	rand   *rand.Rand
}

func newSkipListNode(level int, score float64, member string) *skipListNode {
	return &skipListNode{
		score:  score,
		member: member,
		level:  make([]skipListLevel, level),
	}
}

func NewSkipList() *SkipList {
	header := newSkipListNode(skipListMaxLevel, 0, "")
	return &SkipList{
		header: header,
		level:  1,
		rand:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (sl *SkipList) Len() int {
	return sl.length
}

func (sl *SkipList) randomLevel() int {
	level := 1
	for level < skipListMaxLevel && sl.rand.Float64() < skipListP {
		level++
	}
	return level
}

func less(scoreA float64, memberA string, scoreB float64, memberB string) bool {
	if scoreA < scoreB {
		return true
	}
	if scoreA > scoreB {
		return false
	}
	return memberA < memberB
}

func (sl *SkipList) Insert(score float64, member string) {
	var update [skipListMaxLevel]*skipListNode
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && less(x.level[i].forward.score, x.level[i].forward.member, score, member) {
			x = x.level[i].forward
		}
		update[i] = x
	}

	newLevel := sl.randomLevel()
	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			update[i] = sl.header
		}
		sl.level = newLevel
	}

	x = newSkipListNode(newLevel, score, member)
	for i := 0; i < newLevel; i++ {
		x.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = x
	}

	if update[0] == sl.header {
		x.backward = nil
	} else {
		x.backward = update[0]
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x
	} else {
		sl.tail = x
	}

	sl.length++
}

func (sl *SkipList) Delete(score float64, member string) bool {
	var update [skipListMaxLevel]*skipListNode
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && less(x.level[i].forward.score, x.level[i].forward.member, score, member) {
			x = x.level[i].forward
		}
		update[i] = x
	}

	x = x.level[0].forward
	if x == nil || x.score != score || x.member != member {
		return false
	}

	for i := 0; i < sl.level; i++ {
		if update[i].level[i].forward == x {
			update[i].level[i].forward = x.level[i].forward
		}
	}

	if x.level[0].forward != nil {
		x.level[0].forward.backward = x.backward
	} else {
		sl.tail = x.backward
	}

	for sl.level > 1 && sl.header.level[sl.level-1].forward == nil {
		sl.level--
	}

	sl.length--
	return true
}

func (sl *SkipList) Range(start int, stop int) []SkipListItem {
	start, stop, ok := normalizeRange(start, stop, sl.length)
	if !ok {
		return nil
	}

	x := sl.header.level[0].forward
	for i := 0; i < start; i++ {
		x = x.level[0].forward
	}

	out := make([]SkipListItem, 0, stop-start+1)
	for i := start; i <= stop && x != nil; i++ {
		out = append(out, SkipListItem{Member: x.member, Score: x.score})
		x = x.level[0].forward
	}

	return out
}

func normalizeRange(start int, stop int, length int) (int, int, bool) {
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
	if stop < 0 {
		return 0, 0, false
	}
	if start >= length {
		return 0, 0, false
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop {
		return 0, 0, false
	}

	return start, stop, true
}
