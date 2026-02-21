package engine

type ZSetItem struct {
	Member string
	Score  float64
}

type ZSet struct {
	dict map[string]float64
	sl   *SkipList
}

func NewZSet() *ZSet {
	return &ZSet{
		dict: make(map[string]float64),
		sl:   NewSkipList(),
	}
}

func (z *ZSet) Add(score float64, member string) bool {
	oldScore, exists := z.dict[member]
	if exists {
		if oldScore == score {
			return false
		}
		z.sl.Delete(oldScore, member)
		z.sl.Insert(score, member)
		z.dict[member] = score
		return false
	}

	z.dict[member] = score
	z.sl.Insert(score, member)
	return true
}

func (z *ZSet) Remove(member string) bool {
	score, exists := z.dict[member]
	if !exists {
		return false
	}
	delete(z.dict, member)
	z.sl.Delete(score, member)
	return true
}

func (z *ZSet) Score(member string) (float64, bool) {
	score, ok := z.dict[member]
	return score, ok
}

func (z *ZSet) Len() int {
	return len(z.dict)
}

func (z *ZSet) Range(start int, stop int) []ZSetItem {
	items := z.sl.Range(start, stop)
	out := make([]ZSetItem, 0, len(items))
	for _, item := range items {
		out = append(out, ZSetItem{Member: item.Member, Score: item.Score})
	}
	return out
}
