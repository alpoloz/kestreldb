package engine

// ScanOptions configures cursor-based collection scans. Count is the number
// of entries to examine, not a guaranteed result count. A zero Count selects
// the default. Match is ignored unless UseMatch is true.
type ScanOptions struct {
	Count    int
	Match    string
	UseMatch bool
	Type     string
	UseType  bool
}

const defaultScanCount = 10

// globMatch implements the byte-oriented glob syntax used by Redis scan
// commands: *, ?, character classes, ranges, negated classes, and backslash
// escapes.
func globMatch(pattern, value string) bool {
	type position struct {
		pattern int
		value   int
	}
	memo := make(map[position]bool)
	seen := make(map[position]bool)

	var match func(int, int) bool
	match = func(patternIndex, valueIndex int) bool {
		pos := position{pattern: patternIndex, value: valueIndex}
		if seen[pos] {
			return memo[pos]
		}
		seen[pos] = true

		matched := false
		if patternIndex == len(pattern) {
			matched = valueIndex == len(value)
		} else {
			switch pattern[patternIndex] {
			case '*':
				for patternIndex+1 < len(pattern) && pattern[patternIndex+1] == '*' {
					patternIndex++
				}
				for nextValue := valueIndex; nextValue <= len(value); nextValue++ {
					if match(patternIndex+1, nextValue) {
						matched = true
						break
					}
				}
			case '?':
				matched = valueIndex < len(value) && match(patternIndex+1, valueIndex+1)
			case '[':
				if valueIndex < len(value) {
					classMatched, nextPattern, valid := matchCharacterClass(pattern, patternIndex+1, value[valueIndex])
					if valid {
						matched = classMatched && match(nextPattern, valueIndex+1)
					} else {
						matched = value[valueIndex] == '[' && match(patternIndex+1, valueIndex+1)
					}
				}
			case '\\':
				literal := byte('\\')
				nextPattern := patternIndex + 1
				if nextPattern < len(pattern) {
					literal = pattern[nextPattern]
					nextPattern++
				}
				matched = valueIndex < len(value) && value[valueIndex] == literal && match(nextPattern, valueIndex+1)
			default:
				matched = valueIndex < len(value) && value[valueIndex] == pattern[patternIndex] && match(patternIndex+1, valueIndex+1)
			}
		}

		memo[pos] = matched
		return matched
	}

	return match(0, 0)
}

func matchCharacterClass(pattern string, index int, value byte) (matched bool, next int, valid bool) {
	negated := false
	if index < len(pattern) && pattern[index] == '^' {
		negated = true
		index++
	}

	hasElement := false
	for index < len(pattern) {
		if pattern[index] == ']' && hasElement {
			if negated {
				matched = !matched
			}
			return matched, index + 1, true
		}

		start := pattern[index]
		if start == '\\' && index+1 < len(pattern) {
			index++
			start = pattern[index]
		}
		index++
		hasElement = true

		if index+1 < len(pattern) && pattern[index] == '-' && pattern[index+1] != ']' {
			end := pattern[index+1]
			index += 2
			if start > end {
				start, end = end, start
			}
			if value >= start && value <= end {
				matched = true
			}
			continue
		}
		if value == start {
			matched = true
		}
	}

	return false, 0, false
}
