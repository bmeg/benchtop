package arrowdriver

import (
	"sort"

	"github.com/bmeg/benchtop"
)

// RawDoc is a raw JSON row payload plus id.
type RawDoc struct {
	ID      string
	Payload string
}

// ScanDocRaw streams raw JSON payload rows directly from section files. It is
// intended for high-throughput read paths that can consume raw payloads without
// materializing map[string]any for every row.
func (t *ArrowTable) ScanDocRaw(filter benchtop.RowFilter) chan RawDoc {
	out := make(chan RawDoc, 100)
	go func() {
		defer close(out)

		filterActive := filter != nil && !filter.IsNoOp()
		rawMatcher, hasRawMatcher := filter.(rawPayloadMatcher)
		includeID := false
		if filterActive && !hasRawMatcher {
			includeID = rowFilterNeedsID(filter)
		}

		rows, err := t.listIndexRows()
		if err != nil {
			return
		}
		bySection := map[uint16][]indexedRow{}
		for _, r := range rows {
			bySection[r.loc.Section] = append(bySection[r.loc.Section], r)
		}
		sections := make([]int, 0, len(bySection))
		for sec := range bySection {
			sections = append(sections, int(sec))
		}
		sort.Ints(sections)

		if filterActive {
			if simpleFilters, ok := extractSimpleFieldFilters(filter); ok {
				if matchedBySection, optimized, _ := t.findRowsByTopLevelFilters(simpleFilters); optimized {
					sections = sections[:0]
					for sec := range matchedBySection {
						sections = append(sections, int(sec))
					}
					sort.Ints(sections)
					bySection = matchedBySection
				}
			}
		}

		for _, secInt := range sections {
			sec := uint16(secInt)
			secSet := bySection[sec]
			if len(secSet) == 0 {
				continue
			}
			offsets := make(map[uint32]struct{}, len(secSet))
			for _, r := range secSet {
				offsets[r.loc.Offset] = struct{}{}
			}

			_, err := t.scanSectionRawByOffsets(sec, offsets, func(offset uint32, id string, payload string, hasPayload bool) bool {
				if !hasPayload || payload == "" {
					return true
				}
				if filterActive {
					matched := false
					if hasRawMatcher {
						matched = rawMatcher.MatchesRawPayload(id, payload, t.name)
					} else {
						matched = filter.Matches(rawPayloadForFilter(id, payload, includeID), t.name)
					}
					if !matched {
						return true
					}
				}
				out <- RawDoc{ID: id, Payload: payload}
				return true
			})
			if err != nil {
				continue
			}
		}
	}()
	return out
}
