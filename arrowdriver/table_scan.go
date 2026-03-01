package arrowdriver

import (
	"runtime"
	"sort"
	"strconv"
	"sync"

	"github.com/bmeg/benchtop"
	"github.com/bytedance/sonic"
)

type requiredFieldsProvider interface {
	RequiredFields() []string
}

type rawPayloadMatcher interface {
	MatchesRawPayload(id string, payload string, tableName string) bool
}

func rowFilterNeedsID(filter benchtop.RowFilter) bool {
	if filter == nil {
		return false
	}
	r, ok := filter.(requiredFieldsProvider)
	if !ok {
		return false
	}
	for _, f := range r.RequiredFields() {
		if f == "_id" {
			return true
		}
	}
	return false
}

func rawPayloadForFilter(id, payload string, includeID bool) []byte {
	if !includeID {
		return []byte(payload)
	}
	buf := make([]byte, 0, len(payload)+len(id)+16)
	buf = append(buf, '{')
	buf = append(buf, '"', '_', 'i', 'd', '"', ':')
	buf = strconv.AppendQuote(buf, id)
	if len(payload) > 1 && payload[0] == '{' && payload[len(payload)-1] == '}' {
		if len(payload) > 2 {
			buf = append(buf, ',')
			buf = append(buf, payload[1:len(payload)-1]...)
		}
		buf = append(buf, '}')
		return buf
	}
	if len(payload) > 0 {
		buf = append(buf, ',', '"', '_', 'd', 'a', 't', 'a', '"', ':')
		buf = append(buf, payload...)
	}
	buf = append(buf, '}')
	return buf
}

func (t *ArrowTable) rawFilterRows(section uint16, secRows []indexedRow, filter benchtop.RowFilter) (map[uint32]map[string]any, bool) {
	if filter == nil || len(secRows) == 0 {
		return nil, false
	}
	needed := make(map[uint32]struct{}, len(secRows))
	for _, r := range secRows {
		needed[r.loc.Offset] = struct{}{}
	}

	rawMatcher, hasRawMatcher := filter.(rawPayloadMatcher)
	includeID := false
	if !hasRawMatcher {
		includeID = rowFilterNeedsID(filter)
	}

	out := make(map[uint32]map[string]any, len(secRows))
	complete, err := t.scanSectionRawByOffsets(section, needed, func(offset uint32, id string, payload string, hasPayload bool) bool {
		if !hasPayload {
			return true
		}
		matched := false
		if hasRawMatcher {
			matched = rawMatcher.MatchesRawPayload(id, payload, t.name)
		} else {
			matched = filter.Matches(rawPayloadForFilter(id, payload, includeID), t.name)
		}
		if !matched {
			return true
		}

		var row map[string]any
		if err := sonic.UnmarshalString(payload, &row); err != nil {
			return true
		}
		if row == nil {
			row = map[string]any{}
		}
		row[idColumn] = id
		out[offset] = row
		return true
	})
	if err != nil || !complete {
		return nil, false
	}
	return out, true
}

func (t *ArrowTable) ScanDoc(filter benchtop.RowFilter) chan map[string]any {
	out := make(chan map[string]any, 100)
	go func() {
		defer close(out)
		filterActive := filter != nil && !filter.IsNoOp()
		if !filterActive {
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
			type sectionDocResult struct {
				sec  uint16
				rows []map[string]any
			}
			secCh := make(chan uint16, len(sections))
			resCh := make(chan sectionDocResult, len(sections))
			workers := runtime.NumCPU()
			if workers < 2 {
				workers = 2
			}
			if workers > 16 {
				workers = 16
			}
			var wg sync.WaitGroup
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for sec := range secCh {
						secRows, _, err := t.readSectionRows(sec)
						if err != nil {
							resCh <- sectionDocResult{sec: sec, rows: nil}
							continue
						}
						matched := make([]map[string]any, 0, len(bySection[sec]))
						for _, r := range bySection[sec] {
							if int(r.loc.Offset) >= len(secRows) {
								continue
							}
							matched = append(matched, secRows[int(r.loc.Offset)])
						}
						resCh <- sectionDocResult{sec: sec, rows: matched}
					}
				}()
			}
			for _, secInt := range sections {
				secCh <- uint16(secInt)
			}
			close(secCh)
			go func() {
				wg.Wait()
				close(resCh)
			}()
			sectionRowsOut := map[uint16][]map[string]any{}
			for res := range resCh {
				sectionRowsOut[res.sec] = res.rows
			}
			for _, secInt := range sections {
				sec := uint16(secInt)
				for _, row := range sectionRowsOut[sec] {
					out <- row
				}
			}
			return
		}

		if simpleFilters, ok := extractSimpleFieldFilters(filter); ok {
			if matchedBySection, optimized, _ := t.findRowsByTopLevelFilters(simpleFilters); optimized {
				sections := make([]int, 0, len(matchedBySection))
				for sec := range matchedBySection {
					sections = append(sections, int(sec))
				}
				sort.Ints(sections)
				for _, secInt := range sections {
					sec := uint16(secInt)
					needed := map[uint32]struct{}{}
					for _, r := range matchedBySection[sec] {
						needed[r.loc.Offset] = struct{}{}
					}
					rowsAtOffset, err := t.readSectionRowsByOffsets(sec, needed)
					if err != nil {
						continue
					}
					for _, r := range matchedBySection[sec] {
						row, ok := rowsAtOffset[r.loc.Offset]
						if !ok {
							continue
						}
						out <- row
					}
				}
				return
			}
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
		type sectionDocResult struct {
			sec  uint16
			rows []map[string]any
		}
		secCh := make(chan uint16, len(sections))
		resCh := make(chan sectionDocResult, len(sections))
		workers := runtime.NumCPU()
		if workers < 2 {
			workers = 2
		}
		if workers > 16 {
			workers = 16
		}
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for sec := range secCh {
					secSet := bySection[sec]
					if filterActive {
						if matchedRows, ok := t.rawFilterRows(sec, secSet, filter); ok {
							matched := make([]map[string]any, 0, len(matchedRows))
							for _, r := range secSet {
								if row, ok := matchedRows[r.loc.Offset]; ok {
									matched = append(matched, row)
								}
							}
							resCh <- sectionDocResult{sec: sec, rows: matched}
							continue
						}
					}
					secRows, _, err := t.readSectionRows(sec)
					if err != nil {
						resCh <- sectionDocResult{sec: sec, rows: nil}
						continue
					}
					matched := make([]map[string]any, 0, len(secSet))
					for _, r := range secSet {
						if int(r.loc.Offset) >= len(secRows) {
							continue
						}
						row := secRows[int(r.loc.Offset)]
						if filterActive {
							payload, err := sonic.ConfigFastest.Marshal(row)
							if err != nil || !filter.Matches(payload, t.name) {
								continue
							}
						}
						matched = append(matched, row)
					}
					resCh <- sectionDocResult{sec: sec, rows: matched}
				}
			}()
		}
		for _, secInt := range sections {
			secCh <- uint16(secInt)
		}
		close(secCh)
		go func() {
			wg.Wait()
			close(resCh)
		}()
		sectionRowsOut := map[uint16][]map[string]any{}
		for res := range resCh {
			sectionRowsOut[res.sec] = res.rows
		}
		for _, secInt := range sections {
			sec := uint16(secInt)
			for _, row := range sectionRowsOut[sec] {
				out <- row
			}
		}
	}()
	return out
}

func (t *ArrowTable) ScanDocProjected(fields []string, filter benchtop.RowFilter) chan map[string]any {
	out := make(chan map[string]any, 100)
	go func() {
		defer close(out)

		if len(fields) == 0 {
			for row := range t.ScanDoc(filter) {
				out <- row
			}
			return
		}

		filterActive := filter != nil && !filter.IsNoOp()
		needsJSON := filterActive
		if filterActive {
			if simpleFilters, ok := extractSimpleFieldFilters(filter); ok {
				if matchedBySection, optimized, _ := t.findRowsByTopLevelFilters(simpleFilters); optimized {
					sections := make([]int, 0, len(matchedBySection))
					for sec := range matchedBySection {
						sections = append(sections, int(sec))
					}
					sort.Ints(sections)
					for _, secInt := range sections {
						sec := uint16(secInt)
						needed := map[uint32]struct{}{}
						for _, r := range matchedBySection[sec] {
							needed[r.loc.Offset] = struct{}{}
						}

						if !needsJSON {
							projRows, err := t.readSectionProjectedRowsByOffsets(sec, fields, needed)
							if err == nil {
								for _, r := range matchedBySection[sec] {
									row, ok := projRows[r.loc.Offset]
									if !ok {
										continue
									}
									out <- row
								}
								continue
							}
						}

						fullRows, err := t.readSectionRowsByOffsets(sec, needed)
						if err != nil {
							continue
						}
						for _, r := range matchedBySection[sec] {
							full, ok := fullRows[r.loc.Offset]
							if !ok {
								continue
							}
							proj := map[string]any{"_id": full["_id"]}
							for _, f := range fields {
								if f == "_id" {
									continue
								}
								if v, ok := full[f]; ok {
									proj[f] = v
								}
							}
							out <- proj
						}
					}
					return
				}
			}
		}
		if !filterActive && len(fields) > 0 {
			active, err := t.activeRowLocs()
			if err != nil {
				return
			}
			next, err := t.nextSection()
			if err != nil {
				return
			}
			for sec := uint16(0); sec < next; sec++ {
				if !needsJSON {
					projRows, err := t.readSectionProjectedRows(sec, fields)
					if err == nil {
						for i, row := range projRows {
							id, _ := row["_id"].(string)
							loc, ok := active[id]
							if !ok {
								continue
							}
							if loc.Section != sec || loc.Offset != uint32(i) {
								continue
							}
							out <- row
						}
						continue
					}
				}
				err := t.streamSectionRows(sec, func(id string, full map[string]any, offset uint32) bool {
					loc, ok := active[id]
					if !ok {
						return true
					}
					if loc.Section != sec || loc.Offset != offset {
						return true
					}
					proj := map[string]any{"_id": full["_id"]}
					for _, f := range fields {
						if f == "_id" {
							continue
						}
						if v, ok := full[f]; ok {
							proj[f] = v
						}
					}
					out <- proj
					return true
				})
				if err != nil {
					continue
				}
			}
			return
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

		for _, secInt := range sections {
			sec := uint16(secInt)
			if !needsJSON {
				projRows, err := t.readSectionProjectedRows(sec, fields)
				if err == nil {
					for _, r := range bySection[sec] {
						if int(r.loc.Offset) >= len(projRows) {
							continue
						}
						out <- projRows[int(r.loc.Offset)]
					}
					continue
				}
			}

			if filterActive {
				if matchedRows, ok := t.rawFilterRows(sec, bySection[sec], filter); ok {
					for _, r := range bySection[sec] {
						full, ok := matchedRows[r.loc.Offset]
						if !ok {
							continue
						}
						proj := map[string]any{"_id": full["_id"]}
						for _, f := range fields {
							if f == "_id" {
								continue
							}
							if v, ok := full[f]; ok {
								proj[f] = v
							}
						}
						out <- proj
					}
					continue
				}
			}

			secRows, _, err := t.readSectionRows(sec)
			if err != nil {
				continue
			}
			for _, r := range bySection[sec] {
				if int(r.loc.Offset) >= len(secRows) {
					continue
				}
				full := secRows[int(r.loc.Offset)]
				if filterActive {
					payload, err := sonic.ConfigFastest.Marshal(full)
					if err != nil || !filter.Matches(payload, t.name) {
						continue
					}
				}
				proj := map[string]any{"_id": full["_id"]}
				for _, f := range fields {
					if f == "_id" {
						continue
					}
					if v, ok := full[f]; ok {
						proj[f] = v
					}
				}
				out <- proj
			}
		}
	}()
	return out
}

func (t *ArrowTable) ScanId(filter benchtop.RowFilter) chan string {
	out := make(chan string, 100)
	go func() {
		defer close(out)
		if filter == nil || filter.IsNoOp() {
			// Fast path used by label scans: stream ids directly from index without
			// reading/decompressing Arrow row payloads.
			rows, err := t.listIndexRows()
			if err != nil {
				return
			}
			for _, r := range rows {
				out <- r.id
			}
			return
		}
		if simpleFilters, ok := extractSimpleFieldFilters(filter); ok {
			if matchedBySection, optimized, _ := t.findRowsByTopLevelFilters(simpleFilters); optimized {
				sections := make([]int, 0, len(matchedBySection))
				for sec := range matchedBySection {
					sections = append(sections, int(sec))
				}
				sort.Ints(sections)
				for _, secInt := range sections {
					sec := uint16(secInt)
					for _, r := range matchedBySection[sec] {
						out <- r.id
					}
				}
				return
			}
		}
		for row := range t.ScanDoc(filter) {
			if id, ok := row[idColumn].(string); ok {
				out <- id
			}
		}
	}()
	return out
}

func (t *ArrowTable) ScanFull(filter benchtop.RowFilter) chan benchtop.RowLocData {
	out := make(chan benchtop.RowLocData, 100)
	go func() {
		defer close(out)
		filterActive := filter != nil && !filter.IsNoOp()
		if filterActive {
			if simpleFilters, ok := extractSimpleFieldFilters(filter); ok {
				if matchedBySection, optimized, _ := t.findRowsByTopLevelFilters(simpleFilters); optimized {
					sections := make([]int, 0, len(matchedBySection))
					for sec := range matchedBySection {
						sections = append(sections, int(sec))
					}
					sort.Ints(sections)
					for _, secInt := range sections {
						sec := uint16(secInt)
						needed := map[uint32]struct{}{}
						for _, r := range matchedBySection[sec] {
							needed[r.loc.Offset] = struct{}{}
						}
						rowsAtOffset, err := t.readSectionRowsByOffsets(sec, needed)
						if err != nil {
							continue
						}
						for _, r := range matchedBySection[sec] {
							row, ok := rowsAtOffset[r.loc.Offset]
							if !ok {
								continue
							}
							out <- benchtop.RowLocData{DataMap: row, Loc: r.loc}
						}
					}
					return
				}
			}
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
		type sectionFullResult struct {
			sec  uint16
			rows []benchtop.RowLocData
		}
		secCh := make(chan uint16, len(sections))
		resCh := make(chan sectionFullResult, len(sections))
		workers := runtime.NumCPU()
		if workers < 2 {
			workers = 2
		}
		if workers > 16 {
			workers = 16
		}
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for sec := range secCh {
					secSet := bySection[sec]
					if filterActive {
						if matchedRows, ok := t.rawFilterRows(sec, secSet, filter); ok {
							matched := make([]benchtop.RowLocData, 0, len(matchedRows))
							for _, r := range secSet {
								if row, ok := matchedRows[r.loc.Offset]; ok {
									matched = append(matched, benchtop.RowLocData{DataMap: row, Loc: r.loc})
								}
							}
							resCh <- sectionFullResult{sec: sec, rows: matched}
							continue
						}
					}
					secRows, _, err := t.readSectionRows(sec)
					if err != nil {
						resCh <- sectionFullResult{sec: sec, rows: nil}
						continue
					}
					matched := make([]benchtop.RowLocData, 0, len(secSet))
					for _, r := range secSet {
						if int(r.loc.Offset) >= len(secRows) {
							continue
						}
						row := secRows[int(r.loc.Offset)]
						if filterActive {
							payload, err := sonic.ConfigFastest.Marshal(row)
							if err != nil || !filter.Matches(payload, t.name) {
								continue
							}
						}
						matched = append(matched, benchtop.RowLocData{DataMap: row, Loc: r.loc})
					}
					resCh <- sectionFullResult{sec: sec, rows: matched}
				}
			}()
		}
		for _, secInt := range sections {
			secCh <- uint16(secInt)
		}
		close(secCh)
		go func() {
			wg.Wait()
			close(resCh)
		}()
		sectionRowsOut := map[uint16][]benchtop.RowLocData{}
		for res := range resCh {
			sectionRowsOut[res.sec] = res.rows
		}
		for _, secInt := range sections {
			sec := uint16(secInt)
			for _, row := range sectionRowsOut[sec] {
				out <- row
			}
		}
	}()
	return out
}
