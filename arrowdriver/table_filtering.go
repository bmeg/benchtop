package arrowdriver

import (
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bmeg/benchtop"
	bfilters "github.com/bmeg/benchtop/filters"
	"github.com/bmeg/benchtop/jsontable/tpath"
	"github.com/bmeg/benchtop/query"
	"github.com/bmeg/benchtop/util"
	"github.com/bmeg/grip/log"
)

func isTopLevelField(field string) bool {
	if field == "" {
		return false
	}
	return !strings.Contains(field, ".") && !strings.Contains(field, "[")
}

func tableLabelFromName(name string) string {
	if len(name) > 2 && name[1] == '_' {
		return name[2:]
	}
	return name
}

func extractSimpleFieldFilters(filter benchtop.RowFilter) ([]query.FieldFilter, bool) {
	if filter == nil || filter.IsNoOp() {
		return nil, false
	}
	raw := filter.GetFilter()
	if raw == nil {
		return nil, false
	}
	switch f := raw.(type) {
	case []query.FieldFilter:
		if len(f) == 0 {
			return nil, false
		}
		out := make([]query.FieldFilter, len(f))
		copy(out, f)
		return out, true
	}

	v := reflect.ValueOf(raw)
	if v.Kind() != reflect.Slice {
		return nil, false
	}
	out := make([]query.FieldFilter, 0, v.Len())
	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i)
		if elem.Kind() == reflect.Interface && !elem.IsNil() {
			elem = elem.Elem()
		}
		if !elem.IsValid() || elem.Kind() != reflect.Struct {
			return nil, false
		}

		fField := elem.FieldByName("Field")
		fOp := elem.FieldByName("Operator")
		fVal := elem.FieldByName("Value")
		if !fField.IsValid() || !fOp.IsValid() || !fVal.IsValid() || fField.Kind() != reflect.String {
			return nil, false
		}
		var op int64
		switch fOp.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			op = fOp.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			op = int64(fOp.Uint())
		default:
			return nil, false
		}
		out = append(out, query.FieldFilter{
			Field:    fField.String(),
			Operator: query.Condition(op),
			Value:    fVal.Interface(),
		})
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

func canUseTopLevelFilter(filters []query.FieldFilter) bool {
	for _, f := range filters {
		switch f.Field {
		case "_id", "_label":
			continue
		default:
			if strings.Contains(f.Field, "*") {
				return false
			}
		}
	}
	return true
}

func isIndexedSetOp(op query.Condition) bool {
	return op == query.EQ || op == query.WITHIN
}

func groupIndexedRowsBySection(rows []indexedRow) map[uint16][]indexedRow {
	out := map[uint16][]indexedRow{}
	for _, r := range rows {
		out[r.loc.Section] = append(out[r.loc.Section], r)
	}
	return out
}

func (t *ArrowTable) indexedRowsByIDFilter(f query.FieldFilter) []indexedRow {
	values := []any{f.Value}
	if f.Operator == query.WITHIN {
		values = util.SliceToAny(f.Value)
	}
	out := make([]indexedRow, 0, len(values))
	for _, v := range values {
		id, ok := v.(string)
		if !ok {
			continue
		}
		loc, err := t.GetRowLoc(id)
		if err != nil {
			continue
		}
		out = append(out, indexedRow{id: id, loc: loc})
	}
	return out
}

// tryIndexedConjunction returns a fully-indexed top-level AND filter result.
// It only optimizes EQ/WITHIN conjunctions on indexed fields (plus _id/_label guards).
func (t *ArrowTable) tryIndexedConjunction(filters []query.FieldFilter) (map[uint16][]indexedRow, bool) {
	label := tableLabelFromName(t.name)
	allRows, byID, err := t.getOrLoadRowOrdinalCache()
	if err != nil {
		return nil, false
	}
	if len(allRows) == 0 {
		return map[uint16][]indexedRow{}, true
	}

	seeded := false
	var acc *denseBitset

	for _, f := range filters {
		if !isIndexedSetOp(f.Operator) {
			return nil, false
		}
		switch f.Field {
		case "_label":
			if !bfilters.ApplyFilterCondition(label, &f) {
				return map[uint16][]indexedRow{}, true
			}
			continue
		case "_id":
			rows := t.indexedRowsByIDFilter(f)
			bits := newDenseBitset(len(allRows))
			for _, r := range rows {
				if ord, ok := byID[r.id]; ok {
					bits.set(ord)
				}
			}
			if !seeded {
				acc = bits
				seeded = true
			} else {
				acc.and(bits)
			}
			continue
		default:
			t.lock.RLock()
			_, indexed := t.indexedFields[f.Field]
			t.lock.RUnlock()
			if !indexed {
				return nil, false
			}
			values := []any{f.Value}
			if f.Operator == query.WITHIN {
				values = util.SliceToAny(f.Value)
			}
			rows, _ := t.indexedMatches(f.Field, values)
			bits := newDenseBitset(len(allRows))
			for _, r := range rows {
				if ord, ok := byID[r.id]; ok {
					bits.set(ord)
				}
			}
			if !seeded {
				acc = bits
				seeded = true
			} else {
				acc.and(bits)
			}
		}
		if seeded && !acc.any() {
			return map[uint16][]indexedRow{}, true
		}
	}
	if !seeded {
		return nil, false
	}
	ordinals := acc.indices()
	rows := make([]indexedRow, 0, len(ordinals))
	for _, ord := range ordinals {
		i := int(ord)
		if i >= 0 && i < len(allRows) {
			rows = append(rows, allRows[i])
		}
	}
	return groupIndexedRowsBySection(rows), true
}

func applyConditionsOnOffset(filters []query.FieldFilter, ids []string, cols map[string][]any, offset uint32, label string) bool {
	i := int(offset)
	if i < 0 || i >= len(ids) {
		return false
	}
	for _, cond := range filters {
		var v any
		switch cond.Field {
		case "_id":
			v = ids[i]
		case "_label":
			v = label
		default:
			cv, ok := cols[cond.Field]
			if !ok || i >= len(cv) {
				v = nil
			} else {
				v = cv[i]
			}
		}
		if !bfilters.ApplyFilterCondition(v, &cond) {
			return false
		}
	}
	return true
}

func (t *ArrowTable) findRowsByTopLevelFilters(filters []query.FieldFilter) (map[uint16][]indexedRow, bool, error) {
	if len(filters) == 0 || !canUseTopLevelFilter(filters) {
		return nil, false, nil
	}
	start := time.Now()
	if matchedBySection, ok := t.tryIndexedConjunction(filters); ok {
		matchCount := 0
		for _, m := range matchedBySection {
			matchCount += len(m)
		}
		log.Debugf("arrowtable.findRowsByTopLevelFilters indexed_conjunction table=%s filters=%d matches=%d sections=%d elapsed=%s", t.name, len(filters), matchCount, len(matchedBySection), time.Since(start).Round(time.Millisecond))
		return matchedBySection, true, nil
	}
	rows, err := t.listIndexRows()
	if err != nil {
		return nil, false, err
	}
	bySection := map[uint16][]indexedRow{}
	for _, r := range rows {
		bySection[r.loc.Section] = append(bySection[r.loc.Section], r)
	}
	label := tableLabelFromName(t.name)
	out := map[uint16][]indexedRow{}

	required := make([]string, 0, len(filters))
	seen := map[string]struct{}{}
	for _, f := range filters {
		if f.Field == "_id" || f.Field == "_label" {
			continue
		}
		if _, ok := seen[f.Field]; ok {
			continue
		}
		seen[f.Field] = struct{}{}
		required = append(required, f.Field)
	}

	sections := make([]uint16, 0, len(bySection))
	for sec := range bySection {
		sections = append(sections, sec)
	}
	sort.Slice(sections, func(i, j int) bool { return sections[i] < sections[j] })

	workers := runtime.NumCPU()
	if workers < 2 {
		workers = 2
	}
	if workers > 16 {
		workers = 16
	}
	secCh := make(chan uint16, len(sections))
	var wg sync.WaitGroup
	var outMu sync.Mutex
	var failed atomic.Bool

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sec := range secCh {
				secRows := bySection[sec]
				ids, cols, err := t.readSectionTopLevelColumns(sec, required)
				if err != nil {
					failed.Store(true)
					continue
				}
				matches := make([]indexedRow, 0, len(secRows))
				for _, r := range secRows {
					if applyConditionsOnOffset(filters, ids, cols, r.loc.Offset, label) {
						matches = append(matches, r)
					}
				}
				if len(matches) > 0 {
					outMu.Lock()
					out[sec] = matches
					outMu.Unlock()
				}
			}
		}()
	}
	for _, sec := range sections {
		secCh <- sec
	}
	close(secCh)
	wg.Wait()
	if failed.Load() {
		log.Debugf("arrowtable.findRowsByTopLevelFilters fallback table=%s filters=%d reason=column_read_error elapsed=%s", t.name, len(filters), time.Since(start).Round(time.Millisecond))
		return nil, false, nil
	}
	matchCount := 0
	for _, m := range out {
		matchCount += len(m)
	}
	log.Debugf("arrowtable.findRowsByTopLevelFilters optimized table=%s filters=%d rows=%d matches=%d sections=%d elapsed=%s", t.name, len(filters), len(rows), matchCount, len(out), time.Since(start).Round(time.Millisecond))
	return out, true, nil
}

func (t *ArrowTable) indexedMatches(field string, values []any) ([]indexedRow, int) {
	seen := map[string]struct{}{}
	out := make([]indexedRow, 0, 1024)
	totalMissing := 0
	for _, v := range values {
		valueBytes, ok := encodeIndexValue(v)
		if !ok {
			continue
		}
		rows, missing := t.getOrLoadValuePostings(field, valueBytes)
		totalMissing += missing
		for _, r := range rows {
			if _, ok := seen[r.id]; ok {
				continue
			}
			seen[r.id] = struct{}{}
			out = append(out, r)
		}
	}
	return out, totalMissing
}

func (t *ArrowTable) RowIdsByHas(field string, value any, op query.Condition) chan string {
	out := make(chan string, 100)
	go func() {
		defer close(out)
		if field == "_label" {
			label := tableLabelFromName(t.name)
			rows, err := t.listIndexRows()
			if err != nil {
				return
			}
			cond := &query.FieldFilter{Field: field, Operator: op, Value: value}
			for _, r := range rows {
				if bfilters.ApplyFilterCondition(label, cond) {
					out <- r.id
				}
			}
			return
		}
		if op == query.EQ || op == query.WITHIN {
			t.lock.RLock()
			_, indexed := t.indexedFields[field]
			t.lock.RUnlock()
			if indexed {
				vals := []any{value}
				if op == query.WITHIN {
					vals = util.SliceToAny(value)
				}
				matches, _ := t.indexedMatches(field, vals)
				if len(matches) > 0 {
					for _, m := range matches {
						out <- m.id
					}
					return
				}
			}
		}
		if isTopLevelField(field) && !strings.Contains(field, "*") {
			filters := []query.FieldFilter{{Field: field, Operator: op, Value: value}}
			if matchedBySection, optimized, _ := t.findRowsByTopLevelFilters(filters); optimized {
				matchCount := 0
				for _, rows := range matchedBySection {
					matchCount += len(rows)
				}
				if matchCount == 0 {
					// Defensive fallback: avoid false-zero regressions from
					// top-level-only evaluation; run legacy path below.
				} else {
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

		materializedField := !strings.Contains(field, "*")

		for _, secInt := range sections {
			sec := uint16(secInt)
			if materializedField {
				colVals, found, err := t.readSectionTopLevelColumn(sec, field)
				if err == nil && found {
					for _, r := range bySection[sec] {
						if int(r.loc.Offset) >= len(colVals) {
							continue
						}
						if bfilters.ApplyFilterCondition(colVals[int(r.loc.Offset)], &query.FieldFilter{Field: field, Operator: op, Value: value}) {
							out <- r.id
						}
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
				row := secRows[int(r.loc.Offset)]
				fieldVal := tpath.PathLookup(row, field)
				if bfilters.ApplyFilterCondition(fieldVal, &query.FieldFilter{Field: field, Operator: op, Value: value}) {
					out <- r.id
				}
			}
		}
	}()
	return out
}

func (t *ArrowTable) RowIndexesByHas(field string, value any, op query.Condition) chan benchtop.Index {
	out := make(chan benchtop.Index, 100)
	go func() {
		defer close(out)
		start := time.Now()
		total := 0
		if op == query.EQ || op == query.WITHIN {
			t.lock.RLock()
			_, indexed := t.indexedFields[field]
			t.lock.RUnlock()
			vals := []any{value}
			if op == query.WITHIN {
				vals = util.SliceToAny(value)
			}
			if indexed {
				matches, missing := t.indexedMatches(field, vals)
				for _, m := range matches {
					out <- benchtop.Index{Key: []byte(m.id), Loc: m.loc}
					total++
				}
				if total > 0 {
					log.Debugf("arrowtable.RowIndexesByHas indexed_first table=%s field=%s op=%d results=%d missingLoc=%d elapsed=%s", t.name, field, op, total, missing, time.Since(start).Round(time.Millisecond))
					return
				}
			}
		}

		if isTopLevelField(field) && !strings.Contains(field, "*") {
			filters := []query.FieldFilter{{Field: field, Operator: op, Value: value}}
			if matchedBySection, optimized, _ := t.findRowsByTopLevelFilters(filters); optimized {
				matchCount := 0
				for _, rows := range matchedBySection {
					matchCount += len(rows)
				}
				if matchCount == 0 {
					// Defensive fallback: avoid false-zero regressions from
					// top-level-only evaluation; run legacy path below.
				} else {
					sections := make([]int, 0, len(matchedBySection))
					for sec := range matchedBySection {
						sections = append(sections, int(sec))
					}
					sort.Ints(sections)
					for _, secInt := range sections {
						sec := uint16(secInt)
						for _, r := range matchedBySection[sec] {
							out <- benchtop.Index{Key: []byte(r.id), Loc: r.loc}
							total++
						}
					}
					log.Debugf("arrowtable.RowIndexesByHas top_level_scan table=%s field=%s op=%d results=%d elapsed=%s", t.name, field, op, total, time.Since(start).Round(time.Millisecond))
					return
				}
			}
		}

		log.Debugf("arrowtable.RowIndexesByHas fallback_scan table=%s field=%s op=%d", t.name, field, op)
		for id := range t.RowIdsByHas(field, value, op) {
			loc, err := t.GetRowLoc(id)
			if err != nil {
				continue
			}
			out <- benchtop.Index{Key: []byte(id), Loc: loc}
			total++
		}
		log.Debugf("arrowtable.RowIndexesByHas fallback_done table=%s field=%s op=%d results=%d elapsed=%s", t.name, field, op, total, time.Since(start).Round(time.Millisecond))
	}()
	return out
}
