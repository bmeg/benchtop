package arrowdriver

import (
	"path/filepath"
	"sort"
	"testing"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/query"
)

func TestArrowDriverRoundTrip(t *testing.T) {
	base := t.TempDir()

	drvRaw, err := NewArrowDriver(base)
	if err != nil {
		t.Fatalf("NewArrowDriver failed: %v", err)
	}
	drv := drvRaw.(*ArrowDriver)
	defer drv.Close()

	storeRaw, err := drv.New("v_person", []benchtop.ColumnDef{{Key: "name"}, {Key: "age"}})
	if err != nil {
		t.Fatalf("New table failed: %v", err)
	}
	store := storeRaw.(*ArrowTable)

	rows := []benchtop.Row{
		{Id: []byte("p1"), Data: map[string]any{"name": "alice", "age": 30.0}},
		{Id: []byte("p2"), Data: map[string]any{"name": "bob", "age": 41.0}},
		{Id: []byte("p3"), Data: map[string]any{"name": "cory", "age": 25.0}},
	}
	locs, err := store.AddRows(rows)
	if err != nil {
		t.Fatalf("AddRows failed: %v", err)
	}
	if len(locs) != len(rows) {
		t.Fatalf("unexpected loc count: got=%d want=%d", len(locs), len(rows))
	}

	loc, err := store.GetRowLoc("p2")
	if err != nil {
		t.Fatalf("GetRowLoc failed: %v", err)
	}

	row, err := store.GetRow(loc)
	if err != nil {
		t.Fatalf("GetRow failed: %v", err)
	}
	if row["_id"] != "p2" {
		t.Fatalf("unexpected id: %#v", row["_id"])
	}

	got := map[string]struct{}{}
	for id := range store.RowIdsByHas("age", 30.0, query.GTE) {
		got[id] = struct{}{}
	}
	if _, ok := got["p1"]; !ok {
		t.Fatalf("expected p1 in gte filter")
	}
	if _, ok := got["p2"]; !ok {
		t.Fatalf("expected p2 in gte filter")
	}
	if _, ok := got["p3"]; ok {
		t.Fatalf("did not expect p3 in gte filter")
	}

	keys, err := drv.ListTableKeys(store.TableID())
	if err != nil {
		t.Fatalf("ListTableKeys failed: %v", err)
	}
	seen := []string{}
	for k := range keys {
		seen = append(seen, string(k.Key))
		if k.Loc == nil {
			t.Fatalf("expected location in index entry")
		}
	}
	sort.Strings(seen)
	if len(seen) != 3 {
		t.Fatalf("unexpected key count: %d", len(seen))
	}

	nestedRows := []benchtop.Row{
		{
			Id: []byte("obs1"),
			Data: map[string]any{
				"code": map[string]any{
					"coding": []any{
						map[string]any{"code": "81247-9"},
					},
				},
			},
		},
		{
			Id: []byte("obs2"),
			Data: map[string]any{
				"code": map[string]any{
					"coding": []any{
						map[string]any{"code": "81247-8"},
					},
				},
			},
		},
	}
	if _, err := store.AddRows(nestedRows); err != nil {
		t.Fatalf("AddRows nested failed: %v", err)
	}

	nestedGot := map[string]struct{}{}
	for id := range store.RowIdsByHas("code.coding.[0].code", "81247-8", query.EQ) {
		nestedGot[id] = struct{}{}
	}
	if _, ok := nestedGot["obs2"]; !ok {
		t.Fatalf("expected obs2 in nested eq filter")
	}
	if _, ok := nestedGot["obs1"]; ok {
		t.Fatalf("did not expect obs1 in nested eq filter")
	}
}

func TestArrowDriverReloadPreservesTableIDAndData(t *testing.T) {
	base := t.TempDir()

	drvRaw, err := NewArrowDriver(base)
	if err != nil {
		t.Fatalf("NewArrowDriver failed: %v", err)
	}
	drv := drvRaw.(*ArrowDriver)

	storeRaw, err := drv.New("e_knows", nil)
	if err != nil {
		t.Fatalf("New table failed: %v", err)
	}
	store := storeRaw.(*ArrowTable)
	_, err = store.AddRows([]benchtop.Row{
		{Id: []byte("e1"), Data: map[string]any{"from": "p1", "to": "p2"}},
	})
	if err != nil {
		t.Fatalf("AddRows failed: %v", err)
	}
	origID := store.TableID()
	if origID == 0 {
		t.Fatalf("table id should be non-zero")
	}
	drv.Close()

	drvRaw2, err := NewArrowDriver(base)
	if err != nil {
		t.Fatalf("reload driver failed: %v", err)
	}
	drv2 := drvRaw2.(*ArrowDriver)
	defer drv2.Close()

	tid, err := drv2.LookupTableID("e_knows")
	if err != nil {
		t.Fatalf("LookupTableID failed: %v", err)
	}
	storeRaw2, err := drv2.Get(tid)
	if err != nil {
		t.Fatalf("Get after reload failed: %v", err)
	}
	store2 := storeRaw2.(*ArrowTable)
	if store2.TableID() != origID {
		t.Fatalf("table id changed across reload: got=%d want=%d", store2.TableID(), origID)
	}
	loc, err := store2.GetRowLoc("e1")
	if err != nil {
		t.Fatalf("GetRowLoc after reload failed: %v", err)
	}
	if loc.TableId != origID {
		t.Fatalf("rowloc table id mismatch: got=%d want=%d", loc.TableId, origID)
	}
	row, err := store2.GetRow(loc)
	if err != nil {
		t.Fatalf("GetRow after reload failed: %v", err)
	}
	if row["_id"] != "e1" {
		t.Fatalf("unexpected row id: %#v", row["_id"])
	}

	idxFile := filepath.Join(base, "ARROW_TABLES", "e_knows.idx")
	if _, err := filepath.Glob(idxFile); err != nil {
		t.Fatalf("expected idx file to exist: %v", err)
	}
}
