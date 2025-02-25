package bsontable

import (
	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
)

func (b *BSONTable) bulkGet(u func(s benchtop.DbGet) error) error {
	return u(&pebblebulk.PebbleBulkRead{Db: b.db})
}

func (b *BSONTable) bulkSet(u func(s benchtop.DbSet) error) error {
	batch := b.db.NewBatch()
	ptx := &pebblebulk.PebbleBulkWrite{Db: b.db, Batch: batch, Highest: nil, Lowest: nil, CurSize: 0}
	err := u(ptx)
	batch.Commit(nil)
	batch.Close()
	if ptx.Lowest != nil && ptx.Highest != nil {
		b.db.Compact(ptx.Lowest, ptx.Highest, true)
	}
	return err
}

func (b *BSONTable) bulkDelete(u func(s benchtop.DbDelete) error) error {
	batch := b.db.NewBatch()
	ptx := &pebblebulk.PebbleBulkWrite{Db: b.db, Batch: batch, Highest: nil, Lowest: nil, CurSize: 0}
	err := u(ptx)
	batch.Commit(nil)
	batch.Close()
	if ptx.Lowest != nil && ptx.Highest != nil {
		b.db.Compact(ptx.Lowest, ptx.Highest, true)
	}
	return err
}
