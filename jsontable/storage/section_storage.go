package storage

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable/section"
	"github.com/bmeg/grip/log"
	"github.com/edsrzf/mmap-go"
	"github.com/hashicorp/go-multierror"
)

const (
	PART_FILE_SUFFIX    = ".partition"
	SECTION_FILE_SUFFIX = ".section"
	SECTION_ID_MULT     = 256
)

// SectionStorage implements RowStorage using memory-mapped .section files.
// This is the "Legacy" storage mechanism used by benchtop.
type SectionStorage struct {
	basePath              string
	fileName              string
	numPartitions         uint32
	maxConcurrentSections uint8

	sections       map[uint16]*section.Section // All active or closed sections
	activeSections map[uint8]*section.Section  // Current active section for each partition
	partitionMap   map[uint8][]uint16          // List of section IDs per partition

	lock        sync.RWMutex // Protects map access
	sectionLock sync.Mutex   // Protects section creation

	partitionFunc func(id []byte) uint8
}

func NewSectionStorage(basePath string, fileName string, numPartitions uint32) *SectionStorage {
	storage := &SectionStorage{
		basePath:       basePath,
		fileName:       fileName,
		numPartitions:  numPartitions,
		sections:       make(map[uint16]*section.Section),
		activeSections: make(map[uint8]*section.Section),
		partitionMap:   make(map[uint8][]uint16),
		partitionFunc: func(id []byte) uint8 {
			h := fnv.New32a()
			h.Write(id)
			return uint8(h.Sum32() % numPartitions)
		},
		maxConcurrentSections: 10,
	}

	if err := storage.loadExisting(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to load existing sections: %v\n", err)
	}
	return storage
}

func (s *SectionStorage) GetPartitionId(id []byte) int {
	return int(s.partitionFunc(id))
}

func (s *SectionStorage) AddRows(data [][]byte, ids [][]byte) ([]*benchtop.RowLoc, error) {
	if len(data) == 0 {
		return []*benchtop.RowLoc{}, nil
	}
	if len(data) != len(ids) {
		return nil, fmt.Errorf("data and ids must have same length")
	}

	results := make([]*benchtop.RowLoc, len(data))

	type inputItem struct {
		index int
		data  []byte
		id    []byte
	}

	byPartition := make(map[uint8][]inputItem)
	for i := 0; i < len(data); i++ {
		pId := s.partitionFunc(ids[i])
		byPartition[pId] = append(byPartition[pId], inputItem{i, data[i], ids[i]})
	}

	var errs *multierror.Error

	// Process each partition
	for pId, items := range byPartition {
		s.lock.RLock()
		sec := s.activeSections[pId]
		s.lock.RUnlock()

		if sec == nil {
			var err error
			sec, err = s.createNewSection(pId)
			if err != nil {
				errs = multierror.Append(errs, err)
				continue
			}
		}

		// Calculate total size to check for rotation
		var totalSize uint32
		for _, item := range items {
			totalSize += uint32(len(item.data)) + benchtop.ROW_HSIZE
		}

		if sec.LiveBytes+totalSize > section.MAX_SECTION_SIZE {
			var err error
			sec, err = s.rotateSection(pId, sec)
			if err != nil {
				errs = multierror.Append(errs, err)
				continue
			}
		}

		for _, item := range items {
			loc, err := sec.WriteJsonEntryToSection(item.data)
			if err != nil {
				errs = multierror.Append(errs, err)
				continue // Try next item?
			}
			sec.TotalRows++
			results[item.index] = &benchtop.RowLoc{
				Section: loc.Section,
				Offset:  loc.Offset,
				Size:    loc.Size,
			}
		}
	}

	return results, errs.ErrorOrNil()
}

func (s *SectionStorage) loadExisting() error {
	dir := filepath.Dir(s.fileName)
	base := filepath.Base(s.fileName)

	files, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read directory %s: %w", dir, err)
	}

	type secInfo struct {
		pId        uint8
		localSecId int
		fileName   string
	}
	var secList []secInfo

	for _, f := range files {
		if strings.HasPrefix(f.Name(), base+PART_FILE_SUFFIX) {
			parts := strings.Split(strings.TrimPrefix(f.Name(), base+PART_FILE_SUFFIX), SECTION_FILE_SUFFIX)
			if len(parts) != 2 {
				continue
			}

			pId, err := strconv.Atoi(parts[0])
			if err != nil {
				continue
			}

			localSecId, err := strconv.Atoi(parts[1])
			if err != nil {
				continue
			}

			secList = append(secList, secInfo{
				pId:        uint8(pId),
				localSecId: localSecId,
				fileName:   f.Name(),
			})
		}
	}

	for _, si := range secList {
		secId := uint16(si.pId)*SECTION_ID_MULT + uint16(si.localSecId)
		secPath := filepath.Join(dir, si.fileName)

		handle, err := os.OpenFile(secPath, os.O_RDWR, 0666)
		if err != nil {
			return fmt.Errorf("failed to open section file %s: %w", secPath, err)
		}

		m, err := mmap.Map(handle, mmap.RDWR, 0)
		if err != nil {
			handle.Close()
			return fmt.Errorf("failed to mmap section %s: %w", secPath, err)
		}

		filePool := make(chan *os.File, 10)
		for i := 0; i < 10; i++ {
			f, err := os.OpenFile(secPath, os.O_RDWR, 0666)
			if err != nil {
				return fmt.Errorf("failed to init file pool: %w", err)
			}
			filePool <- f
		}

		var totalRows uint32 = 0
		var deletedRows uint32 = 0
		var offset uint32 = 0
		for offset+benchtop.ROW_HSIZE <= uint32(len(m)) {
			header := m[offset : offset+benchtop.ROW_HSIZE]
			nextOffset := binary.LittleEndian.Uint32(header[:benchtop.ROW_OFFSET_HSIZE])
			bSize := binary.LittleEndian.Uint32(header[benchtop.ROW_OFFSET_HSIZE:benchtop.ROW_HSIZE])

			if nextOffset == 0 || nextOffset <= offset {
				break
			}
			if bSize == 0 {
				deletedRows++
			}
			totalRows++
			offset = nextOffset
		}

		sec := &section.Section{
			ID:              secId,
			PartitionID:     si.pId,
			Path:            secPath,
			File:            handle,
			FilePool:        filePool,
			MMap:            m,
			LiveBytes:       offset,
			Active:          true,
			MMapMode:        mmap.RDWR,
			TotalRows:       totalRows,
			DeletedRows:     deletedRows,
			Lock:            sync.RWMutex{},
			CompressScratch: make([]byte, 0),
		}

		s.lock.Lock()
		s.sections[secId] = sec
		s.partitionMap[si.pId] = append(s.partitionMap[si.pId], secId)
		s.lock.Unlock()
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	for pId, secIds := range s.partitionMap {
		if len(secIds) > 0 {
			var maxId uint16 = 0
			var maxSec *section.Section
			for _, sid := range secIds {
				if sid >= maxId {
					maxId = sid
					maxSec = s.sections[sid]
				}
			}
			s.activeSections[pId] = maxSec
		}
	}

	return nil
}

func (s *SectionStorage) AddRow(data []byte, id []byte) (*benchtop.RowLoc, error) {
	partitionId := s.partitionFunc(id)

	s.lock.RLock()
	sec := s.activeSections[partitionId]
	s.lock.RUnlock()

	if sec == nil {
		var err error
		sec, err = s.createNewSection(partitionId)
		if err != nil {
			return nil, err
		}
	}

	totalSize := uint32(len(data)) + benchtop.ROW_HSIZE
	if sec.LiveBytes+totalSize > section.MAX_SECTION_SIZE {
		// Release lock while we potentially create a new section to avoid deadlock
		var err error
		sec, err = s.rotateSection(partitionId, sec)
		if err != nil {
			return nil, err
		}
	}

	loc, err := sec.WriteJsonEntryToSection(data)
	if err != nil {
		return nil, err
	}

	sec.TotalRows++

	return &benchtop.RowLoc{
		Section: loc.Section,
		Offset:  loc.Offset,
		Size:    loc.Size,
	}, nil
}

func (s *SectionStorage) Get(loc *benchtop.RowLoc) ([]byte, error) {
	s.lock.RLock()
	sec, exists := s.sections[loc.Section]
	s.lock.RUnlock()

	if !exists {
		return nil, fmt.Errorf("section %d not found", loc.Section)
	}

	if len(sec.MMap) == 0 {
		return nil, fmt.Errorf("section %d empty/unmapped", loc.Section)
	}

	start := loc.Offset + benchtop.ROW_HSIZE
	end := start + loc.Size
	if end > uint32(len(sec.MMap)) {
		return nil, fmt.Errorf("out of bounds")
	}

	return sec.MMap[start:end], nil
}

func (s *SectionStorage) GetBatch(locs []*benchtop.RowLoc) ([][]byte, []error) {
	results := make([][]byte, len(locs))
	errors := make([]error, len(locs))

	// Group by section and track original indices
	type locRef struct {
		idx uint32
		loc *benchtop.RowLoc
	}
	bySection := make(map[uint16][]locRef)
	for i, loc := range locs {
		bySection[loc.Section] = append(bySection[loc.Section], locRef{uint32(i), loc})
	}

	for sectionID, refs := range bySection {
		s.lock.RLock()
		sec, exists := s.sections[sectionID]
		s.lock.RUnlock()

		if !exists || len(sec.MMap) == 0 {
			for _, ref := range refs {
				errors[ref.idx] = fmt.Errorf("section %d not found or unmapped", sectionID)
			}
			continue
		}

		// SORT BY OFFSET: This is the critical optimization to ensure linear mmap access
		slices.SortFunc(refs, func(a, b locRef) int {
			return cmp.Compare(a.loc.Offset, b.loc.Offset)
		})

		secLen := uint32(len(sec.MMap))
		for _, ref := range refs {
			start := ref.loc.Offset + benchtop.ROW_HSIZE
			end := start + ref.loc.Size
			if end > secLen {
				errors[ref.idx] = fmt.Errorf("out of bounds in section %d", sectionID)
				continue
			}
			// Linear access of the underlying mmap
			results[ref.idx] = sec.MMap[start:end]
		}
	}

	return results, errors
}

func (s *SectionStorage) ScanFull(concurrency int) chan benchtop.RowLocData {
	outChan := make(chan benchtop.RowLocData, 100*len(s.sections))
	if concurrency <= 0 {
		concurrency = 1
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)

	s.lock.RLock()
	maxPart := s.numPartitions
	s.lock.RUnlock()

	go func() {
		for pId := uint8(0); pId < uint8(maxPart); pId++ {
			s.lock.RLock()
			secIds := s.partitionMap[pId]
			currentSecIds := make([]uint16, len(secIds))
			copy(currentSecIds, secIds)
			s.lock.RUnlock()

			for _, secId := range currentSecIds {
				s.lock.RLock()
				sec, exists := s.sections[secId]
				s.lock.RUnlock()

				if !exists || len(sec.MMap) == 0 {
					continue
				}

				wg.Add(1)
				go func(sec *section.Section) {
					sem <- struct{}{}
					defer func() { <-sem; wg.Done() }()

					m := sec.MMap
					var offset uint32 = 0
					for offset+benchtop.ROW_HSIZE <= uint32(len(m)) {
						header := m[offset : offset+benchtop.ROW_HSIZE]
						nextOffset := binary.LittleEndian.Uint32(header[:benchtop.ROW_OFFSET_HSIZE])
						bSize := binary.LittleEndian.Uint32(header[benchtop.ROW_OFFSET_HSIZE:benchtop.ROW_HSIZE])

						if bSize == 0 {
							if nextOffset == 0 || nextOffset <= offset {
								break
							}
							offset = nextOffset
							continue
						}

						jsonStart := offset + benchtop.ROW_HSIZE
						jsonEnd := jsonStart + bSize
						if jsonEnd > uint32(len(m)) {
							break
						}

						rowData := make([]byte, bSize)
						copy(rowData, m[jsonStart:jsonEnd])
						outChan <- benchtop.RowLocData{
							Data: rowData,
							Loc: &benchtop.RowLoc{
								Section: sec.ID,
								Offset:  offset,
								Size:    bSize,
							},
						}

						if nextOffset == 0 || nextOffset <= offset {
							break
						}
						offset = nextOffset
					}
				}(sec)
			}
		}
		wg.Wait()
		close(outChan)
	}()
	return outChan
}

func (s *SectionStorage) Scan(concurrency int) chan []byte {
	out := make(chan []byte, 100*len(s.sections))
	go func() {
		defer close(out)
		for res := range s.ScanFull(concurrency) {
			out <- res.Data
		}
	}()
	return out
}

func (s *SectionStorage) MarkDelete(loc *benchtop.RowLoc) error {
	s.lock.RLock()
	sec, exists := s.sections[loc.Section]
	s.lock.RUnlock()

	if !exists {
		return fmt.Errorf("section %d not found", loc.Section)
	}

	file := <-sec.FilePool
	defer func() { sec.FilePool <- file }()

	_, err := file.WriteAt(bytes.Repeat([]byte{0x00}, 4), int64(loc.Offset+benchtop.ROW_OFFSET_HSIZE))
	if err != nil {
		return fmt.Errorf("writeAt failed: %w", err)
	}

	sec.Lock.Lock()
	sec.DeletedRows++
	sec.LiveBytes -= loc.Size
	sec.Lock.Unlock()
	return nil
}

func (s *SectionStorage) Sync() error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	var errs *multierror.Error
	for _, sec := range s.sections {
		if sec.File != nil {
			if err := sec.File.Sync(); err != nil {
				errs = multierror.Append(errs, err)
			}
		}
		if sec.MMap != nil {
			if err := sec.MMap.Flush(); err != nil {
				errs = multierror.Append(errs, err)
			}
		}
	}
	return errs.ErrorOrNil()
}

func (s *SectionStorage) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	var errs *multierror.Error
	for _, sec := range s.sections {
		if sec.MMap != nil {
			if err := sec.MMap.Unmap(); err != nil {
				errs = multierror.Append(errs, err)
			}
		}
		if sec.FilePool != nil {
			close(sec.FilePool)
			for f := range sec.FilePool {
				f.Close()
			}
		}
		if sec.File != nil {
			if err := sec.File.Close(); err != nil {
				errs = multierror.Append(errs, err)
			}
		}
	}
	return errs.ErrorOrNil()
}

func (s *SectionStorage) Delete() error {
	if err := s.Close(); err != nil {
		log.Errorf("Close failed during delete: %v", err)
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	var errs *multierror.Error
	for id, sec := range s.sections {
		if err := os.Remove(sec.Path); err != nil {
			errs = multierror.Append(errs, err)
		}
		delete(s.sections, id)
	}
	return errs.ErrorOrNil()
}

func (s *SectionStorage) rotateSection(partitionId uint8, oldSec *section.Section) (*section.Section, error) {
	s.lock.RLock()
	current := s.activeSections[partitionId]
	s.lock.RUnlock()

	// If someone already changed it, use the new one
	if current != oldSec {
		return current, nil
	}

	// Double check under sectionLock
	s.sectionLock.Lock()
	defer s.sectionLock.Unlock()

	s.lock.RLock()
	current = s.activeSections[partitionId]
	s.lock.RUnlock()
	if current != oldSec {
		return current, nil
	}

	// It's definitely full and we are the ones to rotate
	if oldSec != nil {
		oldSec.CloseSection()
	}
	return s.createNewSectionLocked(partitionId)
}

func (s *SectionStorage) createNewSection(partitionId uint8) (*section.Section, error) {
	s.sectionLock.Lock()
	defer s.sectionLock.Unlock()
	return s.createNewSectionLocked(partitionId)
}

func (s *SectionStorage) createNewSectionLocked(partitionId uint8) (*section.Section, error) {
	// Critical: Update shared map under lock
	s.lock.Lock()
	localSecId := len(s.partitionMap[partitionId])
	secId := uint16(partitionId)*uint16(SECTION_ID_MULT) + uint16(localSecId)
	s.lock.Unlock()

	path := fmt.Sprintf("%s%s%d.section%d", s.fileName, PART_FILE_SUFFIX, partitionId, localSecId)

	handle, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	handle.Truncate(section.INITIAL_SECTION_SIZE)

	m, err := mmap.Map(handle, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}

	filePool := make(chan *os.File, 10)
	for range cap(filePool) {
		f, err := os.OpenFile(path, os.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}
		filePool <- f
	}

	sec := &section.Section{
		ID:              secId,
		PartitionID:     partitionId,
		Path:            path,
		File:            handle,
		FilePool:        filePool,
		MMap:            m,
		MMapMode:        mmap.RDWR,
		Active:          true,
		LiveBytes:       0,
		CompressScratch: make([]byte, 0),
	}

	s.lock.Lock()
	s.sections[secId] = sec
	s.partitionMap[partitionId] = append(s.partitionMap[partitionId], secId)
	s.activeSections[partitionId] = sec
	s.lock.Unlock()

	return sec, nil
}
