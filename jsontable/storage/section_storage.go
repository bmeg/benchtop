package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
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
	PART_FILE_SUFFIX = ".partition"
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
	maxSecId       uint16                      // Highest section ID allocated

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
		secId      uint16
		localSecId int
		fileName   string
	}
	var secList []secInfo

	for _, f := range files {
		if strings.Contains(f.Name(), ".id") && strings.Contains(f.Name(), ".part") {
			// New format: data.id[ID].part[P]
			// Format: basePath.id[ID].part[P]
			suffix := strings.TrimPrefix(f.Name(), base+".id")
			parts := strings.Split(suffix, ".part")
			if len(parts) == 2 {
				secId, _ := strconv.Atoi(parts[0])
				pId, _ := strconv.Atoi(parts[1])
				secList = append(secList, secInfo{
					pId:        uint8(pId),
					secId:      uint16(secId),
					localSecId: -1, // Not used for new format
					fileName:   f.Name(),
				})
			}
		}
	}

	for _, si := range secList {
		secId := si.secId
		if secId > s.maxSecId {
			s.maxSecId = secId
		}
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
				sec := s.sections[sid]
				if sid >= maxId {
					maxId = sid
					maxSec = sec
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

	sec.Lock.RLock()
	defer sec.Lock.RUnlock()

	if len(sec.MMap) == 0 {
		return nil, fmt.Errorf("section %d is empty", loc.Section)
	}

	start := loc.Offset + benchtop.ROW_HSIZE
	end := start + loc.Size
	if end > uint32(len(sec.MMap)) {
		return nil, fmt.Errorf("out of bounds for section %d", loc.Section)
	}

	// Copy data to avoid reading from unmapped memory after lock release
	data := make([]byte, loc.Size)
	copy(data, sec.MMap[start:end])
	return data, nil
}

func (s *SectionStorage) GetBatch(locs []*benchtop.RowLoc) ([][]byte, []error) {
	// Fallback to individual gets for simplicity and correctness with collisions
	results := make([][]byte, len(locs))
	errors := make([]error, len(locs))

	for i, loc := range locs {
		res, err := s.Get(loc)
		if err != nil {
			errors[i] = err
		} else {
			results[i] = res
		}
	}
	return results, errors
}

func (s *SectionStorage) ScanFull(concurrency int) chan benchtop.RowLocData {
	// Scan all sections directly from s.sections map to ensure we visit every file exactly once,
	// regardless of partition collisions.
	s.lock.RLock()
	var allSecs []*section.Section
	for _, sec := range s.sections {
		allSecs = append(allSecs, sec)
	}
	s.lock.RUnlock()

	outChan := make(chan benchtop.RowLocData, 100*len(allSecs))
	if concurrency <= 0 {
		concurrency = 1
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)

	go func() {
		for _, sec := range allSecs {
			if len(sec.MMap) == 0 {
				continue
			}

			wg.Add(1)
			go func(sec *section.Section) {
				sem <- struct{}{}
				defer func() { <-sem; wg.Done() }()

				sec.Lock.RLock()
				defer sec.Lock.RUnlock()

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
		wg.Wait()
		close(outChan)
	}()
	return outChan
}

func (s *SectionStorage) Scan(concurrency int) chan []byte {
	out := make(chan []byte, 100)
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

	sec.Lock.RLock()
	if len(sec.MMap) == 0 || loc.Offset+benchtop.ROW_HSIZE > uint32(len(sec.MMap)) {
		sec.Lock.RUnlock()
		return fmt.Errorf("invalid offset or empty section")
	}
	sec.Lock.RUnlock()

	file := <-sec.FilePool
	defer func() { sec.FilePool <- file }()
	_, err := file.WriteAt(bytes.Repeat([]byte{0x00}, 4), int64(loc.Offset+benchtop.ROW_OFFSET_HSIZE))
	if err == nil {
		sec.Lock.Lock()
		sec.DeletedRows++
		sec.Lock.Unlock()
		return nil
	}
	return err
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
	// Sync before closing to ensure flush
	if err := s.Sync(); err != nil {
		log.Errorf("Failed to sync on close: %v", err)
	}

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
	s.maxSecId++
	secId := s.maxSecId
	s.lock.Unlock()

	// Use new naming format to avoid collisions and support unique IDs
	path := fmt.Sprintf("%s.id%d.part%d", s.fileName, secId, partitionId)

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
