package table

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable/section"
	"github.com/edsrzf/mmap-go"
)

func (b *JSONTable) Init(poolSize int) error {
	b.NumPartitions = 4
	if b.NumPartitions > 256 {
		if uint32(b.NumPartitions)*uint32(SECTION_ID_MULT) > 65536 {
			return fmt.Errorf("too many partitions (%d) for section ID multiplier (%d)", b.NumPartitions, SECTION_ID_MULT)
		}
	}

	b.PartitionFunc = defaultPartitionFunc(b.NumPartitions)
	b.Sections = map[uint16]*section.Section{}

	dir := filepath.Dir(b.FileName)
	base := filepath.Base(b.FileName)
	files, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
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

	for _, s := range secList {
		secId := uint16(s.pId)*SECTION_ID_MULT + uint16(s.localSecId)
		secPath := filepath.Join(dir, s.fileName)

		// Open main file handle (for writes)
		oFile, err := os.OpenFile(secPath, os.O_RDWR, 0666)
		if err != nil {
			return fmt.Errorf("failed to open section file %s: %w", secPath, err)
		}
		m, err := mmap.Map(oFile, mmap.RDWR, 0)
		if err != nil {
			return fmt.Errorf("failed to mmap section %s: %w", secPath, err)
		}

		// Init file pool (for writes)
		filePool := make(chan *os.File, poolSize)
		for range poolSize {
			file, err := os.OpenFile(secPath, os.O_RDWR, 0666)
			if err != nil {
				// Clean up
				m.Unmap()
				oFile.Close()
				for len(filePool) > 0 {
					if f, ok := <-filePool; ok {
						f.Close()
					}
				}
				return fmt.Errorf("failed to init file pool for %s: %w", secPath, err)
			}
			filePool <- file
		}

		var liveBytes uint32 = 0
		var totalRows uint32 = 0
		var deletedRows uint32 = 0
		var offset uint32 = 0
		// Loop for Initializing live bytes, deletedRows, totalRows
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
		liveBytes = offset
		sec := &section.Section{
			ID:              secId,
			PartitionID:     s.pId,
			Path:            secPath,
			File:            oFile,
			FilePool:        filePool,
			MMap:            m,
			LiveBytes:       liveBytes,
			Active:          true,
			MMapMode:        mmap.RDWR,
			TotalRows:       totalRows,
			DeletedRows:     deletedRows,
			Lock:            sync.RWMutex{},
			CompressScratch: make([]byte, 0),
		}

		b.Sections[secId] = sec
		b.PartitionMap[s.pId] = append(b.PartitionMap[s.pId], secId)

	}

	for pId, secIds := range b.PartitionMap {
		if len(secIds) > 0 {
			latestSecId := secIds[len(secIds)-1]
			latestSec := b.Sections[latestSecId]

			// Mark the latest section as active for writing
			b.ActiveSections[pId] = latestSec
			b.FlushCounter[pId] = 0 // Reset the counter for the newly active section
		}
	}

	// --- ENSURE ONE SECTION PER PARTITION ---
	for pId := uint8(0); pId < uint8(b.NumPartitions); pId++ {
		if len(b.PartitionMap[pId]) == 0 {
			_, err := b.CreateNewSection(pId)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
