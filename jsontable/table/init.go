package table

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/bmeg/benchtop/jsontable/section"
)

// Update Init to parse and assign stable section IDs based on file names
func (b *JSONTable) Init(poolSize int) error {

	b.NumPartitions = 4
	// Sanity check to make sure that the section id multipler *
	// the number of partitions doesn't exceed the max value of a uint16
	if b.NumPartitions > 256 {
		if uint32(b.NumPartitions)*uint32(SECTION_ID_MULT) > 65536 {
			return fmt.Errorf("too many partitions (%d) for the chosen section ID multiplier (%d)", b.NumPartitions, SECTION_ID_MULT)
		}
	}

	b.PartitionFunc = DefaultPartitionFunc(b.NumPartitions)
	b.Sections = map[uint16]*section.Section{}
	b.PartitionMap = map[uint8][]uint16{}
	b.MaxConcurrentSections = uint8(runtime.NumCPU())

	dir := filepath.Dir(b.FileName)
	base := filepath.Base(b.FileName)
	files, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	// Collect section info from files
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

	// Build Sections and PartitionMap
	for _, s := range secList {
		secId := uint16(s.pId)*SECTION_ID_MULT + uint16(s.localSecId)
		secPath := filepath.Join(dir, s.fileName)

		sec := &section.Section{
			ID:          secId,
			PartitionID: s.pId,
			Path:        secPath,
			Active:      true,
		}
		sec.FilePool = make(chan *os.File, poolSize)
		for i := range poolSize {
			file, err := os.OpenFile(secPath, os.O_RDWR, 0666)
			if err != nil {
				for range i {
					if f, ok := <-sec.FilePool; ok {
						f.Close()
					}
				}
				return fmt.Errorf("failed to init file pool for %s: %w", secPath, err)
			}
			sec.FilePool <- file
		}
		if sec.Handle, err = os.OpenFile(secPath, os.O_RDWR, 0666); err != nil {
			return fmt.Errorf("failed to open section handle: %w", err)
		}
		if stat, err := os.Stat(secPath); err == nil {
			sec.LiveBytes = uint32(stat.Size())
		}
		b.Sections[secId] = sec
		b.PartitionMap[s.pId] = append(b.PartitionMap[s.pId], secId)
	}

	// Ensure at least one section per partition
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
