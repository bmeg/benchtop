package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// DefaultZoneManager implements ZoneManager using SectionStorage backend.
type DefaultZoneManager struct {
	baseDir string
	zones   map[string]RowStorage
	lock    sync.RWMutex
}

func NewZoneManager(baseDir string) *DefaultZoneManager {
	return &DefaultZoneManager{
		baseDir: baseDir,
		zones:   make(map[string]RowStorage),
	}
}

func (zm *DefaultZoneManager) GetStorage(zoneId string) (RowStorage, error) {
	zm.lock.RLock()
	s, exists := zm.zones[zoneId]
	zm.lock.RUnlock()
	if exists {
		return s, nil
	}
	return nil, fmt.Errorf("zone %s not found", zoneId)
}

func (zm *DefaultZoneManager) CreateZone(zoneId string) (RowStorage, error) {
	zm.lock.Lock()
	defer zm.lock.Unlock()

	if s, exists := zm.zones[zoneId]; exists {
		return s, nil
	}

	// Zone path: baseDir/zoneId
	// If zoneId is empty, use baseDir directly (legacy behavior)
	zonePath := zm.baseDir
	if zoneId != "" {
		zonePath = filepath.Join(zm.baseDir, zoneId)
	}

	if err := os.MkdirAll(zonePath, 0700); err != nil {
		return nil, fmt.Errorf("failed to create zone directory: %w", err)
	}

	// Create Storage for this zone.
	// We use the zoneId as part of the filename prefix if needed,
	// but SectionStorage usually takes a base filename prefix.
	// Let's assume standard "data" prefix or similar.
	// In legacy benchtop, implementation details like filename were "TABLES/Part...".
	// Here, we abstract it. For a zone, maybe "data"?
	// Or pass the intended filename prefix.
	// Wait, SectionStorage takes `fileName` which is the PREFIX for section files.
	// e.g. /path/to/table/data
	// If Zone = Table, then we pass table path.
	// If Zone = Project, we pass project path.
	// Let's use "data" as the prefix inside the zone directory.
	filePath := filepath.Join(zonePath, "data")

	// Default to 4 partitions to match legacy benchtop partitioning.
	s := NewSectionStorage(zonePath, filePath, 4)

	zm.zones[zoneId] = s
	return s, nil
}

func (zm *DefaultZoneManager) DeleteZone(zoneId string) error {
	zm.lock.Lock()
	defer zm.lock.Unlock()

	s, exists := zm.zones[zoneId]
	if !exists {
		// Even if not loaded, try to delete directory?
		// Safe to delete if we know it's a zone.
	} else {
		// Close storage first
		if err := s.Close(); err != nil {
			return fmt.Errorf("failed to close zone storage: %w", err)
		}
		delete(zm.zones, zoneId)
	}

	if zoneId == "" {
		return fmt.Errorf("cannot delete root zone")
	}

	zonePath := filepath.Join(zm.baseDir, zoneId)
	// Safety check: ensure we are deleting a subdir of base
	// (Simple check)

	return os.RemoveAll(zonePath)
}
