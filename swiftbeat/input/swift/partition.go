package swift

import (
	"time"
)

// Partition models all necessary info regarding an partition event
type Partition struct {
	PartId        int64
	Mtime         time.Time
	NumDatafiles  int64
	NumTombstones int64
	BytesTotalMB  int64
	LastIndexed   time.Time
	ResourceType  string
	Device        string
	Ip            string
	RingMtime     time.Time
	Handoff       bool
	PeerDevices   string
	PeerIps       string
}
