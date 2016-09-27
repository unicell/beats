package swift

import (
	"time"
)

// Partition models all necessary info regarding an partition event
type Partition struct {
	PartId       int64
	Mtime        time.Time
	LastIndexed  time.Time
	ResourceType string
	Device       string
	Ip           string
	RingMtime    time.Time
	Handoff      bool
	ReplicaId    int64
	PeerDevices  string
	PeerIps      string
}
