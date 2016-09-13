package swift

import (
	"time"
)

// Container models all necessary info regarding an container event
type Container struct {
	Mtime        time.Time
	Path         string
	SizeKB       int64
	Account      string
	Container    string
	Status       string
	ObjectCount  int64
	BytesUsedMB  int64
	PolicyIndex  int64
	LastIndexed  time.Time
	ResourceType string
	Partition    int64
	Device       string
	Ip           string
	RingMtime    time.Time
	Handoff      bool
	PeerDevices  string
	PeerIps      string
}
