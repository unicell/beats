package swift

import (
	"time"
)

// Account models all necessary info regarding an account event
type Account struct {
	Mtime          time.Time
	Path           string
	SizeKB         int64
	Account        string
	Status         string
	ContainerCount int64
	ObjectCount    int64
	BytesUsedMB    int64
	LastIndexed    time.Time
	ResourceType   string
	Partition      int64
	Device         string
	Ip             string
	RingMtime      time.Time
	Handoff        bool
	ReplicaId      int64
	PeerDevices    string
	PeerIps        string
}
