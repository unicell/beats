package swift

import (
	"time"
)

type Object struct {
	Name           string
	Mtime          time.Time
	Hash           string
	HashMtime      time.Time
	Suffix         string
	SuffixMtime    time.Time
	Partition      string
	PartitionMtime time.Time
	Metadata       map[string]string
	Path           string
	Device         string
}
