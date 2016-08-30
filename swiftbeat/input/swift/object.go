package swift

import (
	"time"
)

type Object struct {
	Partition      string
	PartitionMtime time.Time
	Suffix         string
	SuffixMtime    time.Time
	Hash           string
	HashMtime      time.Time
	Datafile       string
	DatafileMtime  time.Time
	Metadata       map[string]string
	Path           string
}
