package indexer

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/swiftbeat/input"
)

type Suffix struct {
	*IndexRecord
	part      *Partition
	eventChan chan *input.Event
	done      chan struct{}
	hashes    []*Hash
}

type SuffixSorter []*Suffix

func (suffixes SuffixSorter) Len() int {
	return len(suffixes)
}

func (suffixes SuffixSorter) Less(i, j int) bool {
	return suffixes[i].mtime.After(suffixes[j].mtime)
}

func (suffixes SuffixSorter) Swap(i, j int) {
	suffixes[i], suffixes[j] = suffixes[j], suffixes[i]
}

func NewSuffix(
	p *Partition,
	file os.FileInfo,
	eventChan chan *input.Event,
	done chan struct{},
) (*Suffix, error) {
	suffix := &Suffix{
		IndexRecord: &IndexRecord{
			name:  file.Name(),
			path:  filepath.Join(p.path, file.Name()),
			mtime: file.ModTime(),
		},
		part:      p,
		eventChan: eventChan,
		done:      done,
		hashes:    nil,
	}
	return suffix, nil
}

func (s *Suffix) init() {
	path := s.path
	logp.Debug("suffix", "Init suffix: %s", path)

	var hashes HashSorter

	files, err := ioutil.ReadDir(path)
	if err != nil {
		logp.Err("list dir(%s) failed: %v", path, err)
		return
	}

	for _, file := range files {
		if !file.IsDir() {
			continue
		}

		hash, _ := NewHash(s, file, s.eventChan, s.done)
		hashes = append(hashes, hash)
	}

	sort.Sort(hashes)
	s.hashes = hashes
}

// BuildIndex builds index for one suffix
// It is a blocking call and return after finishing index build for all
// hashes under the suffix
func (s *Suffix) BuildIndex() {
	logp.Debug("suffix", "Start building index for suffix: %s", s.path)

	// load hash list for the suffix
	s.init()

	for _, hash := range s.hashes {
		hash.BuildIndex()
	}
}
