package indexer

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"

	"github.com/elastic/beats/libbeat/logp"
)

type Suffix struct {
	*IndexRecord
	name   string
	part   *Partition
	hashes []*Hash
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

func NewSuffix(p *Partition, file os.FileInfo) (*Suffix, error) {
	suffix := &Suffix{
		IndexRecord: &IndexRecord{
			path:  filepath.Join(p.path, file.Name()),
			mtime: file.ModTime(),
		},
		name:   file.Name(),
		part:   p,
		hashes: nil,
	}
	return suffix, nil
}

func (s *Suffix) init() {
	path := s.path
	logp.Debug("suffix", "Init suffix layout: %s", path)

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

		hash, _ := NewHash(s, file)
		hashes = append(hashes, hash)
	}

	sort.Sort(hashes)
	s.hashes = hashes
}

// BuildIndex builds index for one suffix
// TODO: move away from naive linear scanning to better strategy
func (s *Suffix) BuildIndex() {
	logp.Debug("suffix", "Build index for suffix: %s", s.path)

	// load hash list for the suffix
	s.init()

	for _, hash := range s.hashes {
		hash.BuildIndex()
	}
}
