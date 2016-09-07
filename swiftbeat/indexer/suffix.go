package indexer

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/swiftbeat/input/swift"
)

type Suffix struct {
	*IndexRecord
	*Partition
	hashes []*Hash
}

type SuffixSorter []*Suffix

func (suffixes SuffixSorter) Len() int {
	return len(suffixes)
}

func (suffixes SuffixSorter) Less(i, j int) bool {
	return suffixes[i].Mtime.After(suffixes[j].Mtime)
}

func (suffixes SuffixSorter) Swap(i, j int) {
	suffixes[i], suffixes[j] = suffixes[j], suffixes[i]
}

func NewSuffix(
	p *Partition,
	file os.FileInfo,
) (*Suffix, error) {
	suffix := &Suffix{
		IndexRecord: &IndexRecord{
			Name:  file.Name(),
			Path:  filepath.Join(p.Path, file.Name()),
			Mtime: file.ModTime(),
		},
		Partition: p,
		hashes:    nil,
	}
	return suffix, nil
}

func (s *Suffix) init() error {
	path := s.Path
	logp.Debug("suffix", "Init suffix: %s", path)

	var hashes HashSorter

	files, err := ioutil.ReadDir(path)
	if err != nil {
		logp.Err("list dir(%s) failed: %v", path, err)
		return err
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
	return nil
}

// BuildIndex builds index for one suffix
// It is a blocking call and return after finishing index build for all
// hashes under the suffix
func (s *Suffix) BuildIndex() {
	logp.Debug("suffix", "Start building index for suffix: %s", s.Path)

	// load hash list for the suffix
	err := s.init()
	if err != nil {
		return
	}

	for _, hash := range s.hashes {
		hash.BuildIndex()
	}
}

// AnnotateSwiftObject add info from indexer to the swift.Object data object
func (s *Suffix) AnnotateSwiftObject(obj *swift.Object) {
	if s.Partition == nil {
		logp.Critical("AnnotateSwiftObject: BUG: partition reference is nil")
	}
	s.Partition.AnnotateSwiftObject(obj)
	obj.Annotate(*s)
}
