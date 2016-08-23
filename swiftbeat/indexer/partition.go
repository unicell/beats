package indexer

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"

	"github.com/elastic/beats/libbeat/logp"
)

type Partition struct {
	*IndexRecord
	name string
	rl   *ResourceLayout
	// TODO: hashes.pkl
	suffixes []*Suffix
}

type PartitionSorter []*Partition

func (parts PartitionSorter) Len() int {
	return len(parts)
}

func (parts PartitionSorter) Less(i, j int) bool {
	return parts[i].mtime.After(parts[j].mtime)
}

func (parts PartitionSorter) Swap(i, j int) {
	parts[i], parts[j] = parts[j], parts[i]
}

func NewPartition(rl *ResourceLayout, file os.FileInfo) (*Partition, error) {
	part := &Partition{
		IndexRecord: &IndexRecord{
			path:  filepath.Join(rl.path, file.Name()),
			mtime: file.ModTime(),
		},
		name:     file.Name(),
		rl:       rl,
		suffixes: nil,
	}
	return part, nil
}

func (p *Partition) init() {
	path := p.path
	logp.Debug("partition", "Init partition layout: %s", path)

	var suffixes SuffixSorter

	files, err := ioutil.ReadDir(path)
	if err != nil {
		logp.Err("list dir(%s) failed: %v", path, err)
		return
	}

	for _, file := range files {
		if !file.IsDir() {
			continue
		}

		suffix, _ := NewSuffix(p, file)
		suffixes = append(suffixes, suffix)
	}

	sort.Sort(suffixes)
	p.suffixes = suffixes
}

// BuildIndex builds index for one partition
// TODO: move away from naive linear scanning to better strategy
func (p *Partition) BuildIndex() {
	logp.Debug("partition", "Build index for partition: %s", p.path)

	// load suffix list for the partition
	p.init()

	for _, suffix := range p.suffixes {
		suffix.BuildIndex()
	}
}
