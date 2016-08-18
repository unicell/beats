package indexer

import (
	"io/ioutil"
	"path/filepath"
	"sort"

	"github.com/elastic/beats/libbeat/logp"
)

type Partition struct {
	*IndexRecord
	// todo: hashes.pkl
	suffixes []Suffix
}

type PartitionSorter []Partition

func (parts PartitionSorter) Len() int {
	return len(parts)
}

func (parts PartitionSorter) Less(i, j int) bool {
	return parts[i].mtime.After(parts[j].mtime)
}

func (parts PartitionSorter) Swap(i, j int) {
	parts[i], parts[j] = parts[j], parts[i]
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

		suffix := Suffix{
			IndexRecord: &IndexRecord{
				path:  filepath.Join(path, file.Name()),
				mtime: file.ModTime(),
			},
			hashes: []Hash{},
		}
		suffixes = append(suffixes, suffix)
	}

	sort.Sort(suffixes)
	p.suffixes = suffixes
}

// BuildIndex builds index for one partition
// TODO: move away from naive linear scanning to better strategy
func (p *Partition) BuildIndex() {

	// load suffix list for the partition
	p.init()

	for _, suffix := range p.suffixes {
		logp.Debug("suffix", "Build index for suffix: %s", suffix.path)
		suffix.BuildIndex()
	}
}
