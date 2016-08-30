package indexer

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/swiftbeat/input"
)

type Partition struct {
	*IndexRecord
	rl        *ResourceLayout
	eventChan chan input.Event
	done      chan struct{}
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

func NewPartition(
	rl *ResourceLayout,
	file os.FileInfo,
	done chan struct{},
) (*Partition, error) {
	part := &Partition{
		IndexRecord: &IndexRecord{
			name:  file.Name(),
			path:  filepath.Join(rl.path, file.Name()),
			mtime: file.ModTime(),
		},
		rl:        rl,
		eventChan: make(chan input.Event),
		done:      done,
		suffixes:  nil,
	}
	return part, nil
}

func (p *Partition) init() {
	path := p.path
	logp.Debug("partition", "Init partition: %s", path)

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

		suffix, _ := NewSuffix(p, file, p.eventChan, p.done)
		suffixes = append(suffixes, suffix)
	}

	sort.Sort(suffixes)
	p.suffixes = suffixes
}

// BuildIndex builds index for one partition
// It is a blocking call and return after finishing index build for all
// suffixes under the partition
func (p *Partition) BuildIndex() {
	defer p.rl.sem.release()

	logp.Debug("partition", "Start building index for partition: %s", p.path)

	// limit num of partition indexers can run simultaneously
	// to avoid heavy IO hit
	p.rl.sem.acquire()

	// load suffix list for the partition
	p.init()

	for _, suffix := range p.suffixes {
		suffix.BuildIndex()
	}
}

// GetEvents returns the event channel for all partition related events
func (p *Partition) GetEvents() <-chan input.Event {
	return p.eventChan
}
