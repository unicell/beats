package indexer

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/swiftbeat/input"
	"github.com/elastic/beats/swiftbeat/input/swift"
)

type Partition struct {
	*IndexRecord
	res       *Resource
	eventChan chan input.Event
	done      chan struct{}
	// TODO: hashes.pkl
	suffixes    []*Suffix
	Handoff     bool
	PeerDevices []string
	PeerIps     []string
}

type PartitionSorter []*Partition

func (parts PartitionSorter) Len() int {
	return len(parts)
}

func (parts PartitionSorter) Less(i, j int) bool {
	return parts[i].Mtime.After(parts[j].Mtime)
}

func (parts PartitionSorter) Swap(i, j int) {
	parts[i], parts[j] = parts[j], parts[i]
}

func NewPartition(
	res *Resource,
	file os.FileInfo,
	done chan struct{},
) (*Partition, error) {
	part := &Partition{
		IndexRecord: &IndexRecord{
			Name:  file.Name(),
			Path:  filepath.Join(res.Path, file.Name()),
			Mtime: file.ModTime(),
		},
		res:       res,
		eventChan: make(chan input.Event),
		done:      done,
		suffixes:  nil,
	}
	return part, nil
}

func (p *Partition) init() error {
	path := p.Path
	logp.Debug("partition", "Init partition: %s", path)

	// mark whether current partition on handoff node according to ring data
	ring := p.res.ring
	partId, _ := strconv.ParseUint(p.Name, 10, 64)

	nodes, handoff := ring.GetJobNodes(partId, p.res.devId)
	p.Handoff = handoff

	// add peer device and Ip info
	for _, n := range nodes {
		p.PeerDevices = append(p.PeerDevices, n.Device)
		p.PeerIps = append(p.PeerIps, n.Ip)
	}

	files, err := ioutil.ReadDir(path)
	if err != nil {
		logp.Err("list dir(%s) failed: %v", path, err)
		return err
	}

	var suffixes SuffixSorter
	for _, file := range files {
		if !file.IsDir() {
			continue
		}

		suffix, _ := NewSuffix(p, file, p.eventChan, p.done)
		suffixes = append(suffixes, suffix)
	}

	sort.Sort(suffixes)
	p.suffixes = suffixes
	return nil
}

// BuildIndex builds index for one partition
// It is a blocking call and return after finishing index build for all
// suffixes under the partition
func (p *Partition) BuildIndex() {
	defer p.res.sem.release()

	logp.Debug("partition", "Start building index for partition: %s", p.Path)

	// limit num of partition indexers can run simultaneously
	// to avoid heavy IO hit
	p.res.sem.acquire()

	// load suffix list for the partition
	err := p.init()
	if err != nil {
		return
	}

	for _, suffix := range p.suffixes {
		suffix.BuildIndex()
	}
}

// GetEvents returns the event channel for all partition related events
func (p *Partition) GetEvents() <-chan input.Event {
	return p.eventChan
}

// AnnotateSwiftObject add info from indexer to the swift.Object data object
func (p *Partition) AnnotateSwiftObject(obj *swift.Object) {
	if p.res == nil {
		logp.Critical("AnnotateSwiftObject: BUG: res reference is nil")
	}
	p.res.AnnotateSwiftObject(obj)

	obj.Annotate(*p)
	obj.PeerDevices = strings.Join(p.PeerDevices, ",")
	obj.PeerIps = strings.Join(p.PeerIps, ",")
}
