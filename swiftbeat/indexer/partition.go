package indexer

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/swiftbeat/input"
	"github.com/elastic/beats/swiftbeat/input/swift"
)

type Partition struct {
	*IndexRecord
	*Resource
	// TODO: hashes.pkl
	suffixes      []*Suffix
	Handoff       bool
	PeerDevices   []string
	PeerIps       []string
	NumDatafiles  int64
	NumTombstones int64
	BytesTotal    int64
	LastIndexed   time.Time
	PartId        int64
	ReplicaId     int64
	IndexableQ    []IndexableFile
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
) (*Partition, error) {
	part := &Partition{
		IndexRecord: &IndexRecord{
			Name:  file.Name(),
			Path:  filepath.Join(res.Path, file.Name()),
			Mtime: file.ModTime(),
		},
		Resource:      res,
		suffixes:      nil,
		NumDatafiles:  0,
		NumTombstones: 0,
		BytesTotal:    0,
		PartId:        -1,
		ReplicaId:     -1,
		IndexableQ:    []IndexableFile{},
	}

	if i, err := strconv.ParseInt(part.Name, 10, 64); err == nil {
		part.PartId = i
	}

	return part, nil
}

func (p *Partition) init() error {
	path := p.Path
	logp.Debug("partition", "Init partition: %s", path)

	// mark whether current partition on handoff node according to ring data
	ring := p.Resource.ring

	nodes, handoff := ring.GetJobNodes(uint64(p.PartId), p.Resource.DevId)
	p.Handoff = handoff

	// add peer device and Ip info
	for _, n := range nodes {
		p.PeerDevices = append(p.PeerDevices, n.Device)
		p.PeerIps = append(p.PeerIps, n.Ip)
	}

	// get replica index number in on primary node
	if !handoff {
		nodesInOrder := ring.GetNodesInOrder(uint64(p.PartId))
		for i, n := range nodesInOrder {
			if n.Id == p.Resource.DevId {
				p.ReplicaId = int64(i)
				break
			}
		}
	}

	// skip non-handoff node if ObjectIndexHandoffOnly turned on to speedup
	if p.Resource.Type == "object" &&
		p.config.ObjectIndexHandoffOnly && (!p.Handoff) {
		// to differentiate non-indexed vs indexed with zero value
		p.NumDatafiles = -1
		p.NumTombstones = -1
		p.BytesTotal = -1
		return nil
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

		suffix, _ := NewSuffix(p, file)
		suffixes = append(suffixes, suffix)
	}

	sort.Sort(suffixes)
	p.suffixes = suffixes
	return nil
}

func (p *Partition) buildSuffixIndex() {
	// only index suffix dirs when needed
	if p.Resource.Type == "object" &&
		p.config.ObjectIndexHandoffOnly && (!p.Handoff) {
		return
	}

	for _, suffix := range p.suffixes {
		suffix.BuildIndex()
	}
}

// BuildIndex builds index for one partition
// It is a blocking call and return after finishing index build for all
// suffixes under the partition
func (p *Partition) BuildIndex() {
	defer p.Resource.sem.release()

	logp.Debug("partition", "Start building index for partition: %s", p.Path)

	// limit num of partition indexers can run simultaneously
	// to avoid heavy IO hit
	p.Resource.sem.acquire()

	// load suffix list for the partition
	err := p.init()
	if err != nil {
		return
	}

	p.buildSuffixIndex()
	// LastIndexed is shared at partition level, need to update before ToEvent()
	p.LastIndexed = time.Now()

	switch p.Resource.Type {
	case "account":
		fallthrough
	case "container":
		sort.Sort(IndexableFileSorter(p.IndexableQ))
		for _, f := range p.IndexableQ {
			f.Index()
			event := f.ToEvent()

			part := event.ToPartition()
			logp.Debug("hack", "77--> : %s - %s", event.ToMapStr()["path"], part.Mtime)

			if event != nil {
				p.eventChan <- event
			}
		}
	case "object":
		//if p.config.EnableObjectPartitionIndex {
		//event := input.NewObjectPartitionEvent(p.ToSwiftObjectPartition())
		//p.eventChan <- event
		//}
	}
}

// GetEvents returns the event channel for all partition related events
// XXX: deprecated
func (p *Partition) GetEvents() <-chan input.Event {
	return p.eventChan
}

// AnnotateSwiftObject add info from indexer to the swift.Object data object
func (p *Partition) AnnotateSwiftObject(obj *swift.Object) {
	if p.Resource == nil {
		logp.Critical("AnnotateSwiftObject: BUG: res reference is nil")
	}
	p.Resource.AnnotateSwiftObject(obj)

	obj.Annotate(*p)

	obj.PeerDevices = strings.Join(p.PeerDevices, ",")
	obj.PeerIps = strings.Join(p.PeerIps, ",")
}

// ToSwiftPartition creates annotated swift.ObjectPartition data object for event publishing
func (p *Partition) ToSwiftObjectPartition() swift.ObjectPartition {
	var bytesTotalMB int64
	if p.BytesTotal == -1 {
		bytesTotalMB = -1
	} else {
		bytesTotalMB = int64(p.BytesTotal / 1024 / 1024)
	}

	objPart := swift.ObjectPartition{
		Partition: &swift.Partition{
			PartId:      p.PartId,
			Mtime:       p.Mtime,
			LastIndexed: p.LastIndexed,
			// fields inherited from parents
			ResourceType: p.Type,
			Device:       p.DevName,
			Ip:           p.Ip,
			RingMtime:    p.RingMtime,
			Handoff:      p.Handoff,
			ReplicaId:    p.ReplicaId,
			PeerDevices:  strings.Join(p.PeerDevices, ","),
			PeerIps:      strings.Join(p.PeerIps, ","),
		},
		NumDatafiles:  p.NumDatafiles,
		NumTombstones: p.NumTombstones,
		BytesTotalMB:  bytesTotalMB,
	}
	return objPart
}
