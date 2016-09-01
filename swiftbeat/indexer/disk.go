package indexer

import (
	"io/ioutil"
	"path/filepath"
	"sort"
	"time"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/swiftbeat/input"
)

// IndexRecord is a struct to be embedded in all indexable structs
type IndexRecord struct {
	name  string
	path  string
	mtime time.Time
}

// Indexer interface for all level indexable resources
type Indexer interface {
	BuildIndex()
	GetEvents() <-chan input.Event
}

// Resource is a generic modeling for all 3 types of resources
type Resource struct {
	*IndexRecord
	disk       *Disk
	eventChan  chan input.Event
	sem        Semaphore
	done       chan struct{}
	partitions []*Partition
}

// Device struct represent the top level Swift disk layout
type Disk struct {
	*IndexRecord
	accounts   *Resource
	containers *Resource
	objects    *Resource
}

// NewDisk returns a new Disk object.
// Disk object initialized with accounts, containers, objects pointing to the
// respective path
func NewDisk(
	name string,
	path string,
	eventChan chan input.Event,
	done chan struct{},
) (*Disk, error) {
	logp.Debug("indexer", "Init Disk: %s", path)

	disk := &Disk{
		IndexRecord: &IndexRecord{
			name: name,
			path: path,
		},
	}

	// list disk files
	files, err := ioutil.ReadDir(path)
	if err != nil {
		logp.Err("list dir(%s) failed: %v", path, err)
		return nil, err
	}

	// init account, container and objects respectively
	for _, file := range files {
		if !file.IsDir() {
			continue
		}

		fname := file.Name()
		subpath := filepath.Join(path, fname)
		resource := &Resource{
			IndexRecord: &IndexRecord{
				name: fname,
				path: subpath,
			},
			disk:       disk,
			eventChan:  eventChan,
			sem:        NewSemaphore(2),
			done:       done,
			partitions: nil,
		}

		switch fname {
		case "accounts":
			disk.accounts = resource
		case "containers":
			disk.containers = resource
		case "objects":
			disk.objects = resource
		}
	}

	return disk, nil
}

// initResource read and initialize partitions for specified resource
func initResource(disk *Disk, resource string) {

	var r *Resource
	var parts PartitionSorter

	switch resource {
	case "accounts":
		r = disk.accounts
	case "containers":
		r = disk.containers
	case "objects":
		r = disk.objects
	}

	logp.Debug("indexer", "Init resource: %s", r.path)

	files, err := ioutil.ReadDir(r.path)
	if err != nil {
		logp.Err("list dir(%s) failed: %v", r.path, err)
		return
	}

	parts = r.partitions
	for _, file := range files {
		if !file.IsDir() {
			continue
		}
		part, _ := NewPartition(r, file, r.done)
		parts = append(parts, part)
	}
	sort.Sort(parts)

	// set initialized partitions struct back to resource layout object
	r.partitions = parts
}

func (l *Disk) init() {
	initResource(l, "accounts")
	initResource(l, "containers")
	initResource(l, "objects")
}

// BuildIndex triggers index build recursively on all top level resources
func (l *Disk) BuildIndex() {

	// load partition list for top level resources
	l.init()

	// TODO: properly handle relation between resources
	//go l.accounts.BuildIndex()
	//go l.containers.BuildIndex()
	go l.objects.BuildIndex()
}

// TODO: handle accounts/containers as well
func (l *Disk) StartEventCollector() {
	l.objects.StartEventCollector()
}

// TODO: handle accounts/containers as well
func (l *Disk) GetEvents() <-chan input.Event {
	return l.objects.GetEvents()
}

// BuildIndex builds index iteratively for all partitions
// It is a non-blocking call to start index build, however the actual time when
// it happens depends on the concurrency settings
func (r *Resource) BuildIndex() {
	logp.Debug("indexer", "Start building index for resource: %s", r.name)

	// number of partition indexer can run simulataneously
	// is controlled by resource level semaphore
	for _, part := range r.partitions {
		go part.BuildIndex()
	}
}

// StartEventCollector pumps all events generated under the resource directory
// through the fan-in channel
func (r *Resource) StartEventCollector() {

	// redirect event from individual channel to resource indexer level
	output := func(ch <-chan input.Event) {
		for ev := range ch {
			select {
			// TODO: update last tracked record
			case r.eventChan <- ev:
			case <-r.done:
				return
			}
		}
	}

	for _, part := range r.partitions {
		go output(part.GetEvents())
	}
}

// GetEvents returns the event channel for all resource related events
func (r *Resource) GetEvents() <-chan input.Event {
	return r.eventChan
}
