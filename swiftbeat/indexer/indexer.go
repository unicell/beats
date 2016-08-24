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
	path       string
	mtime      time.Time
	lastSynced time.Time
}

// Indexer interface for all level indexable resources
type Indexer interface {
	BuildIndex()
	GetEvents() <-chan *input.Event
}

// ResourceLayout is a generic modeling for all 3 types of resources
type ResourceLayout struct {
	*IndexRecord
	resourceType string
	eventChan    chan *input.Event
	sem          Semaphore
	done         chan struct{}
	partitions   []*Partition
}

// Layout struct represent the top level Swift disk layout
type Layout struct {
	accounts   *ResourceLayout
	containers *ResourceLayout
	objects    *ResourceLayout
}

// NewLayout returns a new Layout object.
// Layout object initialized with accounts, containers, objects pointing to the
// respective path
func NewLayout(
	path string,
	done chan struct{},
) (*Layout, error) {
	logp.Debug("indexer", "Init layout: %s", path)

	layout := &Layout{}

	// list layout files
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
		resource := &ResourceLayout{
			IndexRecord: &IndexRecord{
				path: subpath,
			},
			resourceType: fname,
			eventChan:    make(chan *input.Event),
			sem:          NewSemaphore(2),
			done:         done,
			partitions:   nil,
		}

		switch fname {
		case "accounts":
			layout.accounts = resource
		case "containers":
			layout.containers = resource
		case "objects":
			layout.objects = resource
		}
	}

	return layout, nil
}

// initResource read and initialize partitions for specified resource
func initResource(layout *Layout, resource string) {

	var rl *ResourceLayout
	var parts PartitionSorter

	switch resource {
	case "accounts":
		rl = layout.accounts
	case "containers":
		rl = layout.containers
	case "objects":
		rl = layout.objects
	}

	logp.Debug("indexer", "Init resource: %s", rl.path)

	files, err := ioutil.ReadDir(rl.path)
	if err != nil {
		logp.Err("list dir(%s) failed: %v", rl.path, err)
		return
	}

	parts = rl.partitions
	for _, file := range files {
		if !file.IsDir() {
			continue
		}
		part, _ := NewPartition(rl, file, rl.done)
		parts = append(parts, part)
	}
	sort.Sort(parts)

	// set initialized partitions struct back to resource layout object
	rl.partitions = parts
}

func (l *Layout) init() {
	initResource(l, "accounts")
	initResource(l, "containers")
	initResource(l, "objects")
}

// BuildIndex triggers index build recursively on all top level resources
func (l *Layout) BuildIndex() {

	// load partition list for top level resources
	l.init()

	// TODO: properly handle relation between resources
	//go l.accounts.BuildIndex()
	//go l.containers.BuildIndex()
	go l.objects.BuildIndex()
}

// TODO: handle accounts/containers as well
func (l *Layout) StartEventCollector() {
	l.objects.StartEventCollector()
}

// TODO: handle accounts/containers as well
func (l *Layout) GetEvents() <-chan *input.Event {
	return l.objects.GetEvents()
}

// BuildIndex builds index iteratively for all partitions
// It is a non-blocking call to start index build, however the actual time when
// it happens depends on the concurrency settings
func (rl *ResourceLayout) BuildIndex() {
	logp.Debug("indexer", "Start building index for resource: %s", rl.resourceType)

	// number of partition indexer can run simulataneously
	// is controlled by rl level semaphore
	for _, part := range rl.partitions {
		go part.BuildIndex()
	}
}

// StartEventCollector pumps all events generated under the resource directory
// through the fan-in channel
func (rl *ResourceLayout) StartEventCollector() {

	// redirect event from individual channel to rl indexer level
	output := func(ch <-chan *input.Event) {
		for ev := range ch {
			select {
			// TODO: update last tracked record
			case rl.eventChan <- ev:
			case <-rl.done:
				return
			}
		}
	}

	for _, part := range rl.partitions {
		go output(part.GetEvents())
	}
}

// GetEvents returns the event channel for all resource related events
func (rl *ResourceLayout) GetEvents() <-chan *input.Event {
	return rl.eventChan
}
