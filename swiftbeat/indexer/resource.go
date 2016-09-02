package indexer

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/swiftbeat/input"
	"github.com/elastic/beats/swiftbeat/input/swift"
)

// Resource is a generic modeling for all 3 types of resources
type Resource struct {
	*IndexRecord
	disk       *Disk
	eventChan  chan input.Event
	done       chan struct{}
	sem        Semaphore
	wg         sync.WaitGroup
	partitions []*Partition
}

func NewResource(
	d *Disk,
	file os.FileInfo,
	eventChan chan input.Event,
	done chan struct{},
) (*Resource, error) {
	res := &Resource{
		IndexRecord: &IndexRecord{
			Name:  file.Name(),
			Path:  filepath.Join(d.Path, file.Name()),
			Mtime: file.ModTime(),
		},
		disk:       d,
		eventChan:  eventChan,
		done:       done,
		sem:        NewSemaphore(1),
		partitions: nil,
	}
	res.wg.Add(1)
	return res, nil
}

func (r *Resource) init() error {
	defer r.wg.Done()

	path := r.Path
	logp.Debug("resource", "Init resource: %s", path)

	var parts PartitionSorter

	files, err := ioutil.ReadDir(path)
	if err != nil {
		logp.Err("list dir(%s) failed: %v", path, err)
		return err
	}

	for _, file := range files {
		if !file.IsDir() {
			continue
		}

		part, _ := NewPartition(r, file, r.done)
		parts = append(parts, part)
	}

	sort.Sort(parts)
	r.partitions = parts
	return nil
}

// Wait blocks until the resource is ready to collect events
func (r *Resource) Wait() {
	r.wg.Wait()
}

// BuildIndex builds index iteratively for all partitions
// It is a non-blocking call to start index build, however the actual time when
// it happens depends on the concurrency settings
func (r *Resource) BuildIndex() {
	logp.Debug("resource", "Start building index for resource: %s", r.Name)

	// load partition list for the resource
	err := r.init()
	if err != nil {
		return
	}

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

// AnnotateSwiftObject add info from indexer to the swift.Object data object
func (r *Resource) AnnotateSwiftObject(obj *swift.Object) {
	if r.disk == nil {
		logp.Critical("AnnotateSwiftObject: BUG: disk reference is nil")
	}
	r.disk.AnnotateSwiftObject(obj)
	obj.Annotate(*r)
}
