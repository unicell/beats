package indexer

import (
	"io/ioutil"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/swiftbeat/input"
	"github.com/elastic/beats/swiftbeat/input/swift"
)

// Device struct represent the top level Swift disk layout
type Disk struct {
	*IndexRecord
	eventChan  chan input.Event
	done       chan struct{}
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
	disk := &Disk{
		IndexRecord: &IndexRecord{
			Name: name,
			Path: path,
		},
		eventChan: eventChan,
		done:      done,
	}
	return disk, nil
}

func (d *Disk) init() error {
	path := d.Path
	logp.Debug("indexer", "Init disk: %s", path)

	// list disk files
	files, err := ioutil.ReadDir(path)
	if err != nil {
		logp.Err("list dir(%s) failed: %v", path, err)
		return err
	}

	// init account, container and objects respectively
	for _, file := range files {
		if !file.IsDir() {
			continue
		}

		res, _ := NewResource(d, file, d.eventChan, d.done)

		switch file.Name() {
		case "accounts":
			d.accounts = res
		case "containers":
			d.containers = res
		case "objects":
			d.objects = res
		}
	}
	return nil
}

// BuildIndex triggers index build recursively on all top level resources
func (d *Disk) BuildIndex() {

	// load partition list for top level resources
	err := d.init()
	if err != nil {
		return
	}

	// TODO: properly handle relation between resources
	//go d.accounts.BuildIndex()
	//go d.containers.BuildIndex()
	go d.objects.BuildIndex()
}

// TODO: handle accounts/containers as well
func (d *Disk) StartEventCollector() {
	d.objects.Wait()
	d.objects.StartEventCollector()
}

// TODO: handle accounts/containers as well
func (d *Disk) GetEvents() <-chan input.Event {
	return d.objects.GetEvents()
}

// AnnotateSwiftObject add info from indexer to the swift.Object data object
func (d *Disk) AnnotateSwiftObject(obj *swift.Object) {
	obj.Annotate(*d)
}
