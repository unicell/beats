package indexer

import (
	"io/ioutil"
	"path/filepath"
	"time"

	"github.com/elastic/beats/libbeat/logp"
)

type IndexRecord struct {
	path         string
	lastModified time.Time
	lastSynced   time.Time
}

type Indexer interface {
	BuildIndex()
}

type ResourceLayout struct {
	*IndexRecord
	resourceType string
	partitions   []Partition
}

type Layout struct {
	accounts   *ResourceLayout
	containers *ResourceLayout
	objects    *ResourceLayout
}

func NewLayout(path string) (*Layout, error) {
	logp.Debug("indexer", "Init layout path: %s", path)

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
			partitions:   []Partition{},
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

	var path string
	var parts []Partition

	switch resource {
	case "accounts":
		path = layout.accounts.path
		parts = layout.accounts.partitions
	case "containers":
		path = layout.containers.path
		parts = layout.containers.partitions
	case "objects":
		path = layout.objects.path
		parts = layout.objects.partitions
	}

	logp.Debug("indexer", "Init resource layout: %s", path)

	files, err := ioutil.ReadDir(path)
	if err != nil {
		logp.Err("list dir(%s) failed: %v", path, err)
	}

	for _, file := range files {
		part := Partition{
			IndexRecord: &IndexRecord{
				path: filepath.Join(path, file.Name()),
			},
			suffixes: []Suffix{},
		}
		parts = append(parts, part)
	}
}

func (l *Layout) Init() {
	initResource(l, "accounts")
	initResource(l, "containers")
	initResource(l, "objects")
}

func (l *Layout) BuildIndex() {
}
