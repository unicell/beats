package indexer

import (
	"io/ioutil"
	"path/filepath"
	"sort"
	"time"

	"github.com/elastic/beats/libbeat/logp"
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
}

// ResourceLayout is a generic modeling for all 3 types of resources
type ResourceLayout struct {
	*IndexRecord
	resourceType string
	partitions   []Partition
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
	var parts PartitionSorter

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
		return
	}

	for _, file := range files {
		if !file.IsDir() {
			continue
		}
		part := Partition{
			IndexRecord: &IndexRecord{
				path:  filepath.Join(path, file.Name()),
				mtime: file.ModTime(),
			},
			suffixes: []Suffix{},
		}
		parts = append(parts, part)
	}
	sort.Sort(parts)

	// set initialized partitions struct back to resource layout object
	switch resource {
	case "accounts":
		layout.accounts.partitions = parts
	case "containers":
		layout.containers.partitions = parts
	case "objects":
		layout.objects.partitions = parts
	}
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

	// TODO
	//l.accounts.BuildIndex()
	//l.containers.BuildIndex()
	l.objects.BuildIndex()
}

// BuildIndex builds index iteratively for all partitions
func (rl *ResourceLayout) BuildIndex() {
	logp.Debug("indexer", "Build index for resource: %s", rl.resourceType)

	for _, part := range rl.partitions {
		part.BuildIndex()
	}
}
