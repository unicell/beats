package swift

import (
	"reflect"
	"time"
)

// Partition models all necessary info regarding an partition event
// it can be populated automatically by reflection based on tagging info
// for fields need special handling can be done in AnnotateXXXPartition
type Partition struct {
	Name         string    `indexer:"Partition" field:"Name"`
	Mtime        time.Time `indexer:"Partition" field:"Mtime"`
	LastIndexed  time.Time `indexer:"Partition" field:"LastIndexed"`
	ResourceType string    `indexer:"Resource" field:"Type"`
	Device       string    `indexer:"Disk" field:"Name"`
	Ip           string    `indexer:"Resource" field:"Ip"`
	RingMtime    time.Time `indexer:"Resource" field:"RingMtime"`
	NumDatafiles int64     `indexer:"Partition" field:"NumDatafiles"`
	NumTomstones int64     `indexer:"Partition" field:"NumTomestones"`
	Handoff      bool      `indexer:"Partition" field:"Handoff"`
	PeerDevices  string
	PeerIps      string
	BytesMBTotal int64
}

// Annotate copies info fields from indexer based on struct tag and reflection
func (p *Partition) Annotate(indexer interface{}) {

	indexerType := reflect.TypeOf(indexer)
	indexerValue := reflect.ValueOf(indexer)

	objType := reflect.TypeOf(*p)
	objValue := reflect.ValueOf(p)
	for i := 0; i < objType.NumField(); i++ {
		objField := objType.Field(i)

		idxt := objField.Tag.Get("indexer")
		if idxt == indexerType.Name() {
			idxf := objField.Tag.Get("field")

			v := objValue.Elem().FieldByName(objField.Name)
			v.Set(indexerValue.FieldByName(idxf))
		}
	}
}
