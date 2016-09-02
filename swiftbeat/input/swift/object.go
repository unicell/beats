package swift

import (
	"reflect"
	"time"
)

// Object models all necessary info regarding an object event
// it can be populated automatically by reflection based on tagging info
// for fields need special handling can be done in AnnotateXXXObject
type Object struct {
	Name           string            `indexer:"Datafile" field:"Name"`
	Mtime          time.Time         `indexer:"Datafile" field:"Mtime"`
	Hash           string            `indexer:"Hash" field:"Name"`
	HashMtime      time.Time         `indexer:"Hash" field:"Mtime"`
	Suffix         string            `indexer:"Suffix" field:"Name"`
	SuffixMtime    time.Time         `indexer:"Suffix" field:"Mtime"`
	Partition      string            `indexer:"Partition" field:"Name"`
	PartitionMtime time.Time         `indexer:"Partition" field:"Mtime"`
	Metadata       map[string]string `indexer:"Datafile" field:"Metadata"`
	Path           string            `indexer:"Datafile" field:"Path"`
	Device         string            `indexer:"Disk" field:"Name"`
	Handoff        bool              `indexer:"Partition" field:"Handoff"`
	Ip             string            `indexer:"Resource" field:"Ip"`
	PeerDevices    string
	PeerIps        string
}

// Annotate copies info fields from indexer based on struct tag and reflection
func (o *Object) Annotate(indexer interface{}) {

	indexerType := reflect.TypeOf(indexer)
	indexerValue := reflect.ValueOf(indexer)

	objType := reflect.TypeOf(*o)
	objValue := reflect.ValueOf(o)
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
