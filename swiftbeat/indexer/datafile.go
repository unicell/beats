package indexer

import (
	"bytes"
	"syscall"

	pickle "github.com/hydrogen18/stalecucumber"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/swiftbeat/input/swift"
)

const (
	metadataKey = "user.swift.metadata"
	// XXX: is 4096 enough for all the cases?
	xattrBufSize = 4096
)

type Datafile struct {
	*IndexableFile
	// for simplicity, store both kv in string
	// and convert if necessary when use
	Metadata map[string]string
}

// NewDatafile returns a new Datafile object
func NewDatafile(
	f *IndexableFile,
) (*Datafile, error) {
	dfile := &Datafile{
		IndexableFile: f,
		Metadata:      map[string]string{},
	}
	return dfile, nil
}

// Index individual datafile to fill in structured data
func (f *Datafile) Index() {
	buf := make([]byte, xattrBufSize)

	// read from xattr
	size, err := syscall.Getxattr(f.Path, metadataKey, buf)
	if err != nil {
		logp.Err("read xattr file(%s) failed: %v", f.Path, err)
		return
	}

	// wrapping over native buf with io.Reader interface to unpickle
	buffer := bytes.NewBuffer(buf[:size])
	dict, err := pickle.Dict(pickle.Unpickle(buffer))
	if err != nil {
		logp.Err("unpickling data(%s) failed: %v", buffer, err)
		return
	}

	for key, value := range dict {
		f.Metadata[key.(string)] = value.(string)
	}
}

// AnnotateSwiftObject add info from indexer to the swift.Object data object
func (f *Datafile) AnnotateSwiftObject(obj *swift.Object) {
	if f.Hash == nil {
		logp.Critical("AnnotateSwiftObject: BUG: hash reference is nil")
	}
	f.Hash.AnnotateSwiftObject(obj)
	obj.Annotate(*f)
}

// ToSwiftObject creates annotated swift.Object data object for event publishing
func (f *Datafile) ToSwiftObject() swift.Object {
	obj := &swift.Object{}
	f.AnnotateSwiftObject(obj)
	return *obj
}
