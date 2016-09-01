package indexer

import (
	"bytes"
	"os"
	"path/filepath"
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
	*IndexRecord
	hash *Hash
	// for simplicity, store both kv in string
	// and convert if necessary when use
	Metadata map[string]string
}

type DatafileSorter []*Datafile

func (dfiles DatafileSorter) Len() int {
	return len(dfiles)
}

func (dfiles DatafileSorter) Less(i, j int) bool {
	return dfiles[i].Mtime.After(dfiles[j].Mtime)
}

func (dfiles DatafileSorter) Swap(i, j int) {
	dfiles[i], dfiles[j] = dfiles[j], dfiles[i]
}

// NewDatafile returns a new Datafile object
func NewDatafile(
	h *Hash,
	file os.FileInfo,
) (*Datafile, error) {
	dfile := &Datafile{
		IndexRecord: &IndexRecord{
			Name:  file.Name(),
			Path:  filepath.Join(h.Path, file.Name()),
			Mtime: file.ModTime(),
		},
		hash:     h,
		Metadata: map[string]string{},
	}
	return dfile, nil
}

// Parse individual datafile to fill in structured data
func (f *Datafile) Parse() {
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
	if f.hash == nil {
		logp.Critical("AnnotateSwiftObject: BUG: hash reference is nil")
	}
	f.hash.AnnotateSwiftObject(obj)
	obj.Annotate(*f)
}

// ToSwiftObject creates annotated swift.Object data object for event publishing
func (f *Datafile) ToSwiftObject() swift.Object {
	obj := &swift.Object{}
	f.AnnotateSwiftObject(obj)
	return *obj
}
