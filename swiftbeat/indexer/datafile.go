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
	metadata map[string]string
}

type DatafileSorter []*Datafile

func (dfiles DatafileSorter) Len() int {
	return len(dfiles)
}

func (dfiles DatafileSorter) Less(i, j int) bool {
	return dfiles[i].mtime.After(dfiles[j].mtime)
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
			name:  file.Name(),
			path:  filepath.Join(h.path, file.Name()),
			mtime: file.ModTime(),
		},
		hash:     h,
		metadata: map[string]string{},
	}
	return dfile, nil
}

// Parse individual datafile to fill in structured data
func (f *Datafile) Parse() {
	buf := make([]byte, xattrBufSize)

	// read from xattr
	size, err := syscall.Getxattr(f.path, metadataKey, buf)
	if err != nil {
		logp.Err("read xattr file(%s) failed: %v", f.path, err)
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
		f.metadata[key.(string)] = value.(string)
	}
}

func (f *Datafile) ToSwiftObject() swift.Object {
	obj := swift.Object{
		Datafile:       f.name,
		DatafileMtime:  f.mtime,
		Hash:           f.hash.name,
		HashMtime:      f.hash.mtime,
		Suffix:         f.hash.suffix.name,
		SuffixMtime:    f.hash.suffix.mtime,
		Partition:      f.hash.suffix.part.name,
		PartitionMtime: f.hash.suffix.part.mtime,
		Metadata:       f.metadata,
		Path:           f.path,
	}
	return obj
}
