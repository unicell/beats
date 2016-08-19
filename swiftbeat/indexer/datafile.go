package indexer

import (
	"bytes"
	//"os"
	"syscall"

	"github.com/elastic/beats/libbeat/logp"
	pickle "github.com/hydrogen18/stalecucumber"
)

const (
	metadataKey  = "user.swift.metadata"
	xattrBufSize = 4096
)

type DataRecord struct {
	contentLength    int
	contentType      string
	etag             string
	xObjectMetaMtime string
	xTimestamp       string
	name             string
}

type Datafile struct {
	*IndexRecord
	// for simplicity, store both kv in string
	// and convert if necessary when use
	metadata map[string]string
}

type DatafileSorter []Datafile

func (dfiles DatafileSorter) Len() int {
	return len(dfiles)
}

func (dfiles DatafileSorter) Less(i, j int) bool {
	return dfiles[i].mtime.After(dfiles[j].mtime)
}

func (dfiles DatafileSorter) Swap(i, j int) {
	dfiles[i], dfiles[j] = dfiles[j], dfiles[i]
}

func (f *Datafile) init() {
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

	logp.Debug("datafile", "metadata dump %s", f.metadata)
}

// BuildIndex builds index for one datafile
// TODO: move away from naive linear scanning to better strategy
func (f *Datafile) BuildIndex() {
	logp.Debug("datafile", "Build index for datafile: %s", f.path)

	// load file extended attribute
	f.init()

	//os.Exit(1)

	//for _, dfile := range h.datafiles {
	//logp.Debug("datafile", "Build index for datafile: %s", dfile.path)
	//dfile.BuildIndex()
	//}
}
