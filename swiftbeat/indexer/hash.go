package indexer

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/swiftbeat/input"
	"github.com/elastic/beats/swiftbeat/input/swift"
)

type Hash struct {
	*IndexRecord
	*Suffix
	datafiles []*Datafile
}

type HashSorter []*Hash

func (hashes HashSorter) Len() int {
	return len(hashes)
}

func (hashes HashSorter) Less(i, j int) bool {
	return hashes[i].Mtime.After(hashes[j].Mtime)
}

func (hashes HashSorter) Swap(i, j int) {
	hashes[i], hashes[j] = hashes[j], hashes[i]
}

func NewHash(
	s *Suffix,
	file os.FileInfo,
) (*Hash, error) {
	hash := &Hash{
		IndexRecord: &IndexRecord{
			Name:  file.Name(),
			Path:  filepath.Join(s.Path, file.Name()),
			Mtime: file.ModTime(),
		},
		Suffix:    s,
		datafiles: nil,
	}
	return hash, nil
}

func (h *Hash) init() error {
	path := h.Path
	logp.Debug("hash", "Init hash: %s", path)

	var dfiles DatafileSorter

	files, err := ioutil.ReadDir(path)
	if err != nil {
		logp.Err("list dir(%s) failed: %v", path, err)
		return err
	}

	for _, file := range files {
		if !file.Mode().IsRegular() {
			continue
		}

		dfile, _ := NewDatafile(h, file)
		dfiles = append(dfiles, dfile)
	}

	sort.Sort(dfiles)
	h.datafiles = dfiles
	return nil
}

func (h *Hash) buildPartitionIndex() {
	if len(h.datafiles) <= 0 {
		return
	}

	// sort in descedent mtime order
	file := h.datafiles[0]
	// everything inside a partition is serialized
	if strings.HasSuffix(file.Name, ".data") {
		h.Partition.NumDatafiles += 1
	} else if strings.HasSuffix(file.Name, ".ts") {
		h.Partition.NumTomestones += 1
	}
}

func (h *Hash) buildDatafileIndex() {
	for _, dfile := range h.datafiles {
		dfile.Parse()

		event := input.NewObjectEvent(dfile.ToSwiftObject())
		h.eventChan <- event

		logp.Debug("datafile", "Event generated for %s - Dump %s",
			dfile.Path, dfile.Metadata)
	}
}

// BuildIndex builds index for one hash dir
// It is a blocking call and return after finishing index build for all
// datafiles under the hash dir
func (h *Hash) BuildIndex() {
	logp.Debug("hash", "Start building index for hash: %s", h.Path)

	// load file list for the hash
	err := h.init()
	if err != nil {
		return
	}

	if h.config.EnablePartitionIndex {
		h.buildPartitionIndex()
	}

	if h.config.EnableDatafileIndex {
		h.buildDatafileIndex()
	}
}

// AnnotateSwiftObject add info from indexer to the swift.Object data object
func (h *Hash) AnnotateSwiftObject(obj *swift.Object) {
	if h.Suffix == nil {
		logp.Critical("AnnotateSwiftObject: BUG: suffix reference is nil")
	}
	h.Suffix.AnnotateSwiftObject(obj)
	obj.Annotate(*h)
}
