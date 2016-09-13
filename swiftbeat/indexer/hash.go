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
	files []*IndexableFile
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
		Suffix: s,
		files:  nil,
	}
	return hash, nil
}

func (h *Hash) init() error {
	path := h.Path
	logp.Debug("hash", "Init hash: %s", path)

	var ifiles IndexableFileSorter

	files, err := ioutil.ReadDir(path)
	if err != nil {
		logp.Err("list dir(%s) failed: %v", path, err)
		return err
	}

	for _, file := range files {
		if !file.Mode().IsRegular() {
			continue
		}

		ifile, _ := NewIndexableFile(h, file)
		ifiles = append(ifiles, ifile)
	}

	sort.Sort(ifiles)
	h.files = ifiles
	return nil
}

func (h *Hash) buildObjectPartitionIndex() {
	if len(h.files) <= 0 {
		return
	}

	// sort in descedent mtime order
	file := h.files[0]
	// everything inside a partition is serialized
	if strings.HasSuffix(file.Name, ".data") {
		h.Partition.NumDatafiles += 1
		h.Partition.BytesTotal += file.Size
	} else if strings.HasSuffix(file.Name, ".ts") {
		h.Partition.NumTomestones += 1
	}
}

func (h *Hash) buildDatafileIndex() {
	for _, file := range h.files {
		if !strings.HasSuffix(file.Name, ".data") {
			return
		}

		dfile, _ := NewDatafile(file)
		dfile.Index()

		event := input.NewObjectEvent(dfile.ToSwiftObject())
		h.eventChan <- event

		logp.Debug("datafile", "Event generated for %s - Dump %s",
			dfile.Path, dfile.Metadata)
	}
}

func (h *Hash) buildContainerDBIndex() {
	for _, file := range h.files {
		if !strings.HasSuffix(file.Name, ".db") {
			return
		}

		dbfile, _ := NewContainerDBfile(file)
		dbfile.Index()

		event := input.NewContainerEvent(dbfile.ToSwiftContainer())
		h.eventChan <- event
	}
}

// BuildIndex builds index for one hash dir
// It is a blocking call and only return after finishing index build for all
// data under the hash dir
func (h *Hash) BuildIndex() {
	logp.Debug("hash", "Start building index for hash: %s", h.Path)

	// load file list for the hash
	err := h.init()
	if err != nil {
		return
	}

	if h.Type == "object" {
		if h.config.EnableObjectPartitionIndex {
			h.buildObjectPartitionIndex()
		}

		if h.config.EnableDatafileIndex {
			h.buildDatafileIndex()
		}
	} else if h.Type == "container" {
		h.buildContainerDBIndex()
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
