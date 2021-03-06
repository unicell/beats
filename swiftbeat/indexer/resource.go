package indexer

import (
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/openstack/swift/go/hummingbird"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/swiftbeat/input/swift"
)

// Resource is a generic modeling for all 3 types of resources
type Resource struct {
	*IndexRecord
	*Disk
	Type       string
	sem        Semaphore
	wg         sync.WaitGroup
	partitions []*Partition
	ring       hummingbird.Ring
	RingMtime  time.Time
	DevName    string
	DevId      int
	Ip         string
	RingCKSum  string
}

func NewResource(
	d *Disk,
	file os.FileInfo,
) (*Resource, error) {
	res := &Resource{
		IndexRecord: &IndexRecord{
			// Name also represents the resource type
			Name:  file.Name(),
			Path:  filepath.Join(d.Path, file.Name()),
			Mtime: file.ModTime(),
		},
		Disk:       d,
		sem:        NewSemaphore(1),
		partitions: nil,
		DevName:    d.Name,
		DevId:      -1,
	}
	// remove trailing 's' for resource type
	res.Type = res.Name[:len(res.Name)-1]
	res.wg.Add(1)
	return res, nil
}

func (r *Resource) initRing() error {

	// read cluster prefix / suffix from configuration file
	hashPathPrefix, hashPathSuffix, err := hummingbird.GetHashPrefixAndSuffix()
	if err != nil {
		logp.Err("Error getting Swift hash prefix and suffix")
		return err
	}

	logp.Debug("resource", "Swift hash prefix, suffix: %s %s", hashPathPrefix, hashPathSuffix)

	// initialize ring for the resource type
	// TODO: add multi policy support
	ring, err := hummingbird.GetRingSnapshot(r.Type, hashPathPrefix, hashPathSuffix, 0)
	if err != nil {
		logp.Err("Error reading the %s ring", r.Type)
		return err
	}
	r.ring = ring
	r.initDevInfo()

	// probe ring mtime since it is not exposed in Hummingbird
	ringPath := fmt.Sprintf("/etc/swift/%s.ring.gz", r.Type)
	if f, err := os.Stat(ringPath); err == nil {
		r.RingMtime = f.ModTime()
	}

	data, err := ioutil.ReadFile(ringPath)
	if err != nil {
		logp.Err("Error getting checksum for the ring: %s", ringPath)
		return err
	}
	r.RingCKSum = fmt.Sprintf("%x", md5.Sum(data))

	return nil
}

// init Dev Id and IP based on ring lookup with local IP and device name
func (r *Resource) initDevInfo() {

	var localIPs = make(map[string]bool)

	localAddrs, err := net.InterfaceAddrs()
	if err != nil {
		return
	}
	for _, addr := range localAddrs {
		localIPs[strings.Split(addr.String(), "/")[0]] = true
	}

	devs := r.ring.AllDevices()
	for _, dev := range devs {
		if localIPs[dev.Ip] && dev.Device == r.DevName {
			r.DevId = dev.Id
			r.Ip = dev.Ip
			break
		}
	}
}

func (r *Resource) init() error {
	defer r.wg.Done()

	path := r.Path
	logp.Debug("resource", "Init resource: %s", path)

	err := r.initRing()
	if err != nil {
		logp.Err("Failed to init ring data")
		return err
	}

	// load partitions for the resource type
	files, err := ioutil.ReadDir(path)
	if err != nil {
		logp.Err("list dir(%s) failed: %v", path, err)
		return err
	}

	var parts PartitionSorter
	for _, file := range files {
		if !file.IsDir() {
			continue
		}

		part, _ := NewPartition(r, file)
		parts = append(parts, part)
	}

	sort.Sort(parts)
	r.partitions = parts
	return nil
}

// BuildIndex builds index iteratively for all partitions
// It is a non-blocking call to start index build, however the actual time when
// it happens depends on the concurrency settings
func (r *Resource) BuildIndex() {
	logp.Debug("resource", "Start building index for resource: %s", r.Name)

	// load partition list for the resource
	err := r.init()
	if err != nil {
		return
	}

	// number of partition indexer can run simulataneously
	// is controlled by resource level semaphore
	for _, part := range r.partitions {
		go part.BuildIndex()
	}
}

// AnnotateSwiftObject add info from indexer to the swift.Object data object
func (r *Resource) AnnotateSwiftObject(obj *swift.Object) {
	if r.Disk == nil {
		logp.Critical("AnnotateSwiftObject: BUG: disk reference is nil")
	}
	r.Disk.AnnotateSwiftObject(obj)
	obj.Annotate(*r)
}
