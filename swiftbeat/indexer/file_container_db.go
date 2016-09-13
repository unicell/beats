package indexer

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"strings"
	"time"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/swiftbeat/input/swift"
)

type ContainerDBfile struct {
	*IndexableFile
	account      string
	container    string
	status       string
	object_count int64
	bytes_used   int64
	policy_index int64
}

// NewContainerDBfile returns a new ContainerDBfile object
func NewContainerDBfile(
	f *IndexableFile,
) (*ContainerDBfile, error) {
	dbfile := &ContainerDBfile{
		IndexableFile: f,
		object_count:  -1,
		bytes_used:    -1,
		policy_index:  -1,
	}
	return dbfile, nil
}

// Index individual dbfile to fill in structured data
func (f *ContainerDBfile) Index() {
	db, err := sql.Open("sqlite3", f.Path)
	if err != nil {
		logp.Err("open sqlite file(%s) failed: %v", f.Path, err)
		return
	}

	rows, err := db.Query(`SELECT account, container, status,
				      reported_object_count, reported_bytes_used,
				      storage_policy_index
			       FROM container_info
                               LIMIT 1`)
	if err != nil {
		logp.Err("sql query failed on file(%s): %v", f.Path, err)
		return
	}

	for rows.Next() {
		var account string
		var container string
		var status string
		var object_count int64
		var bytes_used int64
		var policy_index int64

		err = rows.Scan(&account, &container, &status,
			&object_count, &bytes_used, &policy_index)
		if err != nil {
			logp.Err("sql rows can failed on file(%s): %v", f.Path, err)
			continue
		}

		f.account = account
		f.container = container
		f.status = status
		f.object_count = object_count
		f.bytes_used = bytes_used
		f.policy_index = policy_index
	}
	err = rows.Err()
	if err != nil {
		logp.Err("sql rows iteration failed on file(%s): %v", f.Path, err)
	}

	f.LastIndexed = time.Now()
}

// ToSwiftContainer creates annotated swift.Container data object for event publishing
func (f *ContainerDBfile) ToSwiftContainer() swift.Container {
	c := swift.Container{
		Mtime:       f.Mtime,
		Path:        f.Path,
		SizeKB:      int64(f.Size / 1024),
		Account:     f.account,
		Container:   f.container,
		Status:      f.status,
		ObjectCount: f.object_count,
		BytesUsedMB: int64(f.bytes_used / 1024 / 1024),
		PolicyIndex: f.policy_index,
		LastIndexed: f.LastIndexed,
		// fields inherited from parents
		ResourceType: f.Type,
		Partition:    f.PartId,
		Device:       f.DevName,
		Ip:           f.Ip,
		RingMtime:    f.RingMtime,
		Handoff:      f.Handoff,
		PeerDevices:  strings.Join(f.PeerDevices, ","),
		PeerIps:      strings.Join(f.PeerIps, ","),
	}
	return c
}
