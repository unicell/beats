package indexer

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"strings"
	"time"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/swiftbeat/input"
	"github.com/elastic/beats/swiftbeat/input/swift"
)

type AccountDBfile struct {
	*FileRecord
	account         string
	status          string
	container_count int64
	object_count    int64
	bytes_used      int64
}

// NewAccountDBfile returns a new AccountDBfile object
func NewAccountDBfile(
	f *FileRecord,
) (*AccountDBfile, error) {
	dbfile := &AccountDBfile{
		FileRecord:      f,
		container_count: -1,
		object_count:    -1,
		bytes_used:      -1,
	}
	return dbfile, nil
}

// Index individual dbfile to fill in structured data
func (f *AccountDBfile) Index() {
	db, err := sql.Open("sqlite3", f.Path)
	if err != nil {
		logp.Err("open sqlite file(%s) failed: %v", f.Path, err)
		return
	}

	rows, err := db.Query(`SELECT account, status,
				      container_count, object_count, bytes_used
			       FROM account_stat
                               LIMIT 1`)
	if err != nil {
		logp.Err("sql query failed on file(%s): %v", f.Path, err)
		return
	}

	for rows.Next() {
		var account string
		var status string
		var container_count int64
		var object_count int64
		var bytes_used int64

		err = rows.Scan(&account, &status,
			&container_count, &object_count, &bytes_used)
		if err != nil {
			logp.Err("sql rows can failed on file(%s): %v", f.Path, err)
			continue
		}

		f.account = account
		f.status = status
		f.container_count = container_count
		f.object_count = object_count
		f.bytes_used = bytes_used
	}
	err = rows.Err()
	if err != nil {
		logp.Err("sql rows iteration failed on file(%s): %v", f.Path, err)
	}

	f.LastIndexed = time.Now()
}

// ToSwiftAccount creates annotated swift.Account data object for event publishing
func (f *AccountDBfile) ToSwiftAccount() swift.Account {
	a := swift.Account{
		Mtime:          f.Mtime,
		Path:           f.Path,
		SizeKB:         int64(f.Size / 1024),
		Account:        f.account,
		Status:         f.status,
		ContainerCount: f.container_count,
		ObjectCount:    f.object_count,
		BytesUsedMB:    int64(f.bytes_used / 1024 / 1024),
		LastIndexed:    f.LastIndexed,
		// fields inherited from parents
		ResourceType: f.Type,
		Partition:    f.PartId,
		Device:       f.DevName,
		Ip:           f.Ip,
		RingMtime:    f.RingMtime,
		Handoff:      f.Handoff,
		ReplicaId:    f.ReplicaId,
		PeerDevices:  strings.Join(f.PeerDevices, ","),
		PeerIps:      strings.Join(f.PeerIps, ","),
	}
	return a
}

func (f *AccountDBfile) ToEvent() input.Event {
	return input.NewAccountEvent(f.ToSwiftAccount())
}

func (f *AccountDBfile) Mtime() time.Time {
	return f.Mtime
}
