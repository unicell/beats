package input

import (
	"strconv"
	"sync"
	"time"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/swiftbeat/input/swift"
)

type HashState struct {
	lastSynced     time.Time
	datafileStates []string
}

type SuffixState struct {
	lastSynced time.Time
	hashKeys   []string
	hashStates map[string]HashState
}

type PartitionState struct {
	LastSynced time.Time
	LastMtime  time.Time
	RingMtime  time.Time
}

func NewPartitionState(part *swift.Partition) *PartitionState {
	return &PartitionState{
		LastSynced: part.LastIndexed,
		LastMtime:  part.Mtime,
		RingMtime:  part.RingMtime,
	}
}

func (ps *PartitionState) update(part *swift.Partition) {
	ps.LastSynced = part.LastIndexed
	ps.LastMtime = part.Mtime
	ps.RingMtime = part.RingMtime
}

// States represent current tracked state for one disk
type DiskState struct {
	AccountState   map[string]*PartitionState `json:"account"`
	ContainerState map[string]*PartitionState `json:"container"`
	ObjectState    map[string]*PartitionState `json:"object"`
}

func NewDiskState() *DiskState {
	return &DiskState{
		AccountState:   map[string]*PartitionState{},
		ContainerState: map[string]*PartitionState{},
		ObjectState:    map[string]*PartitionState{},
	}
}

func (ds *DiskState) getResourceState(resType string) map[string]*PartitionState {
	switch resType {
	case "account":
		return ds.AccountState
	case "container":
		return ds.ContainerState
	case "object":
		return ds.ObjectState
	}
	return nil
}

type States struct {
	states map[string]*DiskState
	mutex  sync.Mutex
}

func NewStates() *States {
	return &States{
		states: map[string]*DiskState{},
	}
}

// Update updates a state. If previous state didn't exist, new one is created
func (s *States) Update(ev Event) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	var part *swift.Partition

	evStr := ev.ToMapStr()
	logp.Debug("hack", "11--> : %s", evStr["path"])

	resType := ev.ResourceType()
	switch resType {
	case "account":
		part = ev.(*AccountEvent).Account.Partition
	case "container":
		part = ev.(*ContainerEvent).Container.Partition
	case "object":
		part = ev.(*ObjectPartitionEvent).ObjPart.Partition
	}

	partId := strconv.FormatInt(part.PartId, 10)
	if diskState, ok := s.states[part.Device]; ok {
		// existing disk state
		resState := diskState.getResourceState(resType)
		if partState, ok := resState[partId]; ok {
			// existing partition state
			if part.Mtime.Unix() >= partState.LastMtime.Unix() {
				logp.Debug("state", "before updating: %s", partState.LastMtime)
				partState.update(part)
				logp.Debug("state", "part %s - persisted %s", part.Mtime, partState.LastMtime)
			} else {
				logp.Critical("Incoming partition %s - %s event happens before persisted %s - %s",
					part.Mtime.Unix(), part.Mtime,
					partState.LastMtime.Unix(), partState.LastMtime)
			}
		} else {
			// new partition state
			partState := NewPartitionState(part)
			resState[partId] = partState
		}
	} else {
		// new disk state
		diskState := NewDiskState()
		resState := diskState.getResourceState(resType)

		partState := NewPartitionState(part)
		resState[partId] = partState

		s.states[part.Device] = diskState
	}
}

// Returns a copy of the file states
func (s *States) GetStates() map[string]DiskState {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	newStates := map[string]DiskState{}
	for k, v := range s.states {
		newAState := map[string]*PartitionState{}
		for ak, av := range v.AccountState {
			newAState[ak] = av
		}

		newCState := map[string]*PartitionState{}
		for ck, cv := range v.ContainerState {
			newCState[ck] = cv
		}

		newOState := map[string]*PartitionState{}
		for ok, ov := range v.ObjectState {
			newAState[ok] = ov
		}

		newDiskState := DiskState{
			AccountState:   newAState,
			ContainerState: newCState,
			ObjectState:    newOState,
		}
		newStates[k] = newDiskState
	}

	return newStates
}
