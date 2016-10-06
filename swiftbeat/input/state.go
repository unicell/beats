package input

import (
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/swiftbeat/input/swift"
)

type PartitionState struct {
	LastIndexed   time.Time
	LastMtime     time.Time
	LastRingMtime time.Time
}

func NewPartitionState(part *swift.Partition) *PartitionState {
	return &PartitionState{
		LastIndexed:   part.IndexedAt,
		LastMtime:     part.Mtime,
		LastRingMtime: part.RingMtime,
	}
}

func (ps *PartitionState) Copy() *PartitionState {
	newPartState := &PartitionState{
		LastIndexed:   ps.LastIndexed,
		LastMtime:     ps.LastMtime,
		LastRingMtime: ps.LastRingMtime,
	}
	return newPartState
}

func (ps *PartitionState) update(part *swift.Partition) {
	ps.LastIndexed = part.IndexedAt
	ps.LastMtime = part.Mtime
	ps.LastRingMtime = part.RingMtime
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

// helper function to return resource state based on resource name string
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

func (s *States) findPrevious(ev Event) *PartitionState {
	part := ev.ToPartition()
	if diskState, ok := s.states[part.Device]; ok {
		resType := ev.ResourceType()
		resState := diskState.getResourceState(resType)

		partId := strconv.FormatInt(part.PartId, 10)
		if partState, ok := resState[partId]; ok {
			return partState
		} else {
			return nil
		}
	} else {
		return nil
	}
}

func (s *States) FindPrevious(ev Event) *PartitionState {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.findPrevious(ev)
}

// helper function to determine whether need to update partition state
func isNewerThanPartState(partState *PartitionState, part *swift.Partition) bool {

	if part.RingMtime.Unix() > partState.LastRingMtime.Unix() {
		// side effect to purge old state happens separately
		return true
	}

	// new event if happens later than last recorded
	if part.Mtime.Unix() > partState.LastMtime.Unix() {
		return true
	} else if part.Mtime.Unix() == partState.LastMtime.Unix() {
		if part.IndexedAt.Unix() == partState.LastIndexed.Unix() {
			// allow same timestamp event if happens within the scan loop
			return true
		} else if part.IndexedAt.Unix() > partState.LastIndexed.Unix() {
			// new scan loop with the same data
			return false
		} else {
			logp.Critical("lastindexed state going backward - dev %s part %d event %s persisted %s",
				part.Device, part.PartId, part.IndexedAt, partState.LastIndexed)
		}
	} else {
		return false
	}

	return false
}

func (s *States) IsNewEvent(ev Event) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	part := ev.ToPartition()
	partState := s.findPrevious(ev)

	if partState != nil {
		return isNewerThanPartState(partState, part)
	} else {
		return true
	}
}

// Update updates a state. If previous state didn't exist, new one is created
func (s *States) Update(ev Event) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	part := ev.ToPartition()
	//logp.Debug("hack", "11--> : %s - %s", ev.ToMapStr()["path"], part.Mtime)
	partState := s.findPrevious(ev)

	// partition state found
	if partState != nil {
		if isNewerThanPartState(partState, part) {
			logp.Debug("state", "dev %s part %d", part.Device, part.PartId)
			logp.Debug("state", "updated from: %s", partState.LastMtime)
			partState.update(part)
			logp.Debug("state", "          to: %s", partState.LastMtime)
			return nil
		} else {
			logp.Critical("state going backward - dev %s part %d\n"+
				"                     event %s, %s, %s\n"+
				"                 persisted %s, %s, %s",
				part.Device, part.PartId,
				part.Mtime, part.IndexedAt, part.RingMtime,
				partState.LastMtime, partState.LastIndexed, partState.LastRingMtime)
			return errors.New("state update: event arrives out of order")
		}
	} else {
		resType := ev.ResourceType()
		partId := strconv.FormatInt(part.PartId, 10)

		// insert new disk state
		diskState, found := s.states[part.Device]
		if !found {
			diskState := NewDiskState()
			resState := diskState.getResourceState(resType)

			partState := NewPartitionState(part)
			resState[partId] = partState

			s.states[part.Device] = diskState
			return nil
		}

		// disk state exists, inserting new partition state
		resState := diskState.getResourceState(resType)
		if _, found := resState[partId]; !found {
			partState := NewPartitionState(part)
			resState[partId] = partState
			return nil
		}

		logp.Critical("Incoming partition %s - %s event missed in findPrevious?",
			part.Mtime.Unix(), part.Mtime,
		)
	}

	return errors.New("state update: unknown")
}

// Count returns number of states
func (s *States) Count() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return len(s.states)
}

// Returns a depcopy of the swift states
func (s *States) GetStatesCopy() map[string]*DiskState {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	newStates := map[string]*DiskState{}
	for k, v := range s.states {
		newAState := map[string]*PartitionState{}
		for ak, av := range v.AccountState {
			newAState[ak] = av.Copy()
		}

		newCState := map[string]*PartitionState{}
		for ck, cv := range v.ContainerState {
			newCState[ck] = cv.Copy()
		}

		newOState := map[string]*PartitionState{}
		for ok, ov := range v.ObjectState {
			newOState[ok] = ov.Copy()
		}

		newDiskState := &DiskState{
			AccountState:   newAState,
			ContainerState: newCState,
			ObjectState:    newOState,
		}
		newStates[k] = newDiskState
	}

	return newStates
}

// SetStates overwrites all internal states with the given states dictionary
func (s *States) SetStates(states map[string]*DiskState) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.states = states
}

// Copy create a new copy of the states object
func (s *States) Copy() *States {
	st := NewStates()
	st.states = s.GetStatesCopy()
	return st
}

func dumpResourceStates(rs map[string]*PartitionState) {
	logp.Debug("state", "dump stat:")
	for k, v := range rs {
		logp.Debug("state", "        key: %s", k)
		logp.Debug("state", "         val: %s", v)
	}
}

func dumpDiskStates(states map[string]*DiskState) {
	for disk, diskState := range states {
		logp.Debug("state", "dump disk: %s", disk)
		dumpResourceStates(diskState.AccountState)
		dumpResourceStates(diskState.ContainerState)
		dumpResourceStates(diskState.ObjectState)
	}
}

func DumpStates(states map[string]*DiskState) {
	dumpDiskStates(states)
}
