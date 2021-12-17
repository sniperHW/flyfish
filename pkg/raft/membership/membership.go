package membership

import (
	"encoding/json"
	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/zap"
	"sort"
	"sync"
)

type MemberShip struct {
	sync.Mutex // guards the fields below
	lg         *zap.Logger
	localID    types.ID
	cid        types.ID
	members    map[types.ID]*Member
	// removed contains the ids of removed members in the cluster.
	// removed id cannot be reused.
	removed map[types.ID]bool
	st      Storage
}

type MemberShipJson struct {
	LocalID types.ID
	Cid     types.ID
	Members map[types.ID]*Member
	Removed map[types.ID]bool
}

// ConfigChangeContext represents a context for confChange.
type ConfigChangeContext struct {
	Member
	// IsPromote indicates if the config change is for promoting a learner member.
	// This flag is needed because both adding a new member and promoting a learner member
	// uses the same config change type 'ConfChangeAddNode'.
	IsPromote bool `json:"isPromote"`
}

func NewMemberShip(lg *zap.Logger, localID, cid types.ID) *MemberShip {
	return &MemberShip{
		lg:      lg,
		localID: localID,
		cid:     cid,
		members: make(map[types.ID]*Member),
		removed: make(map[types.ID]bool),
	}
}

func NewMemberShipFromJson(lg *zap.Logger, b []byte) (*MemberShip, error) {
	var m MemberShipJson
	if err := json.Unmarshal(b, &m); nil != err {
		return nil, err
	} else {
		return &MemberShip{
			lg:      lg,
			localID: m.LocalID,
			cid:     m.Cid,
			members: m.Members,
			removed: m.Removed,
		}, nil
	}
}

func NewMemberShipMembers(lg *zap.Logger, localID, cid types.ID, membs []*Member) *MemberShip {
	c := NewMemberShip(lg, localID, cid)
	for _, m := range membs {
		c.members[m.ID] = m
	}
	return c
}

func (c *MemberShip) SetStorage(st Storage) {
	c.st = st
}

func (c *MemberShip) toJson() (b []byte) {
	b, _ = json.Marshal(&MemberShipJson{
		LocalID: c.localID,
		Cid:     c.cid,
		Members: c.members,
		Removed: c.removed,
	})
	return
}

func (c *MemberShip) ToJson() []byte {
	c.Lock()
	defer c.Unlock()
	return c.toJson()
}

func (c *MemberShip) Members() []*Member {
	c.Lock()
	defer c.Unlock()
	var ms MembersByID
	for _, m := range c.members {
		ms = append(ms, m.Clone())
	}
	sort.Sort(ms)
	return []*Member(ms)
}

func (c *MemberShip) Member(id types.ID) *Member {
	c.Lock()
	defer c.Unlock()
	return c.members[id].Clone()
}

func (c *MemberShip) VotingMembers() []*Member {
	c.Lock()
	defer c.Unlock()
	var ms MembersByID
	for _, m := range c.members {
		if !m.IsLearner {
			ms = append(ms, m.Clone())
		}
	}
	sort.Sort(ms)
	return []*Member(ms)
}

func (c *MemberShip) IsIDRemoved(id types.ID) bool {
	c.Lock()
	defer c.Unlock()
	return c.removed[id]
}

// IsLocalMemberLearner returns if the local member is raft learner
func (c *MemberShip) IsLocalMemberLearner() bool {
	c.Lock()
	defer c.Unlock()
	localMember, ok := c.members[c.localID]
	if !ok {
		c.lg.Panic(
			"failed to find local ID in cluster members",
			zap.String("cluster-id", c.cid.String()),
			zap.String("local-member-id", c.localID.String()),
		)
	}
	return localMember.IsLearner
}

// IsMemberExist returns if the member with the given id exists in cluster.
func (c *MemberShip) IsMemberExist(id types.ID) bool {
	c.Lock()
	defer c.Unlock()
	_, ok := c.members[id]
	return ok
}

// VotingMemberIDs returns the ID of voting members in cluster.
func (c *MemberShip) VotingMemberIDs() []types.ID {
	c.Lock()
	defer c.Unlock()
	var ids []types.ID
	for _, m := range c.members {
		if !m.IsLearner {
			ids = append(ids, m.ID)
		}
	}
	sort.Sort(types.IDSlice(ids))
	return ids
}

func (c *MemberShip) save() error {
	return c.st.SaveMemberShip(c.lg, c.cid, c.localID, c.toJson())
}

func (c *MemberShip) Save() error {
	c.Lock()
	defer c.Unlock()
	return c.st.SaveMemberShip(c.lg, c.cid, c.localID, c.toJson())
}

// AddMember adds a new Member into the cluster, and saves the given member's
// raftAttributes into the store. The given member should have empty attributes.
// A Member with a matching id must not exist.
func (c *MemberShip) AddMember(m *Member) {
	c.Lock()
	defer c.Unlock()
	c.members[m.ID] = m

	if c.st != nil {
		if err := c.save(); nil != err {
			c.lg.Panic(
				"failed to save membership",
				zap.Error(err),
			)
		}
	}
	c.lg.Info(
		"added member",
		zap.String("cluster-id", c.cid.String()),
		zap.String("local-member-id", c.localID.String()),
		zap.String("added-peer-id", m.ID.String()),
		zap.Strings("added-peer-peer-urls", m.PeerURLs),
	)
}

// RemoveMember removes a member from the store.
// The given id MUST exist, or the function panics.
func (c *MemberShip) RemoveMember(id types.ID) {
	c.Lock()
	defer c.Unlock()

	m, ok := c.members[id]
	delete(c.members, id)
	c.removed[id] = true

	if c.st != nil {
		if err := c.save(); nil != err {
			c.lg.Panic(
				"failed to save membership",
				zap.Error(err),
			)
		}
	}

	if ok {
		c.lg.Info(
			"removed member",
			zap.String("cluster-id", c.cid.String()),
			zap.String("local-member-id", c.localID.String()),
			zap.String("removed-remote-peer-id", id.String()),
			zap.Strings("removed-remote-peer-urls", m.PeerURLs),
		)
	} else {
		c.lg.Warn(
			"skipped removing already removed member",
			zap.String("cluster-id", c.cid.String()),
			zap.String("local-member-id", c.localID.String()),
			zap.String("removed-remote-peer-id", id.String()),
		)
	}
}

// PromoteMember marks the member's IsLearner RaftAttributes to false.
func (c *MemberShip) PromoteMember(id types.ID) {
	c.Lock()
	defer c.Unlock()

	c.members[id].IsLearner = false

	if c.st != nil {
		if err := c.save(); nil != err {
			c.lg.Panic(
				"failed to save membership",
				zap.Error(err),
			)
		}
	}

	c.lg.Info(
		"promote member",
		zap.String("cluster-id", c.cid.String()),
		zap.String("local-member-id", c.localID.String()),
	)
}
