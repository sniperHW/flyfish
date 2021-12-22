package membership

import (
	"encoding/json"
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/types"
	"github.com/sniperHW/flyfish/pkg/etcd/raft/raftpb"
	"go.uber.org/zap"
	"sort"
	"sync"
)

const MaxLearners = 1

type ConfChangeContext struct {
	Index          uint64
	ConfChangeType raftpb.ConfChangeType
	IsPromote      bool
	Url            string //for add
	NodeID         uint64
}

type MemberShipJson struct {
	LocalID types.ID
	Cid     types.ID
	Members map[types.ID]*Member
}

type MemberShip struct {
	sync.Mutex  // guards the fields below
	lg          *zap.Logger
	localID     types.ID
	cid         types.ID
	members     map[types.ID]*Member //未经raft确认的members
	raftmembers map[types.ID]*Member //从raft中获取到的members
	st          Storage
}

func NewMemberShip(lg *zap.Logger, localID, cid types.ID) *MemberShip {
	return &MemberShip{
		lg:          lg,
		localID:     localID,
		cid:         cid,
		members:     make(map[types.ID]*Member),
		raftmembers: make(map[types.ID]*Member),
	}
}

func NewMemberShipFromJson(lg *zap.Logger, b []byte) (*MemberShip, error) {
	var m MemberShipJson
	if err := json.Unmarshal(b, &m); nil != err {
		return nil, err
	} else {
		return &MemberShip{
			lg:          lg,
			localID:     m.LocalID,
			cid:         m.Cid,
			members:     m.Members,
			raftmembers: make(map[types.ID]*Member),
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

func (c *MemberShip) RaftMembers() []*Member {
	c.Lock()
	defer c.Unlock()
	var ms MembersByID
	for _, m := range c.raftmembers {
		ms = append(ms, m.Clone())
	}
	sort.Sort(ms)
	return []*Member(ms)
}

func (c *MemberShip) RaftMember(id types.ID) *Member {
	c.Lock()
	defer c.Unlock()
	return c.raftmembers[id].Clone()
}

func (c *MemberShip) VotingMembers() []*Member {
	c.Lock()
	defer c.Unlock()
	var ms MembersByID
	for _, m := range c.raftmembers {
		if !m.IsLearner {
			ms = append(ms, m.Clone())
		}
	}
	sort.Sort(ms)
	return []*Member(ms)
}

// IsLocalMemberLearner returns if the local member is raft learner
func (c *MemberShip) IsLocalMemberLearner() bool {
	c.Lock()
	defer c.Unlock()
	localMember, ok := c.raftmembers[c.localID]
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
func (c *MemberShip) IsRaftMemberExist(id types.ID) bool {
	c.Lock()
	defer c.Unlock()
	_, ok := c.raftmembers[id]
	return ok
}

// VotingMemberIDs returns the ID of voting members in cluster.
func (c *MemberShip) VotingRaftMemberIDs() []types.ID {
	c.Lock()
	defer c.Unlock()
	var ids []types.ID
	for _, m := range c.raftmembers {
		if !m.IsLearner {
			ids = append(ids, m.ID)
		}
	}
	sort.Sort(types.IDSlice(ids))
	return ids
}

func (c *MemberShip) save() error {
	if nil != c.st {
		return c.st.SaveMemberShip(c.lg, c.toJson())
	} else {
		return nil
	}
}

func (c *MemberShip) Save() error {
	c.Lock()
	defer c.Unlock()
	return c.save()
}

// AddMember adds a new Member into the cluster, and saves the given member's
// raftAttributes into the store. The given member should have empty attributes.
// A Member with a matching id must not exist.
func (c *MemberShip) AddRaftMember(id types.ID, isLearner bool, m *Member) {
	c.Lock()
	defer c.Unlock()

	rm := c.raftmembers[id]
	if nil == rm {
		if nil == m {
			m = c.members[id]
		}

		if nil == m {
			c.lg.Panic("AddRaftMember error")
		}
	}

	if isLearner != m.IsLearner {
		c.lg.Panic("isLearner missmatch")
	}

	c.raftmembers[id] = m
	c.members[id] = m

	if c.st != nil {
		if err := c.save(); nil != err {
			c.lg.Panic(
				"failed to save membership",
				zap.Error(err),
			)
		}
	}

	c.lg.Info(
		"added raft member",
		zap.String("cluster-id", c.cid.String()),
		zap.String("local-member-id", c.localID.String()),
		zap.String("added-peer-id", m.ID.String()),
		zap.Strings("added-peer-peer-urls", m.PeerURLs),
	)
}

// RemoveMember removes a member from the store.
// The given id MUST exist, or the function panics.
func (c *MemberShip) RemoveRaftMember(id types.ID) {
	c.Lock()
	defer c.Unlock()

	m, ok := c.raftmembers[id]
	if !ok {
		c.lg.Panic("RemoveRaftMember error")
	}

	delete(c.raftmembers, id)
	delete(c.members, id)

	if c.st != nil {
		if err := c.save(); nil != err {
			c.lg.Panic(
				"failed to save membership",
				zap.Error(err),
			)
		}
	}

	c.lg.Info(
		"removed raft member",
		zap.String("cluster-id", c.cid.String()),
		zap.String("local-member-id", c.localID.String()),
		zap.String("removed-remote-peer-id", id.String()),
		zap.Strings("removed-remote-peer-urls", m.PeerURLs),
	)
}

// PromoteMember marks the member's IsLearner RaftAttributes to false.
func (c *MemberShip) PromoteRaftMember(id types.ID) {
	c.Lock()
	defer c.Unlock()

	m, ok := c.raftmembers[id]
	if !ok {
		c.lg.Panic("PromoteRaftMember error")
	}

	m.IsLearner = false

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

func (c *MemberShip) ValidateConfigurationChange(cc *ConfChangeContext) error {
	c.Lock()
	defer c.Unlock()

	id := types.ID(cc.NodeID)

	members := c.raftmembers

	switch cc.ConfChangeType {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		if cc.IsPromote { // promoting a learner member to voting member
			if members[id] == nil {
				return ErrIDNotFound
			}
			if !members[id].IsLearner {
				return ErrMemberNotLearner
			}
		} else { // adding a new member
			if members[id] != nil {
				return ErrIDExists
			}

			urls := make(map[string]bool)
			for _, m := range members {
				for _, u := range m.PeerURLs {
					urls[u] = true
				}
			}

			if urls[cc.Url] {
				return ErrPeerURLexists
			}

			if cc.ConfChangeType == raftpb.ConfChangeAddLearnerNode { // the new member is a learner
				numLearners := 0
				for _, m := range members {
					if m.IsLearner {
						numLearners++
					}
				}
				if numLearners+1 > MaxLearners {
					return ErrTooManyLearners
				}
			}
		}
	case raftpb.ConfChangeRemoveNode:
		if members[id] == nil {
			return ErrIDNotFound
		}

	case raftpb.ConfChangeUpdateNode:
		if members[id] == nil {
			return ErrIDNotFound
		}
		urls := make(map[string]bool)
		for _, m := range members {
			if m.ID == id {
				continue
			}
			for _, u := range m.PeerURLs {
				urls[u] = true
			}
		}

		if urls[cc.Url] {
			return ErrPeerURLexists
		}

	default:
		c.lg.Panic("unknown ConfChange type", zap.String("type", cc.ConfChangeType.String()))
	}
	return nil
}
