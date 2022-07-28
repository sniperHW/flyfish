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
	ClientUrl      string
	NodeID         uint64
	ProcessID      uint16
}

type MemberShipJson struct {
	LocalID types.ID
	Cid     types.ID
	Members map[types.ID]*Member
}

type MemberShip struct {
	sync.Mutex // guards the fields below
	lg         *zap.Logger
	localID    types.ID
	cid        types.ID
	members    map[types.ID]*Member
}

func NewMemberShip(lg *zap.Logger, localID, cid types.ID) *MemberShip {
	return &MemberShip{
		lg:      lg,
		localID: localID,
		cid:     cid,
		members: make(map[types.ID]*Member),
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

func (c *MemberShip) RecoverFromJson(b []byte) error {
	mb, err := NewMemberShipFromJson(c.lg, b)
	if nil != err {
		return err
	} else {
		c.Lock()
		defer c.Unlock()
		c.members = mb.members
		return nil
	}
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

func (c *MemberShip) AddMember(id types.ID, isLearner bool, m *Member) *Member {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.members[id]; ok {
		c.lg.Panic("AddRaftMember error")
	}

	c.members[id] = m

	c.lg.Debug(
		"added raft member",
		zap.String("cluster-id", c.cid.String()),
		zap.String("local-member-id", c.localID.String()),
		zap.String("added-peer-id", m.ID.String()),
		zap.Strings("added-peer-peer-urls", m.PeerURLs),
	)

	return m
}

// RemoveMember removes a member from the store.
// The given id MUST exist, or the function panics.
func (c *MemberShip) RemoveMember(id types.ID) {
	c.Lock()
	defer c.Unlock()

	m, ok := c.members[id]
	if !ok {
		c.lg.Panic("RemoveRaftMember error")
	}

	delete(c.members, id)

	c.lg.Debug(
		"removed raft member",
		zap.String("cluster-id", c.cid.String()),
		zap.String("local-member-id", c.localID.String()),
		zap.String("removed-remote-peer-id", id.String()),
		zap.Strings("removed-remote-peer-urls", m.PeerURLs),
	)
}

func (c *MemberShip) UpdateURL(id types.ID, PeerURLs []string, ClientURLs []string) {
	c.Lock()
	defer c.Unlock()
	m, ok := c.members[id]
	if !ok {
		c.lg.Panic("UpdateUrl error")
	}

	m.PeerURLs = PeerURLs
	m.ClientURLs = ClientURLs
}

// PromoteMember marks the member's IsLearner RaftAttributes to false.
func (c *MemberShip) PromoteMember(id types.ID) {
	c.Lock()
	defer c.Unlock()

	m, ok := c.members[id]
	if !ok {
		c.lg.Panic("PromoteRaftMember error")
	}

	m.IsLearner = false

	c.lg.Debug(
		"promote member",
		zap.String("cluster-id", c.cid.String()),
		zap.String("local-member-id", c.localID.String()),
	)
}

func (c *MemberShip) ValidateConfigurationChange(cc *ConfChangeContext) error {
	c.Lock()
	defer c.Unlock()

	id := types.ID(cc.NodeID)

	members := c.members

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
			processIDs := make(map[uint16]bool)
			for _, m := range members {
				for _, u := range m.PeerURLs {
					urls[u] = true
				}
				processIDs[m.ProcessID] = true
			}

			if processIDs[cc.ProcessID] {
				return ErrProcessIDexists
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
