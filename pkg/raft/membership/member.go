package membership

import (
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/types"
)

type Member struct {
	ID        types.ID `json:"id"`
	Name      string   `json:"Name"`
	PeerURLs  []string `json:"peerURLs"`
	IsLearner bool     `json:"isLearner,omitempty"`
}

func NewMember(name string, id types.ID, peerURLs types.URLs) *Member {
	return newMember(name, id, peerURLs, false)
}

func NewMemberAsLearner(name string, id types.ID, peerURLs types.URLs) *Member {
	return newMember(name, id, peerURLs, true)
}

func newMember(name string, id types.ID, peerURLs types.URLs, isLearner bool) *Member {
	return &Member{
		Name:      name,
		ID:        id,
		PeerURLs:  peerURLs.StringSlice(),
		IsLearner: isLearner,
	}
}

func (m *Member) Clone() *Member {
	if m == nil {
		return nil
	}
	mm := &Member{
		ID:        m.ID,
		IsLearner: m.IsLearner,
		Name:      m.Name,
	}
	if m.PeerURLs != nil {
		mm.PeerURLs = make([]string, len(m.PeerURLs))
		copy(mm.PeerURLs, m.PeerURLs)
	}
	return mm
}

// MembersByID implements sort by ID interface
type MembersByID []*Member

func (ms MembersByID) Len() int           { return len(ms) }
func (ms MembersByID) Less(i, j int) bool { return ms[i].ID < ms[j].ID }
func (ms MembersByID) Swap(i, j int)      { ms[i], ms[j] = ms[j], ms[i] }

// MembersByPeerURLs implements sort by peer urls interface
type MembersByPeerURLs []*Member

func (ms MembersByPeerURLs) Len() int { return len(ms) }
func (ms MembersByPeerURLs) Less(i, j int) bool {
	return ms[i].PeerURLs[0] < ms[j].PeerURLs[0]
}
func (ms MembersByPeerURLs) Swap(i, j int) { ms[i], ms[j] = ms[j], ms[i] }
