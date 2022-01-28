package membership

import (
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/types"
)

type Member struct {
	ID         types.ID `json:"id"`
	ProcessID  uint16   `json:"ProcessID"`
	PeerURLs   []string `json:"peerURLs"`
	ClientURLs []string `json:"clientURLs,omitempty"`
	IsLearner  bool     `json:"isLearner,omitempty"`
}

func NewMember(processID uint16, id types.ID, peerURLs types.URLs, clientURLs types.URLs) *Member {
	return newMember(processID, id, peerURLs, clientURLs, false)
}

func NewMemberAsLearner(processID uint16, id types.ID, peerURLs types.URLs, clientURLs types.URLs) *Member {
	return newMember(processID, id, peerURLs, clientURLs, true)
}

func newMember(processID uint16, id types.ID, peerURLs types.URLs, clientURLs types.URLs, isLearner bool) *Member {
	return &Member{
		ProcessID:  processID,
		ID:         id,
		PeerURLs:   peerURLs.StringSlice(),
		IsLearner:  isLearner,
		ClientURLs: clientURLs.StringSlice(),
	}
}

func (m *Member) Clone() *Member {
	if m == nil {
		return nil
	}
	mm := &Member{
		ID:        m.ID,
		IsLearner: m.IsLearner,
		ProcessID: m.ProcessID,
	}
	if m.PeerURLs != nil {
		mm.PeerURLs = make([]string, len(m.PeerURLs))
		copy(mm.PeerURLs, m.PeerURLs)
	}

	if m.ClientURLs != nil {
		mm.ClientURLs = make([]string, len(m.ClientURLs))
		copy(mm.ClientURLs, m.ClientURLs)
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
