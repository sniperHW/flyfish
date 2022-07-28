package membership

import (
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/types"
)

type Member struct {
	ID        types.ID `json:"id"`
	ProcessID uint16   `json:"ProcessID"`
	PeerURL   string   `json:"peerURL"`
	ClientURL string   `json:"clientURL,omitempty"`
	IsLearner bool     `json:"isLearner,omitempty"`
}

func NewMember(processID uint16, id types.ID, peerURL string, clientURL string) *Member {
	return newMember(processID, id, peerURL, clientURL, false)
}

func NewMemberAsLearner(processID uint16, id types.ID, peerURL string, clientURL string) *Member {
	return newMember(processID, id, peerURL, clientURL, true)
}

func newMember(processID uint16, id types.ID, peerURL string, clientURL string, isLearner bool) *Member {
	return &Member{
		ProcessID: processID,
		ID:        id,
		PeerURL:   peerURL, //peerURLs.StringSlice(),
		IsLearner: isLearner,
		ClientURL: clientURL, //clientURLs.StringSlice(),
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
		PeerURL:   m.PeerURL,
		ClientURL: m.ClientURL,
	}

	/*if m.PeerURLs != nil {
		mm.PeerURLs = make([]string, len(m.PeerURLs))
		copy(mm.PeerURLs, m.PeerURLs)
	}

	if m.ClientURLs != nil {
		mm.ClientURLs = make([]string, len(m.ClientURLs))
		copy(mm.ClientURLs, m.ClientURLs)
	}*/

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
	return ms[i].PeerURL < ms[j].PeerURL
}
func (ms MembersByPeerURLs) Swap(i, j int) { ms[i], ms[j] = ms[j], ms[i] }
