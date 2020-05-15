package kvpd

//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

import (
	"fmt"
	"github.com/sniperHW/flyfish/net"
	"math/rand"
	"testing"
)

func TestNodeConf(t *testing.T) {

	nMgr := (nodeConfMgr)(map[int]*nodeConf{})
	rMgr := regionMgr{
		regions:      map[int]*regionInfo{},
		slotToRegion: map[int]*regionInfo{},
	}

	//添加3个region
	rMgr.addRegion(1, []int{1, 2, 3})
	rMgr.addRegion(2, []int{4, 5, 6})
	rMgr.addRegion(3, []int{7, 8, 9})

	nMgr.addNodeConf(1, "127.0.0.1", []int{1, 2, 3}, &rMgr)
	nMgr.addNodeConf(2, "127.0.0.1", []int{1, 2, 3}, &rMgr)
	nMgr.addNodeConf(3, "127.0.0.1", []int{1, 2, 3}, &rMgr)

	info1, _ := nMgr.getInfoByID(&rMgr, 1)
	info2, _ := nMgr.getInfoByID(&rMgr, 2)
	info3, _ := nMgr.getInfoByID(&rMgr, 3)

	info1.Show()
	info2.Show()
	info3.Show()

	r := rMgr.GetSlotRegion(4)
	r.Show()

}

func TestBitmap(t *testing.T) {
	b := NewBitmap(32)
	fmt.Println(b.ShowBit())
	b.Set(8)
	b.Set(16)
	b.Set(24)
	b.Set(32)
	fmt.Println(b.ShowBit())
	b.Clear(24)
	fmt.Println(b.ShowBit())
	b.Set(1)
	b.Set(9)
	b.Set(17)
	b.Set(25)
	fmt.Println(b.ShowBit())

	b2 := NewBitmapFromBytes(b.Bytes())
	b.Set(24)
	fmt.Println(b.ShowBit())
	fmt.Println(b2.ShowBit())

	compressor := net.ZipCompressor{}
	b3 := NewBitmap(65536)
	c, _ := compressor.Compress(b3.Bytes())
	fmt.Println(len(c), len(b3.Bytes()))
	for i := 1; i <= 65536; i++ {
		b3.Set(i)
	}
	c, _ = compressor.Compress(b3.Bytes())
	fmt.Println(len(c), len(b3.Bytes()))

	b4 := NewBitmap(65536)
	for i := 0; i < 655; i++ {
		j := rand.Int()%65536 + 1
		b4.Set(j)
	}
	c, _ = compressor.Compress(b4.Bytes())
	fmt.Println(len(c), len(b4.Bytes()))
}
