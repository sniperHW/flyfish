package slot

import (
	"github.com/sniperHW/flyfish/pkg/bitmap"
)

var SlotCount int = 16384

// 字符串转为16位整形值
func StringHash(s string) int {
	var hash uint16
	for _, c := range s {

		ch := uint16(c)

		hash = hash + ((hash) << 5) + ch + (ch << 7)
	}
	return int(hash)
}

func Unikey2Slot(unikey string) int {
	return StringHash(unikey) % SlotCount
}

func MakeStoreBitmap(stores []int) (b []*bitmap.Bitmap) {
	if len(stores) > 0 {
		slotPerStore := SlotCount / len(stores)
		for i, _ := range stores {
			storeBitmap := bitmap.New(SlotCount)
			j := i * slotPerStore
			for ; j < (i+1)*slotPerStore; j++ {
				storeBitmap.Set(j)
			}

			//不能正好平分，剩余的slot全部交给最后一个store
			if i == len(stores)-1 && j < SlotCount {
				for ; j < SlotCount; j++ {
					storeBitmap.Set(j)
				}
			}
			b = append(b, storeBitmap)
		}
	}
	return
}
