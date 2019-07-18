package hutils

import "time"

var (
	DefTimeUtil = NewTimeUtil()
)

// 时间工具，在测试中，很多时候需要控制 time.Now() 产生的时间
// 例如：设置当前时间为 3天 后，用正常时间的 1/10 行进时间
type TimeUtil struct {
	now     time.Time
	isDebug bool
}

func NewTimeUtil() *TimeUtil {
	return &TimeUtil{now: time.Now()}
}

func (t *TimeUtil) SetDebug(isDebug bool) {
	t.isDebug = isDebug
}

func (t *TimeUtil) Now() time.Time {
	if t.isDebug {
		return t.now
	} else {
		return time.Now()
	}
}

func (t *TimeUtil) SetNow(now time.Time) {
	t.now = now
}

func (t *TimeUtil) Add(duration time.Duration) {
	t.now = t.now.Add(duration)
}

func Now() time.Time {
	return DefTimeUtil.Now()
}
