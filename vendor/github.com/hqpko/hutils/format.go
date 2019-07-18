package hutils

import "time"

func FormatTime(t time.Time) string {
	return t.Format("2006-01-02T15:04:05Z")
}
