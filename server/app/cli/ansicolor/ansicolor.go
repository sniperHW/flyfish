package ansicolor

import (
	"fmt"
)

const (
	Red          = 0
	Green        = 1
	Yellow       = 2
	Blue         = 3
	Magenta      = 4
	Cyan         = 5
	LightGray    = 6
	LightRed     = 7
	LightGreen   = 8
	LightYellow  = 9
	LightBlue    = 10
	LightMagenta = 11
	LightCyan    = 12
)

var colorPrefix []string = []string{
	"\x1b[0;31m",
	"\x1b[0;32m",
	"\x1b[0;33m",
	"\x1b[0;34m",
	"\x1b[0;35m",
	"\x1b[0;36m",
	"\x1b[0;37m",
	"\x1b[0;91m",
	"\x1b[0;92m",
	"\x1b[0;93m",
	"\x1b[0;94m",
	"\x1b[0;95m",
	"\x1b[0;96m",
}

func FillColor(color int, str string) string {
	if color < Red {
		color = Red
	} else if color > LightCyan {
		color = LightCyan
	}

	return fmt.Sprintf("%s%s\x1b[0m", colorPrefix[color], str)
}
