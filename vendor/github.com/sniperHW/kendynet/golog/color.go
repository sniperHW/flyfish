package golog

type Color int

const (
	NoColor Color = iota
	Black
	Red
	Green
	Yellow
	Blue
	Purple
	DarkGreen
	White
)

var logColorPrefix = []string{
	"",
	"\x1b[030m",
	"\x1b[031m",
	"\x1b[032m",
	"\x1b[033m",
	"\x1b[034m",
	"\x1b[035m",
	"\x1b[036m",
	"\x1b[037m",
}

var logColorSuffix = "\x1b[0m"

var colorByName = map[string]Color{
	"none":      NoColor,
	"black":     Black,
	"red":       Red,
	"green":     Green,
	"yellow":    Yellow,
	"blue":      Blue,
	"purple":    Purple,
	"darkgreen": DarkGreen,
	"white":     White,
}

func ColorFromLevel(l Level) Color {
	switch l {
	case Level_Warn:
		return Yellow
	case Level_Debug:
		return Purple
	case Level_Error, Level_Fatal:
		return Red
	}

	return NoColor
}


