package moment

import (
	"strconv"
	"strings"
)

// Parse parses a duration string and returns number of nano seconds
// durations can have 'ns', 'us', 'ms', 'min', 'hour', 'day' as suffix
func Parse(str string) (ns int64, err error) {
	switch {
	case strings.HasSuffix(str, "ns"):
		nstr := str[:len(str)-2]
		return strconv.ParseInt(nstr, 10, 0)
	case strings.HasSuffix(str, "us"):
		nstr := str[:len(str)-2]
		if ns, err = strconv.ParseInt(nstr, 10, 0); err != nil {
			return 0, err
		}

		return ns * 1e3, nil
	case strings.HasSuffix(str, "ms"):
		nstr := str[:len(str)-2]
		if ns, err = strconv.ParseInt(nstr, 10, 0); err != nil {
			return 0, err
		}

		return ns * 1e6, nil
	case strings.HasSuffix(str, "s"):
		nstr := str[:len(str)-1]
		if ns, err = strconv.ParseInt(nstr, 10, 0); err != nil {
			return 0, err
		}

		return ns * 1e9, nil
	case strings.HasSuffix(str, "min"):
		nstr := str[:len(str)-3]
		if ns, err = strconv.ParseInt(nstr, 10, 0); err != nil {
			return 0, err
		}

		return ns * 1e9 * 60, nil
	case strings.HasSuffix(str, "hour"):
		nstr := str[:len(str)-4]
		if ns, err = strconv.ParseInt(nstr, 10, 0); err != nil {
			return 0, err
		}

		return ns * 1e9 * 60 * 60, nil
	case strings.HasSuffix(str, "day"):
		nstr := str[:len(str)-3]
		if ns, err = strconv.ParseInt(nstr, 10, 0); err != nil {
			return 0, err
		}

		return ns * 1e9 * 60 * 60 * 24, nil
	default:
		return strconv.ParseInt(str, 10, 0)
	}
}
