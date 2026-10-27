package tui

import "unicode/utf8"

// Decoder accepts split UTF-8 and split terminal escape sequences. An idle call
// with flushEscape=true treats a standalone Escape as a key.
type Decoder struct{ pending []byte }

func (d *Decoder) Feed(data []byte, flushEscape bool) []string {
	d.pending = append(d.pending, data...)
	var keys []string
	for len(d.pending) > 0 {
		b := d.pending[0]
		if b == 27 {
			if len(d.pending) == 1 {
				if !flushEscape {
					break
				}
				keys = append(keys, "esc")
				d.pending = d.pending[1:]
				continue
			}
			if d.pending[1] != '[' && d.pending[1] != 'O' {
				keys = append(keys, "esc")
				d.pending = d.pending[1:]
				continue
			}
			end := 2
			for end < len(d.pending) && !(d.pending[end] >= 0x40 && d.pending[end] <= 0x7e) {
				end++
			}
			if end >= len(d.pending) {
				if !flushEscape && len(d.pending) < 32 {
					break
				}
				d.pending = nil
				keys = append(keys, "esc")
				continue
			}
			sequence := string(d.pending[2 : end+1])
			key := ""
			switch sequence {
			case "A":
				key = "up"
			case "B":
				key = "down"
			case "C":
				key = "right"
			case "D":
				key = "left"
			case "H", "1~", "7~":
				key = "home"
			case "F", "4~", "8~":
				key = "end"
			case "5~":
				key = "pgup"
			case "6~":
				key = "pgdown"
			}
			d.pending = d.pending[end+1:]
			if key != "" {
				keys = append(keys, key)
			}
			continue
		}
		if !utf8.FullRune(d.pending) {
			break
		}
		r, n := utf8.DecodeRune(d.pending)
		d.pending = d.pending[n:]
		switch r {
		case 3:
			keys = append(keys, "ctrlc")
		case 7:
			keys = append(keys, "ctrlg")
		case 21:
			keys = append(keys, "ctrlu")
		case 23:
			keys = append(keys, "ctrlw")
		case 9:
			keys = append(keys, "tab")
		case 10, 13:
			keys = append(keys, "enter")
		case 8, 127:
			keys = append(keys, "backspace")
		default:
			if r >= 32 && r != utf8.RuneError {
				keys = append(keys, string(r))
			}
		}
	}
	return keys
}
