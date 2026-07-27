package main

import (
	"fmt"
	"strings"
	"unicode"
)

// tokenize splits a line into args with simple shell-like quoting.
// Supports "double" and 'single' quotes; \ escapes the next char inside doubles or outside.
func tokenize(line string) ([]string, error) {
	var args []string
	var cur strings.Builder
	inSingle, inDouble := false, false
	escaped := false
	started := false

	flush := func() {
		if started {
			args = append(args, cur.String())
			cur.Reset()
			started = false
		}
	}

	for i := 0; i < len(line); i++ {
		c := line[i]
		if escaped {
			cur.WriteByte(c)
			started = true
			escaped = false
			continue
		}
		if c == '\\' && !inSingle {
			escaped = true
			started = true
			continue
		}
		if inSingle {
			if c == '\'' {
				inSingle = false
			} else {
				cur.WriteByte(c)
				started = true
			}
			continue
		}
		if inDouble {
			if c == '"' {
				inDouble = false
			} else {
				cur.WriteByte(c)
				started = true
			}
			continue
		}
		switch c {
		case '\'':
			inSingle = true
			started = true
		case '"':
			inDouble = true
			started = true
		default:
			if unicode.IsSpace(rune(c)) {
				flush()
			} else {
				cur.WriteByte(c)
				started = true
			}
		}
	}
	if escaped {
		return nil, fmt.Errorf("trailing backslash")
	}
	if inSingle || inDouble {
		return nil, fmt.Errorf("unclosed quote")
	}
	flush()
	return args, nil
}
