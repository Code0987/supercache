package main

import (
	"fmt"
	"os"
	"strings"
)

const (
	ansiReset  = "\033[0m"
	ansiBold   = "\033[1m"
	ansiDim    = "\033[2m"
	ansiRed    = "\033[31m"
	ansiGreen  = "\033[32m"
	ansiYellow = "\033[33m"
	ansiCyan   = "\033[36m"
)

// disableColor is set when -json is on. NO_COLOR / non-TTY also suppress color.
var disableColor bool

func applyJSONColor(jsonOut bool) {
	disableColor = jsonOut
}

func colorOn(f *os.File) bool {
	if disableColor {
		return false
	}
	if os.Getenv("NO_COLOR") != "" || os.Getenv("TERM") == "dumb" {
		return false
	}
	st, err := f.Stat()
	if err != nil {
		return false
	}
	return st.Mode()&os.ModeCharDevice != 0
}

func paint(f *os.File, code, s string) string {
	if !colorOn(f) {
		return s
	}
	return code + s + ansiReset
}

func printOK(msg string) {
	fmt.Println(paint(os.Stdout, ansiGreen+ansiBold, "OK") + " " + msg)
}

func printNil() {
	fmt.Println(paint(os.Stdout, ansiDim, "(nil)"))
}

func printBool(v bool) {
	if v {
		fmt.Println(paint(os.Stdout, ansiGreen, "true"))
		return
	}
	fmt.Println(paint(os.Stdout, ansiDim, "false"))
}

func printCmdErr(cmd string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", paint(os.Stderr, ansiRed+ansiBold, cmd), err)
}

func printCmdMsg(cmd, msg string) {
	fmt.Fprintf(os.Stderr, "%s: %s\n", paint(os.Stderr, ansiRed+ansiBold, cmd), msg)
}

func printUnknown(cmd string) {
	fmt.Fprintf(os.Stderr, "%s: unknown command %q\n", paint(os.Stderr, ansiRed+ansiBold, "sc"), cmd)
}

func printUsageLine(line string) {
	if !strings.HasPrefix(line, "usage:") {
		fmt.Fprintln(os.Stderr, line)
		return
	}
	rest := strings.TrimPrefix(line, "usage:")
	fmt.Fprintln(os.Stderr, paint(os.Stderr, ansiYellow+ansiBold, "usage:")+rest)
}

func printWarn(msg string) {
	fmt.Fprintln(os.Stderr, paint(os.Stderr, ansiYellow+ansiBold, "warning:")+" "+msg)
}

func printNote(msg string) {
	fmt.Fprintln(os.Stderr, paint(os.Stderr, ansiDim, "# "+msg))
}

func colorPrompt(ks, addr string) string {
	return paint(os.Stdout, ansiCyan, "sc") + " " +
		paint(os.Stdout, ansiBold, ks) + paint(os.Stdout, ansiDim, "@"+addr) + "> "
}
