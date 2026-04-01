package main

import (
	"fmt"
	"os"
)

const usage = `LolaDB — embedded relational database engine

Usage:
  loladb create <path>            Create a new database
  loladb info <path>              Display database metadata
  loladb cli <path>               Open an interactive SQL shell
  loladb exec <path> "<sql>"      Execute a SQL statement
  loladb serve <path> [addr] [--tls-cert FILE --tls-key FILE] [--no-tls] [--no-auth]
                                  Start PostgreSQL wire protocol server (default: :5432)
  loladb tui <path>               Open a terminal UI shell
  loladb <path> < file.sql        Import SQL from stdin
  loladb help                     Show this help message
`

func main() {
	if len(os.Args) < 2 {
		fmt.Print(usage)
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "create":
		if len(args) < 1 {
			fatal("Usage: loladb create <path>")
		}
		runCreate(args[0])
	case "info":
		if len(args) < 1 {
			fatal("Usage: loladb info <path>")
		}
		runInfo(args[0])
	case "cli":
		if len(args) < 1 {
			fatal("Usage: loladb cli <path>")
		}
		runCLI(args[0])
	case "exec":
		if len(args) < 2 {
			fatal("Usage: loladb exec <path> \"<sql>\" [--role=<user>]")
		}
		runExec(args[0], args[1], args[2:]...)
	case "serve":
		if len(args) < 1 {
			fatal("Usage: loladb serve <path> [addr] [--tls-cert FILE --tls-key FILE] [--no-tls] [--no-auth]")
		}
		runServe(args)
	case "tui":
		if len(args) < 1 {
			fatal("Usage: loladb tui <path>")
		}
		runTUI(args[0])
	case "help", "-h", "--help":
		fmt.Print(usage)
	default:
		// If the argument looks like a database path (not a known
		// subcommand), check if stdin is piped and import SQL from it.
		if stdinIsPiped() {
			runImport(cmd)
		} else {
			fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", cmd)
			fmt.Print(usage)
			os.Exit(1)
		}
	}
}

func stdinIsPiped() bool {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) == 0
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
