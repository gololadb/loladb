package main

import (
	"fmt"
	"os"

	"github.com/jespino/loladb/pkg/catalog"
	"github.com/jespino/loladb/pkg/engine"
)

func runCreate(path string) {
	if _, err := os.Stat(path); err == nil {
		fatal(fmt.Sprintf("File already exists: %s", path))
	}

	eng, err := engine.Open(path, 0)
	if err != nil {
		fatal(fmt.Sprintf("Failed to create database: %v", err))
	}

	_, err = catalog.New(eng)
	if err != nil {
		eng.Close()
		fatal(fmt.Sprintf("Failed to initialize catalog: %v", err))
	}

	eng.Close()

	info, _ := os.Stat(path)
	fmt.Printf("Database created: %s (%d bytes)\n", path, info.Size())
}
