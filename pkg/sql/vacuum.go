package sql

import (
	"fmt"
	"strings"

	"github.com/gololadb/gopgsql/parser"
)

// execVacuumAnalyze handles VACUUM, VACUUM FULL, ANALYZE, and VACUUM ANALYZE.
func (ex *Executor) execVacuumAnalyze(vs *parser.VacuumStmt) (*Result, error) {
	// Parse options to determine what to do.
	doVacuum := vs.IsVacuum
	doAnalyze := !vs.IsVacuum // standalone ANALYZE
	isFull := false
	isVerbose := false

	for _, opt := range vs.Options {
		switch strings.ToLower(opt.Defname) {
		case "analyze", "analyse":
			doAnalyze = true
		case "full":
			isFull = true
		case "verbose":
			isVerbose = true
		case "freeze":
			// Accepted but no-op (we don't have tuple freezing).
		}
	}
	_ = isVerbose // reserved for future verbose output

	// Determine target tables.
	var tableNames []string
	if len(vs.Relations) > 0 {
		for _, rv := range vs.Relations {
			tableNames = append(tableNames, rv.Relname)
		}
	} else {
		// No tables specified — operate on all user tables.
		tables, err := ex.Cat.ListTables()
		if err != nil {
			return nil, fmt.Errorf("VACUUM: %w", err)
		}
		for _, t := range tables {
			tableNames = append(tableNames, t.Name)
		}
	}

	if doVacuum {
		return ex.execVacuum(tableNames, doAnalyze, isFull)
	}
	return ex.execAnalyze(tableNames)
}

// execVacuum runs VACUUM (and optionally ANALYZE) on the given tables.
func (ex *Executor) execVacuum(tableNames []string, doAnalyze, isFull bool) (*Result, error) {
	totalRemoved := 0

	for _, name := range tableNames {
		result, err := ex.Cat.Vacuum(name)
		if err != nil {
			return nil, fmt.Errorf("VACUUM %s: %w", name, err)
		}
		totalRemoved += result.TuplesRemoved

		// VACUUM FULL: run a second pass. The engine already compacts
		// pages and frees empty ones, so a second vacuum after the first
		// ensures maximum space reclamation.
		if isFull {
			result2, err := ex.Cat.Vacuum(name)
			if err != nil {
				return nil, fmt.Errorf("VACUUM FULL %s: %w", name, err)
			}
			totalRemoved += result2.TuplesRemoved
		}
	}

	// Run ANALYZE if requested (VACUUM ANALYZE).
	if doAnalyze {
		for _, name := range tableNames {
			if _, err := ex.Cat.Stats(name); err != nil {
				return nil, fmt.Errorf("ANALYZE %s: %w", name, err)
			}
		}
	}

	return &Result{Message: "VACUUM"}, nil
}

// execAnalyze runs ANALYZE on the given tables, refreshing statistics.
func (ex *Executor) execAnalyze(tableNames []string) (*Result, error) {
	for _, name := range tableNames {
		if _, err := ex.Cat.Stats(name); err != nil {
			return nil, fmt.Errorf("ANALYZE %s: %w", name, err)
		}
	}
	return &Result{Message: "ANALYZE"}, nil
}
