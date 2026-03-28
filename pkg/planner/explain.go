package planner

import (
	"fmt"
	"strings"
)

// Explain formats a physical plan tree as a human-readable string.
func Explain(node PhysicalNode) string {
	var sb strings.Builder
	explainNode(&sb, node, 0)
	return sb.String()
}

func explainNode(sb *strings.Builder, node PhysicalNode, indent int) {
	prefix := strings.Repeat("  ", indent)
	cost := node.Cost()

	sb.WriteString(prefix)
	sb.WriteString(node.String())
	sb.WriteString(fmt.Sprintf("  (cost=%.2f..%.2f rows=%.0f width=%d)",
		cost.Startup, cost.Total, cost.Rows, cost.Width))
	sb.WriteString("\n")

	for _, child := range node.Children() {
		explainNode(sb, child, indent+1)
	}
}

// ExplainAnalyzeResult holds EXPLAIN ANALYZE output.
type ExplainAnalyzeResult struct {
	Plan           string
	PlanningTimeMs float64
	ExecTimeMs     float64
}
