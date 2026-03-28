package planner

// CostConstants holds the tunable cost parameters for the optimizer.
type CostConstants struct {
	SeqPageCost       float64
	RandomPageCost    float64
	CPUTupleCost      float64
	CPUIndexTupleCost float64
	CPUOperatorCost   float64
}

// DefaultCosts returns PostgreSQL-like default cost constants.
func DefaultCosts() CostConstants {
	return CostConstants{
		SeqPageCost:       1.0,
		RandomPageCost:    4.0,
		CPUTupleCost:      0.01,
		CPUIndexTupleCost: 0.005,
		CPUOperatorCost:   0.0025,
	}
}

// PlanCost holds the estimated cost of a plan node.
type PlanCost struct {
	Startup float64 // cost before first row
	Total   float64 // cost to return all rows
	Rows    float64 // estimated row count
	Width   int     // average row width in bytes
}
