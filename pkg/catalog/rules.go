package catalog

// RuleEvent identifies when a rule fires, mirroring PostgreSQL's
// CmdType used in pg_rewrite.ev_type.
type RuleEvent int

const (
	RuleEventSelect RuleEvent = iota
	RuleEventInsert
	RuleEventUpdate
	RuleEventDelete
)

func (e RuleEvent) String() string {
	switch e {
	case RuleEventSelect:
		return "SELECT"
	case RuleEventInsert:
		return "INSERT"
	case RuleEventUpdate:
		return "UPDATE"
	case RuleEventDelete:
		return "DELETE"
	default:
		return "?"
	}
}

// RuleAction determines whether the rule replaces or supplements
// the original query, mirroring PostgreSQL's INSTEAD vs ALSO.
type RuleAction int

const (
	RuleActionInstead RuleAction = iota // DO INSTEAD: replace the original query
	RuleActionAlso                      // DO ALSO: run in addition to the original
	RuleActionNothing                   // DO NOTHING: suppress the original query
)

// RewriteRule represents a single rewrite rule, mirroring a row in
// PostgreSQL's pg_rewrite system catalog.
//
// In PostgreSQL, rules are stored as serialized query trees. Here we
// store the SQL definition string and re-parse/analyze it on demand,
// which is simpler and sufficient for LolaDB's scope.
type RewriteRule struct {
	// Name is the rule name (e.g., "_RETURN" for view SELECT rules).
	Name string

	// RelOID is the OID of the relation this rule is defined on.
	RelOID int32

	// Event is the command type that triggers this rule.
	Event RuleEvent

	// Action determines INSTEAD vs ALSO behavior.
	Action RuleAction

	// Definition is the SQL text of the rule's action query.
	// For views, this is the SELECT query that defines the view.
	// Empty for DO NOTHING rules.
	Definition string

	// Enabled controls whether the rule is active.
	Enabled bool
}

// ruleStore holds rewrite rules in memory, indexed by relation OID.
// This mirrors PostgreSQL's pg_rewrite catalog table.
type ruleStore struct {
	rules map[int32][]*RewriteRule // relOID → rules
}

func newRuleStore() *ruleStore {
	return &ruleStore{rules: make(map[int32][]*RewriteRule)}
}

// AddRule registers a new rewrite rule for a relation.
func (rs *ruleStore) AddRule(rule *RewriteRule) {
	rs.rules[rule.RelOID] = append(rs.rules[rule.RelOID], rule)
}

// GetRules returns all rules for a given relation OID.
func (rs *ruleStore) GetRules(relOID int32) []*RewriteRule {
	return rs.rules[relOID]
}

// GetRulesForEvent returns rules for a specific relation and event.
func (rs *ruleStore) GetRulesForEvent(relOID int32, event RuleEvent) []*RewriteRule {
	var result []*RewriteRule
	for _, r := range rs.rules[relOID] {
		if r.Event == event && r.Enabled {
			result = append(result, r)
		}
	}
	return result
}

// RemoveRules removes all rules for a relation.
func (rs *ruleStore) RemoveRules(relOID int32) {
	delete(rs.rules, relOID)
}
