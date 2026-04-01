package catalog

// PolicyCmd identifies which SQL command a policy applies to,
// mirroring PostgreSQL's pg_policy.polcmd.
type PolicyCmd int

const (
	PolicyCmdAll    PolicyCmd = iota // applies to all commands
	PolicyCmdSelect                 // SELECT only
	PolicyCmdInsert                 // INSERT only
	PolicyCmdUpdate                 // UPDATE only
	PolicyCmdDelete                 // DELETE only
)

func (c PolicyCmd) String() string {
	switch c {
	case PolicyCmdAll:
		return "ALL"
	case PolicyCmdSelect:
		return "SELECT"
	case PolicyCmdInsert:
		return "INSERT"
	case PolicyCmdUpdate:
		return "UPDATE"
	case PolicyCmdDelete:
		return "DELETE"
	default:
		return "?"
	}
}

// PolicyCmdFromString parses a command name string into a PolicyCmd.
func PolicyCmdFromString(s string) PolicyCmd {
	switch s {
	case "ALL", "all", "*":
		return PolicyCmdAll
	case "SELECT", "select":
		return PolicyCmdSelect
	case "INSERT", "insert":
		return PolicyCmdInsert
	case "UPDATE", "update":
		return PolicyCmdUpdate
	case "DELETE", "delete":
		return PolicyCmdDelete
	default:
		return PolicyCmdAll
	}
}

// RLSPolicy represents a row-level security policy, mirroring a row
// in PostgreSQL's pg_policy system catalog.
//
// In PostgreSQL, policies store parsed expression trees. Here we store
// the SQL text of the USING and WITH CHECK expressions and re-parse
// them when applying the policy.
type RLSPolicy struct {
	// Name is the policy name (unique per table).
	Name string

	// RelOID is the OID of the table this policy is defined on.
	RelOID int32

	// Cmd specifies which command type the policy applies to.
	Cmd PolicyCmd

	// Permissive controls how this policy combines with others.
	// true = PERMISSIVE (OR'd with other permissive policies)
	// false = RESTRICTIVE (AND'd after permissive policies)
	Permissive bool

	// Roles lists the role names this policy applies to.
	// Empty or containing "public" means it applies to all roles.
	Roles []string

	// UsingExpr is the SQL expression for the USING clause.
	// Controls which existing rows are visible (SELECT, UPDATE, DELETE).
	UsingExpr string

	// CheckExpr is the SQL expression for the WITH CHECK clause.
	// Controls which new rows can be written (INSERT, UPDATE).
	// If empty, UsingExpr is used for WITH CHECK as well.
	CheckExpr string
}

// AppliesToCmd returns true if this policy applies to the given command.
func (p *RLSPolicy) AppliesToCmd(cmd PolicyCmd) bool {
	return p.Cmd == PolicyCmdAll || p.Cmd == cmd
}

// AppliesToRole returns true if this policy applies to the given role.
func (p *RLSPolicy) AppliesToRole(role string) bool {
	if len(p.Roles) == 0 {
		return true
	}
	for _, r := range p.Roles {
		if r == "public" || r == role {
			return true
		}
	}
	return false
}

// policyStore holds RLS policies in memory, indexed by relation OID.
// This mirrors PostgreSQL's pg_policy catalog table.
type policyStore struct {
	policies map[int32][]*RLSPolicy // relOID → policies
	rlsFlags map[int32]bool         // relOID → RLS enabled
}

func newPolicyStore() *policyStore {
	return &policyStore{
		policies: make(map[int32][]*RLSPolicy),
		rlsFlags: make(map[int32]bool),
	}
}

// AddPolicy registers a new RLS policy for a relation.
func (ps *policyStore) AddPolicy(policy *RLSPolicy) {
	ps.policies[policy.RelOID] = append(ps.policies[policy.RelOID], policy)
}

// GetPolicies returns all policies for a given relation OID.
func (ps *policyStore) GetPolicies(relOID int32) []*RLSPolicy {
	return ps.policies[relOID]
}

// GetPoliciesForCmd returns policies for a specific relation and command,
// filtered by role.
func (ps *policyStore) GetPoliciesForCmd(relOID int32, cmd PolicyCmd, role string) (permissive, restrictive []*RLSPolicy) {
	for _, p := range ps.policies[relOID] {
		if !p.AppliesToCmd(cmd) {
			continue
		}
		if !p.AppliesToRole(role) {
			continue
		}
		if p.Permissive {
			permissive = append(permissive, p)
		} else {
			restrictive = append(restrictive, p)
		}
	}
	return
}

// EnableRLS marks a relation as having RLS enabled.
func (ps *policyStore) EnableRLS(relOID int32) {
	ps.rlsFlags[relOID] = true
}

// DisableRLS marks a relation as having RLS disabled.
func (ps *policyStore) DisableRLS(relOID int32) {
	ps.rlsFlags[relOID] = false
}

// IsRLSEnabled returns true if RLS is enabled for the relation.
func (ps *policyStore) IsRLSEnabled(relOID int32) bool {
	return ps.rlsFlags[relOID]
}
