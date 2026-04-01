package catalog

import (
	"fmt"
	"strings"

	"github.com/jespino/loladb/pkg/slottedpage"
	"github.com/jespino/loladb/pkg/tuple"
)

// Role attributes — mirrors PostgreSQL's pg_authid columns.
type Role struct {
	OID           int32
	Name          string
	SuperUser     bool
	CreateDB      bool
	CreateRole    bool
	Inherit       bool
	Login         bool
	BypassRLS     bool
	ConnLimit     int32
	Password      string // plaintext for now (PostgreSQL stores hashed)
}

// RoleMembership represents a row in pg_auth_members.
type RoleMembership struct {
	RoleOID   int32  // the granted role
	MemberOID int32  // the member who receives the role
	AdminOption bool // can the member grant this role to others?
}

// Privilege types — mirrors PostgreSQL's AclMode bits.
type Privilege int32

const (
	PrivSelect   Privilege = 1 << 0
	PrivInsert   Privilege = 1 << 1
	PrivUpdate   Privilege = 1 << 2
	PrivDelete   Privilege = 1 << 3
	PrivTruncate Privilege = 1 << 4
	PrivReferences Privilege = 1 << 5
	PrivTrigger  Privilege = 1 << 6
	PrivAll      Privilege = PrivSelect | PrivInsert | PrivUpdate | PrivDelete |
		PrivTruncate | PrivReferences | PrivTrigger
)

// ParsePrivilege converts a privilege name to a Privilege bitmask.
func ParsePrivilege(name string) Privilege {
	switch strings.ToUpper(name) {
	case "SELECT":
		return PrivSelect
	case "INSERT":
		return PrivInsert
	case "UPDATE":
		return PrivUpdate
	case "DELETE":
		return PrivDelete
	case "TRUNCATE":
		return PrivTruncate
	case "REFERENCES":
		return PrivReferences
	case "TRIGGER":
		return PrivTrigger
	case "ALL", "ALL PRIVILEGES":
		return PrivAll
	default:
		return 0
	}
}

// PrivilegeNames returns the names of the set bits.
func PrivilegeNames(p Privilege) []string {
	var names []string
	if p&PrivSelect != 0 {
		names = append(names, "SELECT")
	}
	if p&PrivInsert != 0 {
		names = append(names, "INSERT")
	}
	if p&PrivUpdate != 0 {
		names = append(names, "UPDATE")
	}
	if p&PrivDelete != 0 {
		names = append(names, "DELETE")
	}
	if p&PrivTruncate != 0 {
		names = append(names, "TRUNCATE")
	}
	if p&PrivReferences != 0 {
		names = append(names, "REFERENCES")
	}
	if p&PrivTrigger != 0 {
		names = append(names, "TRIGGER")
	}
	return names
}

// ACLItem represents one entry in an ACL list: (grantor, grantee, privileges).
// Mirrors PostgreSQL's aclitem type.
type ACLItem struct {
	Grantee    int32     // role OID (0 = PUBLIC)
	Grantor    int32     // role OID who granted
	Privileges Privilege // bitmask
	Columns    []string  // if non-empty, privilege applies only to these columns
}

// aclStore is the in-memory ACL cache. Maps relation OID → list of ACL items.
type aclStore struct {
	acls map[int32][]ACLItem
}

func newACLStore() *aclStore {
	return &aclStore{acls: make(map[int32][]ACLItem)}
}

func (s *aclStore) Grant(relOID, grantee, grantor int32, privs Privilege) {
	s.GrantColumns(relOID, grantee, grantor, privs, nil)
}

// GrantColumns grants privileges on specific columns (or table-wide if cols is nil).
func (s *aclStore) GrantColumns(relOID, grantee, grantor int32, privs Privilege, cols []string) {
	items := s.acls[relOID]
	for i, item := range items {
		if item.Grantee == grantee && item.Grantor == grantor && columnsMatch(item.Columns, cols) {
			items[i].Privileges |= privs
			s.acls[relOID] = items
			return
		}
	}
	s.acls[relOID] = append(items, ACLItem{
		Grantee:    grantee,
		Grantor:    grantor,
		Privileges: privs,
		Columns:    cols,
	})
}

func (s *aclStore) Revoke(relOID, grantee, grantor int32, privs Privilege) {
	s.RevokeColumns(relOID, grantee, grantor, privs, nil)
}

// RevokeColumns revokes privileges on specific columns (or table-wide if cols is nil).
func (s *aclStore) RevokeColumns(relOID, grantee, grantor int32, privs Privilege, cols []string) {
	items := s.acls[relOID]
	for i, item := range items {
		if item.Grantee == grantee && item.Grantor == grantor && columnsMatch(item.Columns, cols) {
			items[i].Privileges &^= privs
			if items[i].Privileges == 0 {
				s.acls[relOID] = append(items[:i], items[i+1:]...)
			} else {
				s.acls[relOID] = items
			}
			return
		}
	}
}

// Check returns true if grantee has all of the requested privileges on relOID
// at the table level. Also checks PUBLIC (grantee=0).
func (s *aclStore) Check(relOID, grantee int32, required Privilege) bool {
	items := s.acls[relOID]
	var accumulated Privilege
	for _, item := range items {
		if (item.Grantee == grantee || item.Grantee == 0) && len(item.Columns) == 0 {
			accumulated |= item.Privileges
		}
	}
	return accumulated&required == required
}

// CheckColumn returns true if grantee has the required privilege on a specific
// column. Checks both table-level and column-level grants.
func (s *aclStore) CheckColumn(relOID, grantee int32, required Privilege, column string) bool {
	items := s.acls[relOID]
	var accumulated Privilege
	for _, item := range items {
		if item.Grantee != grantee && item.Grantee != 0 {
			continue
		}
		if len(item.Columns) == 0 {
			// Table-level grant covers all columns.
			accumulated |= item.Privileges
		} else {
			for _, c := range item.Columns {
				if c == column {
					accumulated |= item.Privileges
					break
				}
			}
		}
	}
	return accumulated&required == required
}

// columnsMatch returns true if two column lists are equivalent.
// nil matches nil (table-level), and order doesn't matter for matching.
func columnsMatch(a, b []string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]bool, len(a))
	for _, c := range a {
		set[c] = true
	}
	for _, c := range b {
		if !set[c] {
			return false
		}
	}
	return true
}

// GetACL returns all ACL items for a relation.
func (s *aclStore) GetACL(relOID int32) []ACLItem {
	return s.acls[relOID]
}

// -----------------------------------------------------------------------
// Catalog methods for role management (pg_authid)
// -----------------------------------------------------------------------

// bootstrapAuth allocates pg_authid and pg_auth_members pages and
// creates the default superuser role.
func (c *Catalog) bootstrapAuth() error {
	pgAuthIDPage, err := c.Eng.AllocPage()
	if err != nil {
		return err
	}
	pgAuthMembersPage, err := c.Eng.AllocPage()
	if err != nil {
		return err
	}

	for _, pg := range []uint32{pgAuthIDPage, pgAuthMembersPage} {
		buf, err := c.Eng.Pool.FetchPage(pg)
		if err != nil {
			return err
		}
		sp := slottedpage.Init(slottedpage.PageTypeHeap, pg, 0)
		copy(buf, sp.Bytes())
		c.Eng.Pool.MarkDirty(pg)
		c.Eng.Pool.ReleasePage(pg)
	}

	c.Eng.Super.PgAuthIDPage = pgAuthIDPage
	c.Eng.Super.PgAuthMembersPage = pgAuthMembersPage

	// Create the default superuser role (like PostgreSQL's bootstrap superuser).
	return c.createRoleInternal(&Role{
		OID:       int32(c.Eng.Super.AllocOID()),
		Name:      "loladb",
		SuperUser: true,
		CreateDB:  true,
		CreateRole: true,
		Inherit:   true,
		Login:     true,
		BypassRLS: true,
		ConnLimit: -1,
	})
}

// pg_authid tuple format:
// (oid int32, rolname text, rolsuper int32, rolcreatedb int32,
//  rolcreaterole int32, rolinherit int32, rolcanlogin int32,
//  rolbypassrls int32, rolconnlimit int32, rolpassword text)

func roleToTuple(r *Role) []tuple.Datum {
	return []tuple.Datum{
		tuple.DInt32(r.OID),
		tuple.DText(r.Name),
		tuple.DInt32(boolToInt32(r.SuperUser)),
		tuple.DInt32(boolToInt32(r.CreateDB)),
		tuple.DInt32(boolToInt32(r.CreateRole)),
		tuple.DInt32(boolToInt32(r.Inherit)),
		tuple.DInt32(boolToInt32(r.Login)),
		tuple.DInt32(boolToInt32(r.BypassRLS)),
		tuple.DInt32(r.ConnLimit),
		tuple.DText(r.Password),
	}
}

func tupleToRole(tup *tuple.Tuple) *Role {
	if len(tup.Columns) < 10 {
		return nil
	}
	return &Role{
		OID:        tup.Columns[0].I32,
		Name:       tup.Columns[1].Text,
		SuperUser:  tup.Columns[2].I32 != 0,
		CreateDB:   tup.Columns[3].I32 != 0,
		CreateRole: tup.Columns[4].I32 != 0,
		Inherit:    tup.Columns[5].I32 != 0,
		Login:      tup.Columns[6].I32 != 0,
		BypassRLS:  tup.Columns[7].I32 != 0,
		ConnLimit:  tup.Columns[8].I32,
		Password:   tup.Columns[9].Text,
	}
}

func boolToInt32(b bool) int32 {
	if b {
		return 1
	}
	return 0
}

func (c *Catalog) createRoleInternal(role *Role) error {
	xid := c.Eng.TxMgr.Begin()
	_, err := c.Eng.Insert(xid, c.Eng.Super.PgAuthIDPage, roleToTuple(role))
	if err != nil {
		c.Eng.TxMgr.Abort(xid)
		return err
	}
	c.Eng.TxMgr.Commit(xid)
	return nil
}

// CreateRole creates a new role in pg_authid.
func (c *Catalog) CreateRole(role *Role) error {
	existing, err := c.FindRole(role.Name)
	if err != nil {
		return err
	}
	if existing != nil {
		return fmt.Errorf("role %q already exists", role.Name)
	}
	role.OID = int32(c.Eng.Super.AllocOID())
	return c.createRoleInternal(role)
}

// DropRole removes a role from pg_authid.
func (c *Catalog) DropRole(name string, missingOk bool) error {
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)

	var found bool
	err := c.Eng.SeqScan(c.Eng.Super.PgAuthIDPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) >= 2 && tup.Columns[1].Text == name {
			c.Eng.Delete(xid, id)
			found = true
			return false
		}
		return true
	})
	if err != nil {
		c.Eng.TxMgr.Abort(xid)
		return err
	}
	if !found && !missingOk {
		c.Eng.TxMgr.Abort(xid)
		return fmt.Errorf("role %q does not exist", name)
	}
	c.Eng.TxMgr.Commit(xid)

	// Also remove all memberships involving this role.
	if found {
		c.removeAllMemberships(name)
	}
	return nil
}

// AlterRole updates role attributes. Only non-nil options are changed.
func (c *Catalog) AlterRole(name string, updates map[string]interface{}) error {
	role, err := c.FindRole(name)
	if err != nil {
		return err
	}
	if role == nil {
		return fmt.Errorf("role %q does not exist", name)
	}

	// Apply updates.
	for key, val := range updates {
		switch key {
		case "superuser":
			role.SuperUser = val.(bool)
		case "createdb":
			role.CreateDB = val.(bool)
		case "createrole":
			role.CreateRole = val.(bool)
		case "inherit":
			role.Inherit = val.(bool)
		case "login":
			role.Login = val.(bool)
		case "bypassrls":
			role.BypassRLS = val.(bool)
		case "connlimit":
			role.ConnLimit = val.(int32)
		case "password":
			role.Password = val.(string)
		}
	}

	// Delete old row and insert new one (simple approach).
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)

	err = c.Eng.SeqScan(c.Eng.Super.PgAuthIDPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) >= 2 && tup.Columns[1].Text == name {
			c.Eng.Delete(xid, id)
			return false
		}
		return true
	})
	if err != nil {
		c.Eng.TxMgr.Abort(xid)
		return err
	}

	_, err = c.Eng.Insert(xid, c.Eng.Super.PgAuthIDPage, roleToTuple(role))
	if err != nil {
		c.Eng.TxMgr.Abort(xid)
		return err
	}
	c.Eng.TxMgr.Commit(xid)
	return nil
}

// FindRole looks up a role by name in pg_authid.
func (c *Catalog) FindRole(name string) (*Role, error) {
	if c.Eng.Super.PgAuthIDPage == 0 {
		return nil, nil
	}
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	var found *Role
	err := c.Eng.SeqScan(c.Eng.Super.PgAuthIDPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		r := tupleToRole(tup)
		if r != nil && r.Name == name {
			found = r
			return false
		}
		return true
	})
	return found, err
}

// FindRoleByOID looks up a role by OID.
func (c *Catalog) FindRoleByOID(oid int32) (*Role, error) {
	if c.Eng.Super.PgAuthIDPage == 0 {
		return nil, nil
	}
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	var found *Role
	err := c.Eng.SeqScan(c.Eng.Super.PgAuthIDPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		r := tupleToRole(tup)
		if r != nil && r.OID == oid {
			found = r
			return false
		}
		return true
	})
	return found, err
}

// ListRoles returns all roles from pg_authid.
func (c *Catalog) ListRoles() ([]*Role, error) {
	if c.Eng.Super.PgAuthIDPage == 0 {
		return nil, nil
	}
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	var roles []*Role
	err := c.Eng.SeqScan(c.Eng.Super.PgAuthIDPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		r := tupleToRole(tup)
		if r != nil {
			roles = append(roles, r)
		}
		return true
	})
	return roles, err
}

// -----------------------------------------------------------------------
// Role membership (pg_auth_members)
// -----------------------------------------------------------------------

// pg_auth_members tuple format:
// (roleid int32, member int32, admin_option int32)

// GrantRoleMembership grants a role to a member.
func (c *Catalog) GrantRoleMembership(roleName, memberName string, adminOption bool) error {
	role, err := c.FindRole(roleName)
	if err != nil {
		return err
	}
	if role == nil {
		return fmt.Errorf("role %q does not exist", roleName)
	}

	member, err := c.FindRole(memberName)
	if err != nil {
		return err
	}
	if member == nil {
		return fmt.Errorf("role %q does not exist", memberName)
	}

	// Check if membership already exists.
	existing, err := c.findMembership(role.OID, member.OID)
	if err != nil {
		return err
	}
	if existing {
		return nil // already a member
	}

	xid := c.Eng.TxMgr.Begin()
	adminVal := int32(0)
	if adminOption {
		adminVal = 1
	}
	_, err = c.Eng.Insert(xid, c.Eng.Super.PgAuthMembersPage, []tuple.Datum{
		tuple.DInt32(role.OID),
		tuple.DInt32(member.OID),
		tuple.DInt32(adminVal),
	})
	if err != nil {
		c.Eng.TxMgr.Abort(xid)
		return err
	}
	c.Eng.TxMgr.Commit(xid)
	return nil
}

// RevokeRoleMembership revokes a role from a member.
func (c *Catalog) RevokeRoleMembership(roleName, memberName string) error {
	role, err := c.FindRole(roleName)
	if err != nil {
		return err
	}
	if role == nil {
		return fmt.Errorf("role %q does not exist", roleName)
	}

	member, err := c.FindRole(memberName)
	if err != nil {
		return err
	}
	if member == nil {
		return fmt.Errorf("role %q does not exist", memberName)
	}

	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)

	err = c.Eng.SeqScan(c.Eng.Super.PgAuthMembersPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) >= 2 && tup.Columns[0].I32 == role.OID && tup.Columns[1].I32 == member.OID {
			c.Eng.Delete(xid, id)
			return false
		}
		return true
	})
	if err != nil {
		c.Eng.TxMgr.Abort(xid)
		return err
	}
	c.Eng.TxMgr.Commit(xid)
	return nil
}

func (c *Catalog) findMembership(roleOID, memberOID int32) (bool, error) {
	if c.Eng.Super.PgAuthMembersPage == 0 {
		return false, nil
	}
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	found := false
	err := c.Eng.SeqScan(c.Eng.Super.PgAuthMembersPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) >= 2 && tup.Columns[0].I32 == roleOID && tup.Columns[1].I32 == memberOID {
			found = true
			return false
		}
		return true
	})
	return found, err
}

// GetRoleMemberships returns all roles that the given role is a member of.
func (c *Catalog) GetRoleMemberships(memberOID int32) ([]int32, error) {
	if c.Eng.Super.PgAuthMembersPage == 0 {
		return nil, nil
	}
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	var roleOIDs []int32
	err := c.Eng.SeqScan(c.Eng.Super.PgAuthMembersPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) >= 2 && tup.Columns[1].I32 == memberOID {
			roleOIDs = append(roleOIDs, tup.Columns[0].I32)
		}
		return true
	})
	return roleOIDs, err
}

// GetRoleMembers returns all members of a given role.
func (c *Catalog) GetRoleMembers(roleOID int32) ([]int32, error) {
	if c.Eng.Super.PgAuthMembersPage == 0 {
		return nil, nil
	}
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	var memberOIDs []int32
	err := c.Eng.SeqScan(c.Eng.Super.PgAuthMembersPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) >= 2 && tup.Columns[0].I32 == roleOID {
			memberOIDs = append(memberOIDs, tup.Columns[1].I32)
		}
		return true
	})
	return memberOIDs, err
}

// removeAllMemberships removes all pg_auth_members rows for a role
// (both as granted role and as member).
func (c *Catalog) removeAllMemberships(roleName string) {
	role, err := c.FindRole(roleName)
	if err != nil || role == nil {
		return
	}
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	c.Eng.SeqScan(c.Eng.Super.PgAuthMembersPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) >= 2 && (tup.Columns[0].I32 == role.OID || tup.Columns[1].I32 == role.OID) {
			c.Eng.Delete(xid, id)
		}
		return true
	})
	c.Eng.TxMgr.Commit(xid)
}

// GetAllRoleOIDs returns the set of role OIDs that a user effectively
// has, following the INHERIT chain (transitive membership).
// This mirrors PostgreSQL's roles_is_member_of().
func (c *Catalog) GetAllRoleOIDs(roleOID int32) (map[int32]bool, error) {
	result := map[int32]bool{roleOID: true}
	queue := []int32{roleOID}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		// Check if this role has INHERIT.
		role, err := c.FindRoleByOID(current)
		if err != nil {
			return nil, err
		}
		// Only follow membership if the role has INHERIT (or it's the original role).
		if role != nil && (!role.Inherit && current != roleOID) {
			continue
		}

		memberships, err := c.GetRoleMemberships(current)
		if err != nil {
			return nil, err
		}
		for _, m := range memberships {
			if !result[m] {
				result[m] = true
				queue = append(queue, m)
			}
		}
	}
	return result, nil
}

// -----------------------------------------------------------------------
// Object ACLs
// -----------------------------------------------------------------------

// GrantObjectPrivilege grants table-level privileges on a relation to a role.
func (c *Catalog) GrantObjectPrivilege(relOID, grantee, grantor int32, privs Privilege) {
	c.GrantObjectPrivilegeColumns(relOID, grantee, grantor, privs, nil)
}

// GrantObjectPrivilegeColumns grants privileges on specific columns (or table-wide if cols is nil).
func (c *Catalog) GrantObjectPrivilegeColumns(relOID, grantee, grantor int32, privs Privilege, cols []string) {
	c.ACLs.GrantColumns(relOID, grantee, grantor, privs, cols)
	c.persistACLFull(relOID)
}

// RevokeObjectPrivilege revokes table-level privileges on a relation from a role.
func (c *Catalog) RevokeObjectPrivilege(relOID, grantee, grantor int32, privs Privilege) {
	c.RevokeObjectPrivilegeColumns(relOID, grantee, grantor, privs, nil)
}

// RevokeObjectPrivilegeColumns revokes privileges on specific columns (or table-wide if cols is nil).
func (c *Catalog) RevokeObjectPrivilegeColumns(relOID, grantee, grantor int32, privs Privilege, cols []string) {
	c.ACLs.RevokeColumns(relOID, grantee, grantor, privs, cols)
	c.persistACLFull(relOID)
}

// CheckPrivilege checks if a role (considering inherited roles and ownership)
// has the required privileges on a relation. Superusers bypass all checks.
// Table owners implicitly have all privileges on their tables.
func (c *Catalog) CheckPrivilege(roleName string, relOID int32, required Privilege) (bool, error) {
	role, err := c.FindRole(roleName)
	if err != nil {
		return false, err
	}
	if role == nil {
		// Unknown role — check PUBLIC only.
		return c.ACLs.Check(relOID, 0, required), nil
	}
	if role.SuperUser {
		return true, nil
	}

	// Check ownership — owner has all privileges.
	rel := c.FindRelationByOID(relOID)
	if rel != nil && rel.OwnerOID != 0 && rel.OwnerOID == role.OID {
		return true, nil
	}

	// Check the role itself and all inherited roles.
	allRoles, err := c.GetAllRoleOIDs(role.OID)
	if err != nil {
		return false, err
	}

	// Also check ownership through inherited roles.
	if rel != nil && rel.OwnerOID != 0 {
		if allRoles[rel.OwnerOID] {
			return true, nil
		}
	}

	for oid := range allRoles {
		if c.ACLs.Check(relOID, oid, required) {
			return true, nil
		}
	}
	// Also check PUBLIC.
	if c.ACLs.Check(relOID, 0, required) {
		return true, nil
	}
	return false, nil
}

// CheckColumnPrivilege checks if a role has the required privilege on a
// specific column. Checks table-level grants, column-level grants,
// ownership, and inherited roles.
func (c *Catalog) CheckColumnPrivilege(roleName string, relOID int32, required Privilege, column string) (bool, error) {
	role, err := c.FindRole(roleName)
	if err != nil {
		return false, err
	}
	if role == nil {
		return c.ACLs.CheckColumn(relOID, 0, required, column), nil
	}
	if role.SuperUser {
		return true, nil
	}

	// Owner has all privileges.
	rel := c.FindRelationByOID(relOID)
	if rel != nil && rel.OwnerOID != 0 && rel.OwnerOID == role.OID {
		return true, nil
	}

	allRoles, err := c.GetAllRoleOIDs(role.OID)
	if err != nil {
		return false, err
	}

	if rel != nil && rel.OwnerOID != 0 && allRoles[rel.OwnerOID] {
		return true, nil
	}

	for oid := range allRoles {
		if c.ACLs.CheckColumn(relOID, oid, required, column) {
			return true, nil
		}
	}
	if c.ACLs.CheckColumn(relOID, 0, required, column) {
		return true, nil
	}
	return false, nil
}

// FindRelationByOID looks up a relation by OID in pg_class.
func (c *Catalog) FindRelationByOID(relOID int32) *Relation {
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	var found *Relation
	c.Eng.SeqScan(c.Eng.Super.PgClassPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		r := tupleToRelation(tup)
		if r != nil && r.OID == relOID {
			found = r
			return false
		}
		return true
	})
	return found
}

// bootstrapACL allocates the pg_acl heap page.
func (c *Catalog) bootstrapACL() error {
	pgACLPage, err := c.Eng.AllocPage()
	if err != nil {
		return err
	}
	buf, err := c.Eng.Pool.FetchPage(pgACLPage)
	if err != nil {
		return err
	}
	sp := slottedpage.Init(slottedpage.PageTypeHeap, pgACLPage, 0)
	copy(buf, sp.Bytes())
	c.Eng.Pool.MarkDirty(pgACLPage)
	c.Eng.Pool.ReleasePage(pgACLPage)
	c.Eng.Super.PgACLPage = pgACLPage
	return nil
}

// pg_acl tuple format: (reloid int32, grantee int32, grantor int32, privileges int32, columns text)
// The columns field is a comma-separated list of column names, or empty for table-level.

// persistACLFull deletes all pg_acl rows for a relation and re-inserts
// the current in-memory ACL entries.
func (c *Catalog) persistACLFull(relOID int32) {
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)

	// Delete existing entries for this relation.
	c.Eng.SeqScan(c.Eng.Super.PgACLPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) >= 1 && tup.Columns[0].I32 == relOID {
			c.Eng.Delete(xid, id)
		}
		return true
	})
	c.Eng.TxMgr.Commit(xid)

	// Re-insert current ACL entries.
	for _, item := range c.ACLs.GetACL(relOID) {
		colStr := strings.Join(item.Columns, ",")
		xid2 := c.Eng.TxMgr.Begin()
		c.Eng.Insert(xid2, c.Eng.Super.PgACLPage, []tuple.Datum{
			tuple.DInt32(relOID),
			tuple.DInt32(item.Grantee),
			tuple.DInt32(item.Grantor),
			tuple.DInt32(int32(item.Privileges)),
			tuple.DText(colStr),
		})
		c.Eng.TxMgr.Commit(xid2)
	}
}

// loadACLs reads all pg_acl rows and populates the in-memory ACL store.
func (c *Catalog) loadACLs() error {
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	return c.Eng.SeqScan(c.Eng.Super.PgACLPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) >= 4 {
			relOID := tup.Columns[0].I32
			grantee := tup.Columns[1].I32
			grantor := tup.Columns[2].I32
			privs := Privilege(tup.Columns[3].I32)
			var cols []string
			if len(tup.Columns) >= 5 && tup.Columns[4].Text != "" {
				cols = strings.Split(tup.Columns[4].Text, ",")
			}
			c.ACLs.GrantColumns(relOID, grantee, grantor, privs, cols)
		}
		return true
	})
}
