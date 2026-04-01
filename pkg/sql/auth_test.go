package sql

import (
	"strings"
	"testing"
)

func TestSQL_CreateRole(t *testing.T) {
	ex := newTestExecutor(t)

	r, err := ex.Exec(`CREATE ROLE alice`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "CREATE ROLE" {
		t.Fatalf("unexpected message: %s", r.Message)
	}

	// Duplicate should fail.
	_, err = ex.Exec(`CREATE ROLE alice`)
	if err == nil {
		t.Fatal("expected error for duplicate role")
	}
}

func TestSQL_CreateUser(t *testing.T) {
	ex := newTestExecutor(t)

	r, err := ex.Exec(`CREATE USER bob WITH PASSWORD 'secret'`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "CREATE USER" {
		t.Fatalf("unexpected message: %s", r.Message)
	}

	// Verify the role exists and has LOGIN (CREATE USER implies LOGIN).
	role, err := ex.Cat.FindRole("bob")
	if err != nil {
		t.Fatal(err)
	}
	if role == nil {
		t.Fatal("role bob not found")
	}
	if !role.Login {
		t.Error("CREATE USER should set LOGIN=true")
	}
	if role.Password != "secret" {
		t.Errorf("expected password 'secret', got %q", role.Password)
	}
}

func TestSQL_CreateRoleWithOptions(t *testing.T) {
	ex := newTestExecutor(t)

	_, err := ex.Exec(`CREATE ROLE admin SUPERUSER CREATEDB CREATEROLE LOGIN`)
	if err != nil {
		t.Fatal(err)
	}

	role, err := ex.Cat.FindRole("admin")
	if err != nil {
		t.Fatal(err)
	}
	if !role.SuperUser {
		t.Error("expected SUPERUSER")
	}
	if !role.CreateDB {
		t.Error("expected CREATEDB")
	}
	if !role.CreateRole {
		t.Error("expected CREATEROLE")
	}
	if !role.Login {
		t.Error("expected LOGIN")
	}
}

func TestSQL_AlterRole(t *testing.T) {
	ex := newTestExecutor(t)

	ex.Exec(`CREATE ROLE alice`)

	r, err := ex.Exec(`ALTER ROLE alice SUPERUSER`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "ALTER ROLE" {
		t.Fatalf("unexpected message: %s", r.Message)
	}

	role, err := ex.Cat.FindRole("alice")
	if err != nil {
		t.Fatal(err)
	}
	if !role.SuperUser {
		t.Error("expected SUPERUSER after ALTER")
	}
}

func TestSQL_DropRole(t *testing.T) {
	ex := newTestExecutor(t)

	ex.Exec(`CREATE ROLE alice`)
	ex.Exec(`CREATE ROLE bob`)

	r, err := ex.Exec(`DROP ROLE alice`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "DROP ROLE" {
		t.Fatalf("unexpected message: %s", r.Message)
	}

	role, err := ex.Cat.FindRole("alice")
	if err != nil {
		t.Fatal(err)
	}
	if role != nil {
		t.Error("alice should be dropped")
	}

	// Drop non-existent should fail.
	_, err = ex.Exec(`DROP ROLE nonexistent`)
	if err == nil {
		t.Fatal("expected error for non-existent role")
	}

	// DROP ROLE IF EXISTS should not fail.
	_, err = ex.Exec(`DROP ROLE IF EXISTS nonexistent`)
	if err != nil {
		t.Fatalf("DROP ROLE IF EXISTS should not fail: %v", err)
	}
}

func TestSQL_GrantRevokeRole(t *testing.T) {
	ex := newTestExecutor(t)

	ex.Exec(`CREATE ROLE admin`)
	ex.Exec(`CREATE ROLE alice`)

	r, err := ex.Exec(`GRANT admin TO alice`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "GRANT ROLE" {
		t.Fatalf("unexpected message: %s", r.Message)
	}

	// Verify membership.
	admin, _ := ex.Cat.FindRole("admin")
	alice, _ := ex.Cat.FindRole("alice")
	memberships, err := ex.Cat.GetRoleMemberships(alice.OID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range memberships {
		if m == admin.OID {
			found = true
		}
	}
	if !found {
		t.Error("alice should be a member of admin")
	}

	// Revoke.
	r, err = ex.Exec(`REVOKE admin FROM alice`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "REVOKE ROLE" {
		t.Fatalf("unexpected message: %s", r.Message)
	}

	memberships, err = ex.Cat.GetRoleMemberships(alice.OID)
	if err != nil {
		t.Fatal(err)
	}
	if len(memberships) != 0 {
		t.Error("alice should no longer be a member of admin")
	}
}

func TestSQL_GrantRevokePrivilege(t *testing.T) {
	ex := newTestExecutor(t)

	ex.Exec(`CREATE TABLE docs (id INT, content TEXT)`)
	ex.Exec(`CREATE ROLE reader`)

	r, err := ex.Exec(`GRANT SELECT ON docs TO reader`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "GRANT" {
		t.Fatalf("unexpected message: %s", r.Message)
	}

	// Verify privilege.
	rel, _ := ex.Cat.FindRelation("docs")
	role, _ := ex.Cat.FindRole("reader")
	acls := ex.Cat.ACLs.GetACL(rel.OID)
	if len(acls) == 0 {
		t.Fatal("expected ACL entry")
	}
	found := false
	for _, acl := range acls {
		if acl.Grantee == role.OID {
			found = true
		}
	}
	if !found {
		t.Error("reader should have an ACL entry on docs")
	}

	// Revoke.
	r, err = ex.Exec(`REVOKE SELECT ON docs FROM reader`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "REVOKE" {
		t.Fatalf("unexpected message: %s", r.Message)
	}

	acls = ex.Cat.ACLs.GetACL(rel.OID)
	for _, acl := range acls {
		if acl.Grantee == role.OID {
			t.Error("reader should no longer have ACL on docs")
		}
	}
}

func TestSQL_GrantAllPrivileges(t *testing.T) {
	ex := newTestExecutor(t)

	ex.Exec(`CREATE TABLE t (id INT)`)
	ex.Exec(`CREATE ROLE writer`)

	_, err := ex.Exec(`GRANT ALL ON t TO writer`)
	if err != nil {
		t.Fatal(err)
	}

	rel, _ := ex.Cat.FindRelation("t")
	role, _ := ex.Cat.FindRole("writer")
	acls := ex.Cat.ACLs.GetACL(rel.OID)
	for _, acl := range acls {
		if acl.Grantee == role.OID {
			// Should have all standard privileges.
			if acl.Privileges&0x7F != 0x7F {
				t.Errorf("expected ALL privileges (0x7F), got 0x%X", acl.Privileges)
			}
			return
		}
	}
	t.Error("writer should have ACL on t")
}

func TestSQL_PrivilegeCheckDenied(t *testing.T) {
	ex := newTestExecutor(t)

	ex.Exec(`CREATE TABLE secret (id INT, data TEXT)`)
	ex.Exec(`INSERT INTO secret VALUES (1, 'classified')`)
	ex.Exec(`CREATE ROLE spy LOGIN`)

	// Set role to spy — no privileges granted.
	ex.SetRole("spy")

	_, err := ex.Exec(`SELECT * FROM secret`)
	if err == nil {
		t.Fatal("expected permission denied")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("expected 'permission denied', got: %v", err)
	}

	_, err = ex.Exec(`INSERT INTO secret VALUES (2, 'hack')`)
	if err == nil {
		t.Fatal("expected permission denied for INSERT")
	}

	_, err = ex.Exec(`DELETE FROM secret WHERE id = 1`)
	if err == nil {
		t.Fatal("expected permission denied for DELETE")
	}
}

func TestSQL_PrivilegeCheckGranted(t *testing.T) {
	ex := newTestExecutor(t)

	ex.Exec(`CREATE TABLE docs (id INT, content TEXT)`)
	ex.Exec(`INSERT INTO docs VALUES (1, 'hello')`)
	ex.Exec(`CREATE ROLE reader LOGIN`)
	ex.Exec(`GRANT SELECT ON docs TO reader`)

	ex.SetRole("reader")

	r, err := ex.Exec(`SELECT * FROM docs`)
	if err != nil {
		t.Fatalf("reader should be able to SELECT: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}

	// INSERT should still be denied.
	_, err = ex.Exec(`INSERT INTO docs VALUES (2, 'world')`)
	if err == nil {
		t.Fatal("reader should not be able to INSERT")
	}
}

func TestSQL_SuperuserBypassesPrivileges(t *testing.T) {
	ex := newTestExecutor(t)

	ex.Exec(`CREATE TABLE secret (id INT)`)
	ex.Exec(`INSERT INTO secret VALUES (1)`)
	ex.Exec(`CREATE ROLE admin SUPERUSER LOGIN`)

	ex.SetRole("admin")

	// Superuser should bypass all privilege checks.
	r, err := ex.Exec(`SELECT * FROM secret`)
	if err != nil {
		t.Fatalf("superuser should bypass: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
}

func TestSQL_InheritedRolePrivileges(t *testing.T) {
	ex := newTestExecutor(t)

	ex.Exec(`CREATE TABLE docs (id INT, content TEXT)`)
	ex.Exec(`INSERT INTO docs VALUES (1, 'hello')`)
	ex.Exec(`CREATE ROLE readers`)
	ex.Exec(`CREATE ROLE alice LOGIN`)
	ex.Exec(`GRANT SELECT ON docs TO readers`)
	ex.Exec(`GRANT readers TO alice`)

	ex.SetRole("alice")

	// alice inherits SELECT from readers.
	r, err := ex.Exec(`SELECT * FROM docs`)
	if err != nil {
		t.Fatalf("alice should inherit SELECT from readers: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
}

func TestSQL_GrantToPublic(t *testing.T) {
	ex := newTestExecutor(t)

	ex.Exec(`CREATE TABLE public_data (id INT)`)
	ex.Exec(`INSERT INTO public_data VALUES (42)`)
	ex.Exec(`GRANT SELECT ON public_data TO PUBLIC`)
	ex.Exec(`CREATE ROLE anyone LOGIN`)

	ex.SetRole("anyone")

	r, err := ex.Exec(`SELECT * FROM public_data`)
	if err != nil {
		t.Fatalf("PUBLIC grant should allow access: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
}

func TestSQL_ColumnLevelPrivilege(t *testing.T) {
	ex := newTestExecutor(t)

	ex.Exec(`CREATE TABLE employees (id INT, name TEXT, salary INT)`)
	ex.Exec(`INSERT INTO employees VALUES (1, 'alice', 100000)`)
	ex.Exec(`CREATE ROLE hr LOGIN`)
	// Grant SELECT only on id and name columns.
	ex.Exec(`GRANT SELECT (id, name) ON employees TO hr`)

	ex.SetRole("hr")

	// SELECT on granted columns should work.
	r, err := ex.Exec(`SELECT id, name FROM employees`)
	if err != nil {
		t.Fatalf("should be able to SELECT granted columns: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}

	// SELECT on non-granted column should fail.
	_, err = ex.Exec(`SELECT salary FROM employees`)
	if err == nil {
		t.Fatal("should not be able to SELECT non-granted column")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("expected 'permission denied', got: %v", err)
	}

	// SELECT * includes salary, should fail.
	_, err = ex.Exec(`SELECT * FROM employees`)
	if err == nil {
		t.Fatal("SELECT * should fail when not all columns are granted")
	}
}

func TestSQL_ColumnLevelWithTableLevel(t *testing.T) {
	ex := newTestExecutor(t)

	ex.Exec(`CREATE TABLE data (id INT, value TEXT, secret TEXT)`)
	ex.Exec(`INSERT INTO data VALUES (1, 'public', 'hidden')`)
	ex.Exec(`CREATE ROLE viewer LOGIN`)
	// Grant table-level SELECT — should cover all columns.
	ex.Exec(`GRANT SELECT ON data TO viewer`)

	ex.SetRole("viewer")

	r, err := ex.Exec(`SELECT * FROM data`)
	if err != nil {
		t.Fatalf("table-level grant should cover all columns: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
}

func TestSQL_OwnerBypassesPrivileges(t *testing.T) {
	ex := newTestExecutor(t)

	ex.Exec(`CREATE ROLE owner_user LOGIN`)
	ex.SetRole("owner_user")

	// Table created by owner_user — owner should have all privileges.
	ex.Exec(`CREATE TABLE my_table (id INT, data TEXT)`)
	ex.Exec(`INSERT INTO my_table VALUES (1, 'mine')`)

	r, err := ex.Exec(`SELECT * FROM my_table`)
	if err != nil {
		t.Fatalf("owner should have all privileges: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
}

func TestSQL_RolePersistence(t *testing.T) {
	ex := newTestExecutor(t)

	ex.Exec(`CREATE ROLE persistent_role SUPERUSER LOGIN`)

	// Verify it can be found.
	role, err := ex.Cat.FindRole("persistent_role")
	if err != nil {
		t.Fatal(err)
	}
	if role == nil {
		t.Fatal("role should exist")
	}
	if !role.SuperUser {
		t.Error("role should be superuser")
	}
	if !role.Login {
		t.Error("role should have login")
	}

	// List all roles — should include bootstrap 'loladb' + our role.
	roles, err := ex.Cat.ListRoles()
	if err != nil {
		t.Fatal(err)
	}
	if len(roles) < 2 {
		t.Fatalf("expected at least 2 roles, got %d", len(roles))
	}
}
