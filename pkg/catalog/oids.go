package catalog

// Well-known OIDs mirroring PostgreSQL's pg_catalog constants.
// These are assigned during bootstrap and never change.
const (
	// Namespace OIDs.
	OIDPgCatalog = 11   // pg_catalog schema
	OIDPublic    = 2200 // public schema

	// Type OIDs (matching PostgreSQL).
	OIDInt4      = 23   // INT / INT4
	OIDInt8      = 20   // BIGINT / INT8
	OIDText      = 25   // TEXT
	OIDBool      = 16   // BOOL
	OIDFloat8    = 701  // FLOAT8 / DOUBLE PRECISION
	OIDName      = 19   // NAME (we treat as TEXT internally)
	OIDOid       = 26   // OID type (we treat as INT4 internally)
	OIDInt2      = 21   // SMALLINT / INT2
	OIDChar      = 18   // "char" (single-byte type, we treat as TEXT)
	OIDInt2Vec   = 22   // INT2VECTOR (we treat as TEXT)
	OIDDate      = 1082 // DATE
	OIDTimestamp = 1114 // TIMESTAMP WITHOUT TIME ZONE
	OIDNumeric   = 1700 // NUMERIC / DECIMAL
	OIDJSON      = 114  // JSON
	OIDJSONB     = 3802 // JSONB
	OIDUUID      = 2950 // UUID
	OIDInterval  = 1186 // INTERVAL
	OIDBytea     = 17   // BYTEA
	OIDMoney     = 790  // MONEY
	OIDTextArray = 1009 // TEXT[]

	// Relation OIDs for catalog tables.
	OIDPgNamespace   = 2615
	OIDPgType        = 1247
	OIDPgClass       = 1259
	OIDPgAttribute   = 1249
	OIDPgIndex       = 2610
	OIDPgRewrite     = 2618
	OIDPgPolicy      = 3256
	OIDPgAuthID      = 1260
	OIDPgAuthMembers = 1261
	OIDPgACL         = 6100 // LolaDB-specific, no PG equivalent
	OIDPgProc        = 1255
	OIDPgTrigger     = 2620

	// Relation kinds stored in pg_class.relkind (TEXT, single char).
	RelKindOrdinaryTable_S = "r"
	RelKindIndex_S         = "i"
	RelKindView_S          = "v"
	RelKindSequence_S      = "S"
	RelKindToastTable_S    = "t"

	// First user OID — catalog objects use OIDs below this.
	FirstNormalOID = 16384
)

// pg_class column positions (0-based index into the tuple).
const (
	PgClassOID          = 0  // oid INT
	PgClassRelname      = 1  // relname TEXT
	PgClassRelnamespace = 2  // relnamespace INT (FK → pg_namespace.oid)
	PgClassRelkind      = 3  // relkind TEXT ('r','i','v',...)
	PgClassRelpages     = 4  // relpages INT
	PgClassReltuples    = 5  // reltuples INT
	PgClassRelhasindex  = 6  // relhasindex INT (0/1)
	PgClassRelowner     = 7  // relowner INT (FK → pg_authid.oid)
	PgClassRelam        = 8  // relam TEXT (access method: 'heap','btree',...)
	PgClassRelheadpage  = 9  // relheadpage INT (LolaDB-specific: first heap page)
	PgClassRelindexOID  = 10 // relindexoid INT (for indexes: OID of the indexed table)
	PgClassRelindexCol  = 11 // relindexcol INT (for indexes: 1-based column number)
	PgClassNumCols      = 12
)

// pg_attribute column positions.
const (
	PgAttrAttrelid     = 0 // attrelid INT (FK → pg_class.oid)
	PgAttrAttname      = 1 // attname TEXT
	PgAttrAtttypid     = 2 // atttypid INT (FK → pg_type.oid)
	PgAttrAttlen       = 3 // attlen INT
	PgAttrAttnum       = 4 // attnum INT (1-based)
	PgAttrAtttypmod    = 5 // atttypmod INT
	PgAttrAttnotnull   = 6 // attnotnull INT (0/1)
	PgAttrAttisdropped = 7 // attisdropped INT (0/1)
	PgAttrAtthasdef    = 8 // atthasdef INT (0/1)
	PgAttrAttdefault   = 9 // attdefault TEXT (SQL expression)
	PgAttrNumCols      = 10
)

// pg_type column positions.
const (
	PgTypeOID          = 0 // oid INT
	PgTypeTypname      = 1 // typname TEXT
	PgTypeTypnamespace = 2 // typnamespace INT
	PgTypeTyplen       = 3 // typlen INT
	PgTypeTyptype      = 4 // typtype TEXT ('b'=base, 'c'=composite, etc.)
	PgTypeTypbasetype  = 5 // typbasetype INT (base type OID for domains, 0 otherwise)
	PgTypeTypnotnull   = 6 // typnotnull INT (1 if domain has NOT NULL, 0 otherwise)
	PgTypeTypcheck     = 7 // typcheck TEXT (CHECK expression for domains)
	PgTypeTypenumvals  = 8 // typenumvals TEXT (comma-separated enum values)
	PgTypeNumCols      = 9
)

// pg_namespace column positions.
const (
	PgNamespaceOID      = 0 // oid INT
	PgNamespaceNspname  = 1 // nspname TEXT
	PgNamespaceNspowner = 2 // nspowner INT
	PgNamespaceNumCols  = 3
)

// pg_index column positions.
const (
	PgIndexIndexrelid  = 0 // indexrelid INT (FK → pg_class.oid of the index)
	PgIndexIndrelid    = 1 // indrelid INT (FK → pg_class.oid of the table)
	PgIndexIndkey      = 2 // indkey TEXT (column numbers, space-separated)
	PgIndexIndisunique = 3 // indisunique INT (0/1)
	PgIndexNumCols     = 4
)

// pg_authid column positions.
const (
	PgAuthIDOID           = 0  // oid INT
	PgAuthIDRolname       = 1  // rolname TEXT
	PgAuthIDRolsuper      = 2  // rolsuper INT (0/1)
	PgAuthIDRolcreatedb   = 3  // rolcreatedb INT (0/1)
	PgAuthIDRolcreaterole = 4  // rolcreaterole INT (0/1)
	PgAuthIDRolinherit    = 5  // rolinherit INT (0/1)
	PgAuthIDRolcanlogin   = 6  // rolcanlogin INT (0/1)
	PgAuthIDRolbypassrls  = 7  // rolbypassrls INT (0/1)
	PgAuthIDRolconnlimit  = 8  // rolconnlimit INT
	PgAuthIDRolpassword   = 9  // rolpassword TEXT (nullable)
	PgAuthIDNumCols       = 10
)

// pg_auth_members column positions.
const (
	PgAuthMembersRoleid      = 0 // roleid INT
	PgAuthMembersMember      = 1 // member INT
	PgAuthMembersAdminOption = 2 // admin_option INT (0/1)
	PgAuthMembersNumCols     = 3
)

// pg_acl column positions.
const (
	PgACLObjOID     = 0 // objoid INT (the object being granted on)
	PgACLGrantee    = 1 // grantee INT (role OID, 0 = PUBLIC)
	PgACLGrantor    = 2 // grantor INT (role OID who granted)
	PgACLPrivileges = 3 // privileges INT (bitmask)
	PgACLColumns    = 4 // columns TEXT (comma-separated column names)
	PgACLNumCols    = 5
)

// pg_rewrite column positions.
const (
	PgRewriteRelid    = 0 // ev_class INT (FK → pg_class.oid)
	PgRewriteRulename = 1 // rulename TEXT
	PgRewriteType     = 2 // ev_type INT
	PgRewriteAction   = 3 // ev_action INT
	PgRewriteDef      = 4 // definition TEXT
	PgRewriteNumCols  = 5
)

// pg_policy column positions.
const (
	PgPolicyRelid       = 0 // polrelid INT (FK → pg_class.oid)
	PgPolicyName        = 1 // polname TEXT
	PgPolicyCmd         = 2 // polcmd INT
	PgPolicyPermissive  = 3 // polpermissive INT (0/1)
	PgPolicyRoles       = 4 // polroles TEXT (comma-separated)
	PgPolicyQual        = 5 // polqual TEXT (expression)
	PgPolicyWithCheck   = 6 // polwithcheck TEXT (expression)
	PgPolicyNumCols     = 7
)
