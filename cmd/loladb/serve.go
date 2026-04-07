package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gololadb/loladb/pkg/catalog"
	"github.com/gololadb/loladb/pkg/storage"
	"github.com/gololadb/loladb/pkg/pgwire"
	"github.com/gololadb/loladb/pkg/sql"
)

// sqlAdapter bridges sql.Executor to pgwire.QueryExecutor.
// It implements pgwire.SessionExecutor for per-connection user context.
type sqlAdapter struct {
	cat  *catalog.Catalog
	exec *sql.Executor
}

func (a *sqlAdapter) Execute(sqlStr string) (*pgwire.QueryResult, error) {
	r, err := a.exec.Exec(sqlStr)
	if err != nil {
		return nil, err
	}
	msg := r.Message
	// PostgreSQL sends "SELECT <count>" for queries returning rows.
	if msg == "" && len(r.Columns) > 0 {
		msg = fmt.Sprintf("SELECT %d", len(r.Rows))
	}
	return &pgwire.QueryResult{
		Columns:      r.Columns,
		Rows:         r.Rows,
		RowsAffected: r.RowsAffected,
		Message:      msg,
		CopyData:     r.CopyData,
		CopyStmt:     r.CopyStmt,
	}, nil
}

func (a *sqlAdapter) ExecuteCopyFromData(copyStmt interface{}, lines []string) (*pgwire.QueryResult, error) {
	r, err := a.exec.ExecCopyFromDataRaw(copyStmt, lines)
	if err != nil {
		return nil, err
	}
	return &pgwire.QueryResult{
		RowsAffected: r.RowsAffected,
		Message:      r.Message,
	}, nil
}

func (a *sqlAdapter) SetUser(user string) {
	a.exec.SetRole(user)
}

// CatalogProvider implementation for pg_dump compatibility.

func (a *sqlAdapter) ListTables() []pgwire.TableInfo {
	tables, err := a.cat.ListTables()
	if err != nil {
		return nil
	}
	var result []pgwire.TableInfo
	for _, t := range tables {
		cols, _ := a.cat.GetColumns(t.OID)
		var colInfos []pgwire.ColumnInfo
		for _, c := range cols {
			typeOID := int32(23) // int4 default
			switch c.Type {
			case 1: // TypeInt32
				typeOID = 23
			case 2: // TypeInt64
				typeOID = 20
			case 3: // TypeText
				typeOID = 25
			case 4: // TypeBool
				typeOID = 16
			case 5: // TypeFloat64
				typeOID = 701
			}
			colInfos = append(colInfos, pgwire.ColumnInfo{
				Name:    c.Name,
				TypeOID: typeOID,
				Num:     int16(c.Num),
			})
		}
		result = append(result, pgwire.TableInfo{
			OID:     t.OID,
			Name:    t.Name,
			Columns: colInfos,
		})
	}
	return result
}

func (a *sqlAdapter) GetSearchPath() []string {
	return a.cat.SearchPath
}

func (a *sqlAdapter) ListIndexes() []pgwire.IndexMeta {
	indexes, err := a.cat.ListAllIndexes()
	if err != nil {
		return nil
	}
	var result []pgwire.IndexMeta
	for _, idx := range indexes {
		// Look up the table name and column name.
		tableName := ""
		colName := ""
		tables, _ := a.cat.ListTables()
		for _, t := range tables {
			if t.OID == idx.TableOID {
				tableName = t.Name
				cols, _ := a.cat.GetColumns(t.OID)
				for _, c := range cols {
					if c.Num == idx.ColNum {
						colName = c.Name
						break
					}
				}
				break
			}
		}
		result = append(result, pgwire.IndexMeta{
			OID:       idx.OID,
			Name:      idx.Name,
			TableOID:  idx.TableOID,
			TableName: tableName,
			ColName:   colName,
			Method:    idx.Method,
		})
	}
	return result
}

func (a *sqlAdapter) NewSession() pgwire.QueryExecutor {
	ex := sql.NewExecutor(a.cat)
	return &sqlAdapter{cat: a.cat, exec: ex}
}

// catalogAuthenticator validates passwords against pg_authid.
type catalogAuthenticator struct {
	cat *catalog.Catalog
}

func (a *catalogAuthenticator) Authenticate(user, password string) error {
	role, err := a.cat.FindRole(user)
	if err != nil {
		return fmt.Errorf("authentication failed for user %q", user)
	}
	if role == nil {
		return fmt.Errorf("password authentication failed for user %q", user)
	}
	if !role.Login {
		return fmt.Errorf("role %q is not permitted to log in", user)
	}
	// Empty password in catalog means no password set — allow any password.
	// Non-empty password must match exactly.
	if role.Password != "" && role.Password != password {
		return fmt.Errorf("password authentication failed for user %q", user)
	}
	return nil
}

// serveOpts holds parsed flags for the serve command.
type serveOpts struct {
	path    string
	addr    string
	tlsCert string
	tlsKey  string
	noTLS   bool
	noAuth  bool
}

func parseServeOpts(args []string) serveOpts {
	opts := serveOpts{addr: ":5432"}
	positional := 0
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--tls-cert" && i+1 < len(args):
			i++
			opts.tlsCert = args[i]
		case args[i] == "--tls-key" && i+1 < len(args):
			i++
			opts.tlsKey = args[i]
		case args[i] == "--no-tls":
			opts.noTLS = true
		case args[i] == "--no-auth":
			opts.noAuth = true
		default:
			if positional == 0 {
				opts.path = args[i]
			} else if positional == 1 {
				opts.addr = args[i]
			}
			positional++
		}
	}
	return opts
}

// selfSignedCert generates an in-memory self-signed TLS certificate.
func selfSignedCert() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate serial: %w", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "loladb"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("marshal key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return tls.X509KeyPair(certPEM, keyPEM)
}

func runServe(args []string) {
	opts := parseServeOpts(args)
	if opts.path == "" {
		fatal("Usage: loladb serve <path> [addr] [--tls-cert FILE --tls-key FILE] [--no-tls] [--no-auth]")
	}

	eng, err := storage.Open(opts.path, 0)
	if err != nil {
		fatal(fmt.Sprintf("Failed to open database: %v", err))
	}
	defer eng.Close()

	cat, err := catalog.New(eng)
	if err != nil {
		fatal(fmt.Sprintf("Failed to load catalog: %v", err))
	}

	ex := sql.NewExecutor(cat)
	adapter := &sqlAdapter{cat: cat, exec: ex}

	srv := &pgwire.Server{
		Addr:     opts.addr,
		Executor: adapter,
	}
	if !opts.noAuth {
		srv.Authenticator = &catalogAuthenticator{cat: cat}
	}

	// Configure TLS unless explicitly disabled.
	if !opts.noTLS {
		var cert tls.Certificate
		if opts.tlsCert != "" && opts.tlsKey != "" {
			cert, err = tls.LoadX509KeyPair(opts.tlsCert, opts.tlsKey)
			if err != nil {
				fatal(fmt.Sprintf("Failed to load TLS certificate: %v", err))
			}
			fmt.Println("pgwire: TLS enabled (user-provided certificate)")
		} else {
			cert, err = selfSignedCert()
			if err != nil {
				fatal(fmt.Sprintf("Failed to generate self-signed certificate: %v", err))
			}
			fmt.Println("pgwire: TLS enabled (auto-generated self-signed certificate)")
		}
		srv.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
	}

	// Graceful shutdown on SIGINT/SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nShutting down...")
		srv.Close()
	}()

	if err := srv.ListenAndServe(); err != nil {
		fmt.Fprintf(os.Stderr, "serve: %v\n", err)
		os.Exit(1)
	}
}
