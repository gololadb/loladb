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

	"github.com/jespino/loladb/pkg/catalog"
	"github.com/jespino/loladb/pkg/engine"
	"github.com/jespino/loladb/pkg/pgwire"
	"github.com/jespino/loladb/pkg/sql"
)

// sqlAdapter bridges sql.Executor to pgwire.QueryExecutor.
type sqlAdapter struct {
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
	}, nil
}

// serveOpts holds parsed flags for the serve command.
type serveOpts struct {
	path    string
	addr    string
	tlsCert string
	tlsKey  string
	noTLS   bool
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
		fatal("Usage: loladb serve <path> [addr] [--tls-cert FILE --tls-key FILE] [--no-tls]")
	}

	eng, err := engine.Open(opts.path, 0)
	if err != nil {
		fatal(fmt.Sprintf("Failed to open database: %v", err))
	}
	defer eng.Close()

	cat, err := catalog.New(eng)
	if err != nil {
		fatal(fmt.Sprintf("Failed to load catalog: %v", err))
	}

	ex := sql.NewExecutor(cat)
	adapter := &sqlAdapter{exec: ex}

	srv := &pgwire.Server{
		Addr:     opts.addr,
		Executor: adapter,
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
