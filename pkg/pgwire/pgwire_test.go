package pgwire

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/jespino/loladb/pkg/tuple"
)

// mockExecutor implements QueryExecutor for testing.
type mockExecutor struct {
	results map[string]*QueryResult
}

func (m *mockExecutor) Execute(sql string) (*QueryResult, error) {
	if r, ok := m.results[sql]; ok {
		return r, nil
	}
	return nil, fmt.Errorf("unknown statement: %s", sql)
}

// writeStartup sends a v3 startup message with the given params.
func writeStartup(conn net.Conn, params map[string]string) error {
	// Build payload: int32(version) + key\0value\0...key\0value\0\0
	var payload []byte
	var ver [4]byte
	binary.BigEndian.PutUint32(ver[:], protocolVersion3)
	payload = append(payload, ver[:]...)
	for k, v := range params {
		payload = append(payload, []byte(k)...)
		payload = append(payload, 0)
		payload = append(payload, []byte(v)...)
		payload = append(payload, 0)
	}
	payload = append(payload, 0) // terminator

	// Length includes itself (4 bytes) + payload.
	length := int32(4 + len(payload))
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(length))
	if _, err := conn.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := conn.Write(payload)
	return err
}

// readMessage reads one backend message: type(1) + length(4) + data.
func readMessage(conn net.Conn) (byte, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, nil, err
	}
	msgType := header[0]
	msgLen := int(binary.BigEndian.Uint32(header[1:])) - 4
	if msgLen < 0 {
		return msgType, nil, nil
	}
	data := make([]byte, msgLen)
	if _, err := io.ReadFull(conn, data); err != nil {
		return 0, nil, err
	}
	return msgType, data, nil
}

// writeQuery sends a simple query message.
func writeQuery(conn net.Conn, sql string) error {
	buf := newMsgBuf(msgQuery)
	buf.writeCString(sql)
	_, err := conn.Write(buf.finish())
	return err
}

// writeTerminate sends a Terminate message.
func writeTerminate(conn net.Conn) error {
	buf := newMsgBuf(msgTerminate)
	_, err := conn.Write(buf.finish())
	return err
}

// consumeUntilReady reads messages until ReadyForQuery ('Z') is received.
// Returns all messages read (including the ReadyForQuery).
func consumeUntilReady(conn net.Conn) ([]struct {
	typ  byte
	data []byte
}, error) {
	var msgs []struct {
		typ  byte
		data []byte
	}
	for {
		typ, data, err := readMessage(conn)
		if err != nil {
			return msgs, err
		}
		msgs = append(msgs, struct {
			typ  byte
			data []byte
		}{typ, data})
		if typ == 'Z' {
			return msgs, nil
		}
	}
}

func startTestServer(t *testing.T, exec QueryExecutor) (*Server, string) {
	t.Helper()
	srv := &Server{
		Addr:     "127.0.0.1:0", // random port
		Executor: exec,
	}
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv.listener = ln
	srv.Addr = ln.Addr().String()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.handleConn(conn)
		}
	}()

	return srv, srv.Addr
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn
}

func TestStartupHandshake(t *testing.T) {
	exec := &mockExecutor{results: map[string]*QueryResult{}}
	srv, addr := startTestServer(t, exec)
	defer srv.Close()

	conn := dial(t, addr)
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	if err := writeStartup(conn, map[string]string{
		"user":     "test",
		"database": "testdb",
	}); err != nil {
		t.Fatalf("writeStartup: %v", err)
	}

	msgs, err := consumeUntilReady(conn)
	if err != nil {
		t.Fatalf("consumeUntilReady: %v", err)
	}

	// Expect: AuthOk ('R'), several ParameterStatus ('S'),
	// BackendKeyData ('K'), ReadyForQuery ('Z').
	gotAuth := false
	gotReady := false
	paramCount := 0
	for _, m := range msgs {
		switch m.typ {
		case 'R':
			gotAuth = true
			if len(m.data) < 4 {
				t.Fatal("AuthOk too short")
			}
			code := binary.BigEndian.Uint32(m.data[:4])
			if code != 0 {
				t.Fatalf("expected AUTH_REQ_OK (0), got %d", code)
			}
		case 'S':
			paramCount++
		case 'Z':
			gotReady = true
			if len(m.data) < 1 || m.data[0] != 'I' {
				t.Fatalf("expected ReadyForQuery idle, got %v", m.data)
			}
		}
	}
	if !gotAuth {
		t.Fatal("missing AuthenticationOk")
	}
	if !gotReady {
		t.Fatal("missing ReadyForQuery")
	}
	if paramCount == 0 {
		t.Fatal("no ParameterStatus messages received")
	}
}

func TestSimpleQuery(t *testing.T) {
	exec := &mockExecutor{results: map[string]*QueryResult{
		"SELECT 1": {
			Columns: []string{"?column?"},
			Rows: [][]tuple.Datum{
				{{Type: tuple.TypeInt32, I32: 1}},
			},
			Message: "SELECT 1",
		},
	}}
	srv, addr := startTestServer(t, exec)
	defer srv.Close()

	conn := dial(t, addr)
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Startup.
	if err := writeStartup(conn, map[string]string{"user": "test"}); err != nil {
		t.Fatal(err)
	}
	if _, err := consumeUntilReady(conn); err != nil {
		t.Fatal(err)
	}

	// Send query.
	if err := writeQuery(conn, "SELECT 1"); err != nil {
		t.Fatal(err)
	}

	msgs, err := consumeUntilReady(conn)
	if err != nil {
		t.Fatalf("query response: %v", err)
	}

	var gotRowDesc, gotDataRow, gotComplete, gotReady bool
	for _, m := range msgs {
		switch m.typ {
		case 'T':
			gotRowDesc = true
		case 'D':
			gotDataRow = true
		case 'C':
			gotComplete = true
		case 'Z':
			gotReady = true
		}
	}

	if !gotRowDesc {
		t.Error("missing RowDescription")
	}
	if !gotDataRow {
		t.Error("missing DataRow")
	}
	if !gotComplete {
		t.Error("missing CommandComplete")
	}
	if !gotReady {
		t.Error("missing ReadyForQuery")
	}
}

func TestSSLNegotiation(t *testing.T) {
	exec := &mockExecutor{results: map[string]*QueryResult{}}
	srv, addr := startTestServer(t, exec)
	defer srv.Close()

	conn := dial(t, addr)
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Send SSL request first.
	var sslMsg [8]byte
	binary.BigEndian.PutUint32(sslMsg[:4], 8)          // length
	binary.BigEndian.PutUint32(sslMsg[4:], sslRequestCode) // SSL code
	if _, err := conn.Write(sslMsg[:]); err != nil {
		t.Fatal(err)
	}

	// Read 'N' response.
	resp := make([]byte, 1)
	if _, err := io.ReadFull(conn, resp); err != nil {
		t.Fatal(err)
	}
	if resp[0] != 'N' {
		t.Fatalf("expected 'N', got %c", resp[0])
	}

	// Now send real startup.
	if err := writeStartup(conn, map[string]string{"user": "test"}); err != nil {
		t.Fatal(err)
	}
	msgs, err := consumeUntilReady(conn)
	if err != nil {
		t.Fatal(err)
	}

	gotAuth := false
	for _, m := range msgs {
		if m.typ == 'R' {
			gotAuth = true
		}
	}
	if !gotAuth {
		t.Fatal("missing AuthenticationOk after SSL negotiation")
	}
}

func TestErrorResponse(t *testing.T) {
	exec := &mockExecutor{results: map[string]*QueryResult{}}
	srv, addr := startTestServer(t, exec)
	defer srv.Close()

	conn := dial(t, addr)
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	if err := writeStartup(conn, map[string]string{"user": "test"}); err != nil {
		t.Fatal(err)
	}
	if _, err := consumeUntilReady(conn); err != nil {
		t.Fatal(err)
	}

	// Send a query that the mock doesn't know.
	if err := writeQuery(conn, "SELECT bogus"); err != nil {
		t.Fatal(err)
	}

	msgs, err := consumeUntilReady(conn)
	if err != nil {
		t.Fatal(err)
	}

	gotError := false
	for _, m := range msgs {
		if m.typ == 'E' {
			gotError = true
		}
	}
	if !gotError {
		t.Error("expected ErrorResponse for unknown query")
	}
}

func TestExtendedQueryProtocol(t *testing.T) {
	exec := &mockExecutor{results: map[string]*QueryResult{
		"SELECT 42": {
			Columns: []string{"num"},
			Rows: [][]tuple.Datum{
				{{Type: tuple.TypeInt32, I32: 42}},
			},
			Message: "SELECT 1",
		},
	}}
	srv, addr := startTestServer(t, exec)
	defer srv.Close()

	conn := dial(t, addr)
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	if err := writeStartup(conn, map[string]string{"user": "test"}); err != nil {
		t.Fatal(err)
	}
	if _, err := consumeUntilReady(conn); err != nil {
		t.Fatal(err)
	}

	// Parse: name\0 query\0 int16(0 params)
	parseBuf := newMsgBuf(msgParse)
	parseBuf.writeCString("")          // unnamed statement
	parseBuf.writeCString("SELECT 42") // query
	parseBuf.writeInt16(0)             // no param types
	conn.Write(parseBuf.finish())

	// Bind: portal\0 statement\0 int16(0 formats) int16(0 params) int16(0 result formats)
	bindBuf := newMsgBuf(msgBind)
	bindBuf.writeCString("") // unnamed portal
	bindBuf.writeCString("") // unnamed statement
	bindBuf.writeInt16(0)    // no param format codes
	bindBuf.writeInt16(0)    // no params
	bindBuf.writeInt16(0)    // no result format codes
	conn.Write(bindBuf.finish())

	// Execute: portal\0 int32(0 = no limit)
	execBuf := newMsgBuf(msgExecute)
	execBuf.writeCString("") // unnamed portal
	execBuf.writeInt32(0)    // no row limit
	conn.Write(execBuf.finish())

	// Sync
	syncBuf := newMsgBuf(msgSync)
	conn.Write(syncBuf.finish())

	msgs, err := consumeUntilReady(conn)
	if err != nil {
		t.Fatalf("extended query: %v", err)
	}

	var gotParse, gotBind, gotRowDesc, gotDataRow, gotComplete bool
	for _, m := range msgs {
		switch m.typ {
		case '1':
			gotParse = true
		case '2':
			gotBind = true
		case 'T':
			gotRowDesc = true
		case 'D':
			gotDataRow = true
		case 'C':
			gotComplete = true
		}
	}

	if !gotParse {
		t.Error("missing ParseComplete")
	}
	if !gotBind {
		t.Error("missing BindComplete")
	}
	if !gotRowDesc {
		t.Error("missing RowDescription")
	}
	if !gotDataRow {
		t.Error("missing DataRow")
	}
	if !gotComplete {
		t.Error("missing CommandComplete")
	}
}

func TestTerminate(t *testing.T) {
	exec := &mockExecutor{results: map[string]*QueryResult{}}
	srv, addr := startTestServer(t, exec)
	defer srv.Close()

	conn := dial(t, addr)
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	if err := writeStartup(conn, map[string]string{"user": "test"}); err != nil {
		t.Fatal(err)
	}
	if _, err := consumeUntilReady(conn); err != nil {
		t.Fatal(err)
	}

	if err := writeTerminate(conn); err != nil {
		t.Fatal(err)
	}

	// Server should close the connection. Reading should return EOF.
	buf := make([]byte, 1)
	_, err := conn.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected EOF after Terminate, got: %v", err)
	}
}

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"SELECT 1; SELECT 2", []string{"SELECT 1", "SELECT 2"}},
		{"SELECT 'a;b'", []string{"SELECT 'a;b'"}},
		{`SELECT "col;name"`, []string{`SELECT "col;name"`}},
		{"", nil},
		{"  ;  ;  ", nil},
	}
	for _, tt := range tests {
		got := splitStatements(tt.input)
		if len(got) != len(tt.want) {
			t.Errorf("splitStatements(%q) = %v, want %v", tt.input, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("splitStatements(%q)[%d] = %q, want %q", tt.input, i, got[i], tt.want[i])
			}
		}
	}
}

func TestDatumToText(t *testing.T) {
	tests := []struct {
		d    tuple.Datum
		want string
	}{
		{tuple.Datum{Type: tuple.TypeNull}, ""},
		{tuple.Datum{Type: tuple.TypeInt32, I32: 42}, "42"},
		{tuple.Datum{Type: tuple.TypeInt64, I64: 100}, "100"},
		{tuple.Datum{Type: tuple.TypeText, Text: "hello"}, "hello"},
		{tuple.Datum{Type: tuple.TypeBool, Bool: true}, "t"},
		{tuple.Datum{Type: tuple.TypeBool, Bool: false}, "f"},
		{tuple.Datum{Type: tuple.TypeFloat64, F64: 3.14}, "3.14"},
	}
	for _, tt := range tests {
		got := datumToText(tt.d)
		if got != tt.want {
			t.Errorf("datumToText(%v) = %q, want %q", tt.d, got, tt.want)
		}
	}
}

// testTLSConfig generates a self-signed cert and returns a server and client TLS config.
func testTLSConfig(t *testing.T) (serverCfg *tls.Config, clientCfg *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "loladb-test"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("x509 key pair: %v", err)
	}

	serverCfg = &tls.Config{Certificates: []tls.Certificate{cert}}

	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(certPEM)
	clientCfg = &tls.Config{RootCAs: pool, ServerName: "localhost"}

	return serverCfg, clientCfg
}

func startTLSTestServer(t *testing.T, exec QueryExecutor, tlsCfg *tls.Config) (*Server, string) {
	t.Helper()
	srv := &Server{
		Addr:      "127.0.0.1:0",
		Executor:  exec,
		TLSConfig: tlsCfg,
	}
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv.listener = ln
	srv.Addr = ln.Addr().String()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.handleConn(conn)
		}
	}()

	return srv, srv.Addr
}

func TestTLSUpgrade(t *testing.T) {
	serverCfg, clientCfg := testTLSConfig(t)
	exec := &mockExecutor{results: map[string]*QueryResult{
		"SELECT 1": {
			Columns: []string{"?column?"},
			Rows:    [][]tuple.Datum{{{Type: tuple.TypeInt32, I32: 1}}},
			Message: "SELECT 1",
		},
	}}
	srv, addr := startTLSTestServer(t, exec, serverCfg)
	defer srv.Close()

	// Dial plain TCP.
	rawConn := dial(t, addr)
	defer rawConn.Close()
	rawConn.SetDeadline(time.Now().Add(5 * time.Second))

	// Send SSL request.
	var sslMsg [8]byte
	binary.BigEndian.PutUint32(sslMsg[:4], 8)
	binary.BigEndian.PutUint32(sslMsg[4:], sslRequestCode)
	if _, err := rawConn.Write(sslMsg[:]); err != nil {
		t.Fatal(err)
	}

	// Expect 'S' (SSL accepted).
	resp := make([]byte, 1)
	if _, err := io.ReadFull(rawConn, resp); err != nil {
		t.Fatal(err)
	}
	if resp[0] != 'S' {
		t.Fatalf("expected 'S' (SSL accepted), got %c", resp[0])
	}

	// Upgrade to TLS.
	tlsConn := tls.Client(rawConn, clientCfg)
	if err := tlsConn.Handshake(); err != nil {
		t.Fatalf("TLS handshake: %v", err)
	}

	// Send startup over TLS.
	if err := writeStartup(tlsConn, map[string]string{"user": "test"}); err != nil {
		t.Fatal(err)
	}
	msgs, err := consumeUntilReady(tlsConn)
	if err != nil {
		t.Fatalf("startup over TLS: %v", err)
	}

	gotAuth := false
	for _, m := range msgs {
		if m.typ == 'R' {
			gotAuth = true
		}
	}
	if !gotAuth {
		t.Fatal("missing AuthenticationOk over TLS")
	}

	// Send a query over TLS.
	if err := writeQuery(tlsConn, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	msgs, err = consumeUntilReady(tlsConn)
	if err != nil {
		t.Fatalf("query over TLS: %v", err)
	}

	var gotRowDesc, gotDataRow bool
	for _, m := range msgs {
		switch m.typ {
		case 'T':
			gotRowDesc = true
		case 'D':
			gotDataRow = true
		}
	}
	if !gotRowDesc {
		t.Error("missing RowDescription over TLS")
	}
	if !gotDataRow {
		t.Error("missing DataRow over TLS")
	}
}

func TestTLSDeclinedWithoutConfig(t *testing.T) {
	// Server without TLS config should respond 'N'.
	exec := &mockExecutor{results: map[string]*QueryResult{}}
	srv, addr := startTestServer(t, exec) // no TLS
	defer srv.Close()

	conn := dial(t, addr)
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	var sslMsg [8]byte
	binary.BigEndian.PutUint32(sslMsg[:4], 8)
	binary.BigEndian.PutUint32(sslMsg[4:], sslRequestCode)
	if _, err := conn.Write(sslMsg[:]); err != nil {
		t.Fatal(err)
	}

	resp := make([]byte, 1)
	if _, err := io.ReadFull(conn, resp); err != nil {
		t.Fatal(err)
	}
	if resp[0] != 'N' {
		t.Fatalf("expected 'N' (no SSL), got %c", resp[0])
	}
}

