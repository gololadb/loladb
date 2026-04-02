// Package pgwire implements the PostgreSQL v3 frontend/backend wire protocol,
// mirroring the message formats defined in src/include/libpq/protocol.h and
// the startup sequence from src/backend/postmaster and src/backend/tcop/postgres.c.
//
// The protocol flow:
//
//  1. Client sends StartupMessage (length + protocol version + key=value params).
//  2. Server replies AuthenticationOk ('R', int32=0).
//  3. Server sends ParameterStatus messages for server_version, etc.
//  4. Server sends BackendKeyData (pid, secret).
//  5. Server sends ReadyForQuery ('Z', byte='I' for idle).
//  6. Client sends Query ('Q', string) or extended protocol messages.
//  7. Server replies with RowDescription ('T'), DataRow ('D'), CommandComplete ('C').
//  8. Server sends ReadyForQuery after each command cycle.
//  9. Client sends Terminate ('X') to close.
//
// SSL negotiation: if the first 4 bytes after length are the SSL request
// code (80877103), we reply 'S' and upgrade to TLS when a TLSConfig is set,
// or 'N' (no SSL) otherwise.
package pgwire

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/gololadb/loladb/pkg/tuple"
)

// -----------------------------------------------------------------------
// Protocol constants — mirrors src/include/libpq/protocol.h
// -----------------------------------------------------------------------

// Protocol version 3.0.
const protocolVersion3 = 196608 // (3 << 16) | 0

// Special request codes (not real protocol versions).
const (
	sslRequestCode    = 80877103 // (1234 << 16) | 5679
	cancelRequestCode = 80877102 // (1234 << 16) | 5678
)

// Frontend (client → server) message types.
const (
	msgQuery     byte = 'Q'
	msgParse     byte = 'P'
	msgBind      byte = 'B'
	msgDescribe  byte = 'D'
	msgExecute   byte = 'E'
	msgSync      byte = 'S'
	msgClose     byte = 'C'
	msgFlush     byte = 'H'
	msgTerminate byte = 'X'
	msgPassword  byte = 'p' // PasswordMessage (cleartext or MD5)
)

// Authentication request codes — mirrors AUTH_REQ_* in libpq.
const (
	authReqOk              int32 = 0
	authReqCleartextPasswd int32 = 3
	authReqMD5             int32 = 5
)

// Backend (server → client) message types.
const (
	msgAuthentication   byte = 'R'
	msgParameterStatus  byte = 'S'
	msgBackendKeyData   byte = 'K'
	msgReadyForQuery    byte = 'Z'
	msgRowDescription   byte = 'T'
	msgDataRow          byte = 'D'
	msgCommandComplete  byte = 'C'
	msgErrorResponse    byte = 'E'
	msgNoticeResponse   byte = 'N'
	msgEmptyQueryResp   byte = 'I'
	msgParseComplete    byte = '1'
	msgBindComplete     byte = '2'
	msgCloseComplete    byte = '3'
	msgNoData           byte = 'n'
	msgCopyOutResponse  byte = 'H'
	msgCopyData         byte = 'd'
	msgCopyDone         byte = 'c'
)

// Error/Notice field identifiers — mirrors postgres_ext.h PG_DIAG_*.
const (
	fieldSeverity byte = 'S'
	fieldCode     byte = 'C'
	fieldMessage  byte = 'M'
	fieldDetail   byte = 'D'
)

// Transaction status indicators for ReadyForQuery.
const (
	txIdle    byte = 'I' // not in a transaction
	txInTx    byte = 'T' // in a transaction block
	txFailed  byte = 'E' // in a failed transaction block
)

// -----------------------------------------------------------------------
// QueryExecutor — interface to the SQL engine
// -----------------------------------------------------------------------

// QueryResult represents the result of executing a SQL statement.
type QueryResult struct {
	Columns      []string
	Rows         [][]tuple.Datum
	RowsAffected int64
	Message      string
}

// QueryExecutor is the interface the pgwire server uses to execute SQL.
// The sql.Executor satisfies this via a thin adapter.
type QueryExecutor interface {
	Execute(sql string) (*QueryResult, error)
}

// SessionExecutor extends QueryExecutor with per-connection session setup.
// If the executor implements this, pgwire calls SetUser during startup.
type SessionExecutor interface {
	QueryExecutor
	SetUser(user string)
	// NewSession returns a session-scoped executor for a single connection.
	// If not implemented, the server shares one executor across connections.
	NewSession() QueryExecutor
}

// Authenticator validates user credentials during connection startup.
// If nil on the Server, all connections are accepted without authentication.
type Authenticator interface {
	// Authenticate checks the password for a user. Returns nil on success.
	Authenticate(user, password string) error
}

// -----------------------------------------------------------------------
// Server
// -----------------------------------------------------------------------

// Server listens for PostgreSQL wire protocol connections.
type Server struct {
	Addr          string
	Executor      QueryExecutor
	TLSConfig     *tls.Config    // if non-nil, SSL is offered to clients
	Authenticator Authenticator  // if non-nil, password auth is required
	listener      net.Listener
}

// ListenAndServe starts listening and serving connections.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return fmt.Errorf("pgwire: listen: %w", err)
	}
	s.listener = ln
	fmt.Printf("pgwire: listening on %s\n", s.Addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			// Check if listener was closed.
			if ne, ok := err.(*net.OpError); ok && ne.Err.Error() == "use of closed network connection" {
				return nil
			}
			fmt.Printf("pgwire: accept error: %v\n", err)
			continue
		}
		go s.handleConn(conn)
	}
}

// Close stops the server.
func (s *Server) Close() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

// -----------------------------------------------------------------------
// Connection handling
// -----------------------------------------------------------------------

// conn wraps a net.Conn with buffered read/write helpers.
type conn struct {
	nc            net.Conn
	executor      QueryExecutor
	tlsConfig     *tls.Config
	authenticator Authenticator
	params        map[string]string // startup parameters from client
	preparedSQL   string            // last Parse'd statement
	portalSQL     string            // last Bind'd portal
	namedStmts    map[string]string // SQL-level PREPARE name→query
}

func (s *Server) handleConn(nc net.Conn) {
	defer nc.Close()

	// Create a per-connection executor if supported.
	exec := s.Executor
	if se, ok := s.Executor.(SessionExecutor); ok {
		exec = se.NewSession()
	}

	c := &conn{
		nc:            nc,
		executor:      exec,
		tlsConfig:     s.TLSConfig,
		authenticator: s.Authenticator,
		params:        make(map[string]string),
		namedStmts:    make(map[string]string),
	}

	if err := c.startup(); err != nil {
		fmt.Printf("pgwire: startup error from %s: %v\n", nc.RemoteAddr(), err)
		return
	}

	c.messageLoop()
}

// -----------------------------------------------------------------------
// Startup sequence
// -----------------------------------------------------------------------

func (c *conn) startup() error {
	// Read the startup packet: 4-byte length (includes self) + payload.
	for {
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(c.nc, lenBuf); err != nil {
			return fmt.Errorf("read startup length: %w", err)
		}
		msgLen := int(binary.BigEndian.Uint32(lenBuf)) - 4
		if msgLen < 4 || msgLen > 10000 {
			return fmt.Errorf("invalid startup message length: %d", msgLen)
		}

		payload := make([]byte, msgLen)
		if _, err := io.ReadFull(c.nc, payload); err != nil {
			return fmt.Errorf("read startup payload: %w", err)
		}

		version := binary.BigEndian.Uint32(payload[:4])

		switch version {
		case sslRequestCode:
			if c.tlsConfig != nil {
				// Accept SSL — upgrade to TLS.
				c.nc.Write([]byte{'S'})
				tlsConn := tls.Server(c.nc, c.tlsConfig)
				if err := tlsConn.Handshake(); err != nil {
					return fmt.Errorf("tls handshake: %w", err)
				}
				c.nc = tlsConn
			} else {
				// No TLS configured — decline.
				c.nc.Write([]byte{'N'})
			}
			continue // client will send real startup next

		case cancelRequestCode:
			// Cancel request — ignore (we don't support cancellation).
			return fmt.Errorf("cancel request not supported")

		default:
			if version != protocolVersion3 {
				return fmt.Errorf("unsupported protocol version: %d.%d",
					version>>16, version&0xFFFF)
			}
		}

		// Parse key=value pairs from the rest of the payload.
		// Format: key\0value\0key\0value\0...\0
		rest := payload[4:]
		for len(rest) > 1 {
			keyEnd := indexOf(rest, 0)
			if keyEnd < 0 {
				break
			}
			key := string(rest[:keyEnd])
			rest = rest[keyEnd+1:]
			valEnd := indexOf(rest, 0)
			if valEnd < 0 {
				break
			}
			val := string(rest[:valEnd])
			rest = rest[valEnd+1:]
			c.params[key] = val
		}

		break // startup complete
	}

	// Set the session user and authenticate if an authenticator is configured.
	// When no authenticator is set (--no-auth), we leave the session user
	// empty so that privilege checks are bypassed.
	if c.authenticator != nil {
		if user, ok := c.params["user"]; ok {
			if se, ok := c.executor.(SessionExecutor); ok {
				se.SetUser(user)
			}
		}
	}
	if c.authenticator != nil {
		if err := c.authenticate(); err != nil {
			c.sendError("FATAL", "28P01", err.Error())
			return err
		}
	}

	// Send AuthenticationOk.
	c.sendAuthOk()

	// Send ParameterStatus messages.
	c.sendParameterStatus("server_version", "15.0 (LolaDB)")
	c.sendParameterStatus("server_encoding", "UTF8")
	c.sendParameterStatus("client_encoding", "UTF8")
	c.sendParameterStatus("DateStyle", "ISO, MDY")
	c.sendParameterStatus("integer_datetimes", "on")
	c.sendParameterStatus("standard_conforming_strings", "on")

	// Send BackendKeyData (pid=1, secret=0 — we don't support cancel).
	c.sendBackendKeyData(1, 0)

	// Send ReadyForQuery.
	c.sendReadyForQuery(txIdle)

	return nil
}

// -----------------------------------------------------------------------
// Message loop — simple + extended query protocol
// -----------------------------------------------------------------------

func (c *conn) messageLoop() {
	for {
		// Read message type (1 byte) + length (4 bytes).
		header := make([]byte, 5)
		if _, err := io.ReadFull(c.nc, header); err != nil {
			return // client disconnected
		}

		msgType := header[0]
		msgLen := int(binary.BigEndian.Uint32(header[1:])) - 4
		if msgLen < 0 {
			return
		}

		var payload []byte
		if msgLen > 0 {
			payload = make([]byte, msgLen)
			if _, err := io.ReadFull(c.nc, payload); err != nil {
				return
			}
		}

		switch msgType {
		case msgQuery:
			c.handleQuery(payload)

		case msgParse:
			c.handleParse(payload)

		case msgBind:
			c.handleBind(payload)

		case msgDescribe:
			c.handleDescribe(payload)

		case msgExecute:
			c.handleExecute(payload)

		case msgSync:
			c.sendReadyForQuery(txIdle)

		case msgClose:
			c.sendCloseComplete()

		case msgFlush:
			// Nothing to do — we flush after every message.

		case msgTerminate:
			return

		default:
			c.sendError("ERROR", "42000",
				fmt.Sprintf("unrecognized message type: %c", msgType))
			c.sendReadyForQuery(txIdle)
		}
	}
}

// -----------------------------------------------------------------------
// Simple query protocol ('Q')
// -----------------------------------------------------------------------

func (c *conn) handleQuery(payload []byte) {
	sql := cstring(payload)
	if sql == "" {
		c.sendEmptyQuery()
		c.sendReadyForQuery(txIdle)
		return
	}

	// Split on semicolons for multi-statement queries.
	// (Simplified — doesn't handle semicolons inside strings.)
	statements := splitStatements(sql)

	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		c.executeAndSend(stmt)
	}

	c.sendReadyForQuery(txIdle)
}

func (c *conn) executeAndSend(sql string) {
	// Handle SQL-level PREPARE / EXECUTE / DEALLOCATE for pg_dump.
	upper := strings.ToUpper(strings.TrimSpace(sql))
	if strings.HasPrefix(upper, "PREPARE ") {
		c.handleSQLPrepare(sql)
		return
	}
	if strings.HasPrefix(upper, "EXECUTE ") {
		c.handleSQLExecute(sql)
		return
	}
	if strings.HasPrefix(upper, "COPY ") && strings.Contains(upper, "TO STDOUT") {
		c.handleCopyToStdout(sql, upper)
		return
	}

	// Try the pg_dump compatibility interceptor first.
	var provider CatalogProvider
	if cp, ok := c.executor.(CatalogProvider); ok {
		provider = cp
	}
	if result, handled := interceptQuery(sql, provider); handled {
		if len(result.Columns) > 0 {
			c.sendRowDescription(result.Columns)
			for _, row := range result.Rows {
				c.sendDataRow(row)
			}
		}
		c.sendCommandComplete(result.Message)
		return
	}

	result, err := c.executor.Execute(sql)
	if err != nil {
		c.sendError("ERROR", "42000", err.Error())
		return
	}

	if len(result.Columns) > 0 {
		c.sendRowDescription(result.Columns)
		for _, row := range result.Rows {
			c.sendDataRow(row)
		}
	}

	c.sendCommandComplete(result.Message)
}

// -----------------------------------------------------------------------
// Extended query protocol (Parse/Bind/Describe/Execute/Sync)
// -----------------------------------------------------------------------

func (c *conn) handleParse(payload []byte) {
	// Parse message: name\0 query\0 int16(numParams) [int32(paramOID)...]
	rest := payload
	_ = cstringAdvance(&rest) // statement name (ignored)
	query := cstringAdvance(&rest)
	c.preparedSQL = query
	c.sendParseComplete()
}

func (c *conn) handleBind(payload []byte) {
	// Bind message: portal\0 statement\0 ...params...
	// We ignore parameters for now — just bind the prepared SQL.
	c.portalSQL = c.preparedSQL
	c.sendBindComplete()
}

func (c *conn) handleDescribe(payload []byte) {
	// Describe 'S' (statement) or 'P' (portal).
	// For simplicity, send NoData — the actual description comes
	// after Execute via RowDescription.
	if len(payload) > 0 && payload[0] == 'S' {
		// Statement describe — send ParameterDescription + NoData.
		c.sendNoData()
	} else {
		c.sendNoData()
	}
}

func (c *conn) handleExecute(payload []byte) {
	// Execute message: portal\0 int32(maxRows)
	sql := c.portalSQL
	if sql == "" {
		c.sendCommandComplete("OK")
		return
	}
	c.executeAndSend(sql)
}

// -----------------------------------------------------------------------
// Backend message senders
// -----------------------------------------------------------------------

// authenticate performs cleartext password authentication.
// Sends AuthenticationCleartextPassword, reads the PasswordMessage,
// and validates via the Authenticator.
func (c *conn) authenticate() error {
	// Send AuthenticationCleartextPassword request.
	buf := newMsgBuf(msgAuthentication)
	buf.writeInt32(authReqCleartextPasswd)
	c.send(buf)

	// Read PasswordMessage ('p').
	header := make([]byte, 5)
	if _, err := io.ReadFull(c.nc, header); err != nil {
		return fmt.Errorf("read password message: %w", err)
	}
	if header[0] != msgPassword {
		return fmt.Errorf("expected PasswordMessage, got %c", header[0])
	}
	msgLen := int(binary.BigEndian.Uint32(header[1:])) - 4
	if msgLen < 1 || msgLen > 1024 {
		return fmt.Errorf("invalid password message length: %d", msgLen)
	}
	payload := make([]byte, msgLen)
	if _, err := io.ReadFull(c.nc, payload); err != nil {
		return fmt.Errorf("read password: %w", err)
	}
	password := cstring(payload)

	user := c.params["user"]
	return c.authenticator.Authenticate(user, password)
}

func (c *conn) sendAuthOk() {
	// 'R' + int32(8) + int32(0=AUTH_REQ_OK)
	buf := newMsgBuf(msgAuthentication)
	buf.writeInt32(0) // AUTH_REQ_OK
	c.send(buf)
}

func (c *conn) sendParameterStatus(name, value string) {
	buf := newMsgBuf(msgParameterStatus)
	buf.writeCString(name)
	buf.writeCString(value)
	c.send(buf)
}

func (c *conn) sendBackendKeyData(pid, secret int32) {
	buf := newMsgBuf(msgBackendKeyData)
	buf.writeInt32(pid)
	buf.writeInt32(secret)
	c.send(buf)
}

func (c *conn) sendReadyForQuery(txStatus byte) {
	buf := newMsgBuf(msgReadyForQuery)
	buf.writeByte(txStatus)
	c.send(buf)
}

func (c *conn) sendRowDescription(columns []string) {
	buf := newMsgBuf(msgRowDescription)
	buf.writeInt16(int16(len(columns)))
	for _, col := range columns {
		buf.writeCString(col)  // column name
		buf.writeInt32(0)      // table OID
		buf.writeInt16(0)      // column attr number
		buf.writeInt32(25)     // type OID (25 = text)
		buf.writeInt16(-1)     // type size (-1 = variable)
		buf.writeInt32(-1)     // type modifier
		buf.writeInt16(0)      // format code (0 = text)
	}
	c.send(buf)
}

func (c *conn) sendDataRow(row []tuple.Datum) {
	buf := newMsgBuf(msgDataRow)
	buf.writeInt16(int16(len(row)))
	for _, d := range row {
		s := datumToText(d)
		if d.Type == tuple.TypeNull {
			buf.writeInt32(-1) // NULL
		} else {
			buf.writeInt32(int32(len(s)))
			buf.writeBytes([]byte(s))
		}
	}
	c.send(buf)
}

func (c *conn) sendCommandComplete(tag string) {
	buf := newMsgBuf(msgCommandComplete)
	buf.writeCString(tag)
	c.send(buf)
}

func (c *conn) sendError(severity, code, message string) {
	buf := newMsgBuf(msgErrorResponse)
	buf.writeByte(fieldSeverity)
	buf.writeCString(severity)
	buf.writeByte(fieldCode)
	buf.writeCString(code)
	buf.writeByte(fieldMessage)
	buf.writeCString(message)
	buf.writeByte(0) // terminator
	c.send(buf)
}

func (c *conn) sendEmptyQuery() {
	buf := newMsgBuf(msgEmptyQueryResp)
	c.send(buf)
}

func (c *conn) sendParseComplete() {
	buf := newMsgBuf(msgParseComplete)
	c.send(buf)
}

func (c *conn) sendBindComplete() {
	buf := newMsgBuf(msgBindComplete)
	c.send(buf)
}

func (c *conn) sendCloseComplete() {
	buf := newMsgBuf(msgCloseComplete)
	c.send(buf)
}

func (c *conn) sendNoData() {
	buf := newMsgBuf(msgNoData)
	c.send(buf)
}

func (c *conn) send(buf *msgBuf) {
	c.nc.Write(buf.finish())
}

// -----------------------------------------------------------------------
// Message buffer — mirrors pqformat.c pq_beginmessage / pq_endmessage
// -----------------------------------------------------------------------

type msgBuf struct {
	msgType byte
	data    []byte
}

func newMsgBuf(msgType byte) *msgBuf {
	return &msgBuf{msgType: msgType}
}

func (b *msgBuf) writeByte(v byte) {
	b.data = append(b.data, v)
}

func (b *msgBuf) writeInt16(v int16) {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(v))
	b.data = append(b.data, buf[:]...)
}

func (b *msgBuf) writeInt32(v int32) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(v))
	b.data = append(b.data, buf[:]...)
}

func (b *msgBuf) writeCString(s string) {
	b.data = append(b.data, []byte(s)...)
	b.data = append(b.data, 0)
}

func (b *msgBuf) writeBytes(p []byte) {
	b.data = append(b.data, p...)
}

// finish returns the complete wire message: type(1) + length(4) + data.
func (b *msgBuf) finish() []byte {
	length := int32(len(b.data) + 4) // length includes itself
	out := make([]byte, 1+4+len(b.data))
	out[0] = b.msgType
	binary.BigEndian.PutUint32(out[1:], uint32(length))
	copy(out[5:], b.data)
	return out
}

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

// cstring extracts a null-terminated string from a byte slice.
func cstring(b []byte) string {
	end := indexOf(b, 0)
	if end < 0 {
		return string(b)
	}
	return string(b[:end])
}

// cstringAdvance extracts a null-terminated string and advances the slice.
func cstringAdvance(b *[]byte) string {
	end := indexOf(*b, 0)
	if end < 0 {
		s := string(*b)
		*b = nil
		return s
	}
	s := string((*b)[:end])
	*b = (*b)[end+1:]
	return s
}

func indexOf(b []byte, v byte) int {
	for i, c := range b {
		if c == v {
			return i
		}
	}
	return -1
}

// datumToText converts a Datum to its text representation for the wire protocol.
func datumToText(d tuple.Datum) string {
	switch d.Type {
	case tuple.TypeNull:
		return ""
	case tuple.TypeInt32:
		return fmt.Sprintf("%d", d.I32)
	case tuple.TypeInt64:
		return fmt.Sprintf("%d", d.I64)
	case tuple.TypeText:
		return d.Text
	case tuple.TypeBool:
		if d.Bool {
			return "t"
		}
		return "f"
	case tuple.TypeFloat64:
		return fmt.Sprintf("%g", d.F64)
	case tuple.TypeDate:
		return time.Unix(d.I64*86400, 0).UTC().Format("2006-01-02")
	case tuple.TypeTimestamp:
		return time.Unix(0, d.I64*1000).UTC().Format("2006-01-02 15:04:05")
	case tuple.TypeNumeric, tuple.TypeJSON, tuple.TypeUUID:
		return d.Text
	default:
		return fmt.Sprintf("%v", d)
	}
}

// splitStatements splits a SQL string on semicolons.
// This is a simplified version that doesn't handle quoted strings.
func splitStatements(sql string) []string {
	var stmts []string
	var current strings.Builder
	inSingleQuote := false
	inDoubleQuote := false

	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		switch {
		case ch == '\'' && !inDoubleQuote:
			inSingleQuote = !inSingleQuote
			current.WriteByte(ch)
		case ch == '"' && !inSingleQuote:
			inDoubleQuote = !inDoubleQuote
			current.WriteByte(ch)
		case ch == ';' && !inSingleQuote && !inDoubleQuote:
			s := strings.TrimSpace(current.String())
			if s != "" {
				stmts = append(stmts, s)
			}
			current.Reset()
		default:
			current.WriteByte(ch)
		}
	}
	s := strings.TrimSpace(current.String())
	if s != "" {
		stmts = append(stmts, s)
	}
	return stmts
}


// handleSQLPrepare parses SQL-level PREPARE and stores the query text.
// Syntax: PREPARE name(type,...) AS query
// or:     PREPARE name AS query
func (c *conn) handleSQLPrepare(sql string) {
	// Strip "PREPARE " prefix.
	rest := strings.TrimSpace(sql[len("PREPARE "):])

	// Extract name — everything up to '(' or whitespace.
	var name string
	idx := strings.IndexAny(rest, "( \t")
	if idx < 0 {
		c.sendError("ERROR", "42601", "invalid PREPARE syntax")
		return
	}
	name = strings.ToLower(rest[:idx])
	rest = rest[idx:]

	// Skip optional parameter type list: (...).
	if rest[0] == '(' {
		paren := strings.Index(rest, ")")
		if paren < 0 {
			c.sendError("ERROR", "42601", "invalid PREPARE syntax: unmatched parenthesis")
			return
		}
		rest = rest[paren+1:]
	}

	// Find "AS" keyword (may be followed by space or newline).
	rest = strings.TrimSpace(rest)
	upper := strings.ToUpper(rest)
	if len(upper) >= 3 && upper[:2] == "AS" && (upper[2] == ' ' || upper[2] == '\n' || upper[2] == '\r' || upper[2] == '\t') {
		rest = rest[2:]
	} else {
		c.sendError("ERROR", "42601", "invalid PREPARE syntax: missing AS")
		return
	}
	query := strings.TrimSpace(rest)

	c.namedStmts[name] = query
	c.sendCommandComplete("PREPARE")
}

var reParam = regexp.MustCompile(`\$(\d+)`)

// handleSQLExecute runs a previously PREPAREd statement with parameter substitution.
// Syntax: EXECUTE name(val, val, ...)
// or:     EXECUTE name
func (c *conn) handleSQLExecute(sql string) {
	rest := strings.TrimSpace(sql[len("EXECUTE "):])

	// Extract name.
	var name string
	idx := strings.IndexAny(rest, "( \t;")
	if idx < 0 {
		name = strings.ToLower(strings.TrimRight(rest, "; \t"))
	} else {
		name = strings.ToLower(rest[:idx])
		rest = rest[idx:]
	}

	query, ok := c.namedStmts[name]
	if !ok {
		c.sendError("ERROR", "26000", fmt.Sprintf("prepared statement %q does not exist", name))
		return
	}

	// Extract parameter values if present.
	var params []string
	if idx >= 0 && len(rest) > 0 && rest[0] == '(' {
		end := strings.LastIndex(rest, ")")
		if end > 0 {
			paramStr := rest[1:end]
			params = splitParams(paramStr)
		}
	}

	// Substitute $1, $2, ... with actual values.
	resolved := reParam.ReplaceAllStringFunc(query, func(m string) string {
		numStr := m[1:]
		n := 0
		for _, ch := range numStr {
			n = n*10 + int(ch-'0')
		}
		if n >= 1 && n <= len(params) {
			return params[n-1]
		}
		return m
	})

	// Route through the normal query path (which includes the interceptor).
	c.executeAndSend(resolved)
}

// splitParams splits a comma-separated parameter list, respecting quoted strings.
func splitParams(s string) []string {
	var params []string
	var current strings.Builder
	inQuote := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch == '\'' && !inQuote:
			inQuote = true
			current.WriteByte(ch)
		case ch == '\'' && inQuote:
			if i+1 < len(s) && s[i+1] == '\'' {
				current.WriteString("''")
				i++
			} else {
				inQuote = false
				current.WriteByte(ch)
			}
		case ch == ',' && !inQuote:
			params = append(params, strings.TrimSpace(current.String()))
			current.Reset()
		default:
			current.WriteByte(ch)
		}
	}
	if current.Len() > 0 {
		params = append(params, strings.TrimSpace(current.String()))
	}
	return params
}

// handleCopyToStdout implements COPY table TO stdout for pg_dump.
// Converts to SELECT, then sends results in COPY text format.
func (c *conn) handleCopyToStdout(sql, upper string) {
	// Parse: COPY schema.table (col1, col2, ...) TO stdout;
	// Extract table name and optional column list.
	rest := strings.TrimSpace(sql[len("COPY "):])

	// Find table name (possibly schema-qualified).
	var tableName string
	idx := strings.IndexAny(rest, " (")
	if idx < 0 {
		c.sendError("ERROR", "42601", "invalid COPY syntax")
		return
	}
	tableName = rest[:idx]
	// Strip schema prefix.
	if dot := strings.LastIndex(tableName, "."); dot >= 0 {
		tableName = tableName[dot+1:]
	}

	// Build SELECT query.
	selectSQL := fmt.Sprintf("SELECT * FROM %s", tableName)

	result, err := c.executor.Execute(selectSQL)
	if err != nil {
		c.sendError("ERROR", "42000", err.Error())
		return
	}

	// Send CopyOutResponse: text format, N columns.
	numCols := len(result.Columns)
	{
		buf := newMsgBuf(msgCopyOutResponse)
		buf.writeByte(0) // text format overall
		buf.writeInt16(int16(numCols))
		for i := 0; i < numCols; i++ {
			buf.writeInt16(0) // text format per column
		}
		c.send(buf)
	}

	// Send CopyData for each row (tab-separated, newline-terminated).
	for _, row := range result.Rows {
		var line strings.Builder
		for i, d := range row {
			if i > 0 {
				line.WriteByte('\t')
			}
			if d.Type == tuple.TypeNull {
				line.WriteString("\\N")
			} else {
				line.WriteString(datumToText(d))
			}
		}
		line.WriteByte('\n')

		buf := newMsgBuf(msgCopyData)
		buf.writeBytes([]byte(line.String()))
		c.send(buf)
	}

	// Send CopyDone.
	{
		buf := newMsgBuf(msgCopyDone)
		c.send(buf)
	}

	c.sendCommandComplete(fmt.Sprintf("COPY %d", len(result.Rows)))
}
