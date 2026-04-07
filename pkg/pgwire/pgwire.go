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
	msgCopyInResponse   byte = 'G'
	msgCopyData         byte = 'd'
	msgCopyDone         byte = 'c'
	msgCopyFail         byte = 'f'
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
	// CopyData holds formatted output for COPY TO STDOUT.
	CopyData string
	// CopyStmt is non-nil when a COPY FROM STDIN was parsed and the
	// pgwire layer needs to initiate the COPY sub-protocol.
	CopyStmt interface{}
}

// QueryExecutor is the interface the pgwire server uses to execute SQL.
// The sql.Executor satisfies this via a thin adapter.
type QueryExecutor interface {
	Execute(sql string) (*QueryResult, error)
}

// CopyExecutor extends QueryExecutor with the ability to feed COPY FROM
// STDIN data back into the SQL engine after the pgwire layer collects it.
type CopyExecutor interface {
	QueryExecutor
	// ExecuteCopyFromData inserts rows from COPY data lines.
	// The copyStmt is the opaque CopyStmt from QueryResult.CopyStmt.
	ExecuteCopyFromData(copyStmt interface{}, lines []string) (*QueryResult, error)
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

	// COPY TO STDOUT — send data via the COPY out sub-protocol.
	if result.CopyData != "" {
		c.sendCopyOut(result)
		return
	}

	// COPY FROM STDIN — initiate the COPY in sub-protocol.
	if result.CopyStmt != nil {
		c.handleCopyIn(result)
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
	case tuple.TypeInterval:
		return formatIntervalPgwire(d.I32, d.I64)
	case tuple.TypeBytea:
		return d.Text
	case tuple.TypeArray:
		return d.Text
	case tuple.TypeMoney:
		dollars := d.I64 / 100
		cents := d.I64 % 100
		if cents < 0 {
			cents = -cents
		}
		return fmt.Sprintf("$%d.%02d", dollars, cents)
	default:
		return fmt.Sprintf("%v", d)
	}
}

func formatIntervalPgwire(months int32, microseconds int64) string {
	var parts []string
	if months != 0 {
		years := months / 12
		mons := months % 12
		if years != 0 {
			parts = append(parts, fmt.Sprintf("%d years", years))
		}
		if mons != 0 {
			parts = append(parts, fmt.Sprintf("%d mons", mons))
		}
	}
	totalUS := microseconds
	days := totalUS / (24 * 3600 * 1e6)
	totalUS -= days * 24 * 3600 * 1e6
	if days != 0 {
		parts = append(parts, fmt.Sprintf("%d days", days))
	}
	if totalUS != 0 || len(parts) == 0 {
		negative := totalUS < 0
		if negative {
			totalUS = -totalUS
		}
		hours := totalUS / (3600 * 1e6)
		totalUS -= hours * 3600 * 1e6
		mins := totalUS / (60 * 1e6)
		totalUS -= mins * 60 * 1e6
		secs := totalUS / 1e6
		timeStr := fmt.Sprintf("%02d:%02d:%02d", hours, mins, secs)
		if negative {
			timeStr = "-" + timeStr
		}
		parts = append(parts, timeStr)
	}
	return strings.Join(parts, " ")
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


// sendCopyOut sends COPY TO STDOUT data via the pgwire COPY out sub-protocol.
func (c *conn) sendCopyOut(result *QueryResult) {
	numCols := len(result.Columns)

	// Send CopyOutResponse: text format, N columns.
	{
		buf := newMsgBuf(msgCopyOutResponse)
		buf.writeByte(0) // text format overall
		buf.writeInt16(int16(numCols))
		for i := 0; i < numCols; i++ {
			buf.writeInt16(0) // text format per column
		}
		c.send(buf)
	}

	// Send CopyData — one message per line of formatted output.
	for _, line := range strings.SplitAfter(result.CopyData, "\n") {
		if line == "" {
			continue
		}
		buf := newMsgBuf(msgCopyData)
		buf.writeBytes([]byte(line))
		c.send(buf)
	}

	// Send CopyDone.
	{
		buf := newMsgBuf(msgCopyDone)
		c.send(buf)
	}

	c.sendCommandComplete(result.Message)
}

// handleCopyIn implements the COPY FROM STDIN pgwire sub-protocol.
// Sends CopyInResponse, reads CopyData messages until CopyDone or CopyFail,
// then feeds the collected data to the SQL engine.
func (c *conn) handleCopyIn(result *QueryResult) {
	copyStmt := result.CopyStmt

	ce, ok := c.executor.(CopyExecutor)
	if !ok {
		c.sendError("ERROR", "0A000", "COPY FROM STDIN not supported by this executor")
		return
	}

	// Send CopyInResponse: text format, 0 columns (column count is informational).
	{
		buf := newMsgBuf(msgCopyInResponse)
		buf.writeByte(0) // text format overall
		buf.writeInt16(0)
		c.send(buf)
	}

	// Read CopyData messages until CopyDone or CopyFail.
	var lines []string
	var accumulated strings.Builder

	for {
		header := make([]byte, 5)
		if _, err := io.ReadFull(c.nc, header); err != nil {
			return // client disconnected
		}

		msgType := header[0]
		msgLen := int(binary.BigEndian.Uint32(header[1:])) - 4
		if msgLen < 0 {
			msgLen = 0
		}

		var payload []byte
		if msgLen > 0 {
			payload = make([]byte, msgLen)
			if _, err := io.ReadFull(c.nc, payload); err != nil {
				return
			}
		}

		switch msgType {
		case msgCopyData:
			// Accumulate data. CopyData messages may contain partial lines
			// or multiple lines, so we buffer and split on newlines.
			accumulated.Write(payload)

		case msgCopyDone:
			// Split accumulated data into lines.
			data := accumulated.String()
			for _, line := range strings.Split(data, "\n") {
				if line == "" {
					continue
				}
				// Stop at the \. terminator.
				if line == "\\." {
					break
				}
				lines = append(lines, line)
			}

			// Feed data to the SQL engine.
			insertResult, err := ce.ExecuteCopyFromData(copyStmt, lines)
			if err != nil {
				c.sendError("ERROR", "22000", err.Error())
				return
			}
			c.sendCommandComplete(insertResult.Message)
			return

		case msgCopyFail:
			// Client aborted the COPY.
			errMsg := cstring(payload)
			c.sendError("ERROR", "57014", fmt.Sprintf("COPY FROM STDIN failed: %s", errMsg))
			return

		default:
			// Unexpected message during COPY — protocol error.
			c.sendError("ERROR", "08P01",
				fmt.Sprintf("unexpected message type %c during COPY", msgType))
			return
		}
	}
}
