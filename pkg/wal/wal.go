package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

// Record represents a single WAL entry. Each record captures a
// physical change to a region within a data page.
type Record struct {
	LSN     uint32 // log sequence number (assigned on append)
	XID     uint32 // transaction ID
	PageNum uint32 // target data page
	Offset  uint16 // byte offset within the page
	Len     uint16 // length of Data
	Data    []byte // the bytes written at [Offset : Offset+Len]
}

// headerSize is the fixed portion of a serialized WAL record (before Data).
const headerSize = 4 + 4 + 4 + 2 + 2 // LSN + XID + PageNum + Offset + Len = 16 bytes

// WAL is an append-only write-ahead log backed by a file.
type WAL struct {
	mu      sync.Mutex
	file    *os.File
	nextLSN uint32
}

// Open opens (or creates) a WAL file. If the file already contains
// records the next LSN is set past the last record so that new
// appends continue the sequence.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}

	w := &WAL{file: f, nextLSN: 1}

	// Scan to find the highest existing LSN.
	recs, err := w.readAll()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: scan existing records: %w", err)
	}
	if len(recs) > 0 {
		w.nextLSN = recs[len(recs)-1].LSN + 1
	}

	// Seek to end for appending.
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: seek to end: %w", err)
	}

	return w, nil
}

// Append writes a WAL record and fsyncs the WAL file. It assigns
// and returns the LSN for the new record.
func (w *WAL) Append(xid, pageNum uint32, offset, length uint16, data []byte) (uint32, error) {
	if int(length) != len(data) {
		return 0, fmt.Errorf("wal: length %d does not match data size %d", length, len(data))
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	lsn := w.nextLSN
	w.nextLSN++

	rec := Record{
		LSN:     lsn,
		XID:     xid,
		PageNum: pageNum,
		Offset:  offset,
		Len:     length,
		Data:    data,
	}

	buf := serializeRecord(&rec)
	if _, err := w.file.Write(buf); err != nil {
		return 0, fmt.Errorf("wal: write record LSN %d: %w", lsn, err)
	}
	if err := w.file.Sync(); err != nil {
		return 0, fmt.Errorf("wal: sync after LSN %d: %w", lsn, err)
	}
	return lsn, nil
}

// ReadAll returns every record currently in the WAL file, in order.
func (w *WAL) ReadAll() ([]Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.readAll()
}

// readAll is the unlocked version of ReadAll. Caller must hold w.mu.
func (w *WAL) readAll() ([]Record, error) {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var recs []Record
	for {
		rec, err := deserializeRecord(w.file)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, err
		}
		recs = append(recs, *rec)
	}
	return recs, nil
}

// Truncate discards all WAL records by truncating the file to zero length.
// This is called after a successful checkpoint.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(0); err != nil {
		return fmt.Errorf("wal: truncate: %w", err)
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: seek after truncate: %w", err)
	}
	return w.file.Sync()
}

// NextLSN returns the LSN that will be assigned to the next appended record.
func (w *WAL) NextLSN() uint32 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nextLSN
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// serializeRecord encodes a record into bytes.
func serializeRecord(r *Record) []byte {
	buf := make([]byte, headerSize+int(r.Len))
	binary.LittleEndian.PutUint32(buf[0:4], r.LSN)
	binary.LittleEndian.PutUint32(buf[4:8], r.XID)
	binary.LittleEndian.PutUint32(buf[8:12], r.PageNum)
	binary.LittleEndian.PutUint16(buf[12:14], r.Offset)
	binary.LittleEndian.PutUint16(buf[14:16], r.Len)
	copy(buf[16:], r.Data)
	return buf
}

// deserializeRecord reads one record from r. Returns io.EOF at end of file.
func deserializeRecord(r io.Reader) (*Record, error) {
	hdr := make([]byte, headerSize)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, err // io.EOF or io.ErrUnexpectedEOF
	}

	rec := &Record{
		LSN:     binary.LittleEndian.Uint32(hdr[0:4]),
		XID:     binary.LittleEndian.Uint32(hdr[4:8]),
		PageNum: binary.LittleEndian.Uint32(hdr[8:12]),
		Offset:  binary.LittleEndian.Uint16(hdr[12:14]),
		Len:     binary.LittleEndian.Uint16(hdr[14:16]),
	}

	if rec.Len > 0 {
		rec.Data = make([]byte, rec.Len)
		if _, err := io.ReadFull(r, rec.Data); err != nil {
			return nil, err
		}
	}
	return rec, nil
}
