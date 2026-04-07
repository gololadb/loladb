package pageio

import (
	"fmt"
	"os"
	"sync"
)

const PageSize = 4096

// PageIO provides raw page-level read/write access to a database file.
// All access is serialized through a mutex. Pages are fixed-size blocks
// of PageSize bytes, addressed by page number.
type PageIO struct {
	mu   sync.Mutex
	file *os.File
}

// Open opens (or creates) the file at path for page I/O.
func Open(path string) (*PageIO, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("pageio: open %s: %w", path, err)
	}
	return &PageIO{file: f}, nil
}

// ReadPage reads the page at pageNum into buf. buf must be exactly PageSize bytes.
// If the page is beyond the current end of file, buf is zeroed out.
func (p *PageIO) ReadPage(pageNum uint32, buf []byte) error {
	if len(buf) != PageSize {
		return fmt.Errorf("pageio: buffer must be %d bytes, got %d", PageSize, len(buf))
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	offset := int64(pageNum) * PageSize
	n, err := p.file.ReadAt(buf, offset)
	if err != nil && n == 0 {
		// Beyond EOF — return a zeroed page
		clear(buf)
		return nil
	}
	if n < PageSize {
		// Partial read (page at end of file) — zero the rest
		clear(buf[n:])
	}
	return nil
}

// WritePage writes buf to the page at pageNum. buf must be exactly PageSize bytes.
// The file is extended with zero pages if pageNum is beyond the current end.
func (p *PageIO) WritePage(pageNum uint32, buf []byte) error {
	if len(buf) != PageSize {
		return fmt.Errorf("pageio: buffer must be %d bytes, got %d", PageSize, len(buf))
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	offset := int64(pageNum) * PageSize
	_, err := p.file.WriteAt(buf, offset)
	if err != nil {
		return fmt.Errorf("pageio: write page %d: %w", pageNum, err)
	}
	return nil
}

// Sync flushes the file's contents to stable storage.
func (p *PageIO) Sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.file.Sync()
}

// FilePages returns the total number of pages currently in the file.
func (p *PageIO) FilePages() (uint32, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	info, err := p.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("pageio: stat: %w", err)
	}
	return uint32(info.Size() / PageSize), nil
}

// Close closes the underlying file.
func (p *PageIO) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.file.Close()
}
