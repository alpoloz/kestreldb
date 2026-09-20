package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	snapshotMagic = "KDBSNP1\n"
	aofMagic      = "KDBAOF1\n"
	maxDiskImage  = 1 << 30
)

var ErrCorruptData = errors.New("corrupt database data")

type FsyncPolicy uint8

const (
	FsyncAlways FsyncPolicy = iota
	FsyncEverySecond
	FsyncNever
)

type aofFile struct {
	mu     sync.Mutex
	file   *os.File
	policy FsyncPolicy
	stop   chan struct{}
	done   chan struct{}
	err    error
}

// SaveSnapshot atomically replaces a checksum-protected snapshot file.
func (db *DB) SaveSnapshot(path string) error {
	data, err := db.Snapshot()
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, ".kestreldb-snapshot-*")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	header := make([]byte, 16)
	copy(header, snapshotMagic)
	binary.BigEndian.PutUint32(header[8:12], uint32(len(data)))
	binary.BigEndian.PutUint32(header[12:16], crc32.ChecksumIEEE(data))
	if _, err = file.Write(header); err == nil {
		_, err = file.Write(data)
	}
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	parent, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer parent.Close()
	return parent.Sync()
}

func (db *DB) LoadSnapshotFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	header := make([]byte, 16)
	if _, err := io.ReadFull(file, header); err != nil {
		return ErrCorruptData
	}
	if string(header[:8]) != snapshotMagic {
		return ErrCorruptData
	}
	size := binary.BigEndian.Uint32(header[8:12])
	if size > maxDiskImage {
		return ErrCorruptData
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(file, data); err != nil {
		return ErrCorruptData
	}
	if crc32.ChecksumIEEE(data) != binary.BigEndian.Uint32(header[12:16]) {
		return ErrCorruptData
	}
	var trailing [1]byte
	if n, err := file.Read(trailing[:]); n != 0 || err != io.EOF {
		return ErrCorruptData
	}
	return db.LoadSnapshot(data)
}

// OpenAOF replays committed images, truncates an incomplete final record, and
// begins appending subsequent mutations. A complete corrupt record is fatal.
func (db *DB) OpenAOF(path string, policy FsyncPolicy) error {
	if policy > FsyncNever {
		return errors.New("invalid fsync policy")
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	closeOnError := func(err error) error { file.Close(); return err }
	last, offset, validEnd, err := readAOF(file)
	if err != nil {
		return closeOnError(err)
	}
	if len(last) > 0 {
		if err := db.LoadSnapshot(last); err != nil {
			return closeOnError(fmt.Errorf("AOF replay: %w", err))
		}
	}
	if err := file.Truncate(validEnd); err != nil {
		return closeOnError(err)
	}
	if _, err := file.Seek(validEnd, io.SeekStart); err != nil {
		return closeOnError(err)
	}
	aof := &aofFile{file: file, policy: policy}
	if policy == FsyncEverySecond {
		aof.stop = make(chan struct{})
		aof.done = make(chan struct{})
		go aof.syncLoop()
	}
	db.gate.Lock()
	db.mu.Lock()
	if db.journal != nil {
		db.mu.Unlock()
		db.gate.Unlock()
		aof.close()
		return errors.New("journal is already enabled")
	}
	db.journal = &mutationJournal{offset: offset, subscribers: make(map[chan MutationRecord]struct{}), appendFile: aof.append}
	db.aof = aof
	db.mu.Unlock()
	db.gate.Unlock()
	return nil
}

func readAOF(file *os.File) ([]byte, uint64, int64, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, 0, 0, err
	}
	if info.Size() == 0 {
		if _, err := file.Write([]byte(aofMagic)); err != nil {
			return nil, 0, 0, err
		}
		if err := file.Sync(); err != nil {
			return nil, 0, 0, err
		}
		return nil, 0, 8, nil
	}
	magic := make([]byte, 8)
	if _, err := io.ReadFull(file, magic); err != nil || string(magic) != aofMagic {
		return nil, 0, 0, ErrCorruptData
	}
	validEnd := int64(8)
	var last []byte
	var offset uint64
	for {
		header := make([]byte, 16)
		_, err := io.ReadFull(file, header)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, 0, 0, err
		}
		next := binary.BigEndian.Uint64(header[:8])
		size := binary.BigEndian.Uint32(header[8:12])
		if next != offset+1 || size > maxDiskImage {
			return nil, 0, 0, ErrCorruptData
		}
		data := make([]byte, size)
		_, err = io.ReadFull(file, data)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, 0, 0, err
		}
		if crc32.ChecksumIEEE(data) != binary.BigEndian.Uint32(header[12:16]) {
			return nil, 0, 0, ErrCorruptData
		}
		last = data
		offset = next
		validEnd += int64(16 + size)
	}
	return last, offset, validEnd, nil
}

func (a *aofFile) append(record MutationRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return a.err
	}
	if len(record.Snapshot) > maxDiskImage {
		a.err = errors.New("AOF image exceeds maximum size")
		return a.err
	}
	header := make([]byte, 16)
	binary.BigEndian.PutUint64(header[:8], record.Offset)
	binary.BigEndian.PutUint32(header[8:12], uint32(len(record.Snapshot)))
	binary.BigEndian.PutUint32(header[12:16], crc32.ChecksumIEEE(record.Snapshot))
	if _, a.err = a.file.Write(header); a.err != nil {
		return a.err
	}
	if _, a.err = a.file.Write(record.Snapshot); a.err != nil {
		return a.err
	}
	if a.policy == FsyncAlways {
		a.err = a.file.Sync()
	}
	return a.err
}

func (a *aofFile) syncLoop() {
	defer close(a.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			a.mu.Lock()
			if a.err == nil {
				a.err = a.file.Sync()
			}
			a.mu.Unlock()
		case <-a.stop:
			return
		}
	}
}

func (a *aofFile) close() error {
	if a.stop != nil {
		close(a.stop)
		<-a.done
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err == nil {
		a.err = a.file.Sync()
	}
	closeErr := a.file.Close()
	if a.err != nil {
		return a.err
	}
	return closeErr
}

func (db *DB) CloseAOF() error {
	db.gate.Lock()
	db.mu.Lock()
	aof := db.aof
	if aof != nil {
		db.aof = nil
		for ch := range db.journal.subscribers {
			close(ch)
			delete(db.journal.subscribers, ch)
		}
		db.journal = nil
	}
	db.mu.Unlock()
	db.gate.Unlock()
	if aof == nil {
		return nil
	}
	return aof.close()
}
