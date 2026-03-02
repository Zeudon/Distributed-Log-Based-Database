package storage

import "sync"

// MemTable is a thread-safe in-memory key-value store that represents the
// current applied state machine. It is rebuilt from the WAL and snapshot on
// node startup and kept up-to-date as Raft log entries are applied.
type MemTable struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewMemTable constructs an empty MemTable.
func NewMemTable() *MemTable {
	return &MemTable{data: make(map[string]string)}
}

// Get retrieves a value by key. Returns ("", false) if not found.
func (m *MemTable) Get(key string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.data[key]
	return v, ok
}

// Set inserts or overwrites key with value.
func (m *MemTable) Set(key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
}

// Delete removes key from the table. No-op if the key does not exist.
func (m *MemTable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
}

// Snapshot returns a deep copy of the current map, safe to serialise without
// holding the lock.
func (m *MemTable) Snapshot() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cp := make(map[string]string, len(m.data))
	for k, v := range m.data {
		cp[k] = v
	}
	return cp
}

// Restore replaces the entire MemTable contents with the provided map.
// Used when installing a snapshot from the leader.
func (m *MemTable) Restore(data map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string]string, len(data))
	for k, v := range data {
		m.data[k] = v
	}
}

// Len returns the number of keys currently stored.
func (m *MemTable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}
