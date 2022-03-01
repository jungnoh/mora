package storage

func (s *Storage) FlushWal() {
	s.wal.Flush()
}
