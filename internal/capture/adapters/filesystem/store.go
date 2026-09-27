package filesystem

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type Store struct {
	root string
}

func NewStore(root string) (*Store, error) {
	if strings.TrimSpace(root) == "" {
		return nil, fmt.Errorf("storage root is required")
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("create storage root: %w", err)
	}
	return &Store{root: root}, nil
}

func (s *Store) PutAtomically(ctx context.Context, key string, src io.Reader) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	path, err := s.safePath(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create image directory: %w", err)
	}

	// The database advisory lock serializes writers for the same idempotency key.
	// A leftover final file therefore means a previous attempt reached the filesystem
	// but did not commit its DB transaction; it is safe to replace it here because the
	// transaction has already proved that no committed capture exists for the key.
	tmp, err := os.CreateTemp(filepath.Dir(path), ".capture-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary image: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}
	defer cleanup()

	if _, err := io.Copy(tmp, src); err != nil {
		return fmt.Errorf("write temporary image: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync temporary image: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary image: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("promote image: %w", err)
	}
	// Reaching this point means the final file exists before the database commit.
	return nil
}

func (s *Store) safePath(key string) (string, error) {
	clean := filepath.Clean(key)
	if clean == "." || strings.HasPrefix(clean, "../") || strings.Contains(clean, string(filepath.Separator)+"../") || filepath.IsAbs(clean) {
		return "", fmt.Errorf("unsafe storage key")
	}
	return filepath.Join(s.root, clean), nil
}

func (s *Store) Root() string { return s.root }
