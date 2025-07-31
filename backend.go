package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"time"
)

// Backend implements PostgreSQL data storage
type Backend struct {
	db        *sql.DB
	tableName string
}

// createTable creates the data table if it doesn't exist
func (b *Backend) createTable() error {
	query := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		message_id VARCHAR(255) PRIMARY KEY,
		data BYTEA NOT NULL,
		size BIGINT NOT NULL,
		created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
		updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
	);
	
	-- Create index for timestamp-based queries
	CREATE INDEX IF NOT EXISTS idx_%s_created ON %s(created);
	CREATE INDEX IF NOT EXISTS idx_%s_updated ON %s(updated);
	`,
		b.tableName,
		b.tableName, b.tableName,
		b.tableName, b.tableName,
	)

	_, err := b.db.Exec(query)
	return err
}

// StoreData stores message data and returns the actual size written
func (b *Backend) StoreData(ctx context.Context, messageID string, data io.Reader) (int64, error) {
	// Read all data into memory (PostgreSQL bytea limitation)
	dataBytes, err := io.ReadAll(data)
	if err != nil {
		return 0, fmt.Errorf("failed to read data: %w", err)
	}

	size := int64(len(dataBytes))
	now := time.Now()

	query := fmt.Sprintf(`
		INSERT INTO %s (message_id, data, size, created, updated)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (message_id) DO UPDATE SET
			data = EXCLUDED.data,
			size = EXCLUDED.size,
			updated = EXCLUDED.updated
	`, b.tableName)

	_, err = b.db.ExecContext(ctx, query, messageID, dataBytes, size, now, now)
	if err != nil {
		return 0, fmt.Errorf("failed to store data for message %s: %w", messageID, err)
	}

	return size, nil
}

// GetDataReader returns a reader for message data
func (b *Backend) GetDataReader(ctx context.Context, messageID string) (io.ReadCloser, error) {
	query := fmt.Sprintf(`SELECT data FROM %s WHERE message_id = $1`, b.tableName)
	
	var data []byte
	row := b.db.QueryRowContext(ctx, query, messageID)
	err := row.Scan(&data)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("data for message %s not found", messageID)
		}
		return nil, fmt.Errorf("failed to get data for message %s: %w", messageID, err)
	}

	return &bytesReadCloser{data: data}, nil
}

// bytesReadCloser wraps byte slice as ReadCloser
type bytesReadCloser struct {
	data   []byte
	offset int
}

func (brc *bytesReadCloser) Read(p []byte) (n int, err error) {
	if brc.offset >= len(brc.data) {
		return 0, io.EOF
	}
	
	n = copy(p, brc.data[brc.offset:])
	brc.offset += n
	return n, nil
}

func (brc *bytesReadCloser) Close() error {
	return nil
}

// GetDataWriter returns a writer for message data
func (b *Backend) GetDataWriter(ctx context.Context, messageID string) (io.WriteCloser, error) {
	return &postgresDataWriter{
		backend:   b,
		messageID: messageID,
		ctx:       ctx,
		buffer:    make([]byte, 0),
	}, nil
}

// postgresDataWriter implements WriteCloser for PostgreSQL data storage
type postgresDataWriter struct {
	backend   *Backend
	messageID string
	ctx       context.Context
	buffer    []byte
	closed    bool
}

func (pdw *postgresDataWriter) Write(p []byte) (int, error) {
	if pdw.closed {
		return 0, fmt.Errorf("writer is closed")
	}
	
	pdw.buffer = append(pdw.buffer, p...)
	return len(p), nil
}

func (pdw *postgresDataWriter) Close() error {
	if pdw.closed {
		return nil
	}
	
	pdw.closed = true
	
	if len(pdw.buffer) == 0 {
		return nil
	}

	size := int64(len(pdw.buffer))
	now := time.Now()

	query := fmt.Sprintf(`
		INSERT INTO %s (message_id, data, size, created, updated)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (message_id) DO UPDATE SET
			data = EXCLUDED.data,
			size = EXCLUDED.size,
			updated = EXCLUDED.updated
	`, pdw.backend.tableName)

	_, err := pdw.backend.db.ExecContext(pdw.ctx, query, pdw.messageID, pdw.buffer, size, now, now)
	if err != nil {
		return fmt.Errorf("failed to store data for message %s: %w", pdw.messageID, err)
	}

	return nil
}

// DeleteData removes message data
func (b *Backend) DeleteData(ctx context.Context, messageID string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE message_id = $1`, b.tableName)
	
	result, err := b.db.ExecContext(ctx, query, messageID)
	if err != nil {
		return fmt.Errorf("failed to delete data for message %s: %w", messageID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("data for message %s not found", messageID)
	}

	return nil
}

// Close closes the database connection
func (b *Backend) Close() error {
	return b.db.Close()
}