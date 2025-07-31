package postgres

import (
	"database/sql"
	
	datastorage "schneider.vip/retryspool/storage/data"
	_ "github.com/lib/pq" // PostgreSQL driver
)

// Factory creates PostgreSQL data storage backends
type Factory struct {
	dsn           string
	tableName     string
	maxOpenConns  int
	maxIdleConns  int
}

// NewFactory creates a new PostgreSQL data storage factory
func NewFactory(dsn string) *Factory {
	return &Factory{
		dsn:          dsn,
		tableName:    "retryspool_data",
		maxOpenConns: 25,
		maxIdleConns: 5,
	}
}

// WithTableName sets a custom table name for message data
func (f *Factory) WithTableName(tableName string) *Factory {
	f.tableName = tableName
	return f
}

// WithConnectionLimits sets the maximum number of open and idle connections
func (f *Factory) WithConnectionLimits(maxOpen, maxIdle int) *Factory {
	f.maxOpenConns = maxOpen
	f.maxIdleConns = maxIdle
	return f
}

// Create creates a new PostgreSQL data storage backend
func (f *Factory) Create() (datastorage.Backend, error) {
	db, err := sql.Open("postgres", f.dsn)
	if err != nil {
		return nil, err
	}

	// Configure connection pool
	db.SetMaxOpenConns(f.maxOpenConns)
	db.SetMaxIdleConns(f.maxIdleConns)

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	backend := &Backend{
		db:        db,
		tableName: f.tableName,
	}

	// Create table if it doesn't exist
	if err := backend.createTable(); err != nil {
		db.Close()
		return nil, err
	}

	return backend, nil
}

// Name returns the factory name
func (f *Factory) Name() string {
	return "postgres-data"
}