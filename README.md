# PostgreSQL Data Storage for RetrySpool

PostgreSQL implementation for RetrySpool message data storage.

## Overview

This module provides a PostgreSQL backend for storing message data in RetrySpool queues. It implements the `datastorage.Backend` interface using PostgreSQL BYTEA columns for binary data storage.

## Features

- **Binary Data Storage**: Uses BYTEA for efficient binary data storage
- **Upsert Operations**: ON CONFLICT support for updating existing messages
- **Streaming Interface**: Support for both Reader and Writer interfaces
- **Connection Pooling**: Configurable connection limits for optimal resource usage
- **Custom Table Names**: Support for custom table names in multi-tenant scenarios
- **Large Data Support**: Handles large message payloads efficiently

## Installation

```bash
go get schneider.vip/retryspool/storage/data/postgres
```

## Usage

### Basic Usage

```go
package main

import (
    "context"
    
    "schneider.vip/retryspool"
    postgres "schneider.vip/retryspool/storage/data/postgres"
)

func main() {
    // Create PostgreSQL data storage factory
    dataFactory := postgres.NewFactory("postgres://user:password@localhost/retryspool?sslmode=disable")
    
    // Create queue with PostgreSQL data storage
    queue := retryspool.New(
        retryspool.WithDataStorage(dataFactory),
        // ... other options
    )
    defer queue.Close()
}
```

### Combined with Meta Storage

```go
import (
    metaPostgres "schneider.vip/retryspool/storage/meta/postgres"
    dataPostgres "schneider.vip/retryspool/storage/data/postgres"
)

func main() {
    dsn := "postgres://user:password@localhost/retryspool?sslmode=disable"
    
    // Use PostgreSQL for both meta and data
    metaFactory := metaPostgres.NewFactory(dsn)
    dataFactory := dataPostgres.NewFactory(dsn)
    
    queue := retryspool.New(
        retryspool.WithMetaStorage(metaFactory),
        retryspool.WithDataStorage(dataFactory),
    )
    defer queue.Close()
}
```

### Advanced Configuration

```go
// Custom table name and connection limits
dataFactory := postgres.NewFactory(dsn).
    WithTableName("custom_data").
    WithConnectionLimits(50, 10) // max 50 open, 10 idle connections

queue := retryspool.New(
    retryspool.WithDataStorage(dataFactory),
)
```

## Database Schema

The module automatically creates the following table structure:

```sql
CREATE TABLE retryspool_data (
    message_id VARCHAR(255) PRIMARY KEY,
    data BYTEA NOT NULL,
    size BIGINT NOT NULL,
    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Indexes for timestamp-based queries
CREATE INDEX idx_retryspool_data_created ON retryspool_data(created);
CREATE INDEX idx_retryspool_data_updated ON retryspool_data(updated);
```

## Configuration Options

### Factory Options

- `WithTableName(name string)`: Set custom table name (default: "retryspool_data")
- `WithConnectionLimits(maxOpen, maxIdle int)`: Configure connection pool (default: 25, 5)

### Environment Variables

For testing, you can set:
- `POSTGRES_TEST_DSN`: PostgreSQL connection string for tests

## API Methods

### StoreData
```go
size, err := backend.StoreData(ctx, messageID, dataReader)
```
Stores message data and returns the actual size written. Uses upsert (INSERT ... ON CONFLICT) to handle updates.

### GetDataReader
```go
reader, err := backend.GetDataReader(ctx, messageID)
defer reader.Close()
```
Returns a ReadCloser for streaming message data.

### GetDataWriter
```go
writer, err := backend.GetDataWriter(ctx, messageID)
defer writer.Close()
```
Returns a WriteCloser for streaming message data. Data is buffered and written on Close().

### DeleteData
```go
err := backend.DeleteData(ctx, messageID)
```
Removes message data from storage.

## Performance Considerations

- **BYTEA Storage**: Efficient binary storage with automatic compression
- **Connection Pooling**: Configurable limits to balance performance and resource usage
- **Streaming Interface**: Memory-efficient handling of large messages
- **Upsert Operations**: Efficient updates using ON CONFLICT
- **Indexes**: Optimized for timestamp-based cleanup operations

## Limitations

- **Memory Usage**: Data is loaded into memory during read/write operations
- **BYTEA Limits**: Subject to PostgreSQL's BYTEA size limitations
- **No Transactions**: Data operations don't use transactions (single operations are atomic)

## Error Handling

The module handles various error conditions:
- **Not Found**: Returns descriptive errors when message data doesn't exist
- **Connection Errors**: Proper error wrapping for debugging
- **Size Mismatches**: Validation of data sizes during operations

## Testing

```bash
# Set PostgreSQL connection for testing
export POSTGRES_TEST_DSN="postgres://user:password@localhost/test_db?sslmode=disable"

# Run tests
go test -v
```

Tests include:
- Basic CRUD operations
- Large data handling (100KB+ messages)
- Writer interface functionality
- Update operations
- Error conditions

## Dependencies

- `github.com/lib/pq`: PostgreSQL driver
- `schneider.vip/retryspool/storage/data`: Data storage interface

## License

Same as RetrySpool main project.