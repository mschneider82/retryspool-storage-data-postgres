package postgres

import (
	"context"
	"database/sql"
	"io"
	"os"
	"strings"
	"testing"

	_ "github.com/lib/pq"
)

func getTestDSN() string {
	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:password@localhost/retryspool_test?sslmode=disable"
	}
	return dsn
}

func setupTestDB(t *testing.T) *Backend {
	dsn := getTestDSN()
	
	// Try to create a test database
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}

	factory := NewFactory(dsn).WithTableName("test_data")
	backend, err := factory.Create()
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}

	return backend.(*Backend)
}

func cleanupTestDB(t *testing.T, backend *Backend) {
	// Clean up test data
	_, err := backend.db.Exec("DROP TABLE IF EXISTS test_data")
	if err != nil {
		t.Logf("Failed to cleanup test table: %v", err)
	}
	backend.Close()
}

func TestBackend_StoreData(t *testing.T) {
	backend := setupTestDB(t)
	defer cleanupTestDB(t, backend)

	ctx := context.Background()
	messageID := "test-message-1"
	testData := "This is test message data that will be stored in PostgreSQL."

	// Store data
	size, err := backend.StoreData(ctx, messageID, strings.NewReader(testData))
	if err != nil {
		t.Fatalf("Failed to store data: %v", err)
	}

	expectedSize := int64(len(testData))
	if size != expectedSize {
		t.Errorf("Size mismatch: expected %d, got %d", expectedSize, size)
	}

	// Verify data was stored by reading it back
	reader, err := backend.GetDataReader(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get data reader: %v", err)
	}
	defer reader.Close()

	retrievedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	if string(retrievedData) != testData {
		t.Errorf("Data mismatch: expected %s, got %s", testData, string(retrievedData))
	}
}

func TestBackend_GetDataReader(t *testing.T) {
	backend := setupTestDB(t)
	defer cleanupTestDB(t, backend)

	ctx := context.Background()
	messageID := "test-message-2"
	testData := "Test data for reader functionality."

	// Store data first
	_, err := backend.StoreData(ctx, messageID, strings.NewReader(testData))
	if err != nil {
		t.Fatalf("Failed to store data: %v", err)
	}

	// Get reader
	reader, err := backend.GetDataReader(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get data reader: %v", err)
	}
	defer reader.Close()

	// Read data
	retrievedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	if string(retrievedData) != testData {
		t.Errorf("Data mismatch: expected %s, got %s", testData, string(retrievedData))
	}

	// Test reading from non-existent message
	_, err = backend.GetDataReader(ctx, "non-existent")
	if err == nil {
		t.Error("Expected error when getting reader for non-existent message")
	}
}

func TestBackend_GetDataWriter(t *testing.T) {
	backend := setupTestDB(t)
	defer cleanupTestDB(t, backend)

	ctx := context.Background()
	messageID := "test-message-3"
	testData := "Test data written using writer interface."

	// Get writer
	writer, err := backend.GetDataWriter(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get data writer: %v", err)
	}

	// Write data in chunks
	chunk1 := "Test data written "
	chunk2 := "using writer interface."

	_, err = writer.Write([]byte(chunk1))
	if err != nil {
		t.Fatalf("Failed to write first chunk: %v", err)
	}

	_, err = writer.Write([]byte(chunk2))
	if err != nil {
		t.Fatalf("Failed to write second chunk: %v", err)
	}

	// Close writer to commit data
	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Verify written data
	reader, err := backend.GetDataReader(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get data reader: %v", err)
	}
	defer reader.Close()

	retrievedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read written data: %v", err)
	}

	if string(retrievedData) != testData {
		t.Errorf("Written data mismatch: expected %s, got %s", testData, string(retrievedData))
	}
}

func TestBackend_DeleteData(t *testing.T) {
	backend := setupTestDB(t)
	defer cleanupTestDB(t, backend)

	ctx := context.Background()
	messageID := "test-message-4"
	testData := "Test data for deletion."

	// Store data
	_, err := backend.StoreData(ctx, messageID, strings.NewReader(testData))
	if err != nil {
		t.Fatalf("Failed to store data: %v", err)
	}

	// Delete data
	err = backend.DeleteData(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to delete data: %v", err)
	}

	// Verify deletion
	_, err = backend.GetDataReader(ctx, messageID)
	if err == nil {
		t.Error("Expected error when getting deleted data")
	}

	// Try to delete non-existent data
	err = backend.DeleteData(ctx, "non-existent")
	if err == nil {
		t.Error("Expected error when deleting non-existent data")
	}
}

func TestBackend_LargeData(t *testing.T) {
	backend := setupTestDB(t)
	defer cleanupTestDB(t, backend)

	ctx := context.Background()
	messageID := "test-large-message"
	
	// Create large test data (100KB)
	largeData := strings.Repeat("This is a line of test data that will be repeated many times to create a large dataset.\n", 1000)

	// Store large data
	size, err := backend.StoreData(ctx, messageID, strings.NewReader(largeData))
	if err != nil {
		t.Fatalf("Failed to store large data: %v", err)
	}

	expectedSize := int64(len(largeData))
	if size != expectedSize {
		t.Errorf("Large data size mismatch: expected %d, got %d", expectedSize, size)
	}

	// Retrieve large data
	reader, err := backend.GetDataReader(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get large data reader: %v", err)
	}
	defer reader.Close()

	retrievedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read large data: %v", err)
	}

	if string(retrievedData) != largeData {
		t.Errorf("Large data content mismatch (lengths: expected %d, got %d)", len(largeData), len(retrievedData))
	}

	// Clean up
	err = backend.DeleteData(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to delete large data: %v", err)
	}
}

func TestBackend_UpdateData(t *testing.T) {
	backend := setupTestDB(t)
	defer cleanupTestDB(t, backend)

	ctx := context.Background()
	messageID := "test-message-5"
	
	// Store initial data
	initialData := "Initial test data."
	_, err := backend.StoreData(ctx, messageID, strings.NewReader(initialData))
	if err != nil {
		t.Fatalf("Failed to store initial data: %v", err)
	}

	// Update with new data
	updatedData := "Updated test data with different content."
	size, err := backend.StoreData(ctx, messageID, strings.NewReader(updatedData))
	if err != nil {
		t.Fatalf("Failed to update data: %v", err)
	}

	expectedSize := int64(len(updatedData))
	if size != expectedSize {
		t.Errorf("Updated data size mismatch: expected %d, got %d", expectedSize, size)
	}

	// Verify updated data
	reader, err := backend.GetDataReader(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get updated data reader: %v", err)
	}
	defer reader.Close()

	retrievedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read updated data: %v", err)
	}

	if string(retrievedData) != updatedData {
		t.Errorf("Updated data mismatch: expected %s, got %s", updatedData, string(retrievedData))
	}
}

func TestFactory_Create(t *testing.T) {
	dsn := getTestDSN()
	
	factory := NewFactory(dsn).WithTableName("factory_test_data")
	backend, err := factory.Create()
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}
	defer backend.Close()

	if factory.Name() != "postgres-data" {
		t.Errorf("Expected factory name 'postgres-data', got %s", factory.Name())
	}
}