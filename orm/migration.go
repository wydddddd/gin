package orm

import (
	"fmt"
	"strings"
	"time"
)

// Migrator handles database schema migrations
type Migrator struct {
	db         *DB
	migrations []Migration
}

// Migration represents a single schema migration
type Migration struct {
	ID        string
	Name      string
	Up        func(m *Migrator) error
	Down      func(m *Migrator) error
	CreatedAt time.Time
}

// NewMigrator creates a new Migrator
func NewMigrator(db *DB) *Migrator {
	return &Migrator{db: db}
}

// AddMigration registers a new migration
func (m *Migrator) AddMigration(migration Migration) {
	m.migrations = append(m.migrations, migration)
}

// Migrate runs all pending migrations
func (m *Migrator) Migrate() error {
	// Ensure migrations table exists
	if err := m.createMigrationsTable(); err != nil {
		return err
	}

	applied, err := m.getAppliedMigrations()
	if err != nil {
		return err
	}

	for _, migration := range m.migrations {
		if _, ok := applied[migration.ID]; ok {
			continue
		}

		if err := m.db.Transaction(func(tx *Tx) error {
			if err := migration.Up(m); err != nil {
				return fmt.Errorf("migration %s failed: %w", migration.ID, err)
			}
			_, err := tx.Exec(
				"INSERT INTO schema_migrations (id, name, applied_at) VALUES (?, ?, ?)",
				migration.ID, migration.Name, time.Now(),
			)
			return err
		}); err != nil {
			return err
		}
	}

	return nil
}

// Rollback rolls back the last migration
func (m *Migrator) Rollback() error {
	applied, err := m.getAppliedMigrations()
	if err != nil {
		return err
	}

	if len(applied) == 0 {
		return nil
	}

	// Find last applied migration
	var lastID string
	for id := range applied {
		lastID = id
	}

	for _, migration := range m.migrations {
		if migration.ID == lastID {
			if migration.Down == nil {
				return fmt.Errorf("migration %s has no rollback function", lastID)
			}
			if err := migration.Down(m); err != nil {
				return err
			}
			_, err := m.db.Exec("DELETE FROM schema_migrations WHERE id = ?", lastID)
			return err
		}
	}

	return fmt.Errorf("migration %s not found", lastID)
}

// RollbackAll rolls back all migrations
func (m *Migrator) RollbackAll() error {
	for i := len(m.migrations) - 1; i >= 0; i-- {
		if err := m.Rollback(); err != nil {
			return err
		}
	}
	return nil
}

func (m *Migrator) createMigrationsTable() error {
	query := `CREATE TABLE IF NOT EXISTS schema_migrations (
		id VARCHAR(255) PRIMARY KEY,
		name VARCHAR(255),
		applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`
	_, err := m.db.Exec(query)
	return err
}

func (m *Migrator) getAppliedMigrations() (map[string]time.Time, error) {
	rows, err := m.db.Query("SELECT id, applied_at FROM schema_migrations")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := make(map[string]time.Time)
	for rows.Next() {
		var id string
		var appliedAt time.Time
		if err := rows.Scan(&id, &appliedAt); err != nil {
			return nil, err
		}
		applied[id] = appliedAt
	}
	return applied, nil
}

// Schema Builder methods

// CreateTable creates a new table
func (m *Migrator) CreateTable(name string, fn func(t *TableBuilder)) error {
	tb := &TableBuilder{name: name, dialect: m.db.dialect}
	fn(tb)
	query := tb.Build()
	_, err := m.db.Exec(query)
	return err
}

// DropTable drops a table
func (m *Migrator) DropTable(name string) error {
	_, err := m.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
	return err
}

// AddColumn adds a column to an existing table
func (m *Migrator) AddColumn(table, column, colType string) error {
	_, err := m.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, colType))
	return err
}

// DropColumn removes a column from a table
func (m *Migrator) DropColumn(table, column string) error {
	_, err := m.db.Exec(fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", table, column))
	return err
}

// RenameColumn renames a column
func (m *Migrator) RenameColumn(table, oldName, newName string) error {
	_, err := m.db.Exec(fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", table, oldName, newName))
	return err
}

// AddIndex creates an index
func (m *Migrator) AddIndex(table, name string, columns ...string) error {
	query := fmt.Sprintf("CREATE INDEX %s ON %s (%s)", name, table, strings.Join(columns, ", "))
	_, err := m.db.Exec(query)
	return err
}

// AddUniqueIndex creates a unique index
func (m *Migrator) AddUniqueIndex(table, name string, columns ...string) error {
	query := fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s (%s)", name, table, strings.Join(columns, ", "))
	_, err := m.db.Exec(query)
	return err
}

// DropIndex removes an index
func (m *Migrator) DropIndex(table, name string) error {
	_, err := m.db.Exec(fmt.Sprintf("DROP INDEX %s ON %s", name, table))
	return err
}

// TableBuilder constructs CREATE TABLE statements
type TableBuilder struct {
	name       string
	columns    []columnDef
	primaryKey []string
	indexes    []indexDef
	dialect    Dialect
	engine     string
	charset    string
}

type columnDef struct {
	name         string
	colType      string
	nullable     bool
	defaultValue string
	autoInc      bool
	unique       bool
	comment      string
}

type indexDef struct {
	name    string
	columns []string
	unique  bool
}

// ID adds an auto-incrementing primary key
func (tb *TableBuilder) ID() {
	tb.columns = append(tb.columns, columnDef{
		name:    "id",
		colType: "BIGINT UNSIGNED",
		autoInc: true,
	})
	tb.primaryKey = []string{"id"}
}

// String adds a VARCHAR column
func (tb *TableBuilder) String(name string, size int) *columnBuilder {
	col := columnDef{
		name:    name,
		colType: fmt.Sprintf("VARCHAR(%d)", size),
	}
	tb.columns = append(tb.columns, col)
	return &columnBuilder{table: tb, index: len(tb.columns) - 1}
}

// Text adds a TEXT column
func (tb *TableBuilder) Text(name string) *columnBuilder {
	col := columnDef{name: name, colType: "TEXT"}
	tb.columns = append(tb.columns, col)
	return &columnBuilder{table: tb, index: len(tb.columns) - 1}
}

// Integer adds an INT column
func (tb *TableBuilder) Integer(name string) *columnBuilder {
	col := columnDef{name: name, colType: "INT"}
	tb.columns = append(tb.columns, col)
	return &columnBuilder{table: tb, index: len(tb.columns) - 1}
}

// BigInt adds a BIGINT column
func (tb *TableBuilder) BigInt(name string) *columnBuilder {
	col := columnDef{name: name, colType: "BIGINT"}
	tb.columns = append(tb.columns, col)
	return &columnBuilder{table: tb, index: len(tb.columns) - 1}
}

// Float adds a FLOAT column
func (tb *TableBuilder) Float(name string) *columnBuilder {
	col := columnDef{name: name, colType: "FLOAT"}
	tb.columns = append(tb.columns, col)
	return &columnBuilder{table: tb, index: len(tb.columns) - 1}
}

// Decimal adds a DECIMAL column
func (tb *TableBuilder) Decimal(name string, precision, scale int) *columnBuilder {
	col := columnDef{name: name, colType: fmt.Sprintf("DECIMAL(%d,%d)", precision, scale)}
	tb.columns = append(tb.columns, col)
	return &columnBuilder{table: tb, index: len(tb.columns) - 1}
}

// Boolean adds a BOOLEAN column
func (tb *TableBuilder) Boolean(name string) *columnBuilder {
	col := columnDef{name: name, colType: "BOOLEAN"}
	tb.columns = append(tb.columns, col)
	return &columnBuilder{table: tb, index: len(tb.columns) - 1}
}

// Timestamp adds a TIMESTAMP column
func (tb *TableBuilder) Timestamp(name string) *columnBuilder {
	col := columnDef{name: name, colType: "TIMESTAMP"}
	tb.columns = append(tb.columns, col)
	return &columnBuilder{table: tb, index: len(tb.columns) - 1}
}

// DateTime adds a DATETIME column
func (tb *TableBuilder) DateTime(name string) *columnBuilder {
	col := columnDef{name: name, colType: "DATETIME"}
	tb.columns = append(tb.columns, col)
	return &columnBuilder{table: tb, index: len(tb.columns) - 1}
}

// JSON adds a JSON column
func (tb *TableBuilder) JSON(name string) *columnBuilder {
	col := columnDef{name: name, colType: "JSON"}
	tb.columns = append(tb.columns, col)
	return &columnBuilder{table: tb, index: len(tb.columns) - 1}
}

// Timestamps adds created_at and updated_at columns
func (tb *TableBuilder) Timestamps() {
	tb.columns = append(tb.columns, columnDef{
		name:         "created_at",
		colType:      "TIMESTAMP",
		defaultValue: "CURRENT_TIMESTAMP",
	})
	tb.columns = append(tb.columns, columnDef{
		name:         "updated_at",
		colType:      "TIMESTAMP",
		defaultValue: "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
	})
}

// SoftDelete adds a deleted_at column
func (tb *TableBuilder) SoftDelete() {
	tb.columns = append(tb.columns, columnDef{
		name:     "deleted_at",
		colType:  "TIMESTAMP",
		nullable: true,
	})
}

// Index adds an index
func (tb *TableBuilder) Index(name string, columns ...string) {
	tb.indexes = append(tb.indexes, indexDef{name: name, columns: columns})
}

// UniqueIndex adds a unique index
func (tb *TableBuilder) UniqueIndex(name string, columns ...string) {
	tb.indexes = append(tb.indexes, indexDef{name: name, columns: columns, unique: true})
}

// Engine sets the storage engine (MySQL only)
func (tb *TableBuilder) Engine(engine string) {
	tb.engine = engine
}

// Charset sets the character set (MySQL only)
func (tb *TableBuilder) Charset(charset string) {
	tb.charset = charset
}

// Build generates the CREATE TABLE SQL
func (tb *TableBuilder) Build() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", tb.name))

	for i, col := range tb.columns {
		if i > 0 {
			sb.WriteString(",\n")
		}
		sb.WriteString(fmt.Sprintf("  %s %s", col.name, col.colType))
		if !col.nullable {
			sb.WriteString(" NOT NULL")
		}
		if col.autoInc {
			sb.WriteString(" AUTO_INCREMENT")
		}
		if col.unique {
			sb.WriteString(" UNIQUE")
		}
		if col.defaultValue != "" {
			sb.WriteString(fmt.Sprintf(" DEFAULT %s", col.defaultValue))
		}
		if col.comment != "" {
			sb.WriteString(fmt.Sprintf(" COMMENT '%s'", col.comment))
		}
	}

	if len(tb.primaryKey) > 0 {
		sb.WriteString(fmt.Sprintf(",\n  PRIMARY KEY (%s)", strings.Join(tb.primaryKey, ", ")))
	}

	for _, idx := range tb.indexes {
		if idx.unique {
			sb.WriteString(fmt.Sprintf(",\n  UNIQUE INDEX %s (%s)", idx.name, strings.Join(idx.columns, ", ")))
		} else {
			sb.WriteString(fmt.Sprintf(",\n  INDEX %s (%s)", idx.name, strings.Join(idx.columns, ", ")))
		}
	}

	sb.WriteString("\n)")

	if tb.engine != "" {
		sb.WriteString(fmt.Sprintf(" ENGINE=%s", tb.engine))
	}
	if tb.charset != "" {
		sb.WriteString(fmt.Sprintf(" DEFAULT CHARSET=%s", tb.charset))
	}

	return sb.String()
}

// columnBuilder provides fluent column configuration
type columnBuilder struct {
	table *TableBuilder
	index int
}

func (cb *columnBuilder) Nullable() *columnBuilder {
	cb.table.columns[cb.index].nullable = true
	return cb
}

func (cb *columnBuilder) Default(value string) *columnBuilder {
	cb.table.columns[cb.index].defaultValue = value
	return cb
}

func (cb *columnBuilder) Unique() *columnBuilder {
	cb.table.columns[cb.index].unique = true
	return cb
}

func (cb *columnBuilder) Comment(comment string) *columnBuilder {
	cb.table.columns[cb.index].comment = comment
	return cb
}
