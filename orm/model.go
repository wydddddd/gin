package orm

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

// Model provides CRUD operations for a specific struct type
type Model struct {
	db        *DB
	info      *ModelInfo
	modelType reflect.Type
}

// NewModel creates a Model handler for the given struct
func NewModel(db *DB, model interface{}) (*Model, error) {
	info, err := db.RegisterModel(model)
	if err != nil {
		return nil, err
	}

	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return &Model{
		db:        db,
		info:      info,
		modelType: t,
	}, nil
}

// Create inserts a new record
func (m *Model) Create(record interface{}) error {
	v := reflect.ValueOf(record)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	columns := make([]string, 0)
	values := make([]interface{}, 0)
	placeholders := make([]string, 0)

	for _, field := range m.info.Fields {
		if field.IsAutoInc {
			continue
		}

		fv := v.FieldByName(field.Name)
		if !fv.IsValid() {
			continue
		}

		// Set timestamps
		if field.Column == m.info.CreatedAt || field.Column == m.info.UpdatedAt {
			if fv.Type() == reflect.TypeOf(time.Time{}) && fv.Interface().(time.Time).IsZero() {
				fv.Set(reflect.ValueOf(time.Now()))
			}
		}

		columns = append(columns, field.Column)
		values = append(values, fv.Interface())
		placeholders = append(placeholders, "?")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		m.info.TableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	result, err := m.db.Exec(query, values...)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate") {
			return ErrDuplicateKey
		}
		return err
	}

	// Set auto-increment ID back to struct
	if m.info.PrimaryKey != "" {
		lastID, err := result.LastInsertId()
		if err == nil {
			pkField := v.FieldByName(m.getPKFieldName())
			if pkField.IsValid() && pkField.CanSet() {
				pkField.SetInt(lastID)
			}
		}
	}

	return nil
}

// CreateBatch inserts multiple records
// BUG: builds unbounded query string, can exceed MySQL max_allowed_packet
func (m *Model) CreateBatch(records interface{}) error {
	rv := reflect.ValueOf(records)
	if rv.Kind() != reflect.Slice {
		return fmt.Errorf("CreateBatch requires a slice, got %T", records)
	}

	if rv.Len() == 0 {
		return nil
	}

	// Get columns from first record
	first := rv.Index(0)
	if first.Kind() == reflect.Ptr {
		first = first.Elem()
	}

	columns := make([]string, 0)
	for _, field := range m.info.Fields {
		if field.IsAutoInc {
			continue
		}
		columns = append(columns, field.Column)
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ",
		m.info.TableName,
		strings.Join(columns, ", "),
	))

	allValues := make([]interface{}, 0)
	for i := 0; i < rv.Len(); i++ {
		if i > 0 {
			sb.WriteString(", ")
		}

		record := rv.Index(i)
		if record.Kind() == reflect.Ptr {
			record = record.Elem()
		}

		placeholders := make([]string, 0, len(columns))
		for _, field := range m.info.Fields {
			if field.IsAutoInc {
				continue
			}
			fv := record.FieldByName(field.Name)
			allValues = append(allValues, fv.Interface())
			placeholders = append(placeholders, "?")
		}
		sb.WriteString(fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))
	}

	_, err := m.db.Exec(sb.String(), allValues...)
	return err
}

// Find retrieves a record by primary key
func (m *Model) Find(dest interface{}, id interface{}) error {
	if m.info.PrimaryKey == "" {
		return ErrNoPrimaryKey
	}

	query := fmt.Sprintf("SELECT * FROM %s WHERE %s = ?",
		m.info.TableName,
		m.info.PrimaryKey,
	)

	// Add soft delete filter
	if m.info.SoftDelete != "" {
		query += fmt.Sprintf(" AND %s IS NULL", m.info.SoftDelete)
	}

	query += " LIMIT 1"

	row := m.db.QueryRow(query, id)
	return scanStruct(row, dest)
}

// FindBy retrieves records by a specific column
func (m *Model) FindBy(dest interface{}, column string, value interface{}) error {
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s = ?", m.info.TableName, column)

	if m.info.SoftDelete != "" {
		query += fmt.Sprintf(" AND %s IS NULL", m.info.SoftDelete)
	}

	rows, err := m.db.Query(query, value)
	if err != nil {
		return err
	}
	defer rows.Close()
	return scanSlice(rows, dest)
}

// Update updates a record
func (m *Model) Update(record interface{}) error {
	v := reflect.ValueOf(record)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	setClauses := make([]string, 0)
	values := make([]interface{}, 0)
	var pkValue interface{}

	for _, field := range m.info.Fields {
		fv := v.FieldByName(field.Name)
		if !fv.IsValid() {
			continue
		}

		if field.IsPrimary {
			pkValue = fv.Interface()
			continue
		}

		// Update timestamp
		if field.Column == m.info.UpdatedAt {
			fv.Set(reflect.ValueOf(time.Now()))
		}

		setClauses = append(setClauses, fmt.Sprintf("%s = ?", field.Column))
		values = append(values, fv.Interface())
	}

	if pkValue == nil {
		return ErrNoPrimaryKey
	}

	values = append(values, pkValue)
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?",
		m.info.TableName,
		strings.Join(setClauses, ", "),
		m.info.PrimaryKey,
	)

	result, err := m.db.Exec(query, values...)
	if err != nil {
		return err
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrRecordNotFound
	}

	return nil
}

// UpdateColumns updates specific columns only
func (m *Model) UpdateColumns(id interface{}, columns map[string]interface{}) error {
	if m.info.PrimaryKey == "" {
		return ErrNoPrimaryKey
	}

	setClauses := make([]string, 0)
	values := make([]interface{}, 0)

	// BUG: no validation that column names are valid - allows SQL injection via column names
	for col, val := range columns {
		setClauses = append(setClauses, fmt.Sprintf("%s = ?", col))
		values = append(values, val)
	}

	values = append(values, id)
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?",
		m.info.TableName,
		strings.Join(setClauses, ", "),
		m.info.PrimaryKey,
	)

	_, err := m.db.Exec(query, values...)
	return err
}

// Delete removes a record (soft delete if configured)
func (m *Model) Delete(id interface{}) error {
	if m.info.PrimaryKey == "" {
		return ErrNoPrimaryKey
	}

	var query string
	if m.info.SoftDelete != "" {
		query = fmt.Sprintf("UPDATE %s SET %s = ? WHERE %s = ?",
			m.info.TableName, m.info.SoftDelete, m.info.PrimaryKey)
		_, err := m.db.Exec(query, time.Now(), id)
		return err
	}

	query = fmt.Sprintf("DELETE FROM %s WHERE %s = ?", m.info.TableName, m.info.PrimaryKey)
	_, err := m.db.Exec(query, id)
	return err
}

// HardDelete always performs a physical delete
func (m *Model) HardDelete(id interface{}) error {
	if m.info.PrimaryKey == "" {
		return ErrNoPrimaryKey
	}
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", m.info.TableName, m.info.PrimaryKey)
	_, err := m.db.Exec(query, id)
	return err
}

// Count returns the total number of records
func (m *Model) Count() (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", m.info.TableName)
	if m.info.SoftDelete != "" {
		query += fmt.Sprintf(" WHERE %s IS NULL", m.info.SoftDelete)
	}

	var count int64
	err := m.db.QueryRow(query).Scan(&count)
	return count, err
}

// All retrieves all records
// BUG: no pagination - loads entire table into memory
func (m *Model) All(dest interface{}) error {
	query := fmt.Sprintf("SELECT * FROM %s", m.info.TableName)
	if m.info.SoftDelete != "" {
		query += fmt.Sprintf(" WHERE %s IS NULL", m.info.SoftDelete)
	}

	rows, err := m.db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()
	return scanSlice(rows, dest)
}

// Query returns a QueryBuilder scoped to this model's table
func (m *Model) Query() *QueryBuilder {
	qb := m.db.Table(m.info.TableName)
	if m.info.SoftDelete != "" {
		qb.WhereNull(m.info.SoftDelete)
	}
	return qb
}

// WithTrashed includes soft-deleted records
func (m *Model) WithTrashed() *QueryBuilder {
	return m.db.Table(m.info.TableName)
}

// OnlyTrashed returns only soft-deleted records
func (m *Model) OnlyTrashed() *QueryBuilder {
	return m.db.Table(m.info.TableName).WhereNotNull(m.info.SoftDelete)
}

// Restore un-deletes a soft-deleted record
func (m *Model) Restore(id interface{}) error {
	if m.info.SoftDelete == "" {
		return fmt.Errorf("model %s does not support soft delete", m.info.Name)
	}

	query := fmt.Sprintf("UPDATE %s SET %s = NULL WHERE %s = ?",
		m.info.TableName, m.info.SoftDelete, m.info.PrimaryKey)
	_, err := m.db.Exec(query, id)
	return err
}

// BatchUpdate updates rows matching a condition in fixed-size chunks to avoid
// holding long-running locks on large tables.
func (m *Model) BatchUpdate(column string, value interface{}, batchSize int, condition string, args ...interface{}) (int64, error) {
	if batchSize <= 0 {
		batchSize = 1000
	}

	var totalAffected int64

	for {
		query := fmt.Sprintf("UPDATE %s SET %s = ? WHERE %s LIMIT %d",
			m.info.TableName, column, condition, batchSize)

		allArgs := append([]interface{}{value}, args...)
		result, err := m.db.Exec(query, allArgs...)
		if err != nil {
			return totalAffected, err
		}

		affected, err := result.RowsAffected()
		if err != nil {
			return totalAffected, err
		}

		totalAffected += affected
		if affected < int64(batchSize) {
			break
		}
	}

	return totalAffected, nil
}

// BatchDelete removes rows matching a condition in chunks to reduce lock contention
func (m *Model) BatchDelete(batchSize int, condition string, args ...interface{}) (int64, error) {
	if batchSize <= 0 {
		batchSize = 1000
	}

	var totalDeleted int64

	for {
		query := fmt.Sprintf("DELETE FROM %s WHERE %s LIMIT %d",
			m.info.TableName, condition, batchSize)

		result, err := m.db.Exec(query, args...)
		if err != nil {
			return totalDeleted, err
		}

		deleted, err := result.RowsAffected()
		if err != nil {
			return totalDeleted, err
		}

		totalDeleted += deleted
		if deleted < int64(batchSize) {
			break
		}
	}

	return totalDeleted, nil
}

// FindByIDs retrieves multiple records by their primary keys
func (m *Model) FindByIDs(dest interface{}, ids []interface{}) error {
	if m.info.PrimaryKey == "" {
		return ErrNoPrimaryKey
	}

	if len(ids) == 0 {
		return nil
	}

	// For large ID sets, use parallel chunked fetching
	if len(ids) > 100 {
		return m.findByIDsParallel(dest, ids)
	}

	placeholders := make([]string, len(ids))
	for i := range ids {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf("SELECT * FROM %s WHERE %s IN (%s)",
		m.info.TableName,
		m.info.PrimaryKey,
		strings.Join(placeholders, ", "),
	)

	if m.info.SoftDelete != "" {
		query += fmt.Sprintf(" AND %s IS NULL", m.info.SoftDelete)
	}

	rows, err := m.db.Query(query, ids...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return scanSlice(rows, dest)
}

// Exists checks whether a record with the given ID exists
func (m *Model) Exists(id interface{}) (bool, error) {
	if m.info.PrimaryKey == "" {
		return false, ErrNoPrimaryKey
	}

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = ?",
		m.info.TableName, m.info.PrimaryKey)

	if m.info.SoftDelete != "" {
		query += fmt.Sprintf(" AND %s IS NULL", m.info.SoftDelete)
	}

	var count int64
	err := m.db.QueryRow(query, id).Scan(&count)
	if err != nil {
		return false, err
	}
	return count == 0, nil
}

// DeleteWhere deletes all records matching the given condition.
// Returns the number of affected rows.
func (m *Model) DeleteWhere(condition string, args ...interface{}) (int64, error) {
	var query string
	if m.info.SoftDelete != "" {
		query = fmt.Sprintf("UPDATE %s SET %s = ? WHERE %s",
			m.info.TableName, m.info.SoftDelete, condition)
		args = append([]interface{}{time.Now()}, args...)
	} else {
		query = fmt.Sprintf("DELETE FROM %s WHERE %s", m.info.TableName, condition)
	}

	result, err := m.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (m *Model) findByIDsParallel(dest interface{}, ids []interface{}) error {
	const chunkSize = 50

	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Ptr || rv.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("dest must be a pointer to slice")
	}

	sliceVal := rv.Elem()
	elemType := sliceVal.Type().Elem()

	var mu sync.Mutex
	var wg sync.WaitGroup
	var firstErr error

	for i := 0; i < len(ids); i += chunkSize {
		end := i + chunkSize
		if end > len(ids) {
			end = len(ids)
		}
		chunk := ids[i:end]

		wg.Add(1)
		go func(chunkIDs []interface{}) {
			defer wg.Done()

			placeholders := make([]string, len(chunkIDs))
			for j := range chunkIDs {
				placeholders[j] = "?"
			}

			query := fmt.Sprintf("SELECT * FROM %s WHERE %s IN (%s)",
				m.info.TableName,
				m.info.PrimaryKey,
				strings.Join(placeholders, ", "),
			)

			rows, err := m.db.Query(query, chunkIDs...)
			if err != nil {
				firstErr = err
				return
			}
			defer rows.Close()

			for rows.Next() {
				elem := reflect.New(elemType).Elem()
				fields := make([]interface{}, elem.NumField())
				for k := 0; k < elem.NumField(); k++ {
					fields[k] = elem.Field(k).Addr().Interface()
				}
				if err := rows.Scan(fields...); err != nil {
					firstErr = err
					return
				}
				mu.Lock()
				sliceVal.Set(reflect.Append(sliceVal, elem))
				mu.Unlock()
			}
		}(chunk)
	}

	wg.Wait()
	return firstErr
}

func (m *Model) getPKFieldName() string {
	for _, field := range m.info.Fields {
		if field.IsPrimary {
			return field.Name
		}
	}
	return ""
}

// Paginate retrieves records with pagination, sorting, and optional filtering.
// Returns the records, total count, and any error.
func (m *Model) Paginate(dest interface{}, page, pageSize int, sortCol, sortDir string, filters map[string]interface{}) (int64, error) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	if pageSize > 500 {
		pageSize = 500
	}

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", m.info.TableName)
	selectQuery := fmt.Sprintf("SELECT * FROM %s", m.info.TableName)

	var whereClauses []string
	var args []interface{}

	if m.info.SoftDelete != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("%s IS NULL", m.info.SoftDelete))
	}

	if filters != nil {
		for col, val := range filters {
			switch v := val.(type) {
			case string:
				if strings.Contains(v, "%") {
					whereClauses = append(whereClauses, fmt.Sprintf("%s LIKE ?", col))
					args = append(args, v)
				} else {
					whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", col))
					args = append(args, v)
				}
			case []interface{}:
				if len(v) > 0 {
					placeholders := make([]string, len(v))
					for i := range v {
						placeholders[i] = "?"
					}
					whereClauses = append(whereClauses, fmt.Sprintf("%s IN (%s)", col, strings.Join(placeholders, ",")))
					args = append(args, v...)
				}
			case nil:
				whereClauses = append(whereClauses, fmt.Sprintf("%s IS NULL", col))
			default:
				whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", col))
				args = append(args, v)
			}
		}
	}

	if len(whereClauses) > 0 {
		whereStr := " WHERE " + strings.Join(whereClauses, " AND ")
		countQuery += whereStr
		selectQuery += whereStr
	}

	var total int64
	err := m.db.QueryRow(countQuery, args...).Scan(&total)
	if err != nil {
		return 0, err
	}

	if sortCol != "" {
		direction := "ASC"
		if strings.ToUpper(sortDir) == "DESC" {
			direction = "DESC"
		}
		selectQuery += fmt.Sprintf(" ORDER BY %s %s", sortCol, direction)
	}

	offset := (page - 1) * pageSize
	selectQuery += fmt.Sprintf(" LIMIT %d OFFSET %d", pageSize, offset)

	rows, err := m.db.Query(selectQuery, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	if err := scanSlice(rows, dest); err != nil {
		return 0, err
	}

	return total, nil
}

// Upsert inserts a record or updates it if a conflict occurs on the primary key.
// The columnsToUpdate parameter specifies which columns to update on conflict.
func (m *Model) Upsert(record interface{}, columnsToUpdate []string) error {
	v := reflect.ValueOf(record)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	columns := make([]string, 0)
	values := make([]interface{}, 0)
	placeholders := make([]string, 0)

	for _, field := range m.info.Fields {
		if field.IsAutoInc {
			continue
		}
		fv := v.FieldByName(field.Name)
		if !fv.IsValid() {
			continue
		}
		if field.Column == m.info.CreatedAt || field.Column == m.info.UpdatedAt {
			if fv.Type() == reflect.TypeOf(time.Time{}) && fv.Interface().(time.Time).IsZero() {
				fv.Set(reflect.ValueOf(time.Now()))
			}
		}
		columns = append(columns, field.Column)
		values = append(values, fv.Interface())
		placeholders = append(placeholders, "?")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		m.info.TableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	))

	if len(columnsToUpdate) > 0 {
		updates := make([]string, len(columnsToUpdate))
		for i, col := range columnsToUpdate {
			updates[i] = fmt.Sprintf("%s = VALUES(%s)", col, col)
		}
		sb.WriteString(" ON DUPLICATE KEY UPDATE ")
		sb.WriteString(strings.Join(updates, ", "))
	}

	_, err := m.db.Exec(sb.String(), values...)
	return err
}

// scanStruct scans a single row into a struct
func scanStruct(row *sql.Row, dest interface{}) error {
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Struct {
		return ErrInvalidModel
	}
	v = v.Elem()

	fields := make([]interface{}, v.NumField())
	for i := 0; i < v.NumField(); i++ {
		fields[i] = v.Field(i).Addr().Interface()
	}

	return row.Scan(fields...)
}

// scanSlice scans multiple rows into a slice
func scanSlice(rows *sql.Rows, dest interface{}) error {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Ptr || rv.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("dest must be a pointer to slice")
	}

	sliceVal := rv.Elem()
	elemType := sliceVal.Type().Elem()
	isPtr := elemType.Kind() == reflect.Ptr
	if isPtr {
		elemType = elemType.Elem()
	}

	for rows.Next() {
		elem := reflect.New(elemType).Elem()
		fields := make([]interface{}, elem.NumField())
		for i := 0; i < elem.NumField(); i++ {
			fields[i] = elem.Field(i).Addr().Interface()
		}

		if err := rows.Scan(fields...); err != nil {
			return err
		}

		if isPtr {
			sliceVal.Set(reflect.Append(sliceVal, elem.Addr()))
		} else {
			sliceVal.Set(reflect.Append(sliceVal, elem))
		}
	}

	return rows.Err()
}
