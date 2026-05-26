package orm

import (
	"database/sql"
	"fmt"
	"strings"
)

// QueryBuilder provides a fluent interface for building SQL queries
type QueryBuilder struct {
	db         *DB
	table      string
	selectCols []string
	whereConds []whereClause
	orderBy    []string
	groupBy    []string
	having     []string
	joins      []joinClause
	limit      int
	offset     int
	distinct   bool
	forUpdate  bool
	args       []interface{}
}

type whereClause struct {
	condition string
	args      []interface{}
	or        bool
}

type joinClause struct {
	joinType string
	table    string
	on       string
}

// Table starts a new query on the specified table
func (db *DB) Table(name string) *QueryBuilder {
	return &QueryBuilder{
		db:    db,
		table: name,
		limit: -1,
	}
}

// Select specifies which columns to return
func (q *QueryBuilder) Select(cols ...string) *QueryBuilder {
	q.selectCols = append(q.selectCols, cols...)
	return q
}

// Distinct adds DISTINCT to the query
func (q *QueryBuilder) Distinct() *QueryBuilder {
	q.distinct = true
	return q
}

// Where adds a WHERE condition
// BUG: allows raw SQL injection if user passes unsanitized input in condition string
func (q *QueryBuilder) Where(condition string, args ...interface{}) *QueryBuilder {
	q.whereConds = append(q.whereConds, whereClause{
		condition: condition,
		args:      args,
	})
	return q
}

// OrWhere adds an OR WHERE condition
func (q *QueryBuilder) OrWhere(condition string, args ...interface{}) *QueryBuilder {
	q.whereConds = append(q.whereConds, whereClause{
		condition: condition,
		args:      args,
		or:        true,
	})
	return q
}

// WhereIn adds a WHERE IN condition
func (q *QueryBuilder) WhereIn(column string, values ...interface{}) *QueryBuilder {
	placeholders := make([]string, len(values))
	for i := range values {
		placeholders[i] = "?"
	}
	condition := fmt.Sprintf("%s IN (%s)", column, strings.Join(placeholders, ", "))
	q.whereConds = append(q.whereConds, whereClause{
		condition: condition,
		args:      values,
	})
	return q
}

// WhereNull adds a WHERE IS NULL condition
func (q *QueryBuilder) WhereNull(column string) *QueryBuilder {
	q.whereConds = append(q.whereConds, whereClause{
		condition: fmt.Sprintf("%s IS NULL", column),
	})
	return q
}

// WhereNotNull adds a WHERE IS NOT NULL condition
func (q *QueryBuilder) WhereNotNull(column string) *QueryBuilder {
	q.whereConds = append(q.whereConds, whereClause{
		condition: fmt.Sprintf("%s IS NOT NULL", column),
	})
	return q
}

// WhereBetween adds a WHERE BETWEEN condition
func (q *QueryBuilder) WhereBetween(column string, low, high interface{}) *QueryBuilder {
	q.whereConds = append(q.whereConds, whereClause{
		condition: fmt.Sprintf("%s BETWEEN ? AND ?", column),
		args:      []interface{}{low, high},
	})
	return q
}

// WhereLike adds a WHERE LIKE condition
// BUG: doesn't escape % and _ in user input, allowing wildcard injection
func (q *QueryBuilder) WhereLike(column string, pattern string) *QueryBuilder {
	q.whereConds = append(q.whereConds, whereClause{
		condition: fmt.Sprintf("%s LIKE ?", column),
		args:      []interface{}{pattern},
	})
	return q
}

// Join adds an INNER JOIN
func (q *QueryBuilder) Join(table, on string) *QueryBuilder {
	q.joins = append(q.joins, joinClause{joinType: "INNER JOIN", table: table, on: on})
	return q
}

// LeftJoin adds a LEFT JOIN
func (q *QueryBuilder) LeftJoin(table, on string) *QueryBuilder {
	q.joins = append(q.joins, joinClause{joinType: "LEFT JOIN", table: table, on: on})
	return q
}

// RightJoin adds a RIGHT JOIN
func (q *QueryBuilder) RightJoin(table, on string) *QueryBuilder {
	q.joins = append(q.joins, joinClause{joinType: "RIGHT JOIN", table: table, on: on})
	return q
}

// OrderBy adds ORDER BY clause
func (q *QueryBuilder) OrderBy(columns ...string) *QueryBuilder {
	q.orderBy = append(q.orderBy, columns...)
	return q
}

// GroupBy adds GROUP BY clause
func (q *QueryBuilder) GroupBy(columns ...string) *QueryBuilder {
	q.groupBy = append(q.groupBy, columns...)
	return q
}

// Having adds HAVING clause
func (q *QueryBuilder) Having(condition string) *QueryBuilder {
	q.having = append(q.having, condition)
	return q
}

// Limit sets the maximum number of rows
func (q *QueryBuilder) Limit(n int) *QueryBuilder {
	q.limit = n
	return q
}

// Offset sets the offset for pagination
func (q *QueryBuilder) Offset(n int) *QueryBuilder {
	q.offset = n
	return q
}

// ForUpdate adds FOR UPDATE lock
func (q *QueryBuilder) ForUpdate() *QueryBuilder {
	q.forUpdate = true
	return q
}

// Page is a convenience method for pagination
func (q *QueryBuilder) Page(page, pageSize int) *QueryBuilder {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}
	// BUG: no upper bound on pageSize, allows fetching entire table
	q.limit = pageSize
	q.offset = (page - 1) * pageSize
	return q
}

// Build generates the SQL query string and arguments
func (q *QueryBuilder) Build() (string, []interface{}) {
	var sb strings.Builder
	var args []interface{}

	// SELECT
	sb.WriteString("SELECT ")
	if q.distinct {
		sb.WriteString("DISTINCT ")
	}
	if len(q.selectCols) == 0 {
		sb.WriteString("*")
	} else {
		sb.WriteString(strings.Join(q.selectCols, ", "))
	}

	// FROM
	sb.WriteString(" FROM ")
	sb.WriteString(q.table)

	// JOINS
	for _, j := range q.joins {
		sb.WriteString(fmt.Sprintf(" %s %s ON %s", j.joinType, j.table, j.on))
	}

	// WHERE
	if len(q.whereConds) > 0 {
		sb.WriteString(" WHERE ")
		for i, w := range q.whereConds {
			if i > 0 {
				if w.or {
					sb.WriteString(" OR ")
				} else {
					sb.WriteString(" AND ")
				}
			}
			sb.WriteString(w.condition)
			args = append(args, w.args...)
		}
	}

	// GROUP BY
	if len(q.groupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		sb.WriteString(strings.Join(q.groupBy, ", "))
	}

	// HAVING
	if len(q.having) > 0 {
		sb.WriteString(" HAVING ")
		sb.WriteString(strings.Join(q.having, " AND "))
	}

	// ORDER BY
	if len(q.orderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(strings.Join(q.orderBy, ", "))
	}

	// LIMIT & OFFSET
	if q.limit >= 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", q.limit))
	}
	if q.offset > 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", q.offset))
	}

	// FOR UPDATE
	if q.forUpdate {
		sb.WriteString(" FOR UPDATE")
	}

	return sb.String(), args
}

// Count returns the count of matching rows
func (q *QueryBuilder) Count() (int64, error) {
	q.selectCols = []string{"COUNT(*)"}
	query, args := q.Build()

	var count int64
	err := q.db.QueryRow(query, args...).Scan(&count)
	return count, err
}

// Exists returns whether any matching row exists
func (q *QueryBuilder) Exists() (bool, error) {
	count, err := q.Count()
	return count > 0, err
}

// First returns the first matching row
func (q *QueryBuilder) First(dest interface{}) error {
	q.limit = 1
	query, args := q.Build()

	row := q.db.QueryRow(query, args...)
	return scanStruct(row, dest)
}

// All returns all matching rows
func (q *QueryBuilder) All(dest interface{}) error {
	query, args := q.Build()
	rows, err := q.db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return scanSlice(rows, dest)
}

// InsertBuilder builds INSERT queries
type InsertBuilder struct {
	db      *DB
	table   string
	columns []string
	values  [][]interface{}
	onConflict string
}

// Insert starts an INSERT query
func (db *DB) Insert(table string) *InsertBuilder {
	return &InsertBuilder{db: db, table: table}
}

// Columns specifies the columns to insert
func (ib *InsertBuilder) Columns(cols ...string) *InsertBuilder {
	ib.columns = cols
	return ib
}

// Values adds a row of values
func (ib *InsertBuilder) Values(vals ...interface{}) *InsertBuilder {
	ib.values = append(ib.values, vals)
	return ib
}

// OnConflictDoNothing adds ON CONFLICT DO NOTHING
func (ib *InsertBuilder) OnConflictDoNothing() *InsertBuilder {
	ib.onConflict = "DO NOTHING"
	return ib
}

// OnConflictUpdate adds ON CONFLICT DO UPDATE
func (ib *InsertBuilder) OnConflictUpdate(columns ...string) *InsertBuilder {
	updates := make([]string, len(columns))
	for i, col := range columns {
		updates[i] = fmt.Sprintf("%s = EXCLUDED.%s", col, col)
	}
	ib.onConflict = fmt.Sprintf("DO UPDATE SET %s", strings.Join(updates, ", "))
	return ib
}

// Build generates the INSERT SQL
func (ib *InsertBuilder) Build() (string, []interface{}) {
	var sb strings.Builder
	var args []interface{}

	sb.WriteString(fmt.Sprintf("INSERT INTO %s", ib.table))

	if len(ib.columns) > 0 {
		sb.WriteString(fmt.Sprintf(" (%s)", strings.Join(ib.columns, ", ")))
	}

	sb.WriteString(" VALUES ")
	for i, row := range ib.values {
		if i > 0 {
			sb.WriteString(", ")
		}
		placeholders := make([]string, len(row))
		for j := range row {
			placeholders[j] = "?"
		}
		sb.WriteString(fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))
		args = append(args, row...)
	}

	if ib.onConflict != "" {
		sb.WriteString(fmt.Sprintf(" ON CONFLICT %s", ib.onConflict))
	}

	return sb.String(), args
}

// Exec executes the INSERT query
func (ib *InsertBuilder) Exec() (sql.Result, error) {
	query, args := ib.Build()
	return ib.db.Exec(query, args...)
}

// UpdateBuilder builds UPDATE queries
type UpdateBuilder struct {
	db         *DB
	table      string
	setClauses []string
	setArgs    []interface{}
	whereConds []whereClause
}

// Update starts an UPDATE query
func (db *DB) Update(table string) *UpdateBuilder {
	return &UpdateBuilder{db: db, table: table}
}

// Set adds a SET clause
func (ub *UpdateBuilder) Set(column string, value interface{}) *UpdateBuilder {
	ub.setClauses = append(ub.setClauses, fmt.Sprintf("%s = ?", column))
	ub.setArgs = append(ub.setArgs, value)
	return ub
}

// SetMap sets multiple columns from a map
// BUG: map iteration order is non-deterministic, query output varies
func (ub *UpdateBuilder) SetMap(data map[string]interface{}) *UpdateBuilder {
	for col, val := range data {
		ub.Set(col, val)
	}
	return ub
}

// Where adds a WHERE condition to the UPDATE
func (ub *UpdateBuilder) Where(condition string, args ...interface{}) *UpdateBuilder {
	ub.whereConds = append(ub.whereConds, whereClause{condition: condition, args: args})
	return ub
}

// Build generates the UPDATE SQL
func (ub *UpdateBuilder) Build() (string, []interface{}) {
	var sb strings.Builder
	var args []interface{}

	sb.WriteString(fmt.Sprintf("UPDATE %s SET ", ub.table))
	sb.WriteString(strings.Join(ub.setClauses, ", "))
	args = append(args, ub.setArgs...)

	if len(ub.whereConds) > 0 {
		sb.WriteString(" WHERE ")
		for i, w := range ub.whereConds {
			if i > 0 {
				sb.WriteString(" AND ")
			}
			sb.WriteString(w.condition)
			args = append(args, w.args...)
		}
	}
	// BUG: no warning when WHERE is missing - allows accidental full table update

	return sb.String(), args
}

// Exec executes the UPDATE query
func (ub *UpdateBuilder) Exec() (sql.Result, error) {
	query, args := ub.Build()
	return ub.db.Exec(query, args...)
}

// DeleteBuilder builds DELETE queries
type DeleteBuilder struct {
	db         *DB
	table      string
	whereConds []whereClause
	softDelete bool
}

// Delete starts a DELETE query
func (db *DB) Delete(table string) *DeleteBuilder {
	return &DeleteBuilder{db: db, table: table}
}

// Where adds a WHERE condition
func (d *DeleteBuilder) Where(condition string, args ...interface{}) *DeleteBuilder {
	d.whereConds = append(d.whereConds, whereClause{condition: condition, args: args})
	return d
}

// Soft marks this as a soft delete (sets deleted_at instead of removing)
func (d *DeleteBuilder) Soft() *DeleteBuilder {
	d.softDelete = true
	return d
}

// Build generates the DELETE SQL
func (d *DeleteBuilder) Build() (string, []interface{}) {
	var sb strings.Builder
	var args []interface{}

	if d.softDelete {
		sb.WriteString(fmt.Sprintf("UPDATE %s SET deleted_at = NOW()", d.table))
	} else {
		sb.WriteString(fmt.Sprintf("DELETE FROM %s", d.table))
	}

	if len(d.whereConds) > 0 {
		sb.WriteString(" WHERE ")
		for i, w := range d.whereConds {
			if i > 0 {
				sb.WriteString(" AND ")
			}
			sb.WriteString(w.condition)
			args = append(args, w.args...)
		}
	}
	// BUG: same as update - no protection against accidental full table delete

	return sb.String(), args
}

// Exec executes the DELETE query
func (d *DeleteBuilder) Exec() (sql.Result, error) {
	query, args := d.Build()
	return d.db.Exec(query, args...)
}

// RawQuery executes raw SQL and returns the result
// WARNING: This bypasses all query building safety checks
func (db *DB) RawQuery(query string, args ...interface{}) (*sql.Rows, error) {
	return db.Query(query, args...)
}

// RawExec executes raw SQL without returning rows
func (db *DB) RawExec(query string, args ...interface{}) (sql.Result, error) {
	return db.Exec(query, args...)
}
