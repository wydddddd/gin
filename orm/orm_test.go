package orm

import (
	"reflect"
	"testing"
	"time"
)

func TestToSnakeCase(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"UserProfile", "user_profile"},
		{"ID", "i_d"},
		{"HTTPServer", "h_t_t_p_server"},
		{"SimpleTest", "simple_test"},
		{"lowercase", "lowercase"},
		{"A", "a"},
		{"ABCDef", "a_b_c_def"},
		{"CreatedAt", "created_at"},
	}

	for _, tt := range tests {
		got := toSnakeCase(tt.input)
		if got != tt.want {
			t.Errorf("toSnakeCase(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestParseFieldTag(t *testing.T) {
	type TestModel struct {
		ID    int64  `db:"id,pk,auto"`
		Name  string `db:"username,size:255,unique"`
		Email string `db:"email,null,index:idx_email"`
		Age   int    `db:",default:0"`
		Skip  string `db:"-"`
	}

	typ := reflect.TypeOf(TestModel{})

	tests := []struct {
		fieldName string
		wantCol   string
		wantPK    bool
		wantAuto  bool
		wantNull  bool
		wantUniq  bool
		wantSize  int
	}{
		{"ID", "id", true, true, false, false, 0},
		{"Name", "username", false, false, false, true, 255},
		{"Email", "email", false, false, true, false, 0},
		{"Age", "age", false, false, false, false, 0},
	}

	for _, tt := range tests {
		field, _ := typ.FieldByName(tt.fieldName)
		tag := field.Tag.Get("db")
		fi := parseFieldTag(field, tag)

		if fi.Column != tt.wantCol {
			t.Errorf("%s: Column = %q, want %q", tt.fieldName, fi.Column, tt.wantCol)
		}
		if fi.IsPrimary != tt.wantPK {
			t.Errorf("%s: IsPrimary = %v, want %v", tt.fieldName, fi.IsPrimary, tt.wantPK)
		}
		if fi.IsAutoInc != tt.wantAuto {
			t.Errorf("%s: IsAutoInc = %v, want %v", tt.fieldName, fi.IsAutoInc, tt.wantAuto)
		}
		if fi.IsNullable != tt.wantNull {
			t.Errorf("%s: IsNullable = %v, want %v", tt.fieldName, fi.IsNullable, tt.wantNull)
		}
		if fi.IsUnique != tt.wantUniq {
			t.Errorf("%s: IsUnique = %v, want %v", tt.fieldName, fi.IsUnique, tt.wantUniq)
		}
		if fi.Size != tt.wantSize {
			t.Errorf("%s: Size = %d, want %d", tt.fieldName, fi.Size, tt.wantSize)
		}
	}
}

func TestQueryBuilder_Build(t *testing.T) {
	db := &DB{}

	tests := []struct {
		name  string
		build func() (string, []interface{})
		want  string
		args  int
	}{
		{
			"simple select",
			func() (string, []interface{}) {
				return db.Table("users").Select("id", "name").Build()
			},
			"SELECT id, name FROM users",
			0,
		},
		{
			"select with where",
			func() (string, []interface{}) {
				return db.Table("users").Where("age > ?", 18).Build()
			},
			"SELECT * FROM users WHERE age > ?",
			1,
		},
		{
			"select with multiple conditions",
			func() (string, []interface{}) {
				return db.Table("users").
					Where("age > ?", 18).
					Where("status = ?", "active").
					Build()
			},
			"SELECT * FROM users WHERE age > ? AND status = ?",
			2,
		},
		{
			"select with or",
			func() (string, []interface{}) {
				return db.Table("users").
					Where("role = ?", "admin").
					OrWhere("role = ?", "superadmin").
					Build()
			},
			"SELECT * FROM users WHERE role = ? OR role = ?",
			2,
		},
		{
			"select with join",
			func() (string, []interface{}) {
				return db.Table("users").
					Select("users.name", "orders.total").
					Join("orders", "orders.user_id = users.id").
					Build()
			},
			"SELECT users.name, orders.total FROM users INNER JOIN orders ON orders.user_id = users.id",
			0,
		},
		{
			"select with pagination",
			func() (string, []interface{}) {
				return db.Table("products").
					OrderBy("created_at DESC").
					Page(3, 20).
					Build()
			},
			"SELECT * FROM products ORDER BY created_at DESC LIMIT 20 OFFSET 40",
			0,
		},
		{
			"select with group by and having",
			func() (string, []interface{}) {
				return db.Table("orders").
					Select("user_id", "SUM(total) as total_spent").
					GroupBy("user_id").
					Having("SUM(total) > 1000").
					Build()
			},
			"SELECT user_id, SUM(total) as total_spent FROM orders GROUP BY user_id HAVING SUM(total) > 1000",
			0,
		},
		{
			"distinct with for update",
			func() (string, []interface{}) {
				return db.Table("inventory").
					Select("product_id").
					Distinct().
					Where("quantity > ?", 0).
					ForUpdate().
					Build()
			},
			"SELECT DISTINCT product_id FROM inventory WHERE quantity > ? FOR UPDATE",
			1,
		},
		{
			"where in",
			func() (string, []interface{}) {
				return db.Table("users").
					WhereIn("id", 1, 2, 3, 4, 5).
					Build()
			},
			"SELECT * FROM users WHERE id IN (?, ?, ?, ?, ?)",
			5,
		},
		{
			"where between",
			func() (string, []interface{}) {
				return db.Table("products").
					WhereBetween("price", 10.0, 100.0).
					Build()
			},
			"SELECT * FROM products WHERE price BETWEEN ? AND ?",
			2,
		},
		{
			"where null and not null",
			func() (string, []interface{}) {
				return db.Table("users").
					WhereNotNull("email").
					WhereNull("deleted_at").
					Build()
			},
			"SELECT * FROM users WHERE email IS NOT NULL AND deleted_at IS NULL",
			0,
		},
		{
			"complex left join",
			func() (string, []interface{}) {
				return db.Table("users").
					Select("users.*", "profiles.avatar").
					LeftJoin("profiles", "profiles.user_id = users.id").
					Where("users.active = ?", true).
					OrderBy("users.created_at DESC").
					Limit(10).
					Build()
			},
			"SELECT users.*, profiles.avatar FROM users LEFT JOIN profiles ON profiles.user_id = users.id WHERE users.active = ? ORDER BY users.created_at DESC LIMIT 10",
			1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, args := tt.build()
			if got != tt.want {
				t.Errorf("Build() =\n  %q\nwant:\n  %q", got, tt.want)
			}
			if len(args) != tt.args {
				t.Errorf("args count = %d, want %d", len(args), tt.args)
			}
		})
	}
}

func TestInsertBuilder_Build(t *testing.T) {
	db := &DB{}

	tests := []struct {
		name string
		build func() (string, []interface{})
		want  string
		args  int
	}{
		{
			"single insert",
			func() (string, []interface{}) {
				return db.Insert("users").
					Columns("name", "email", "age").
					Values("Alice", "alice@example.com", 25).
					Build()
			},
			"INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
			3,
		},
		{
			"batch insert",
			func() (string, []interface{}) {
				return db.Insert("users").
					Columns("name", "email").
					Values("Alice", "alice@example.com").
					Values("Bob", "bob@example.com").
					Values("Charlie", "charlie@example.com").
					Build()
			},
			"INSERT INTO users (name, email) VALUES (?, ?), (?, ?), (?, ?)",
			6,
		},
		{
			"insert with on conflict",
			func() (string, []interface{}) {
				return db.Insert("users").
					Columns("name", "email").
					Values("Alice", "alice@example.com").
					OnConflictDoNothing().
					Build()
			},
			"INSERT INTO users (name, email) VALUES (?, ?) ON CONFLICT DO NOTHING",
			2,
		},
		{
			"upsert",
			func() (string, []interface{}) {
				return db.Insert("users").
					Columns("name", "email", "login_count").
					Values("Alice", "alice@example.com", 1).
					OnConflictUpdate("email", "login_count").
					Build()
			},
			"INSERT INTO users (name, email, login_count) VALUES (?, ?, ?) ON CONFLICT DO UPDATE SET email = EXCLUDED.email, login_count = EXCLUDED.login_count",
			3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, args := tt.build()
			if got != tt.want {
				t.Errorf("Build() =\n  %q\nwant:\n  %q", got, tt.want)
			}
			if len(args) != tt.args {
				t.Errorf("args count = %d, want %d", len(args), tt.args)
			}
		})
	}
}

func TestUpdateBuilder_Build(t *testing.T) {
	db := &DB{}

	tests := []struct {
		name string
		build func() (string, []interface{})
		want  string
		args  int
	}{
		{
			"simple update",
			func() (string, []interface{}) {
				return db.Update("users").
					Set("name", "Bob").
					Where("id = ?", 1).
					Build()
			},
			"UPDATE users SET name = ? WHERE id = ?",
			2,
		},
		{
			"multi-column update",
			func() (string, []interface{}) {
				return db.Update("users").
					Set("name", "Bob").
					Set("email", "bob@example.com").
					Set("updated_at", time.Now()).
					Where("id = ?", 1).
					Build()
			},
			"UPDATE users SET name = ?, email = ?, updated_at = ? WHERE id = ?",
			4,
		},
		{
			"update without where (dangerous!)",
			func() (string, []interface{}) {
				return db.Update("users").
					Set("status", "inactive").
					Build()
			},
			"UPDATE users SET status = ?",
			1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, args := tt.build()
			if got != tt.want {
				t.Errorf("Build() =\n  %q\nwant:\n  %q", got, tt.want)
			}
			if len(args) != tt.args {
				t.Errorf("args count = %d, want %d", len(args), tt.args)
			}
		})
	}
}

func TestDeleteBuilder_Build(t *testing.T) {
	db := &DB{}

	tests := []struct {
		name string
		build func() (string, []interface{})
		want  string
	}{
		{
			"hard delete",
			func() (string, []interface{}) {
				return db.Delete("users").
					Where("id = ?", 1).
					Build()
			},
			"DELETE FROM users WHERE id = ?",
		},
		{
			"soft delete",
			func() (string, []interface{}) {
				return db.Delete("users").
					Soft().
					Where("id = ?", 1).
					Build()
			},
			"UPDATE users SET deleted_at = NOW() WHERE id = ?",
		},
		{
			"delete with multiple conditions",
			func() (string, []interface{}) {
				return db.Delete("sessions").
					Where("expired_at < ?", time.Now()).
					Where("user_id = ?", 42).
					Build()
			},
			"DELETE FROM sessions WHERE expired_at < ? AND user_id = ?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := tt.build()
			if got != tt.want {
				t.Errorf("Build() =\n  %q\nwant:\n  %q", got, tt.want)
			}
		})
	}
}

func TestTableBuilder_Build(t *testing.T) {
	tb := &TableBuilder{name: "users"}
	tb.ID()
	tb.String("username", 100).Unique()
	tb.String("email", 255).Unique()
	tb.String("password_hash", 255)
	tb.Integer("age").Nullable()
	tb.Text("bio").Nullable()
	tb.Boolean("is_active").Default("true")
	tb.JSON("preferences").Nullable()
	tb.Timestamps()
	tb.SoftDelete()
	tb.Index("idx_users_email", "email")
	tb.UniqueIndex("idx_users_username", "username")

	sql := tb.Build()

	// Check key parts exist
	expectedParts := []string{
		"CREATE TABLE IF NOT EXISTS users",
		"id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT",
		"username VARCHAR(100) NOT NULL UNIQUE",
		"email VARCHAR(255) NOT NULL UNIQUE",
		"password_hash VARCHAR(255) NOT NULL",
		"age INT",
		"bio TEXT",
		"is_active BOOLEAN NOT NULL DEFAULT true",
		"preferences JSON",
		"created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
		"deleted_at TIMESTAMP",
		"PRIMARY KEY (id)",
		"INDEX idx_users_email (email)",
		"UNIQUE INDEX idx_users_username (username)",
	}

	for _, part := range expectedParts {
		if !contains(sql, part) {
			t.Errorf("Build() missing expected part: %q\nGot:\n%s", part, sql)
		}
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.MaxOpenConns != 25 {
		t.Errorf("MaxOpenConns = %d, want 25", cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns != 10 {
		t.Errorf("MaxIdleConns = %d, want 10", cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime != time.Hour {
		t.Errorf("ConnMaxLifetime = %v, want 1h", cfg.ConnMaxLifetime)
	}
	if cfg.SlowThreshold != 200*time.Millisecond {
		t.Errorf("SlowThreshold = %v, want 200ms", cfg.SlowThreshold)
	}
}

func TestDialect_String(t *testing.T) {
	tests := []struct {
		d    Dialect
		want string
	}{
		{MySQL, "mysql"},
		{PostgreSQL, "postgres"},
		{SQLite, "sqlite3"},
		{Dialect(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.d.String(); got != tt.want {
			t.Errorf("Dialect(%d).String() = %q, want %q", tt.d, got, tt.want)
		}
	}
}

func TestDBMetrics_Snapshot(t *testing.T) {
	m := &DBMetrics{
		QueryCount:     100,
		SlowQueryCount: 5,
		ErrorCount:     2,
		TotalDuration:  10 * time.Second,
	}

	snap := m.Snapshot()
	if snap["query_count"] != int64(100) {
		t.Errorf("query_count = %v, want 100", snap["query_count"])
	}
	if snap["slow_query_count"] != int64(5) {
		t.Errorf("slow_query_count = %v, want 5", snap["slow_query_count"])
	}
	if snap["error_count"] != int64(2) {
		t.Errorf("error_count = %v, want 2", snap["error_count"])
	}
}

func TestPoolConfig_Defaults(t *testing.T) {
	cfg := DefaultPoolConfig()

	if cfg.MaxConnections != 50 {
		t.Errorf("MaxConnections = %d, want 50", cfg.MaxConnections)
	}
	if cfg.MinConnections != 5 {
		t.Errorf("MinConnections = %d, want 5", cfg.MinConnections)
	}
	if cfg.AcquireTimeout != 5*time.Second {
		t.Errorf("AcquireTimeout = %v, want 5s", cfg.AcquireTimeout)
	}
	if cfg.RetryAttempts != 3 {
		t.Errorf("RetryAttempts = %d, want 3", cfg.RetryAttempts)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstr(s, substr))
}

func containsSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
