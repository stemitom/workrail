package migrations

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Migration struct {
	Version string
	Name    string
	Path    string
	SQL     string
}

type Applied struct {
	Version   string
	Name      string
	AppliedAt time.Time
}

func LoadDir(dir string) ([]Migration, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var migrations []Migration
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		version, name, ok := parseName(entry.Name())
		if !ok {
			return nil, fmt.Errorf("migration %q must be named like 001_name.sql", entry.Name())
		}
		path := filepath.Join(dir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		migrations = append(migrations, Migration{
			Version: version,
			Name:    name,
			Path:    path,
			SQL:     string(data),
		})
	}
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})
	for i := 1; i < len(migrations); i++ {
		if migrations[i].Version == migrations[i-1].Version {
			return nil, fmt.Errorf("duplicate migration version %s", migrations[i].Version)
		}
	}
	return migrations, nil
}

func Up(ctx context.Context, db *pgxpool.Pool, dir string) ([]Applied, error) {
	migrations, err := LoadDir(dir)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version text PRIMARY KEY,
			name text NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now()
		)
	`); err != nil {
		return nil, err
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('workrail_schema_migrations'))`); err != nil {
		return nil, err
	}

	appliedSet := map[string]struct{}{}
	rows, err := tx.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			rows.Close()
			return nil, err
		}
		appliedSet[version] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	var applied []Applied
	for _, migration := range migrations {
		if _, ok := appliedSet[migration.Version]; ok {
			continue
		}
		if _, err := tx.Exec(ctx, migration.SQL); err != nil {
			return nil, fmt.Errorf("apply migration %s %s: %w", migration.Version, migration.Name, err)
		}
		var appliedAt time.Time
		if err := tx.QueryRow(ctx, `
			INSERT INTO schema_migrations (version, name)
			VALUES ($1, $2)
			RETURNING applied_at
		`, migration.Version, migration.Name).Scan(&appliedAt); err != nil {
			return nil, err
		}
		applied = append(applied, Applied{
			Version:   migration.Version,
			Name:      migration.Name,
			AppliedAt: appliedAt,
		})
	}
	return applied, tx.Commit(ctx)
}

func parseName(filename string) (string, string, bool) {
	base := strings.TrimSuffix(filename, filepath.Ext(filename))
	parts := strings.SplitN(base, "_", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	for _, ch := range parts[0] {
		if ch < '0' || ch > '9' {
			return "", "", false
		}
	}
	return parts[0], parts[1], true
}
