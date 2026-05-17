package migrations

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDirOrdersMigrations(t *testing.T) {
	dir := t.TempDir()
	writeMigration(t, dir, "002_second.sql", "select 2;")
	writeMigration(t, dir, "001_first.sql", "select 1;")

	migrations, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir() error = %v", err)
	}
	if len(migrations) != 2 {
		t.Fatalf("len = %d, want 2", len(migrations))
	}
	if migrations[0].Version != "001" || migrations[1].Version != "002" {
		t.Fatalf("versions = %s, %s; want 001, 002", migrations[0].Version, migrations[1].Version)
	}
}

func TestLoadDirRejectsBadName(t *testing.T) {
	dir := t.TempDir()
	writeMigration(t, dir, "init.sql", "select 1;")
	if _, err := LoadDir(dir); err == nil {
		t.Fatal("LoadDir() error = nil, want bad name error")
	}
}

func TestLoadDirRejectsDuplicateVersion(t *testing.T) {
	dir := t.TempDir()
	writeMigration(t, dir, "001_first.sql", "select 1;")
	writeMigration(t, dir, "001_second.sql", "select 2;")
	if _, err := LoadDir(dir); err == nil {
		t.Fatal("LoadDir() error = nil, want duplicate version error")
	}
}

func writeMigration(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}
