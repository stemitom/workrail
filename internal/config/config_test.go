package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefaultsWhenDefaultFileMissing(t *testing.T) {
	t.Setenv("WORKRAIL_CONFIG", "")
	dir := t.TempDir()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.DatabaseURL != DefaultDatabaseURL {
		t.Fatalf("database_url = %q, want default", cfg.DatabaseURL)
	}
	if cfg.API.Addr != ":8080" {
		t.Fatalf("api addr = %q, want :8080", cfg.API.Addr)
	}
}

func TestLoadYAMLAndEnvOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workrail.yaml")
	if err := os.WriteFile(path, []byte(`
database_url: postgres://file
api:
  addr: :8081
worker:
  id: worker-file
  queue: emails
  concurrency: 7
  shutdown_timeout: 45s
  metrics_addr: :9091
tracing:
  enabled: true
  endpoint: collector:4317
  insecure: false
`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DATABASE_URL", "postgres://env")
	t.Setenv("WORKRAIL_QUEUE", "billing")
	t.Setenv("WORKRAIL_WORKER_METRICS_ADDR", "")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.DatabaseURL != "postgres://env" {
		t.Fatalf("database_url = %q, want env override", cfg.DatabaseURL)
	}
	if cfg.API.Addr != ":8081" {
		t.Fatalf("api addr = %q, want file value", cfg.API.Addr)
	}
	if cfg.Worker.Queue != "billing" {
		t.Fatalf("worker queue = %q, want env override", cfg.Worker.Queue)
	}
	if cfg.Worker.MetricsAddr != "" {
		t.Fatalf("metrics addr = %q, want empty env override", cfg.Worker.MetricsAddr)
	}
	if !cfg.Tracing.Enabled {
		t.Fatal("tracing enabled = false, want true")
	}
	if cfg.Tracing.Endpoint != "collector:4317" {
		t.Fatalf("tracing endpoint = %q, want collector:4317", cfg.Tracing.Endpoint)
	}
	if cfg.Tracing.Insecure {
		t.Fatal("tracing insecure = true, want false")
	}
}
