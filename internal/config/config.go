package config

import (
	"errors"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

const DefaultDatabaseURL = "postgres://durable:durable@localhost:5432/durable?sslmode=disable"

type Config struct {
	DatabaseURL string       `yaml:"database_url"`
	API         APIConfig    `yaml:"api"`
	Worker      WorkerConfig `yaml:"worker"`
}

type APIConfig struct {
	Addr string `yaml:"addr"`
}

type WorkerConfig struct {
	ID              string `yaml:"id"`
	Queue           string `yaml:"queue"`
	Concurrency     int    `yaml:"concurrency"`
	ShutdownTimeout string `yaml:"shutdown_timeout"`
	MetricsAddr     string `yaml:"metrics_addr"`
}

func Default() Config {
	return Config{
		DatabaseURL: DefaultDatabaseURL,
		API: APIConfig{
			Addr: ":8080",
		},
		Worker: WorkerConfig{
			Queue:           "default",
			Concurrency:     4,
			ShutdownTimeout: "30s",
			MetricsAddr:     ":9090",
		},
	}
}

func Load(path string) (Config, error) {
	cfg := Default()
	if path == "" {
		path = os.Getenv("WORKRAIL_CONFIG")
	}
	if path == "" {
		path = "workrail.yaml"
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && path == "workrail.yaml" {
			applyEnv(&cfg)
			return cfg, nil
		}
		return cfg, err
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	applyDefaults(&cfg)
	applyEnv(&cfg)
	return cfg, nil
}

func applyDefaults(cfg *Config) {
	defaults := Default()
	if cfg.DatabaseURL == "" {
		cfg.DatabaseURL = defaults.DatabaseURL
	}
	if cfg.API.Addr == "" {
		cfg.API.Addr = defaults.API.Addr
	}
	if cfg.Worker.Queue == "" {
		cfg.Worker.Queue = defaults.Worker.Queue
	}
	if cfg.Worker.Concurrency == 0 {
		cfg.Worker.Concurrency = defaults.Worker.Concurrency
	}
	if cfg.Worker.ShutdownTimeout == "" {
		cfg.Worker.ShutdownTimeout = defaults.Worker.ShutdownTimeout
	}
	if cfg.Worker.MetricsAddr == "" {
		cfg.Worker.MetricsAddr = defaults.Worker.MetricsAddr
	}
}

func applyEnv(cfg *Config) {
	if value := os.Getenv("DATABASE_URL"); value != "" {
		cfg.DatabaseURL = value
	}
	if value := firstEnv("WORKRAIL_API_ADDR", "DWF_API_ADDR"); value != "" {
		cfg.API.Addr = value
	}
	if value := firstEnv("WORKRAIL_WORKER_ID", "DWF_WORKER_ID"); value != "" {
		cfg.Worker.ID = value
	}
	if value := os.Getenv("WORKRAIL_QUEUE"); value != "" {
		cfg.Worker.Queue = value
	}
	if value := os.Getenv("WORKRAIL_WORKER_CONCURRENCY"); value != "" {
		if n, err := strconv.Atoi(value); err == nil {
			cfg.Worker.Concurrency = n
		}
	}
	if value := os.Getenv("WORKRAIL_SHUTDOWN_TIMEOUT"); value != "" {
		cfg.Worker.ShutdownTimeout = value
	}
	if value, ok := os.LookupEnv("WORKRAIL_WORKER_METRICS_ADDR"); ok {
		cfg.Worker.MetricsAddr = value
	}
}

func firstEnv(keys ...string) string {
	for _, key := range keys {
		if value := os.Getenv(key); value != "" {
			return value
		}
	}
	return ""
}
