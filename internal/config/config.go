package config

import (
	"os"
	"strconv"
)

type Config struct {
	DatabaseURL string
	RedisURL    string
	Port        int
	Env         string
	WorkerCount int
}

func Load() *Config {
	return &Config{
		DatabaseURL: getEnvStr("DATABASE_URL", "postgres://taskqueue:taskqueue@localhost:5432/taskqueue?sslmode=disable"),
		RedisURL:    getEnvStr("REDIS_URL", "redis://localhost:6379"),
		Port:        getEnvInt("PORT", 8081),
		Env:         getEnvStr("ENV", "development"),
		WorkerCount: getEnvInt("WORKER_COUNT", 3),
	}
}

func getEnvStr(key, fallback string) string {
	val := os.Getenv(key)
	if val != "" {
		return val
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	valStr := os.Getenv(key)
	if valStr != "" {
		valInt, err := strconv.Atoi(valStr)
		if err == nil {
			return valInt
		}
	}
	return fallback
}
