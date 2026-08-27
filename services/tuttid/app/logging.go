package app

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"

	tuttitypes "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/types"
)

const (
	LogOutputFile   = "file"
	LogOutputStdout = "stdout"
	LogOutputTee    = "tee"
)

type loggerSetup struct {
	Logger      *slog.Logger
	LogFilePath string
	Close       func() error
}

func SetupLoggerFromEnv() (loggerSetup, error) {
	output, err := resolveLogOutput()
	if err != nil {
		return loggerSetup{}, err
	}

	level, err := resolveLogLevel()
	if err != nil {
		return loggerSetup{}, err
	}

	writer, logFilePath, closeFn, err := resolveLogWriter(output)
	if err != nil {
		return loggerSetup{}, err
	}

	handler := slog.NewTextHandler(writer, &slog.HandlerOptions{Level: level})
	logger := slog.New(handler).With(
		"component", "tuttid",
		"pid", os.Getpid(),
	)
	if sessionID := resolveSessionID(); sessionID != "" {
		logger = logger.With("session_id", sessionID)
	}

	return loggerSetup{
		Logger:      logger,
		LogFilePath: logFilePath,
		Close:       closeFn,
	}, nil
}

func resolveLogOutput() (string, error) {
	value := strings.ToLower(strings.TrimSpace(os.Getenv("TUTTID_LOG_OUTPUT")))
	if value == "" {
		return tuttitypes.ResolveDefaultsFromEnv().Logging.DefaultOutput, nil
	}

	switch value {
	case LogOutputFile, LogOutputStdout, LogOutputTee:
		return value, nil
	default:
		return "", fmt.Errorf("invalid TUTTID_LOG_OUTPUT %q", value)
	}
}

func resolveLogLevel() (slog.Leveler, error) {
	value := strings.ToLower(strings.TrimSpace(os.Getenv("TUTTID_LOG_LEVEL")))
	if value == "" {
		value = tuttitypes.ResolveDefaultsFromEnv().Logging.DefaultLevel
	}

	switch value {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return nil, fmt.Errorf("invalid TUTTID_LOG_LEVEL %q", value)
	}
}

func resolveLogWriter(output string) (io.Writer, string, func() error, error) {
	switch output {
	case LogOutputStdout:
		return os.Stdout, "", func() error { return nil }, nil
	case LogOutputFile:
		file, logFilePath, err := openLogFile()
		if err != nil {
			return nil, "", nil, err
		}
		return file, logFilePath, file.Close, nil
	case LogOutputTee:
		file, logFilePath, err := openLogFile()
		if err != nil {
			return nil, "", nil, err
		}
		return io.MultiWriter(os.Stdout, file), logFilePath, file.Close, nil
	default:
		return nil, "", nil, fmt.Errorf("unsupported log output %q", output)
	}
}

func openLogFile() (*RotatingFileWriter, string, error) {
	logFilePath := resolveLogFilePath()
	writer, err := NewRotatingFileWriter(logFilePath, rotatingFileWriterOptionsFromEnv())
	if err != nil {
		return nil, "", err
	}

	return writer, writer.Path(), nil
}

func resolveLogFilePath() string {
	return tuttitypes.ResolveDefaultsFromEnv().State.TuttidLogPath
}

func rotatingFileWriterOptionsFromEnv() RotatingFileWriterOptions {
	defaults := tuttitypes.ResolveDefaultsFromEnv().Logging
	return RotatingFileWriterOptions{
		MaxSizeBytes:  int64(envIntOrDefault("TUTTI_LOG_MAX_SIZE_MB", defaults.MaxSizeMB)) * 1024 * 1024,
		MaxBackups:    envIntOrDefault("TUTTI_LOG_MAX_BACKUPS", defaults.MaxBackups),
		MaxAgeDays:    envIntOrDefault("TUTTI_LOG_MAX_AGE_DAYS", defaults.MaxAgeDays),
		MaxTotalBytes: int64(envIntOrDefault("TUTTI_LOG_MAX_TOTAL_MB", defaults.MaxTotalMB)) * 1024 * 1024,
	}
}

func envIntOrDefault(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}

	return parsed
}

func resolveSessionID() string {
	return strings.TrimSpace(os.Getenv("TUTTI_SESSION_ID"))
}
