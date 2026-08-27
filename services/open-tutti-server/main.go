// open-tutti-server is the self-hosted scheduling core of OpenTuttiVM:
// room lifecycle, the authoritative workspace operation sequencer, CAS
// object storage, realtime events, and the multiplexed preview relay. It is
// a single Go binary configured by environment variables and a .env file.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	vmcas "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas"

	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/api"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/borrow"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/config"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/preview"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/realtime"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/room"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/sequencer"
	store_sqlite "github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/store/sqlite"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/tunnel"
)

func main() {
	if err := run(); err != nil {
		slog.Error("open-tutti-server exited", "err", err)
		os.Exit(1)
	}
}

func run() error {
	envFile := ".env"
	if v := os.Getenv("OPEN_TUTTI_ENV_FILE"); v != "" {
		envFile = v
	}
	cfg, err := config.Load(envFile)
	if err != nil {
		return err
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel(cfg.LogLevel)}))
	slog.SetDefault(log)

	if err := os.MkdirAll(filepath.Dir(cfg.DatabasePath), 0o755); err != nil {
		return err
	}
	repo, err := store_sqlite.Open(cfg.DatabasePath)
	if err != nil {
		return err
	}
	defer repo.Close()

	cas, err := vmcas.NewLocalStore(cfg.ObjectsDir)
	if err != nil {
		return err
	}

	rooms := room.NewService(repo, cfg, room.RealClock{}, nil)
	previews := preview.NewRegistry()
	borrows := borrow.NewRegistry()
	hub := realtime.NewHub(nil, rooms, previews, borrows, log)
	seq := sequencer.NewManager(repo, cfg, cas, hub, log)
	hub.SetSequencer(seq)
	relay := tunnel.NewRelay(log)
	server := api.New(cfg, rooms, seq, hub, previews, borrows, relay, cas, repo, log)
	rooms.SetBroadcaster(hub)

	// The server never restores rooms across restarts: end everything still
	// marked active so CAS references release and objects can be collected.
	if err := rooms.DissolveAllActive(context.Background()); err != nil {
		log.Warn("startup dissolution of stale rooms failed", "err", err)
	}

	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			active, err := repo.ListActiveRooms(context.Background())
			if err != nil {
				continue
			}
			for _, r := range active {
				if err := rooms.CheckGracePeriods(context.Background(), r.ID); err != nil {
					log.Warn("grace check", "room", r.ID, "err", err)
				}
			}
		}
	}()

	log.Info("open-tutti-server listening", "addr", cfg.ListenAddr, "public_url", cfg.PublicURL)
	errCh := make(chan error, 1)
	go func() { errCh <- httpServer.ListenAndServe() }()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		return err
	case <-sig:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return httpServer.Shutdown(ctx)
	}
}

func logLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
