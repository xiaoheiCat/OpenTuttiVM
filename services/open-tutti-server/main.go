// open-tutti-server is the self-hosted scheduling core of OpenTuttiVM:
// room lifecycle, the authoritative workspace operation sequencer, CAS
// object storage, realtime events, and the multiplexed preview relay. It is
// a single Go binary configured by environment variables and a .env file.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
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
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/store"
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
	borrows := borrow.NewRegistry(cfg.BorrowerDisconnectGrace)
	hub := realtime.NewHub(nil, rooms, previews, borrows, log)
	seq := sequencer.NewManager(repo, cfg, cas, hub, log)
	hub.SetSequencer(seq)
	// The relay only dials routes the target device advertised in the
	// preview registry — announced ports, not arbitrary session-network
	// TCP targets.
	relay := tunnel.NewRelay(log, previews)
	server := api.New(cfg, rooms, seq, hub, previews, borrows, relay, cas, repo, log)
	rooms.SetBroadcaster(hub)
	// Reference-aware CAS collection: dissolution (per-room and the
	// startup sweep) drops object-store entries whose last reference
	// died, keeping OPEN_TUTTI_OBJECTS_DIR bounded.
	collector := &casCollector{repo: repo, cas: cas, log: log}
	rooms.SetCASCollector(collector)
	// Quota/reference failures can create orphans without dissolving a room.
	// Sweep before serving requests, then retry during ordinary maintenance.
	collector.Collect(context.Background(), nil)
	// Token refresh (rejoin recovery) tears the device's live
	// transports down, matching kick and leave semantics.
	rooms.SetLeaveFence(seq.FreezeAt, seq.UnfreezeAt)
	rooms.SetCurrentSequence(seq.CurrentSequence)
	rooms.SetBarrierClean(seq.ClearBarriersOf)
	rooms.SetMembershipGuard(seq.MembershipMutation)
	rooms.SetConnectionDropper(func(roomID, deviceID string) {
		hub.DropDevice(roomID, deviceID)
		relay.DropDevice(roomID, deviceID)
	})

	// The server never restores rooms across restarts: end everything still
	// marked active so CAS references release and objects can be
	// collected. Fail CLOSED on dissolution errors: a room left active
	// keeps valid memberships and tokens, but the sequencer restores no
	// workspace state, so reconnecting clients would authenticate into
	// a fresh empty engine and could overwrite apparent room state.
	if err := rooms.DissolveAllActive(context.Background()); err != nil {
		return fmt.Errorf("startup dissolution of stale rooms: %w", err)
	}

	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			_ = repo.SweepCASPending(context.Background(), time.Now())
			collector.Collect(context.Background(), nil)
			hub.ExpireBorrowerGrace(time.Now())
			active, err := repo.ListActiveRooms(context.Background())
			if err != nil {
				continue
			}
			for _, r := range active {
				dissolved, err := rooms.CheckGracePeriods(context.Background(), r.ID)
				if err != nil {
					log.Warn("grace check", "room", r.ID, "err", err)
					continue
				}
				if !dissolved {
					continue
				}
				// Terminal teardown mirrors the HTTP leave path: a
				// dissolved room must not keep sequencing state, live
				// sockets, or relay tunnels — tunnel sockets are
				// independent of business presence and would otherwise
				// keep relaying to stale advertised routes.
				seq.CloseRoom(r.ID)
				previews.ClearRoom(r.ID)
				borrows.ClearRoom(r.ID)
				hub.DropRoom(r.ID)
				relay.DropRoom(r.ID)
				log.Info("room dissolved by grace period", "room", r.ID)
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

// casCollector deletes dissolved rooms' objects once no surviving room
// references them.
type casCollector struct {
	repo       store.Repository
	cas        vmcas.Store
	log        *slog.Logger
	mu         sync.Mutex
	localAfter string
	dbAfter    string
}

const casSweepPageSize = 256

func (c *casCollector) Collect(ctx context.Context, hashes []string) {
	if len(hashes) == 0 {
		if local, ok := c.cas.(*vmcas.LocalStore); ok {
			c.mu.Lock()
			localAfter, dbAfter := c.localAfter, c.dbAfter
			c.mu.Unlock()
			objects, err := local.List(localAfter, casSweepPageSize)
			if err != nil {
				c.log.Warn("cas file enumeration", "err", err)
			} else {
				hashes = append(hashes, objects...)
				if len(objects) > 0 {
					localAfter = objects[len(objects)-1]
				}
				if len(objects) < casSweepPageSize {
					localAfter = ""
				}
			}
			objectsDB, err := c.repo.ListCASObjects(ctx, dbAfter, casSweepPageSize)
			if err != nil {
				c.log.Warn("cas object enumeration", "err", err)
			} else {
				for _, object := range objectsDB {
					dbAfter = object.Hash
					if ok, err := local.Has(object.Hash); err == nil && ok {
						hashes = append(hashes, object.Hash)
					}
				}
				if len(objectsDB) < casSweepPageSize {
					dbAfter = ""
				}
			}
			c.mu.Lock()
			c.localAfter, c.dbAfter = localAfter, dbAfter
			c.mu.Unlock()
		}
	}
	if orphaned, err := c.repo.ListCASOrphans(ctx); err == nil {
		hashes = append(hashes, orphaned...)
	}
	// The repository holds the CAS publication fence across its short global
	// ref check and the subsequent deletes, so a surviving room can never
	// publish a reference to an object being removed.
	err := c.repo.CollectUnreferencedCAS(ctx, hashes, func(hash string) error {
		delErr := c.cas.Delete(hash)
		if delErr != nil {
			c.log.Warn("cas collection: delete", "hash", hash, "err", delErr)
		}
		return delErr
	})
	if err != nil {
		c.log.Warn("cas collection", "err", err)
	}
}
