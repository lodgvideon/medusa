// Command medusa-node runs a single medusa cluster member, configured from the
// environment, with an HTTP admin/health surface. It is the artifact deployed
// in containers and Kubernetes.
//
// Environment:
//
//	MEDUSA_ID         unique node id (default: hostname)
//	MEDUSA_ADDR       data-plane listen+advertise address peers dial
//	                  (default: ":7700"; in k8s set to "$(POD_IP):7700")
//	MEDUSA_HTTP_ADDR  admin/health listen address (default: ":8080")
//	MEDUSA_SEEDS      comma-separated seed addresses to join (optional)
//	MEDUSA_BACKUPS    backup copies per partition / replication factor − 1
//	                  (default: 1; values below 1 are treated as 1)
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lodgvideon/medusa"
	"github.com/lodgvideon/medusa/httpapi"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	id := env("MEDUSA_ID", hostname())
	addr := env("MEDUSA_ADDR", ":7700")
	bindAddr := env("MEDUSA_BIND_ADDR", addr)
	httpAddr := env("MEDUSA_HTTP_ADDR", ":8080")
	dataDir := os.Getenv("MEDUSA_DATA_DIR")
	seeds := splitSeeds(os.Getenv("MEDUSA_SEEDS"))
	backups := envInt("MEDUSA_BACKUPS", 0) // 0 → node defaults it to 1

	// Seeds are passed to the node, whose maintenance loop retries joining until
	// the cluster converges — so startup order does not matter. BindAddr lets a
	// node listen on ":7700" while advertising a stable DNS name. DataDir, when
	// set, persists a snapshot so the cluster survives a whole-cluster restart.
	// Backups sets how many copies of each partition the cluster keeps.
	node, err := medusa.New(medusa.Config{ID: id, Addr: addr, BindAddr: bindAddr, Seeds: seeds, DataDir: dataDir, Backups: backups})
	if err != nil {
		slog.Error("start node", "err", err)
		os.Exit(1)
	}

	srv := &http.Server{Addr: httpAddr, Handler: httpapi.New(node)}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server", "err", err)
			os.Exit(1)
		}
	}()
	slog.Info("node listening", "id", id, "advertise", addr, "bind", node.Addr(), "http", httpAddr, "seeds", seeds, "backups", node.BackupCount())

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	slog.Info("leaving cluster gracefully")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	// Hand off partitions and announce departure so peers rebalance before this
	// node disappears — no data is left stranded on a backup.
	_ = node.Leave(ctx)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envInt reads an integer env var, falling back to def when unset or malformed.
func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
	}
	return def
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "medusa-node"
	}
	return h
}

func splitSeeds(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
