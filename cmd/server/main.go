package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"kestreldb/internal/engine"
	"kestreldb/internal/server"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6380", "listen address")
	snapshot := flag.String("snapshot", "", "snapshot file to load and save")
	appendOnly := flag.String("appendonly", "", "append-only file path")
	appendFsync := flag.String("appendfsync", "everysec", "AOF fsync policy: always, everysec, or no")
	replicaOf := flag.String("replicaof", "", "primary address for read-only replication")
	replicaID := flag.String("replica-id", "", "stable replica identifier")
	flag.Parse()

	db := engine.NewDB()
	if *snapshot != "" {
		if err := db.LoadSnapshotFile(*snapshot); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Fatal(err)
		}
	}
	if *appendOnly != "" {
		var policy engine.FsyncPolicy
		switch strings.ToLower(*appendFsync) {
		case "always":
			policy = engine.FsyncAlways
		case "everysec":
			policy = engine.FsyncEverySecond
		case "no":
			policy = engine.FsyncNever
		default:
			log.Fatal("invalid -appendfsync policy")
		}
		if err := db.OpenAOF(*appendOnly, policy); err != nil {
			log.Fatal(err)
		}
		defer func() {
			if err := db.CloseAOF(); err != nil {
				log.Print(err)
			}
		}()
	}
	srv := server.New(*addr, db)
	if *replicaOf != "" {
		srv.SetReplicaOf(*replicaOf, *replicaID)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() { <-ctx.Done(); _ = srv.Close() }()
	if err := srv.ListenAndServe(); err != nil {
		log.Print(err)
		return
	}
	if *snapshot != "" {
		if err := db.SaveSnapshot(*snapshot); err != nil {
			log.Print(err)
		}
	}
}
