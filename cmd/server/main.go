package main

import (
	"flag"
	"log"

	"kestreldb/internal/engine"
	"kestreldb/internal/server"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6380", "listen address")
	flag.Parse()

	db := engine.NewDB()
	srv := server.New(*addr, db)
	log.Printf("kestreldb server listening on %s", *addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
