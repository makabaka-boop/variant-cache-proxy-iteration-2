// 可控源站进程。监听 :8081（可用 ORIGIN_ADDR 覆盖）。
package main

import (
	"log"
	"net/http"
	"os"

	"cacheproxy/internal/origin"
)

func main() {
	addr := os.Getenv("ORIGIN_ADDR")
	if addr == "" {
		addr = ":8081"
	}
	c := origin.New()
	log.Printf("controllable origin listening on %s", addr)
	if err := http.ListenAndServe(addr, c.Handler()); err != nil {
		log.Fatal(err)
	}
}
