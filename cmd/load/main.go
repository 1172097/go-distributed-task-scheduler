package main

import (
	"bytes"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

func main() {
	base := "http://localhost:8080/tasks"
	prs := []string{"critical", "high", "default", "low"}
	http.DefaultClient.Timeout = 5 * time.Second
	n := 5000
	w := 16
	work := make(chan int, n)
	for i := 0; i < n; i++ {
		work <- i
	}
	close(work)
	var wg sync.WaitGroup
	wg.Add(w)
	for g := 0; g < w; g++ {
		go func() {
			defer wg.Done()
			for i := range work {
				body := map[string]any{
					"type":     "noop",
					"priority": prs[rand.Intn(len(prs))],
					"payload":  map[string]any{"i": i},
				}
				b, _ := json.Marshal(body)
				   resp, err := http.Post(base, "application/json", bytes.NewReader(b))
				   if err != nil {
					   log.Println("post err:", err)
				   } else {
					   log.Printf("POST %d: %s", i, resp.Status)
					   resp.Body.Close()
				   }
			}
		}()
	}
	wg.Wait()
}
