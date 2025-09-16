package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type option struct {
	label  string
	weight int
}

type chooser struct {
	options []option
	total   int
}

func newChooser(opts []option) chooser {
	filtered := make([]option, 0, len(opts))
	total := 0
	for _, opt := range opts {
		if opt.weight <= 0 {
			continue
		}
		total += opt.weight
		filtered = append(filtered, opt)
	}
	return chooser{options: filtered, total: total}
}

func (c chooser) pick(r *rand.Rand) string {
	if c.total <= 0 || len(c.options) == 0 {
		if len(c.options) > 0 {
			return c.options[0].label
		}
		return ""
	}
	n := r.Intn(c.total)
	cumulative := 0
	for _, opt := range c.options {
		cumulative += opt.weight
		if n < cumulative {
			return opt.label
		}
	}
	return c.options[len(c.options)-1].label
}

func main() {
	total := flag.Int("n", 10000, "total tasks to enqueue")
	workers := flag.Int("w", 16, "number of concurrent request goroutines")
	pctNoop := flag.Int("pct-noop", 80, "percentage of noop tasks")
	pctFlaky := flag.Int("pct-flaky", 10, "percentage of flaky tasks")
	pctFail := flag.Int("pct-fail", 5, "percentage of always-fail tasks")
	pctSleep := flag.Int("pct-sleep", 5, "percentage of sleeper tasks")
	sleepMS := flag.Int("sleep-ms", 62000, "sleep duration for sleeper tasks in ms")
	flakyAttempts := flag.Int("flaky-attempts", 2, "number of initial failures for flaky tasks")
	pctCritical := flag.Int("pct-critical", 5, "critical priority weight")
	pctHigh := flag.Int("pct-high", 30, "high priority weight")
	pctDefault := flag.Int("pct-default", 50, "default priority weight")
	pctLow := flag.Int("pct-low", 15, "low priority weight")
	endpoint := flag.String("url", "http://localhost:8080/tasks", "enqueue endpoint")
	flag.Parse()

	typeChooser := newChooser([]option{
		{label: "noop", weight: *pctNoop},
		{label: "flaky", weight: *pctFlaky},
		{label: "always_fail", weight: *pctFail},
		{label: "sleeper", weight: *pctSleep},
	})
	priorityChooser := newChooser([]option{
		{label: "critical", weight: *pctCritical},
		{label: "high", weight: *pctHigh},
		{label: "default", weight: *pctDefault},
		{label: "low", weight: *pctLow},
	})

	client := &http.Client{Timeout: 5 * time.Second}
	work := make(chan int, *total)
	for i := 0; i < *total; i++ {
		work <- i
	}
	close(work)

	var wg sync.WaitGroup
	wg.Add(*workers)
	for g := 0; g < *workers; g++ {
		go func(id int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			for i := range work {
				taskType := typeChooser.pick(rnd)
				priority := priorityChooser.pick(rnd)
				if taskType == "" {
					taskType = "noop"
				}
				if priority == "" {
					priority = "default"
				}

				payload := make(map[string]any)
				switch taskType {
				case "noop":
					payload["i"] = i
				case "always_fail":
					// no additional fields
				case "flaky":
					payload["fail_first_n"] = *flakyAttempts
				case "sleeper":
					payload["ms"] = *sleepMS
				}

				body := map[string]any{
					"type":     taskType,
					"priority": priority,
					"payload":  payload,
				}
				b, _ := json.Marshal(body)
				resp, err := client.Post(*endpoint, "application/json", bytes.NewReader(b))
				if err != nil {
					log.Println("post err:", err)
					continue
				}
				log.Printf("POST %d: %s", i, resp.Status)
				resp.Body.Close()
			}
		}(g)
	}
	wg.Wait()
}
