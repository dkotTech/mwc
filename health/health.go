package health

import (
	"encoding/json"
	"maps"
	"net/http"
	"slices"

	"github.com/dkotTech/mwc"
)

type Response struct {
	Healthy bool  `json:"healthy"`
	Jobs    []Job `json:"jobs,omitempty"` // the unhealthy ones, sorted by name
}

type Job struct {
	Name      string `json:"name"`
	State     string `json:"state"`
	Restarts  int    `json:"restarts"`
	LastError string `json:"lastError,omitempty"`
}

func Handler(m *mwc.Manager) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		res := check(m)
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		if !res.Healthy {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		if r.Method == http.MethodHead {
			return
		}
		json.NewEncoder(w).Encode(res)
	})
}

func check(m *mwc.Manager) Response {
	status := m.Status()
	res := Response{Healthy: true}
	for _, name := range slices.Sorted(maps.Keys(status)) {
		s := status[name]
		if s.State.Healthy() {
			continue
		}
		res.Healthy = false
		j := Job{Name: name, State: s.State.String(), Restarts: s.Restarts}
		if s.LastErr != nil {
			j.LastError = s.LastErr.Error()
		}
		res.Jobs = append(res.Jobs, j)
	}
	return res
}
