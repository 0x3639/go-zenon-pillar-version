package runner

import (
	"errors"
	"github.com/0x3639/go-zenon-pillar-version/internal/httpapi"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/0x3639/go-zenon-pillar-version/internal/netguard"
)

const UpstreamRevision = "bb39e31c608dc397a91b98b21b096bca99a9f4d6"
const Release = "0.3.0-associations"

type Config struct {
	ObserverID      string
	DataDir         string
	MaxPeers        int
	Duration        time.Duration
	Seeds           []netguard.Candidate
	RPCURLs         []string
	Ingress, Egress uint64
	DataBudget      int64
	Mode            string
	Port, Retention int
	Origins         []string
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
func number(key string, def, min, max int) (int, error) {
	n, e := strconv.Atoi(env(key, strconv.Itoa(def)))
	if e != nil || n < min || n > max {
		return 0, errors.New("invalid " + key)
	}
	return n, nil
}
func Load() (Config, error) {
	c := Config{ObserverID: env("OBSERVER_ID", "local-feasibility"), DataDir: env("DATA_DIR", "/data")}
	if len(c.ObserverID) > 128 {
		return c, errors.New("OBSERVER_ID too long")
	}
	var e error
	c.Mode = env("MODE", "service")
	if c.Mode != "service" && c.Mode != "feasibility" {
		return c, errors.New("MODE must be service or feasibility")
	}
	c.Port, e = number("PORT", 8080, 1, 65535)
	if e != nil {
		return c, e
	}
	c.Retention, e = number("RAW_RETENTION_DAYS", 7, 1, 7)
	if e != nil {
		return c, e
	}
	for _, origin := range strings.Split(os.Getenv("ALLOWED_ORIGINS"), ",") {
		origin = strings.TrimSpace(origin)
		if origin == "" {
			continue
		}
		if !httpapi.ValidOrigin(origin) {
			return c, errors.New("ALLOWED_ORIGINS must contain exact HTTP(S) origins without paths")
		}
		c.Origins = append(c.Origins, origin)
	}
	c.MaxPeers, e = number("MAX_PEERS", 4, 1, 8)
	if e != nil {
		return c, e
	}
	if env("MAX_DIALS", "1") != "1" || env("PEER_ROTATION_ENABLED", "false") != "false" {
		return c, errors.New("milestone 1 requires MAX_DIALS=1 and rotation disabled")
	}
	defaultDuration := "0s"
	if c.Mode == "feasibility" {
		defaultDuration = "65m"
	}
	c.Duration, e = time.ParseDuration(env("RUN_DURATION", defaultDuration))
	if e != nil || c.Duration < 0 || (c.Duration > 0 && c.Duration < time.Second) || c.Duration > 48*time.Hour || (c.Mode == "feasibility" && (c.Duration < time.Second || c.Duration > 2*time.Hour)) {
		return c, errors.New("RUN_DURATION must be 0s (continuous service) or 1s–48h; feasibility requires 1s–2h")
	}
	for _, item := range strings.Split(os.Getenv("PEER_SEEDS"), ",") {
		if strings.TrimSpace(item) == "" {
			continue
		}
		s, e := netguard.Parse(strings.TrimSpace(item))
		if e != nil {
			return c, e
		}
		for _, v := range c.Seeds {
			if s.ID == v.ID {
				return c, errors.New("duplicate seed identity")
			}
		}
		c.Seeds = append(c.Seeds, s)
	}
	if len(c.Seeds) == 0 || len(c.Seeds) > 32 {
		return c, errors.New("configure 1–32 explicit PEER_SEEDS; candidate discovery is not automatic")
	}
	c.RPCURLs = strings.Split(env("RPC_REFERENCE_URLS", "wss://my.hc1node.com:35998,wss://node.zenonhub.io:35998,wss://node.atsocy.com:35998"), ",")
	in, e := number("INGRESS_BUDGET_MIB_PER_HOUR", 250, 1, 250)
	if e != nil {
		return c, e
	}
	out, e := number("EGRESS_BUDGET_MIB_PER_HOUR", 50, 1, 50)
	if e != nil {
		return c, e
	}
	// The 10% reserve must accommodate the bounded evidence file and report
	// between storage checks. Smaller budgets need a smaller writer cap first.
	data, e := number("DATA_BUDGET_MIB", 2048, 1024, 2048)
	if e != nil {
		return c, e
	}
	c.Ingress = uint64(in) << 20
	c.Egress = uint64(out) << 20
	c.DataBudget = int64(data) << 20
	return c, nil
}
