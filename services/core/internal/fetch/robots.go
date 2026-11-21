package fetch

import (
	"context"
	"io"
	"net/url"
	"regexp"
	"strings"
	"time"
)

type robotRule struct {
	pattern *regexp.Regexp
	length  int
	allow   bool
}

type rules []robotRule

// parseRobots combines matching agent groups, preferring them over wildcard groups.
func parseRobots(body []byte, agent string) rules {
	type group struct {
		agents []string
		rules  rules
	}
	var groups []group
	current := group{}
	hasDirectives := false
	flush := func() {
		if len(current.agents) > 0 {
			groups = append(groups, current)
		}
		current = group{}
		hasDirectives = false
	}
	for line := range strings.SplitSeq(string(body), "\n") {
		line, _, _ = strings.Cut(line, "#")
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key, value = strings.ToLower(strings.TrimSpace(key)), strings.TrimSpace(value)
		if key == "user-agent" {
			if hasDirectives {
				flush()
			}
			current.agents = append(current.agents, strings.ToLower(value))
			continue
		}
		if len(current.agents) == 0 {
			continue
		}
		hasDirectives = true
		if (key != "allow" && key != "disallow") || value == "" || !strings.HasPrefix(value, "/") {
			continue
		}
		anchored := strings.HasSuffix(value, "$")
		if anchored {
			value = strings.TrimSuffix(value, "$")
		}
		pattern := "^" + strings.ReplaceAll(regexp.QuoteMeta(value), `\*`, ".*")
		if anchored {
			pattern += "$"
		}
		current.rules = append(current.rules, robotRule{
			pattern: regexp.MustCompile(pattern), length: len(strings.ReplaceAll(value, "*", "")), allow: key == "allow",
		})
	}
	flush()
	var specific, wildcard rules
	hasSpecific := false
	for _, g := range groups {
		matched, star := false, false
		for _, token := range g.agents {
			matched = matched || token == strings.ToLower(agent)
			star = star || token == "*"
		}
		if matched {
			hasSpecific = true
			specific = append(specific, g.rules...)
		} else if star {
			wildcard = append(wildcard, g.rules...)
		}
	}
	if hasSpecific {
		return specific
	}
	return wildcard
}

func (r rules) allowed(path string) bool {
	allowed, longest := true, -1
	for _, rule := range r {
		if rule.pattern.MatchString(path) && (rule.length > longest || rule.length == longest && rule.allow) {
			allowed, longest = rule.allow, rule.length
		}
	}
	return allowed
}

func (f *Fetcher) checkRobots(ctx context.Context, u *url.URL, host *hostState) error {
	select {
	case host.robotsLock <- struct{}{}:
		defer func() { <-host.robotsLock }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if !host.robotsLoaded || f.now().Sub(host.robotsAt) >= 24*time.Hour {
		robotURL := &url.URL{Scheme: u.Scheme, Host: u.Host, Path: "/robots.txt"}
		resp, err := f.request(ctx, robotURL, nil, false)
		var policy rules
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			policy = parseRobots([]byte("User-agent: *\nDisallow: /"), "rowanbot")
		} else {
			switch {
			case resp.StatusCode >= 200 && resp.StatusCode < 300:
				const maxRobotsBytes = 1 << 20
				body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxRobotsBytes+1))
				if readErr == nil && len(body) <= maxRobotsBytes {
					policy = parseRobots(body, "rowanbot")
				} else {
					policy = parseRobots([]byte("User-agent: *\nDisallow: /"), "rowanbot")
				}
			case resp.StatusCode >= 400 && resp.StatusCode < 500:
				// A missing or inaccessible robots file permits all paths.
			default:
				policy = parseRobots([]byte("User-agent: *\nDisallow: /"), "rowanbot")
			}
			resp.Body.Close()
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
		host.robots, host.robotsAt, host.robotsLoaded = policy, f.now(), true
	}
	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	if u.RawQuery != "" {
		path += "?" + u.RawQuery
	}
	if !host.robots.allowed(path) {
		return ErrDisallowed
	}
	return nil
}
