package fetch

import "testing"

func TestRobotsAgentGroupWins(t *testing.T) {
	r := parseRobots([]byte("User-agent: *\nDisallow: /\n\nUser-agent: rowanbot\nDisallow: /private\n"), "rowanbot")
	if !r.allowed("/data") || r.allowed("/private/x") {
		t.Fatal("specific agent group must override wildcard group")
	}
}

func TestRobotsLongestMatchAllowTie(t *testing.T) {
	r := parseRobots([]byte("User-agent: rowanbot\nDisallow: /a\nAllow: /a/b\nDisallow: /tie\nAllow: /tie\n"), "rowanbot")
	if !r.allowed("/a/b/c") || r.allowed("/a/c") || !r.allowed("/tie") {
		t.Fatal("longest match wins; allow wins equal-length ties")
	}
}

func TestRobotsWildcards(t *testing.T) {
	r := parseRobots([]byte("User-agent: *\nDisallow: /*.zip$\n"), "rowanbot")
	if r.allowed("/f.zip") || !r.allowed("/f.zip?x=1") {
		t.Fatal("wildcards and end anchors must match path plus query")
	}
}

func TestRobotsEmptyDisallow(t *testing.T) {
	r := parseRobots([]byte("User-agent: *\nDisallow:\n"), "rowanbot")
	if !r.allowed("/anything") {
		t.Fatal("empty disallow must allow everything")
	}
}

func TestRobotsRepeatedAndSharedGroups(t *testing.T) {
	r := parseRobots([]byte("User-agent: other\nUser-agent: RowanBot\nDisallow: /one # comment\nUser-agent: rowanbot\nDisallow: /two\nUser-agent: *\nDisallow: /\n"), "rowanbot")
	if r.allowed("/one") || r.allowed("/two") || !r.allowed("/three") {
		t.Fatal("combine matching groups and shared user agents")
	}
}
