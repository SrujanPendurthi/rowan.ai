package enrich

import (
	"context"
	"sync/atomic"

	"rowan.ai/rowan/services/core/internal/resolver"
)

// FakeProvider is a deterministic Provider for tests and ROWAN_PROVIDER=fake.
// A ref absent from Results and Errs is a zero-hit.
type FakeProvider struct {
	Results map[string][]resolver.SourceRecordInput
	Errs    map[string]error
	Calls   atomic.Int64
	// OnLookup, when set, runs at the start of every Lookup (tests cancel ctx or refund mid-run).
	OnLookup func(ctx context.Context, c Candidate)
}

func (*FakeProvider) Name() string { return "fake" }

func (f *FakeProvider) Lookup(ctx context.Context, c Candidate) ([]resolver.SourceRecordInput, error) {
	f.Calls.Add(1)
	if f.OnLookup != nil {
		f.OnLookup(ctx, c)
	}
	if err := f.Errs[c.Ref]; err != nil {
		return nil, err
	}
	return f.Results[c.Ref], nil
}
