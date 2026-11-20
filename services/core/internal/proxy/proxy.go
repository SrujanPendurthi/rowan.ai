// Package proxy summarizes cited signals of a possible post-sale transfer.
package proxy

import (
	"sort"
	"time"

	"rowan.ai/rowan/services/core/internal/quality"
)

type SignalType string

const (
	UCCFiling             SignalType = "ucc_filing"
	UCCAmendment          SignalType = "ucc_amendment"
	OfficerChange         SignalType = "officer_change"
	RegisteredAgentChange SignalType = "registered_agent_change"
	EntityChange          SignalType = "entity_change"
	NewHoldco             SignalType = "new_holdco"
	DomainShift           SignalType = "domain_shift"
	LeadershipChange      SignalType = "leadership_change"
	BrokerListingRemoved  SignalType = "broker_listing_removed"
	DisclosedTransaction  SignalType = "disclosed_transaction"
)

const (
	LabelPostSaleProxy     = "post_sale_proxy"
	LabelConfirmedTransfer = "confirmed_transfer"
	LabelNone              = "none"
)

type Signal struct {
	Type       SignalType       `json:"type"`
	ObservedAt time.Time        `json:"observed_at"`
	Citation   quality.Citation `json:"citation"`
}

type Summary struct {
	Label       string    `json:"label"`
	InWindow    bool      `json:"in_window"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	SignalCount int       `json:"signal_count"`
	Signals     []Signal  `json:"signals"`
}

// Analyze includes known signals within the inclusive six-to-nine-month UTC
// window. Only a verified disclosed transaction confirms a transfer.
func Analyze(signals []Signal, now time.Time) Summary {
	now = now.UTC()
	summary := Summary{
		Label:       LabelNone,
		WindowStart: now.AddDate(0, -9, 0),
		WindowEnd:   now.AddDate(0, -6, 0),
		Signals:     make([]Signal, 0),
	}
	for _, signal := range signals {
		if !knownType(signal.Type) || signal.ObservedAt.Before(summary.WindowStart) || signal.ObservedAt.After(summary.WindowEnd) {
			continue
		}
		summary.Signals = append(summary.Signals, signal)
		if signal.Type == DisclosedTransaction && signal.Citation.Status == quality.VerifiedFiling {
			summary.Label = LabelConfirmedTransfer
		}
	}
	sort.SliceStable(summary.Signals, func(i, j int) bool { return summary.Signals[i].ObservedAt.Before(summary.Signals[j].ObservedAt) })
	summary.SignalCount = len(summary.Signals)
	summary.InWindow = summary.SignalCount > 0
	if summary.InWindow && summary.Label != LabelConfirmedTransfer {
		summary.Label = LabelPostSaleProxy
	}
	return summary
}

func knownType(typ SignalType) bool {
	switch typ {
	case UCCFiling, UCCAmendment, OfficerChange, RegisteredAgentChange, EntityChange, NewHoldco, DomainShift, LeadershipChange, BrokerListingRemoved, DisclosedTransaction:
		return true
	default:
		return false
	}
}
