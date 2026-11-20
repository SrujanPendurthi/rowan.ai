package ledger

// Tier identifies a workspace subscription.
type Tier string

const (
	Solo     Tier = "solo"
	DealTeam Tier = "deal_team"
	Fund     Tier = "fund"
)

// TierSpec returns included credits, lot lifetime in days, and overage cents.
// Unknown tiers return zeros and must not be used for grants.
func TierSpec(t Tier) (included int64, lotDays int, overageCents int64) {
	switch t {
	case Solo:
		return 500, 60, 60
	case DealTeam:
		return 2500, 90, 45
	case Fund:
		return 7500, 90, 35
	default:
		return 0, 0, 0
	}
}
