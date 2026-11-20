package api

import (
	"strings"

	"rowan.ai/rowan/services/core/internal/export"
)

func (s *server) exportCSV(c *call) (response, error) {
	var in struct {
		BuyBoxID string   `json:"buy_box_id"`
		Columns  []string `json:"columns"`
		Fit      []string `json:"fit"`
	}
	if err := c.decode(&in); err != nil {
		return response{}, err
	}
	if !validUUID(in.BuyBoxID) {
		return response{}, badRequest("buy_box_id must be a UUID")
	}
	res, err := export.Export(c.ctx(), s.Pool, s.ArtifactDir, export.Request{
		WorkspaceID: c.key.WorkspaceID, ActorID: c.actor(), BuyBoxID: in.BuyBoxID, Columns: in.Columns, Fit: in.Fit,
	}, c.now)
	if err != nil {
		return response{}, err
	}
	return response{status: 200, body: res.CSV, headers: map[string]string{
		"Content-Type":            "text/csv; charset=utf-8",
		"X-Rowan-Redacted-Fields": strings.Join(res.RedactedFields, ","),
	}}, nil
}
