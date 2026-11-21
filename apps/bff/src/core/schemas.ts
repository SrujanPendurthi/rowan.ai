import { z } from 'zod';

// Mirrors the Go API's JSON tags, including embedded quality/evaluate/proxy types.
// Ordinary objects accept additive fields without weakening known field validation.
const dataQualityStatus = z.enum(['verified_filing', 'commercial_estimate', 'scraped_web', 'missing']);
const criterionStatus = z.enum(['MET', 'NOT_MET', 'UNVERIFIED_LOW_CONFIDENCE']);
const timestamp = z.iso.datetime({ offset: true });
const count = z.number().int().nonnegative();
const citation = z.object({
  field_name: z.string(),
  field_value: z.json(),
  origin_type: z.enum(['raw_source', 'derived_enrichment']),
  data_quality_status: dataQualityStatus,
  export_policy: z.enum(['allow', 'deal_team_plus', 'fund_only', 'display_only']),
  source_system: z.string(),
  source_ref: z.string(),
  table_name: z.string().optional(),
  row_key: z.string().optional(),
  page_or_span: z.string().optional(),
  retrieved_at: timestamp,
  source_record_id: z.uuid().optional(),
});
const criterion = z.object({
  criterion: z.string(),
  status: criterionStatus,
  observed: z.json().optional(),
  data_quality_status: dataQualityStatus,
  citation: citation.optional(),
});
const evaluation = z.object({
  id: z.uuid(),
  display_name: z.string(),
  pending_review: z.boolean(),
  fit: z.enum(['match', 'partial_unverified', 'fail']),
  overall_score: z.number(),
  criteria_breakdown: z.array(criterion),
  evaluated_at: timestamp,
});

export const ErrorBodySchema = z.object({
  error: z.object({ code: z.string(), message: z.string() }),
});
export type ErrorBody = z.infer<typeof ErrorBodySchema>;

export const BuyBoxSchema = z.object({
  id: z.uuid(),
  name: z.string(),
  criteria: z.object({
    naics_in: z.array(z.string()).nullable().optional(),
    exclude_naics: z.array(z.string()).nullable().optional(),
    states_in: z.array(z.string()).nullable().optional(),
    exclude_states: z.array(z.string()).nullable().optional(),
    revenue_min_cents: z.number().nullable().optional(),
    revenue_max_cents: z.number().nullable().optional(),
    headcount_min: z.number().nullable().optional(),
    headcount_max: z.number().nullable().optional(),
    contract_value_min_cents: z.number().nullable().optional(),
  }),
  thesis_notes: z.string(),
  created_at: timestamp,
  updated_at: timestamp,
});
export type BuyBox = z.infer<typeof BuyBoxSchema>;

export const EvaluationsSchema = z.object({ buy_box_id: z.uuid(), evaluations: z.array(evaluation) });
export type Evaluations = z.infer<typeof EvaluationsSchema>;

export const DiscoveryResultSchema = z.object({ buy_box_id: z.uuid(), results: z.array(evaluation) });
export type DiscoveryResult = z.infer<typeof DiscoveryResultSchema>;

export const QuoteSchema = z.object({
  candidate_count: count,
  already_enriched: count,
  billable: count,
  credits_required: count,
  credits_available: count,
  shortfall: count,
  overage_estimate_cents: count,
});
export type Quote = z.infer<typeof QuoteSchema>;

export const EnrichmentRunSchema = z.object({
  run_id: z.uuid(),
  // An entirely cached run has no reservation and emits an empty string.
  reservation_id: z.union([z.uuid(), z.literal('')]),
  hits: count,
  zero_hits: count,
  failed: count,
  cached: count,
  unprocessed: count,
  status: z.enum(['completed', 'partial', 'failed']),
  credits_committed: count,
  credits_refunded: count,
  companies: z.array(z.uuid()),
});
export type EnrichmentRun = z.infer<typeof EnrichmentRunSchema>;

export const CompanySchema = z.object({
  id: z.uuid(),
  display_name: z.string(),
  pending_review: z.boolean(),
  created_at: timestamp,
  updated_at: timestamp,
  proxy_summary: z.object({
    label: z.enum(['post_sale_proxy', 'confirmed_transfer', 'none']),
    in_window: z.boolean(),
    window_start: timestamp,
    window_end: timestamp,
    signal_count: count,
    signals: z.array(z.object({
      type: z.enum(['ucc_filing', 'ucc_amendment', 'officer_change', 'registered_agent_change',
        'entity_change', 'new_holdco', 'domain_shift', 'leadership_change', 'broker_listing_removed', 'disclosed_transaction']),
      observed_at: timestamp,
      citation,
    })),
  }).nullable(),
  citations: z.array(citation),
  links: z.array(z.object({
    source_record_id: z.uuid(),
    merge_confidence_score: z.number(),
    match_reasons: z.array(z.string()),
    merge_mode: z.enum(['auto', 'manual', 'seed']),
    status: z.enum(['active', 'split']),
  })),
});
export type Company = z.infer<typeof CompanySchema>;

export const ReviewQueueSchema = z.object({
  candidates: z.array(z.object({
    id: z.uuid(),
    canonical_company_id: z.uuid(),
    source_record_id: z.uuid(),
    source_system: z.string(),
    external_ref: z.string(),
    score: z.number(),
    match_reasons: z.array(z.string()),
  })),
});
export type ReviewQueue = z.infer<typeof ReviewQueueSchema>;

export const CreditsSchema = z.object({
  available: z.object({ rollover: count, included: count, topup: count }),
  total: count,
  // Session caps are Go int64 max, serialized as JSON numbers beyond JS safe integers.
  month_to_date: z.object({ spent: count, cap: z.number().nonnegative() }),
});
export type Credits = z.infer<typeof CreditsSchema>;
