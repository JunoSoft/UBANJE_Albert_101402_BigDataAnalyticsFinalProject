# HBase Schema Design (Part 1)

## Why HBase for sessions / metrics?

HBase (wide-column) shines for:

- **High-volume time-series** page views and sessions
- **Sparse columns** (not every session has cart / conversion fields)
- **Row-key range scans** (all activity for one user, or one product over dates)

MongoDB keeps nested “whole document” entities; HBase keeps **event/metric streams**.

---

## Table 1: `user_sessions`

**Purpose:** Retrieve a user’s browsing activity quickly in time order.

| Design choice | Value |
|---|---|
| Row key | `user_id#reverse_ts#session_id` |
| Why reverse timestamp | Newest sessions appear first under the user prefix |
| Column family `info` | session metadata (duration, device, referrer, status) |
| Column family `geo` | city/state/country/ip |
| Column family `events` | page_view JSON blobs keyed by sequence |

Example row key:
```
user_000042#9223370338#sess_a7b3c9d8e2
```
`reverse_ts = (10**10 - unix_seconds)` keeps lexicographic order ≈ newest-first.

### Queries enabled
- Get recent sessions for `user_000042` (prefix scan)
- Pull device/referrer/conversion for funnel enrichment

---

## Table 2: `product_metrics`

**Purpose:** Product performance over time (views, carts, purchases per day).

| Design choice | Value |
|---|---|
| Row key | `product_id#yyyy-MM-dd` |
| Column family `stats` | `views`, `cart_adds`, `purchases`, `revenue` |

Example:
```
prod_00123#2025-03-12  →  stats:views=42, stats:cart_adds=7, stats:purchases=3
```

### Queries enabled
- Scan one product’s daily series
- Compare view-to-purchase conversion across dates

---

## MongoDB vs HBase (summary)

| Workload | Prefer |
|---|---|
| Product catalog + flexible filters | MongoDB |
| Order documents with line items | MongoDB |
| User profile + purchase summary | MongoDB |
| Millions of page-view events | HBase |
| Per-user activity stream | HBase |
| Per-product daily counters | HBase |
