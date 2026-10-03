# Week 3 Task 4 validation framework design

## 1. Objective

Extend the Week 1 validation approach into a configurable rule engine that detects duplicate records, invalid attribute values, missing reference records, unsupported schema changes, and incomplete records. Invalid rows must not stop the pipeline: they are quarantined, reported, and excluded from Silver/Gold analytical paths.

## 2. Architecture

```text
config/validation_rules.yml          # enabled rule names per dataset kind
        |
        v
validation/rules.py                # rule registry (name, category, mask builder)
        |
        v
validation/engine.py               # apply_validation_rules -> ValidationOutcome
        |
        +--> accepted rows -> Silver / Gold / products
        +--> quarantined rows -> lakehouse/quarantine/* with validation_rules column
        +--> rule_counts -> monitoring.validation_events
        |
validation/schema.py               # contract + schema evolution checks (file level)
validation/report.py               # validate-report CLI summary
```

Adding a new rule requires:

1. Implement a mask builder in `validation/rules.py` and register it in `RULE_REGISTRY`.
2. Add the rule name to `config/validation_rules.yml` under the target dataset kind.

No changes to ingest orchestration or the engine are required.

## 3. Rule Catalog

| Rule | Kind | Category | Detects |
| --- | --- | --- | --- |
| `null_or_invalid_trip_times` | taxi | incomplete | Missing timestamps or dropoff not after pickup |
| `missing_zone_reference` | taxi | missing_reference | PU/DO LocationID absent from zone lookup |
| `duplicate_trip_fingerprint` | taxi | duplicate | Duplicate trip fingerprint within the batch |
| `missing_location_id` | zones | incomplete | NULL LocationID |
| `duplicate_location_id` | zones | duplicate | Duplicate LocationID |
| `incomplete_time_parts` | weather | incomplete | Missing date/time parts or derived timestamp |
| `duplicate_hourly_observation` | weather | duplicate | Duplicate hourly observation timestamp |
| `null_observation_timestamp` | air | incomplete | Unparseable GMT timestamp |
| `invalid_pm25_value` | air | invalid_value | NULL or negative PM2.5 |
| `duplicate_candidate_key` | air | duplicate | Duplicate site/time candidate key |

Schema-level unsupported changes are detected in `validation/schema.py`. Failures abort the file read but are recorded as `unsupported_schema_change` validation events when a run fails.

Soft quality warnings on accepted taxi rows (`quality_flags` such as negative fare) remain warnings, not quarantine events, so suspicious but structurally usable trips still reach analytics.

## 4. Isolation and Reporting

- Quarantine Delta tables store a `validation_rules` column (comma-separated rule names) and a `quarantine_reason` column for backward compatibility.
- `monitoring.record_validation_events()` stores per-rule counts for each pipeline run.
- `python -m urban_data validate-report` summarizes quarantine totals by dataset and validation events by rule.

Invalid rows never reach Silver acceptance sets, so analytical products built from Silver/Gold remain excluded from quarantined data by construction.

## 5. Discussion

### Which validation rules are generic?

Generic patterns reused across datasets:

- **Duplicate natural or surrogate keys** (`duplicate_location_id`, `duplicate_hourly_observation`, `duplicate_candidate_key`, `duplicate_trip_fingerprint`)
- **Incomplete required fields** (`missing_location_id`, `incomplete_time_parts`, `null_observation_timestamp`, `null_or_invalid_trip_times`)
- **Invalid numeric measures** (`invalid_pm25_value`; taxi negative amounts are warnings, not hard failures)
- **Unsupported schema changes** (schema module + `unsupported_schema_change` event)

Referential checks such as `missing_zone_reference` follow the same engine pattern but require dataset-specific context (valid LocationID set).

### Which rules are dataset-specific?

| Dataset | Specific logic |
| --- | --- |
| Taxi | Trip duration constraint, zone lookup join, trip fingerprint dedup |
| Taxi zones | LocationID uniqueness for TLC lookup |
| Weather | Hourly timestamp construction and duplicate hour detection |
| Air quality | NYC filter happens before validation; PM2.5 bounds and EPA candidate-key duplicates |

Aggregation to hourly NYC PM2.5 remains a transform step after station-level validation.

### How can new rules be added without modifying the core framework?

1. Add a mask function and `ValidationRule` entry in `validation/rules.py`.
2. Enable it in `config/validation_rules.yml`.
3. Optionally extend `validate-report` documentation; the engine, ingest hooks, and monitoring integration pick up the rule automatically.

The core engine only iterates registered rules; it does not encode dataset semantics.

## 6. Trade-offs

| Decision | Benefit | Cost |
| --- | --- | --- |
| Row-level masks in Spark | Scales with ingest volume | Multiple rule columns computed per batch |
| YAML-enabled rule sets | Operators can toggle rules without code edits | Rule logic still lives in Python |
| Quarantine instead of fail-fast for row rules | Pipeline continues on partial bad data | Operators must monitor quarantine growth |
| Schema errors remain fatal at read time | Prevents writing corrupt Bronze files | Requires re-delivery or `--force` after fixing source |

Verified CLI:

```powershell
.\scripts\run_win.ps1 validate-report
```
