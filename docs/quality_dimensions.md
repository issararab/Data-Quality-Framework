# Quality Dimensions Taxonomy

LakeScore evaluates each table across 5 dimensions, with sub-metrics summing to a total score of
**100 points**. Scores are computed by `notebooks/compute_summary.py` and stored in
`<catalog>.data_quality.dq_summary`.

## 🏛 Stewardship — 25 pts

| Metric | Points | Description | Computed by |
|---|---|---|---|
| `has_a_valid_owner` | 5 | Table has a valid, non-empty owner assigned | `lakescore.metadata.tables.retrieve_table_metadata` |
| `is_delta_table` | 5 | Table is stored in Delta format | `lakescore.metadata.tables.retrieve_table_metadata` |
| `uses_a_production_pipeline` | 5 | Table is produced by a production pipeline | `lakescore.metadata.tables.retrieve_table_metadata` |
| `has_enforced_retention_duration` | 5 | A retention policy is explicitly configured | `lakescore.metadata.tables.retrieve_table_metadata` |
| `is_managed_location` | 5 | Table resides in a managed storage location | `lakescore.metadata.tables.retrieve_table_metadata` |

## 📖 Usability — 15 pts

| Metric | Points | Description | Computed by |
|---|---|---|---|
| `table_description` | 7 | Table has a meaningful description | `lakescore.metadata.tables.retrieve_table_metadata` |
| `columns_description` | 8 | All (or most) columns have descriptions | `lakescore.metadata.columns.retrieve_columns_metadata` |

## ⏱ Freshness — 10 pts

| Metric | Points | Description | Computed by |
|---|---|---|---|
| `is_fresh` | 10 | Table data was updated within the configured freshness window | `lakescore.quality.freshness.check_table_freshness` |

## ✅ Validity — 20 pts

| Metric | Points | Description | Computed by |
|---|---|---|---|
| `columns_valid` | 10 | Column set matches a past version's column set | `lakescore.quality.validity.compare_table_schema_with_version` |
| `columns_datatype_valid` | 10 | Common columns' datatypes match a past version's | `lakescore.quality.validity.compare_table_schema_with_version` |

## 🎯 Accuracy — 30 pts

| Metric | Points | Description | Computed by |
|---|---|---|---|
| `table_implement_checks` | 5 | At least one data check is defined for the table | `lakescore.metadata.tables.retrieve_table_metadata` |
| `all_columns_have_checks` | 5 | Every column has at least one check defined | `lakescore.metadata.tables.retrieve_table_metadata` |
| `has_check_passed` | 20 | All defined checks pass on the latest data | `lakescore.soda_execution.execute_soda_checks` |

## Scoreboard

| Score Range | Category |
|---|---|
| 65–75+ | 🟢 Good |
| 45–64 | 🟡 Okay |
| < 45 | 🔴 Poor |
