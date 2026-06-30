# Understanding your report
## Import Metrics Reference

This document provides a comprehensive reference of the metrics defined and computed in the reporting dashboard.

| Metric Name | Aggregation Level | Technical Calculation Logic | Definition |
| :--- | :--- | :--- | :--- |
| **Success Percentage** | Data Type / Batch | `Successful Items / (Successful Items + Failed Items)` | Shows the number of successfully imported items in a batch, compared to the total number of items in a batch. |
| **User Completion Percentage** | Batch | `Completed Users / Total Attempted Users` | Shows the number of successfully imported users in a batch, compared to the total number of users in the batch. |
| **Total Imported Items** | User / Batch / Overall | Number of items with `SUCCEEDED` or `SUCCEEDED_WITH_WARNINGS` status. | Shows the number of successfully imported items by data type (including calendar events, email messages, and contact data), compared to the number of items in the batch, and the total number of items being imported. |
| **Total Imported Users** | Batch / Data type | Total number of imported users. | Shows the number of user identifiers, such as `source_name`, that are included in one batch or data type. |
| **Total Processed Items** | Batch / Data type | Total number of processed items. | Shows the total number of items that imported items or failed to import for one batch or data type. |
| **Total Failed Items** | User / Batch / Overall | Total number of failed events in the latest data import for an item. | Shows the number of items that failed to import for one user or batch, deduplicated by user and identifier, excluding `CRAWL_FAILURE` events. |
| **Average Items per User** | Batch | `Total number of items / users` | Shows the average number of items that were processed for each user in a batch. The average is calculated by dividing the total number of successes, failures, and crawl impediments by the number of users. |
| **User Import Status** | Individual User | Import status, such as `Failed`, `Completed`, or `In Progress`, based on event logs. | Shows the import status for a user:<br>• **Failed:** Zero successful imports<br>• **Completed:** All items successfully imported<br>• **Failed:** Items still to be imported (with some successes) |
| **Batch Status** | Batch | Shows as `Completed` if the number of completed users is the same as the total number of users. Otherwise, shows as `In Progress`. | Shows if the batch is completely imported. The batch is **Completed** when the number of imported users matches the total user count. All other states are shown as **In progress**. |
| **Error Occurrence** | Batch / User | Total number of unique `migration_error_code` instances in the last data import. | Shows a list of unique `migration_error_code` occurrences for the last data import, including a list of errors according to how often they occur. |
| **Top Errors (Top Blocker Reasons)** | Global / Migration | Top 10 unique `migration_error_title` entries ranked by the sum of `occurrence_count` in the latest execution of each active batch. | Shows the top 10 unique migration errors across all batches based on total occurrences in the latest runs, highlighting the most affected user and error message. |
| **Discovery Failure Count** | Batch / User | Total number of failed crawler events. | Shows the number of `CRAWL_FAILURE` events in the activity logs for each user's data import, without item-level deduplication. |
| **Undiscoverable Users** | Batch | Total number of users with crawler failures. | Shows the number of users that failed to crawl during the last data import. |
| **Completed Users** | Batch | Total number of users with completed imports. | Shows the number of users that successfully imported in the last batch. |
