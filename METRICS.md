# Understanding your report
## Import Metrics Reference

This document provides a comprehensive reference of the metrics defined and computed in the reporting dashboard.

> [!NOTE]
> **Imported** means the item finished with status `SUCCEEDED` or `SUCCEEDED_WITH_WARNINGS`. **Processed** means
> finished, whether imported or failed. A user or site is **processed** when its migration finished in the latest
> run (the completion event was received, or the site failed), even if some of its items failed.

### Exchange Online

| Metric Name | Aggregation Level | Technical Calculation Logic | Definition |
| :--- | :--- | :--- | :--- |
| **Success Percentage** | User / Batch / Data Type | `Total Imported Items / Total Processed Items` | The percentage of emails, events and contacts that were imported successfully, out of all items that were either imported or failed. |
| **Failure Percentage** | User | `Total Failed Items / Total Processed Items` | Shows the share of a user's processed items that failed to import. |
| **Total Users** | Batch / Data Type | Distinct users in the batch (or across all batches for the data type). | Shows the number of users included in a batch or data type. |
| **Total Processed Users** / **Processed Users** | Batch | Users whose `USER_MIGRATION_COMPLETE` event was received for their latest run. | Shows the number of users whose migration finished in the latest run, including users with failed items. |
| **User Processing Percentage** | Batch | `Processed Users / Total Users` | Shows how many users in a batch have finished processing. |
| **Total Imported Items** | User / Batch / Overall | Number of email messages, calendar events and contacts with `SUCCEEDED` or `SUCCEEDED_WITH_WARNINGS` status. | Shows the number of successfully imported items. |
| **Total Imported Mails / Calendar Events / Contacts** | User / Batch / Overall | Imported items of each type. | Shows the number of successfully imported items of each data type. |
| **Total Failed Items** | User / Batch / Overall | Items whose final status is `FAILED`, de-duplicated by user and item, excluding `CRAWL_FAILURE` events. | Shows the number of items that failed to import. Items that later succeeded on retry are not counted. |
| **Total Processed Items** | User / Batch / Overall | `Total Imported Items + Total Failed Items` | Shows the total number of items that were imported or failed to import. |
| **Average Items per User** | Batch | `Total Processed Items / Total Users` | Shows the average number of items processed for each user in a batch. |
| **Discovery Failures** | User / Batch | Number of `CRAWL_FAILURE` events in the latest run (all runs on the overall user page). | Shows the number of errors that occurred while scanning a mailbox. |
| **Users with Discovery Failures** | Batch | Users with at least one discovery failure in the latest run, whatever their Processing Status. | Shows the users where some mailbox content couldn't be discovered. These users can still be Completed. |
| **Processing Status (User)** | Individual User | `Failed`, `Completed` or `Running`. | • **Failed:** No items imported, and at least one failed item or discovery failure.<br>• **Completed:** The completion event was received for the latest run.<br>• **Running:** All other cases.<br>On the overall user page, a user is Completed only when completed in every batch. |
| **Processing Status (Batch)** | Batch | `Completed` if Processed Users equals Total Users; otherwise `Running`. | Shows if every user in the batch has finished processing. |

### OneDrive and SharePoint

OneDrive and SharePoint batches are shown only if their first (Full) run started on or after the cutoff date set in
`script.sh` (`OD_SP_CUTOFF`). Earlier runs don't contain the site URL, the OneDrive migration type and the site
completion events needed for accurate reporting. Exchange Online is not affected.

| Metric Name | Aggregation Level | Technical Calculation Logic | Definition |
| :--- | :--- | :--- | :--- |
| **Success Percentage** | Site / Batch / Data Type | `Total Imported Items / Total Processed Items` | The percentage of files and folders that were imported successfully, out of all items that were either imported or failed. |
| **Total Sites** | Batch / Data Type | Distinct SharePoint sites or OneDrives. At data type level, a site in several batches is counted once. | Shows the number of sites or OneDrives included in a batch or data type. |
| **Total Processed Sites** / **Processed Sites** | Batch | Sites whose completion event (`SITE_MIGRATION_COMPLETE` for SharePoint sites and OneDrives) was received for their latest run, or that failed. | Shows the number of sites whose migration finished in the latest run, whether completed or failed. |
| **Site Processing Percentage** | Batch | `Processed Sites / Total Sites` | Shows how many sites in a batch have finished processing. |
| **Total Imported Files** | Site / Batch / Overall | Files with `SUCCEEDED` or `SUCCEEDED_WITH_WARNINGS` status. A file is counted once, however many versions it has. | Shows the number of successfully imported files. Older file versions aren't counted as files. |
| **Total Imported Folders** | Site / Batch / Overall | Folders with `SUCCEEDED` or `SUCCEEDED_WITH_WARNINGS` status, excluding the SharePoint site container. | Shows the number of successfully imported folders. |
| **Total Imported Document Libraries** | Site / Batch / Overall | Distinct document libraries with at least one imported item. | Shows the number of document libraries imported. |
| **Total Imported Items** | Site / Batch / Overall | `Total Imported Files + Total Imported Folders` | Shows the number of successfully imported files and folders. |
| **Total Failed Items** | Site / Batch / Overall | Files and folders whose final status is `FAILED`. A file counts at most once, whichever step failed; a failed older version doesn't count if the file was imported. The site container and `CRAWL_FAILURE` events are excluded. | Shows the number of files and folders that failed to import. |
| **Total Processed Items** | Site / Batch / Overall | `Total Imported Items + Total Failed Items` | Shows the total number of files and folders that were imported or failed to import. |
| **Average Items per Site** | Batch | `Total Processed Items / Total Sites` | Shows the average number of items processed for each site in a batch. |
| **Discovery Failures** | Site / Batch | Number of `CRAWL_FAILURE` events in the latest run. | Shows the number of errors that occurred while discovering a site's content. A discovery failure can refer to the same folder as a failed item; each is counted in its own metric. |
| **Sites with Discovery Failures** | Batch | Sites (including OneDrives) with at least one discovery failure in the latest run, whatever their Processing Status. | Shows the sites where some content couldn't be discovered. These sites can still be Completed. |
| **Processing Status (Site)** | Individual Site | `Failed`, `Completed` or `Running`. | • **Failed:** In the latest run, the site container failed, the site permission resolver failed, or (document library mappings and OneDrive) a document library couldn't be discovered.<br>• **Completed:** The completion event (`SITE_MIGRATION_COMPLETE`) was received for the latest run.<br>• **Running:** All other cases.<br>Failed items and discovery failures alone don't change the status. On the overall site page, a site is Failed if it failed in any of its batches, and Completed only when completed in every batch. |
| **Processing Status (Batch)** | Batch | `Failed` if every site failed; `Completed` if every site is processed; otherwise `Running`. | Shows if every site in the batch has finished processing. |

### All data types

| Metric Name | Aggregation Level | Technical Calculation Logic | Definition |
| :--- | :--- | :--- | :--- |
| **Migration Timeline** | Day / Batch | Unique items imported per day. Older file versions and the SharePoint site container aren't counted. | Shows how many items were imported each day. |
| **Error Occurrence** | Batch / User / Site | Number of failed events with an error code in the latest run, grouped by error title and message. | Shows the errors in the latest data import and how often they occur. Each failed event is counted, so a file with several failed versions appears once per version. |
| **Top Errors (Top Blocker Reasons)** | Data Type | Top 10 unique `migration_error_title` entries ranked by the sum of `occurrence_count` in the latest run of each batch. | Shows the most frequent errors, with the most affected user or site and an example error message. |
