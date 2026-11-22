# Data Import Reporting Looker Dashboard (Exchange Online, OneDrive and SharePoint)

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

## Terms & Disclaimer

### Modification Rights
While not necessary, you are permitted to modify and run the deployment script (`script.sh`) and database configurations to accommodate custom directory structures, regional compliance regulations, or unique organizational metadata schemas. However, Google assumes no responsibility or liability for the outcomes of executing original or modified scripts, nor for any errors, discrepancies, or inaccuracies in the resulting data.

### Policy Ownership
You must configure appropriate Identity and Access Management (IAM) permissions on your BigQuery datasets, Cloud Projects, and Looker Studio reports. Ensuring that sensitive Workspace audit logs are restricted to authorized administrators is your organization's sole responsibility.

### Exclusion of Damages
In no event shall Google, the authors of the scripts, or any contributors be liable for any direct, indirect, incidental, special, exemplary, or consequential damages (including, but not limited to, procurement of substitute goods or services; loss of use, data, or profits; or business interruption) however caused and on any theory of liability, whether in contract, strict liability, or tort (including negligence or otherwise) arising in any way out of the use of this software, even if advised of the possibility of such damage.

### License
This project is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for details.

---

## Introduction

An automated deployment tool to set up a comprehensive Looker Studio monitoring dashboard for your Google Workspace data import runs from Exchange Online, OneDrive and SharePoint. It automates Google Cloud API configuration (including BigQuery Reservation API enablement) and sets up BigQuery schemas and scheduled queries to deliver continuous, up-to-date reporting.

For details on the metrics tracked by the dashboard, see the [Import Metrics Reference](METRICS.md).

### Data covered

- **Exchange Online:** emails, calendar events and contacts, per user and per batch.
- **OneDrive and SharePoint:** files and folders, per site (each OneDrive is shown as a site) and per batch.
- **History:** the first run of the script loads up to 365 days of audit logs. After that, the scheduled query
  refreshes the last 2 days on every run.

> [!NOTE]
> OneDrive and SharePoint batches are shown only if their first (Full) run started on or after
> **21 November 2026** (`OD_SP_CUTOFF` in `script.sh`). Earlier runs don't contain the information needed for
> accurate reporting. Exchange Online batches are not affected by this date.

### Processed vs Imported

- **Processed** users or sites are those whose migration finished in the latest run, whether completed or failed.
- **Imported** items are items that were imported successfully.
- **Processing Status** is `Completed`, `Failed` or `Running`. See [METRICS.md](METRICS.md) for the exact rules.

---

## Architecture Overview

The tool sets up the following resources in your Google Cloud Project:
- **API Enablement:** Actives BigQuery Reservation and BigQuery Data Transfer Service APIs.
- **BigQuery Tables:** Creates a schema of tables and incremental staging tables.
- **Scheduled Queries:** Automatically sets up scheduled queries to continuously keep reports updated.
- **Looker Studio Dashboard:** Dynamically generates a dashboard copy URL pre-linked to your BigQuery tables.

---

## Requirements

Before beginning the deployment, ensure your environment meets the following prerequisites:

> [!WARNING]
> If any of the prerequisites or setup requirements are missed—specifically:
> - Not having Google Cloud billing enabled.
> - An incorrect export configuration of the BigQuery admin logs in the Google Admin Console.
>
> The dashboard will not function correctly, and the reporting tables may show incomplete or incorrect migration data.

- **Google Cloud Project:** An active project with billing enabled. *Do not use sandbox projects.*
- **Audit Log Dataset:** A BigQuery dataset populated with Google Workspace Admin Audit Logs.
- **Admin Console BQ Export:** BigQuery export settings must be enabled in the Google Admin Console. Follow the [Google Workspace Setup Guide](https://knowledge.workspace.google.com/admin/reports/set-up-service-log-exports-to-bigquery#before-begin) to get started.
- **Cloud Shell or SDK:** The `gcloud` CLI and `bq` command-line tool configured for access.
- **Required Permissions:** Administrative privileges to enable APIs, manage BigQuery tables, and create scheduled queries.
- **Utility Tools:** `jq` installed in your environment. It is pre-installed in Cloud Shell. If you run the script
  anywhere else (for example, on your own Linux or macOS machine), install it first:
  - Debian or Ubuntu: `sudo apt-get install jq`
  - macOS (Homebrew): `brew install jq`

  Check that it is installed with `jq --version`.

> [!IMPORTANT]
> Without `jq`, the script prints a warning and skips the scheduled query. The dashboard then shows only the data
> loaded by the first run and doesn't refresh. If an older scheduled query exists, it keeps running with the old SQL.

---

## Deployment Steps

### Step 1: Upload and Prepare Files
1. Open the **Google Cloud Console** and activate **Cloud Shell**.
2. Upload the deployment files and ensure they are unzipped in a single folder.
3. Make the deployment script executable:
   ```bash
   chmod +x script.sh
   ```

### Step 2: Run the Deployment Script
1. Execute the interactive script:
   ```bash
   ./script.sh
   ```
2. Enter the configuration inputs when prompted:
   - `Project ID`
   - `Data Location` (defaults to `US`)
   - `DMS Dataset Name` (defaults to `GWS_Audit`)
   - `Refresh Schedule` (defaults to `every day 02:00`)

### Step 3: Connect and Save Your Dashboard
1. Copy the generated **Magic Link** output at the end of the script and paste it into your browser.
2. Verify that the dashboard successfully connects to your BigQuery data tables.
3. Click **Edit and Share** to save a private copy to your Looker Studio account.
4. **Data Freshness:** To adjust how often Looker Studio pulls new data, go to `Resource` > `Manage added data sources` > `Edit` > `Data freshness`.

---

## Updating the Deployment

If updates or bug fixes are released, you can apply them to your existing deployment by following these steps:

1. Fetch the updated code and re-run the interactive deployment script (`./script.sh`) in Cloud Shell.
2. Provide the same configuration inputs (Project ID, Dataset Name, etc.) when prompted.
3. The script will automatically update your BigQuery schemas, tables, and scheduled queries.
4. If the release adds new dashboard pages (for example, the OneDrive and SharePoint pages), open the new
   **Magic Link** printed by the script and save a new copy of the dashboard. Existing copies keep working, but they
   don't get the new pages.
