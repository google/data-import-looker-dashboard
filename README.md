# Quick Start Guide: Exchange Online Import Reporting Looker Dashboard

The Exchange Online Import Reporting feature uses automated scripts to rapidly deploy a consolidated Looker Studio dashboard for your Google Workspace data import content.

## Requirements

Before beginning the deployment, ensure your environment meets the following prerequisites:

*   **Google Cloud Project:** An active project with billing turned on to host the BigQuery reporting tables. *Note: Do not use sandbox projects.*
*   **Audit Log Dataset:** A BigQuery dataset that is already populated with Google Workspace Admin Audit Logs.
*   **Admin Console BQ Export:** BigQuery export settings must be turned on in the Google Admin console for auditing a project's dataset.
*   **Cloud Shell or SDK:** Google Cloud Command Line Interface (`gcloud` CLI) and the BigQuery command-line tool configured to set up access to Google Cloud Shell or a local environment.
*   **Required Permissions:** Administrative permissions to enable Google Application Programming Interfaces (APIs), create BigQuery BI Engine reservations, and manage BigQuery tables and scheduled queries.
*   **Utility Tools:** `jq` must be installed for the automatic creation of scheduled queries.

---

## Deployment Steps

### Step 1: Upload and Prepare Files
1. Open the **Google Cloud Console** and activate **Cloud Shell**.
2. Upload the deployment ZIP files to Cloud Shell and unzip them.
3. Make the deployment script executable by running:
   ```bash
   chmod +x script.sh
   ```

### Step 2: Run the Deployment Script
1. Execute the interactive configuration script:
   ```bash
   ./script.sh
   ```
2. When prompted, provide the required baseline inputs:
   *   `Project ID`
   *   `Data Location`
   *   `Dataset Name`
   *   `Refresh Schedule`

#### What the Script Does:
*   **API Activation:** Enables BigQuery Reservation and Data Transfer APIs.
*   **BI Engine Reservation:** Allocates 1GB of BI Engine capacity for instant dashboard loading.
*   **Schema & Backfill:** Automates the creation of reporting tables and runs the initial historical data load.
*   **Automated Refresh:** Establishes scheduled BigQuery queries for continuous data updates.
*   **URL Generation:** Produces a unique Looker Studio Magic Link upon completion.

### Step 3: Launch and Save Your Dashboard
1. Copy the generated **Magic Link** and paste it into your web browser.
2. Verify that the dashboard successfully connects to your BigQuery data tables.
3. Click **Edit and Share** to save a private copy to your personal account. *This ensures that the data freshness frequency is configured specifically in your personal version.*
4. **Adjusting Frequency Settings:** If you need to modify update intervals later, navigate to:
   `Resource` > `Manage added data sources` > `Edit` > `Data freshness`.
