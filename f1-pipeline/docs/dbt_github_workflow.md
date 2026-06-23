# GitHub Workflow for dbt Run (Snowflake)

This document explains the setup and execution of the GitHub Actions workflow used to automate running our dbt models against Snowflake.

## Overview

We use GitHub Actions to automate our dbt transformations. The workflow is defined in `.github/workflows/dbt_run.yml` and is designed to execute `dbt run` inside a Python environment whenever it receives a specific trigger event. This ensures that our data transformations are consistently applied to the Snowflake data warehouse in a reliable, automated fashion without requiring manual execution from a local machine.

## How it gets triggered

The workflow uses a `repository_dispatch` trigger:
```yaml
on:
  repository_dispatch:
    types: [trigger-dbt-snowflake]
```
This means the workflow does not run automatically on every push or pull request. Instead, it waits for an external event (an API call to GitHub) containing the event type `trigger-dbt-snowflake`. This is particularly useful for data pipelines, as you can trigger the dbt transformation step precisely when upstream data extraction and loading processes (e.g., from AWS Glue or Fivetran) have successfully completed.

### Triggering the Workflow via API

You can manually trigger this workflow from any terminal or external system (like Airflow or an AWS Lambda function) by making a POST request to the GitHub API. 

Here is the `curl` command you can run in a Bash/Linux terminal:

```bash
curl -L \
  -X POST \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer <YOUR_GITHUB_TOKEN>" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  https://api.github.com/repos/stva01/ETL_F1/dispatches \
  -d '{"event_type": "trigger-dbt-snowflake"}'
```

### Triggering via AWS Lambda or Apache Airflow (Python)

If you are automating this from a Python environment like an AWS Lambda function or an Airflow `PythonOperator`, you can use the `requests` library:

```python
import requests

def trigger_dbt_workflow():
    url = "https://api.github.com/repos/stva01/ETL_F1/dispatches"
    
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": "Bearer <YOUR_GITHUB_TOKEN>",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    
    data = {
        "event_type": "trigger-dbt-snowflake"
    }
    
    response = requests.post(url, headers=headers, json=data)
    
    if response.status_code == 204:
        print("Successfully triggered workflow!")
    else:
        print(f"Failed to trigger: {response.status_code} - {response.text}")

# In Lambda, call this within your handler:
# def lambda_handler(event, context):
#     trigger_dbt_workflow()
```

### Triggering via Windows PowerShell

Since you are developing on a Windows machine, the standard `curl` command will not work directly in PowerShell because `curl` is an alias for `Invoke-WebRequest`. Use the following PowerShell command instead:

```powershell
$token = "<YOUR_GITHUB_TOKEN>"
$url = "https://api.github.com/repos/stva01/ETL_F1/dispatches"

$headers = @{
    "Accept" = "application/vnd.github+json"
    "Authorization" = "Bearer $token"
    "X-GitHub-Api-Version" = "2022-11-28"
}

$body = @{
    "event_type" = "trigger-dbt-snowflake"
} | ConvertTo-Json

Invoke-RestMethod -Uri $url -Method Post -Headers $headers -Body $body -ContentType "application/json"
```

*Note: In all examples, replace `<YOUR_GITHUB_TOKEN>` with a personal access token (PAT) that has `repo` access.*

## Detailed Breakdown of `dbt_run.yml`

The workflow runs on an `ubuntu-latest` runner and executes the following sequence of steps:

### 1. Checkout Repository
Uses the `actions/checkout@v4` action to pull the latest code from the repository so the workflow has access to the dbt project files.

### 2. Set up Python
Uses the `actions/setup-python@v5` action to install Python 3.10, ensuring a stable environment for installing dbt.

### 3. Install dbt Dependencies
Upgrades `pip` and installs the required packages: `dbt-core` and the specific adapter for our database, `dbt-snowflake`.

### 4. Create `profiles.yml`
dbt requires a `profiles.yml` file to know how to connect to the database. The workflow dynamically creates this file in the `~/.dbt/` directory using environment variables and GitHub Secrets/Variables. 

The profile (`f1_transform_profile`) targets the `dev` output and injects the following configuration:
- **Account:** The Snowflake account locator
- **User / Password:** Authentication credentials
- **Role / Warehouse / Database / Schema:** The Snowflake execution context
- **Threads:** Set to 4 for parallel model execution

### 5. Install dbt Packages
Changes the working directory to `f1-pipeline/warehouse/dbt` and runs `dbt deps`. This command reads the `packages.yml` file and downloads any external dbt packages required by the project.

### 6. Execute dbt Run
The final step runs the core command: `dbt run`.
It explicitly maps the GitHub Actions environment variables to the variables referenced in the dynamically generated `profiles.yml` file. When this step executes, dbt connects to Snowflake and materializes all configured models, views, and tables in the target schema.

## Required GitHub Configuration

To make this workflow function correctly, the following must be configured in the GitHub repository settings (under **Settings > Secrets and variables > Actions**):

### Repository Variables
- `DBT_SNOWFLAKE_ACCOUNT`: Your Snowflake account locator (e.g., `xy12345.us-east-1`)
- `DBT_SNOWFLAKE_USER`: The Snowflake username executing the runs
- `DBT_SNOWFLAKE_ROLE`: The role granted to the user (e.g., `ACCOUNTADMIN` or `TRANSFORMER_ROLE`)
- `DBT_SNOWFLAKE_WAREHOUSE`: The compute warehouse to use (e.g., `COMPUTE_WH`)
- `DBT_SNOWFLAKE_DATABASE`: The target database (e.g., `F1_ANALYTICS_WAREHOUSE`)
- `DBT_SNOWFLAKE_SCHEMA`: The target schema for the materialized models (e.g., `MARTS` or `DEV_SCHEMA`)

### Repository Secrets
- `DBT_SNOWFLAKE_PASSWORD`: The password for the Snowflake user. This must be stored as a secret to ensure it is encrypted and never exposed in workflow logs.
