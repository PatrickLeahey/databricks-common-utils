# Migrate to Automated Cluster (from interactive)

This Python script is designed to interact with the Databricks API to update a Databricks job with new settings and configurations. The script performs the following tasks:

1. **Get Job ID by Name**: Retrieves the job ID associated with a specified job name using the `/api/2.1/jobs/list` endpoint.

2. **Get Tasks**: Fetches the existing tasks associated with the identified job using the `/api/2.1/jobs/get` endpoint.

3. **Update Job Settings**: Modifies the existing job settings, specifically updating the job cluster and related configurations.

## Requirements

Before executing the script, ensure that:

- You have the necessary permissions to interact with the Databricks API.
- Environment variables `DATABRICKS_HOST` and `DATABRICKS_TOKEN` are defined, representing your Databricks workspace URL and bearer token, respectively.

## Usage

Execute the script by providing the job name as a command-line argument:

```bash
python migrate.py <job_name>
