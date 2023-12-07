# Migrate a Databricks Job from an Interactive to Automated Cluster

This Python script is designed to interact with the Databricks API to update a Databricks Job with new settings and configurations. The script performs the following tasks:

1. **Get Job Settings**: Retrieves the Job settings associated with a specified Job ID using the `/api/2.1/jobs/get` endpoint.

2. **Get Cluster Settings**: Retrieves the Cluster settings associated with a the Job using the `/api/2.0/clusters/get` endpoint. Warning: Assumes that all tasks for Job use the same, existing Cluster.

3. **Update Job Settings**: Partially updates the existing Job, specifying settings for a new Automated Cluster, using the `/api/2.1/jobs/update` endpoint.

## Requirements

Before executing the script, ensure that:

- You have the necessary permissions to interact with the Databricks API.
- Environment variables `DATABRICKS_HOST` and `DATABRICKS_TOKEN` are defined, representing your Databricks workspace URL and bearer token, respectively.

## Usage

Execute the script by providing the Job ID as a command-line argument:

```bash
python migrate.py <job_id>
