import os
import sys
import requests

def get_job_id_by_name(host, token, job_name):
    endpoint = f"{host}/api/2.1/jobs/list"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"name": job_name}

    response = requests.get(endpoint, headers=headers, params=params)
    response_json = response.json()

    if "jobs" in response_json and response_json["jobs"]:
        return response_json["jobs"][0]["job_id"]
    else:
        return None


def get_tasks(host, token, job_id):
    endpoint = f"{host}/api/2.1/jobs/get"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {"job_id": job_id}

    response = requests.get(endpoint, headers=headers, json=payload)
    response_json = response.json()

    if "settings" in response_json and response_json["settings"]:
        return list(response_json["settings"]["tasks"])
    else:
        return None


def update_job(host, token, job_name, job_id, tasks):
    # Update or parameterize as necessary
    job_cluster_key = f"{job_name}_job_cluster"

    for task in tasks:
        task.update({"job_cluster_key": job_cluster_key})
        task.pop("existing_cluster_id")

    update_payload = {
        "job_clusters": [
            {   
                "job_cluster_key": job_cluster_key,
                "new_cluster": {
                    "autoscale": {
                        "min_workers": 1,
                        "max_workers": 4
                    },
                    "spark_version": "9.1.x-scala2.12",
                    "spark_conf": {
                        "spark.databricks.io.cache.enabled": "true",
                        "spark.databricks.io.cache.compression.enabled": "false",
                        "spark.databricks.delta.preview.enabled": "true",
                        "spark.memory.offHeap.enabled": "false",
                        "spark.databricks.io.cache.maxDiskUsage": "1750g",
                        "spark.databricks.io.cache.maxMetaDataCache": "10g",
                        "spark.databricks.aggressiveWindowDownS": "600",
                        "spark.sql.shuffle.partitions": "auto"
                    },
                    "node_type_id": "Standard_E4ds_v5",
                    "spark_env_vars": {
                        "SPARK_NICENESS": "0",
                        "JAVA_OPTS": "\"$JAVA_OPTS -D...\"\""
                    },
                    "init_scripts": [
                        {
                            "dbfs": {
                                "destination": "dbfs:/FileStore/network_connection.sh"
                            }
                        },
                        {
                            "dbfs": {
                                "destination": "dbfs:/FileStore/tcp_dump_api_test.sh"
                            }
                        },
                        {
                            "dbfs": {
                                "destination": "dbfs:/FileStore/apps/init/init_updated20231011.sh"
                            }
                        }
                    ],
                    "enable_local_disk_encryption": False,
                    "runtime_engine": "PHOTON",
                }
            },
        ],
        "tasks": tasks,
    }

    endpoint = f"{host}/api/2.1/jobs/update"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {"job_id": job_id, "new_settings": update_payload}

    response = requests.post(endpoint, headers=headers, json=payload)
    return response.json()


def main(databricks_host, databricks_token, job_name):

    # Get the job id by name
    job_id = get_job_id_by_name(databricks_host, databricks_token, job_name)

    if job_id:
        # Update the job with the new settings
        tasks = get_tasks(databricks_host, databricks_token, job_id)
        update_response = update_job(databricks_host, databricks_token, job_name, job_id, tasks)
        print("Job updated successfully:", update_response)
    else:
        print(f"Job with name '{job_name}' not found.")


if __name__ == "__main__":

    try:
        job_name = sys.argv[1]
    except:
        raise Exception("Please pass job name as argument upon execution.")

    # Replace these values with your actual Databricks host and token
    databricks_host = os.getenv("DATABRICKS_HOST")
    databricks_token = os.getenv("DATABRICKS_TOKEN")

    if not databricks_host or not databricks_token:
        raise Exception("Please define enviromnet variables DATABRICKS_HOST, DATABRICKS_TOKEN prior to execution.")

    main(databricks_host, databricks_token, job_name)
    
