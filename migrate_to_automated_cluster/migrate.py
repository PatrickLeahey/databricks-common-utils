import os
import sys
import requests


def get_job_settings(host, token, job_id):
    endpoint = f"{host}/api/2.1/jobs/get"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {"job_id": job_id}

    response = requests.get(endpoint, headers=headers, json=payload)
    response_json = response.json()

    if "settings" in response_json and response_json["settings"]:
        return response_json["settings"]
    else:
        raise Exception(f"Job not found for id: {job_id}")


def update_job(host, token, job_id, settings):
    job_name = settings["name"]
    job_cluster_key = f"{job_name}_job_cluster"

    job_tasks = settings["tasks"]
    for task in job_tasks:
        task.update({"job_cluster_key": job_cluster_key})
        task.pop("existing_cluster_id")

    # Update or parameterize as necessary
    update_payload = {
        "job_clusters": [
            {   
                "job_cluster_key": job_cluster_key,
                "new_cluster": {
                    "autoscale": {
                        "min_workers": 5,
                        "max_workers": 12
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
        "tasks": job_tasks,
    }

    endpoint = f"{host}/api/2.1/jobs/update"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {"job_id": job_id, "new_settings": update_payload}

    response = requests.post(endpoint, headers=headers, json=payload)
    return response.json()


def main(databricks_host, databricks_token, job_id):
    settings = get_job_settings(databricks_host, databricks_token, job_id)
    update_response = update_job(databricks_host, databricks_token, job_id, settings)
    print("Job updated successfully:", update_response)


if __name__ == "__main__":
    try:
        job_id = sys.argv[1]
    except:
        raise Exception("Please pass job id as argument upon execution.")

    # Replace these values with your actual Databricks host and token
    databricks_host = os.getenv("DATABRICKS_HOST")
    databricks_token = os.getenv("DATABRICKS_TOKEN")

    if not databricks_host or not databricks_token:
        raise Exception("Please define enviromnet variables DATABRICKS_HOST, DATABRICKS_TOKEN prior to execution.")

    main(databricks_host, databricks_token, job_id)
    
