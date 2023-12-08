import os
import sys
import json
import requests

DRY_RUN = False
CLUSTER_DEFAULTS = {
    "autoscale": {
        "min_workers": 1,
        "max_workers": 4
    },
    "cluster_source": "API",
    "driver_instance_pool_id": None,
    "driver_node_type_id": "Standard_E4ds_v5", # Set to None if driver_instance_pool_id is specified 
    "instance_pool_id": None,
    "node_type_id": "Standard_E4ds_v5", # Set to None if instance_pool_id is specified 
    "runtime_engine": "PHOTON",
    "spark_version": "9.1.x-scala2.12",    
}

class Migrate:

    def __init__(self, host, token, job_id):
        self.host = host
        self.token = token
        self.job_id = job_id
        self.job_settings = None
        self.cluster_id = None
        self.cluster_settings = None

    def set_job_settings(self):
        endpoint = f"{host}/api/2.1/jobs/get"
        headers = {"Authorization": f"Bearer {token}"}
        payload = {"job_id": self.job_id}

        response = requests.get(endpoint, headers=headers, json=payload)
        response_json = response.json()

        if response.status_code == 200:
            self.job_settings = response_json["settings"]
        else:
            raise Exception(f"Error: {response.status_code} - {response.text}")


    def set_cluster_settings(self):
        endpoint = f"{host}/api/2.0/clusters/get"
        headers = {"Authorization": f"Bearer {token}"}
        payload = {"cluster_id": self.cluster_id}

        response = requests.get(endpoint, headers=headers, json=payload)
        response_json = response.json()

        if response.status_code == 200:
            self.cluster_settings = response_json
        else:
            raise Exception(f"Error: {response.status_code} - {response.text}")


    def update_job(self, update_payload):
        endpoint = f"{host}/api/2.1/jobs/update"
        headers = {"Authorization": f"Bearer {token}"}
        payload = {"job_id": self.job_id, "new_settings": update_payload}

        response = requests.post(endpoint, headers=headers, json=payload)

        if response.status_code == 200:
            print("Job update successfully.")
        else:
            raise Exception(f"Error: {response.status_code} - {response.text}")


    def build_update_payload(self):
        job_name = self.job_settings["name"]
        job_cluster_key = f"{job_name}_job_cluster"

        # Modify tasks settings
        job_tasks = self.job_settings["tasks"]
        for task in job_tasks:
            try:
                self.cluster_id = task.pop("existing_cluster_id")
            except:
                raise Exception("No existing interactive cluster specified")
            task.update({"job_cluster_key": job_cluster_key})

        # Get cluster settings 
        # WARNING: Assumes that all tasks use same cluster
        cluster_settings = self.set_cluster_settings()

        # Modify cluster settings
        cluster_update = CLUSTER_DEFAULTS
        for param in [
            "azure_attributes",
            "cluster_log_conf",
            "custom_tags",
            "data_security_mode",
            "docker_image",
            "enable_local_disk_encryption",
            "init_scripts",
            "spark_conf",
            "spark_env_vars",
            "ssh_public_keys"
        ]:
            try:
                cluster_update.update({param: self.cluster_settings[param]})
            except KeyError:
                pass

        # Return partial update payload
        return {
            "job_clusters": [
                {   
                    "job_cluster_key": job_cluster_key,
                    "new_cluster": cluster_update
                },
            ],
            "tasks": job_tasks,
        }


def main(host, token, job_id):
    m = Migrate(host, token, job_id)

    m.set_job_settings()
    update_payload = m.build_update_payload()
    if not DRY_RUN:
        m.update_job(update_payload)
    else:
        print(json.dumps(update_payload, indent=2))

if __name__ == "__main__":
    try:
        job_id = sys.argv[1]
    except:
        raise Exception("Please pass job id as argument e.g. python migrate.py <job_id>.")

    # TODO: Replace these values with your actual Databricks host and token
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")

    if not host or not token:
        raise Exception("Please define enviromnet variables DATABRICKS_HOST, DATABRICKS_TOKEN prior to execution.")

    main(host, token, job_id)
