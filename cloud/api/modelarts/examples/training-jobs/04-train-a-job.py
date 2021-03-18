import requests
import json
import datetime
import getpass


# get credentials
def get_token(domain_name="", project_id=None):
    if project_id:
        filename = '.project-credentials'
        scope = {
                    "project": {
                        "id": project_id
                    }
                }
    else:
        filename = '.domain-credentials'
        scope = {
                    "domain": {
                        "name": domain_name
                    }
                }

    try:
        with open(filename, "r") as token_file:
            credentials = json.loads(token_file.readline())

        expires_at = datetime.datetime.strptime(
            credentials["expires_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
        now = datetime.datetime.now()
        if expires_at < now:
            raise Exception("Token expired.")

    except Exception:
        user = input("Username: ")
        password = getpass.getpass("Password: ")

        url = "https://iam.ae-ad-1.g42cloud.com/v3/auth/tokens"
        headers = {"Content-Type": "application/json;charset=utf8"}
        data = json.dumps({
            "auth": {
                "identity": {
                    "methods": [
                        "password"
                    ],
                    "password": {
                        "user": {
                            "domain": {
                                "name": domain_name
                            },
                            "name": user,
                            "password": password
                        }
                    }
                },
                "scope": scope
            }
        })

        response = requests.post(url, headers=headers, data=data)

        if response.status_code != 201:
            raise Exception(
                f"Failed to get credentials. Code {response.status_code} - Error {response.text}")

        obj = json.loads(response.text)

        credentials = {
            "token": response.headers["X-Subject-Token"], "expires_at": obj["token"]["expires_at"]}

        with open(filename, 'w') as token_file:
            token_file.write(json.dumps(credentials))

    return credentials["token"]


def get_projects(token):
    url = f"https://iam.ae-ad-1.g42cloud.com/v3/projects"
    headers = {"X-Auth-Token": token, "Content-Type": "application/json;charset=utf8"}

    response = requests.get(url, headers=headers)

    if response.status_code >= 400:
        raise Exception(
            f"Failed to get the projects. Code {response.status_code} - Error {response.text}")

    obj = json.loads(response.text)
    return obj


def get_resources(project_id, token):
    url = f"https://modelarts.ae-ad-1.g42cloud.com/v1/{project_id}/job/resource-specs"
    headers = {"X-Auth-Token": token, "Content-Type": "application/json;charset=utf8"}
    data = {"job_type": "train"}

    response = requests.get(url, headers=headers, params=data)

    if response.status_code >= 400:
        raise Exception(
            f"Failed to get the resources specifications. Code {response.status_code} - Error {response.text}")

    obj = json.loads(response.text)
    return obj


def get_engines(project_id, token):
    url = f"https://modelarts.ae-ad-1.g42cloud.com/v1/{project_id}/job/ai-engines"
    headers = {"X-Auth-Token": token, "Content-Type": "application/json;charset=utf8"}
    data = {"job_type": "train"}

    response = requests.get(url, headers=headers, params=data)

    if response.status_code >= 400:
        raise Exception(
            f"Failed to get the engines specifications. Code {response.status_code} - Error {response.text}")

    obj = json.loads(response.text)
    return obj


# create and run the training job
def submit_job(project_id, token, name, description="", spec_id=2, engine_id=100, worker_server_num=1):

    url = f"https://modelarts.ae-ad-1.g42cloud.com/v1/{project_id}/training-jobs"
    headers = {"X-Auth-Token": token, "Content-Type": "application/json;charset=utf8"}

    data = json.dumps({
        "job_name": name,
        "job_desc": description,
        "config": {
            "worker_server_num": worker_server_num,
            "app_url": "/roberto-public-read/modelarts-annottator-integration/code/",
            "boot_file_url": "/roberto-public-read/modelarts-annottator-integration/code/03-detect-spelling-anomalies.py",
            "parameter": [],
            "data_url": "/roberto-public-read/modelarts-annottator-integration/data/",
            # "dataset_id": None,
            # "dataset_version_id": None,
            # "data_source": {
            #     "dataset_id": "", # for dataset only
            #     "dataset_version": "", # for dataset only
            #     "data_url": "", # for obs only
            #     "type": "", # "dataset" | "obs"
            # },
            "spec_id": spec_id,
            "engine_id": engine_id,
            # "model_id": None,
            "train_url": "/roberto-public-read/modelarts-annottator-integration/model/",
            # "log_url": "",
            # "user_image_url": None,
            # "user_command": None,
            "create_version": True,
            # "volumes": {
            #     "nfs": {
            #         "id": "",
            #         "src_path": "",
            #         "dest_path": "",
            #         "read_only": False
            #     },
            #     "host_path": {
            #         "src_path": "",
            #         "dest_path": "",
            #         "read_only": False    
            #     }
            # }
        }
    })

    response = requests.post(url, headers=headers, data=data)

    if response.status_code >= 400:
        raise Exception(
            f"Failed to submit the job. Code {response.status_code} - Error {response.text}")

    obj = json.loads(response.text)
    return obj


def get_job_details(project_id, token, job_id, version_id):
    url = f"https://modelarts.ae-ad-1.g42cloud.com/v1/{project_id}/training-jobs/{job_id}/versions/{version_id}"
    headers = {"X-Auth-Token": token, "Content-Type": "application/json;charset=utf8"}

    response = requests.get(url, headers=headers)

    if response.status_code >= 400:
        raise Exception(
            f"Failed to get the project details. Code {response.status_code} - Error {response.text}")

    obj = json.loads(response.text)
    return obj


if __name__ == "__main__":
    import pprint


    # getting project_id
    domain_name = input("Informe your G42 Cloud domain name: ")
    domain_token = get_token(domain_name=domain_name)
    projects = get_projects(domain_token)["projects"]
    for project in projects:
        if project["name"] == "ae-ad-1":
            project_id = project["id"]

    # getting token for project
    token = get_token(domain_name, project_id)

    # TODO create OBS folders
    
    # TODO upload script and data to OBS

    # get resources specifications
    resources = get_resources(project_id, token)["specs"]
    for resource in resources:
        if resource["spec_code"] == "modelarts.vm.cpu.2u":
            spec_id = resource["spec_id"]
            worker_server_num = resource["max_num"]
    
    # get engines specifications
    engines = get_engines(project_id, token)["engines"]
    for engine in engines:
        if engine["engine_name"] == "XGBoost-Sklearn":
            engine_id = engine["engine_id"]

    # run training job
    job = submit_job(project_id, 
                    token, 
                    name="train-with-api", 
                    spec_id=spec_id, 
                    engine_id=engine_id, 
                    worker_server_num=worker_server_num)
    job_id = job["job_id"]
    version_id = job["version_id"]

    # get job details
    pprint.pprint(get_job_details(project_id, token, job_id, version_id))
    
    # TODO download the result file