import requests
import json
import datetime
import getpass
import uuid
from obs import ObsClient


# get credentials for ModelArts
def get_token(domain_name="", project_id=None, user="", password=""):
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
        if not user:
            user = input("Username: ")
        if not password:
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


def create_temporary_ak_sk(token):
    url = "https://iam.ae-ad-1.g42cloud.com/v3.0/OS-CREDENTIAL/securitytokens"
    headers = {"Content-Type": "application/json;charset=utf8"}

    data = json.dumps({
        "auth": {
            "identity": {
                "methods": [
                      "token"
                      ],
                "token": {
                    "id": token,
                    "duration-seconds": "900"
                }
            }
        }
    })

    response = requests.post(url, headers=headers, data=data)

    if response.status_code != 201:
        raise Exception(
            f"Failed to get credentials. Code {response.status_code} - Error {response.text}")

    obj = json.loads(response.text)["credential"]
    return obj


def get_projects(token):
    url = f"https://iam.ae-ad-1.g42cloud.com/v3/projects"
    headers = {"X-Auth-Token": token,
               "Content-Type": "application/json;charset=utf8"}

    response = requests.get(url, headers=headers)

    if response.status_code >= 400:
        raise Exception(
            f"Failed to get the projects. Code {response.status_code} - Error {response.text}")

    obj = json.loads(response.text)
    return obj


def get_resources(project_id, token):
    url = f"https://modelarts.ae-ad-1.g42cloud.com/v1/{project_id}/job/resource-specs"
    headers = {"X-Auth-Token": token,
               "Content-Type": "application/json;charset=utf8"}
    data = {"job_type": "train"}

    response = requests.get(url, headers=headers, params=data)

    if response.status_code >= 400:
        raise Exception(
            f"Failed to get the resources specifications. Code {response.status_code} - Error {response.text}")

    obj = json.loads(response.text)
    return obj


def get_engines(project_id, token):
    url = f"https://modelarts.ae-ad-1.g42cloud.com/v1/{project_id}/job/ai-engines"
    headers = {"X-Auth-Token": token,
               "Content-Type": "application/json;charset=utf8"}
    data = {"job_type": "train"}

    response = requests.get(url, headers=headers, params=data)

    if response.status_code >= 400:
        raise Exception(
            f"Failed to get the engines specifications. Code {response.status_code} - Error {response.text}")

    obj = json.loads(response.text)
    return obj


# create and run the training job
def submit_job(project_id, token, name, description="", spec_id=2, engine_id=100, worker_server_num=1,
               app_url="", boot_file_url="", data_url="", train_url=""):

    url = f"https://modelarts.ae-ad-1.g42cloud.com/v1/{project_id}/training-jobs"
    headers = {"X-Auth-Token": token,
               "Content-Type": "application/json;charset=utf8"}

    data = json.dumps({
        "job_name": name,
        "job_desc": description,
        "config": {
            "worker_server_num": worker_server_num,
            "app_url": app_url,
            "boot_file_url": boot_file_url,
            "parameter": [],
            "data_url": data_url,
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
            "train_url": train_url,
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
    headers = {"X-Auth-Token": token,
               "Content-Type": "application/json;charset=utf8"}

    response = requests.get(url, headers=headers)

    if response.status_code >= 400:
        raise Exception(
            f"Failed to get the project details. Code {response.status_code} - Error {response.text}")

    obj = json.loads(response.text)
    return obj


if __name__ == "__main__":
    import pprint

    # generate unique id for the execution
    uid = uuid.uuid1()

    # get user information
    domain_name = input("Kindly inform your G42 Cloud domain name: ")
    user = input("Username: ")
    password = getpass.getpass("Password: ")
    bucket_name = input("Bucket name: ")
    server = "https://obs.ae-ad-1.g42cloud.com"

    # get project_id
    domain_token = get_token(domain_name=domain_name,
                             user=user, password=password)
    projects = get_projects(domain_token)["projects"]
    for project in projects:
        if project["name"] == "ae-ad-1":
            project_id = project["id"]

    # get token for project
    token = get_token(domain_name, project_id, user=user, password=password)

    # get ak/sk credentials
    credentials = create_temporary_ak_sk(token)
    AK = credentials["access"]
    SK = credentials["secret"]

    # Constructs a obs client instance with your account for accessing OBS
    obs_client = ObsClient(
        access_key_id=AK, secret_access_key=SK, server=server)
    bucket_client = obs_client.bucketClient(bucket_name)

    # TODO create OBS folders (not working)
    base_folder = f"{uid}/"
    obs_client.putContent(bucket_name, base_folder)

    resp = obs_client.getObjectMetadata(bucket_name, base_folder)
    print('Size of the empty folder ' + base_folder + ' is ' + str(dict(resp.header).get('content-length')))



    code_folder = f"{base_folder}code/"
    obs_client.putContent(bucket_name, code_folder)
    data_folder = f"{base_folder}data/"
    obs_client.putContent(bucket_name, data_folder)
    model_folder = f"{base_folder}model/"
    obs_client.putContent(bucket_name, model_folder)

    # upload script and data to OBS
    train_file = code_folder + "train.py"
    obs_client.putFile(bucket_name, train_file,
                       "03-detect-annotation-anomalies.py")

    data_file = data_folder + "kindness.csv"
    obs_client.putFile(bucket_name, data_file, "data/kindness.csv")

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
                     name=f"train-with-api-{uid}",
                     spec_id=spec_id,
                     engine_id=engine_id,
                     worker_server_num=worker_server_num,
                     app_url=f"/{bucket_name}/{code_folder}",
                     boot_file_url=f"/{bucket_name}/{code_folder}train.py",
                     data_url=f"/{bucket_name}/{data_folder}",
                     train_url=f"/{bucket_name}/{model_folder}")
    job_id = job["job_id"]
    version_id = job["version_id"]

    # get job details
    pprint.pprint(get_job_details(project_id, token, job_id, version_id))

    # download the result file
    obs_client.downloadFile(bucket_name, data_folder +
                            "evaluation.csv", "data/evaluation.csv")
