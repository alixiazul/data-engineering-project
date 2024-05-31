# from get_secrets_extract import get_secret_db
from pg8000.native import Connection
from pg8000 import InterfaceError
import boto3
import json
from botocore.exceptions import ClientError
import logging

# ssm_client = boto3.client("ssm", region_name="eu-west-2")
secretsmanager_client = boto3.client("secretsmanager", region_name="eu-west-2")


def connect_to_extract_db_cloud(secretsmanager_client=secretsmanager_client):
    try:
        db_info = get_secret_db(
            "totesysinfo", secretsmanager_client=secretsmanager_client
        )
    except ClientError as e:
        logging.error(f"Data base creadentials are incorrect {e}")
        raise e

    return Connection(
        user=db_info["username"],
        password=db_info["password"],
        database=db_info["database"],
        host=db_info["host"],
        port=db_info["port"],
    )


def get_secret_db(secret, secretsmanager_client=secretsmanager_client):
    # Create a Secrets Manager client
    # session = boto3.session.Session()
    # client = session.client(service_name="secretsmanager", region_name="eu-west-2")

    try:
        get_secret_value_response = secretsmanager_client.get_secret_value(
            SecretId=secret
        )
    except ClientError as e:
        raise e

    if "SecretString" in get_secret_value_response:
        secret_string = get_secret_value_response["SecretString"]

    secrets_dict = json.loads(secret_string)

    host = secrets_dict.get("host")
    port = secrets_dict.get("port")
    database = secrets_dict.get("dbname")
    username = secrets_dict.get("username")
    password = secrets_dict.get("password")

    return {
        "host": host,
        "port": port,
        "database": database,
        "username": username,
        "password": password,
    }
