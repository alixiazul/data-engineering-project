from src.extract_lambda.connection_extract import (
    connect_to_extract_db_cloud,
    get_secret_db,
)
from dotenv import load_dotenv
import os
from pg8000.native import Connection
from pg8000 import InterfaceError
from moto import mock_aws
import pytest
import boto3
from botocore.exceptions import ClientError
from unittest.mock import Mock


@pytest.fixture(scope="function")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def ssm_client(aws_credentials):
    with mock_aws():
        yield boto3.client("ssm", region_name="eu-west-2")


@pytest.fixture(scope="function")
def secretsmanager_client(aws_credentials):
    with mock_aws():
        yield boto3.client("secretsmanager", region_name="eu-west-2")


def making_secret(secretsmanager_client):
    secretsmanager_client.create_secret(
        Name="totesysinfo",
        SecretString='{"host":"test","port":5432,"dbname":"test","username":"test","password":"test"}',
    )


def test_get_secret_db_returns_db_cretentials(secretsmanager_client):
    making_secret(secretsmanager_client)
    res = get_secret_db("totesysinfo", secretsmanager_client)
    assert res == {
        "host": "test",
        "port": 5432,
        "database": "test",
        "username": "test",
        "password": "test",
    }


def test_get_secret_db_secret_does_not_exist(secretsmanager_client):
    making_secret(secretsmanager_client)
    with pytest.raises(ClientError):
        res = get_secret_db("totsysinfo", secretsmanager_client)


# database error
def test_database_error(secretsmanager_client):
    secretsmanager_client.create_secret(
        Name="totesysinfo",
        SecretString='{"host":"test","port":"lkfadl","dbname":"test","username":"test","password":"test"}',
    )
    with pytest.raises(InterfaceError):
        connect_to_extract_db_cloud(secretsmanager_client)


def test_connect_to_extract_db_cloud_interfase_error(secretsmanager_client):
    mock_get_secret_db = Mock()
    mock_get_secret_db.return_value = ClientError(
        error_response={
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": "ResourceNotFoundException",
            }
        },
        operation_name="GetSecretValue",
    )
    with pytest.raises(ClientError):
        connect_to_extract_db_cloud(secretsmanager_client)
