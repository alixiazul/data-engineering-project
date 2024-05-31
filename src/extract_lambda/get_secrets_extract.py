# import boto3
# import json
# from botocore.exceptions import ClientError


# def get_secret_db(secret):
#     # Create a Secrets Manager client
#     session = boto3.session.Session()
#     client = session.client(service_name="secretsmanager", region_name="eu-west-2")

#     try:
#         get_secret_value_response = client.get_secret_value(SecretId=secret)
#     except ClientError as e:
#         raise e

#     if "SecretString" in get_secret_value_response:
#         secret_string = get_secret_value_response["SecretString"]

#     secrets_dict = json.loads(secret_string)

#     host = secrets_dict.get("host")
#     port = secrets_dict.get("port")
#     database = secrets_dict.get("dbname")
#     username = secrets_dict.get("username")
#     password = secrets_dict.get("password")

#     return {
#         "host": host,
#         "port": port,
#         "database": database,
#         "username": username,
#         "password": password,
#     }
