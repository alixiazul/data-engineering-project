# import json
# import boto3
# from pg8000.native import Connection
# from pg8000 import DatabaseError
# from datetime import datetime
# from decimal import Decimal
# from botocore.exceptions import ClientError, NoCredentialsError
# import logging
# import os


# logger = logging.getLogger("extraction")
# logger.setLevel(logging.INFO)


# # ---------------------------- CONNECTION ----------------------------------


# def connect_to_extract_db_cloud():
#     totesysinfo = get_secret()
#     return Connection(
#         user=totesysinfo["username"],
#         password=totesysinfo["password"],
#         database=totesysinfo["database"],
#         host=totesysinfo["host"],
#         port=totesysinfo["port"],
#     )


# # --------------------------- ERRORS ------------------------------------


# class EventInfo:
#     def __init__(self, exception, resource, client):
#         self.location = os.path.basename(__file__)
#         self.resource = resource
#         self.client = client
#         self.reason = str(exception)

#     def log_error(self):
#         error_info = {
#             "location": self.location,
#             "resource": self.resource,
#             "client": self.client,
#             "reason": self.reason,
#         }
#         logger.error(f"{error_info}")

#     def log_info(self):
#         event_info = {
#             "location": self.location,
#             "resource": self.resource,
#             "client": self.client,
#             "reason": self.reason,
#         }
#         logger.info(f"{event_info}")


# # --------------------------- GET_SECRETS--------------------------------


# def get_secret():
#     session = boto3.session.Session(profile_name="default")
#     client = session.client(service_name="secretsmanager", region_name="eu-west-2")

#     try:
#         get_secret_value_response = client.get_secret_value(SecretId="totesysinfo")
#     except (ClientError, NoCredentialsError) as e:
#         event_info = EventInfo(exception=e, resource="secretsmanager", client="boto3")
#         event_info.log_error()
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


# # --------------------------- UTILS --------------------------------------


# class DateTimeEncoder(json.JSONEncoder):
#     """
#     Formats the JSON file to the right format in Python.
#     Returns:
#         - Datetimes are converted to ISO format.
#         - Decimal points are converted to floats.
#         - Any other format is maintained and not converted.
#     """

#     def default(self, o):
#         if isinstance(o, datetime):
#             return o.isoformat()
#         elif isinstance(o, Decimal):
#             return float(o)
#         else:
#             return super().default(o)


# def formatted_addresses(result):
#     addresses_dict = [
#         {
#             "address_id": row[0],
#             "address_line_1": row[1],
#             "address_line_2": row[2],
#             "district": row[3],
#             "city": row[4],
#             "postal_code": row[5],
#             "country": row[6],
#             "phone": row[7],
#             "created_at": row[8],
#             "last_updated": row[9],
#         }
#         for row in result
#     ]
#     return addresses_dict


# def formatted_counterparties(result):
#     counterparties_dict = [
#         {
#             "counterparty_id": row[0],
#             "counterparty_legal_name": row[1],
#             "legal_address_id": row[2],
#             "commercial_contact": row[3],
#             "delivery_contact": row[4],
#             "created_at": row[5],
#             "last_updated": row[6],
#         }
#         for row in result
#     ]
#     return counterparties_dict


# def formatted_currencies(result):
#     currencies_dict = [
#         {
#             "currency_id": row[0],
#             "currency_code": row[1],
#             "created_at": row[2],
#             "last_updated": row[3],
#         }
#         for row in result
#     ]
#     return currencies_dict


# def formatted_departments(result):
#     departments_dict = [
#         {
#             "department_id": row[0],
#             "department_name": row[1],
#             "location": row[2],
#             "manager": row[3],
#             "created_at": row[4],
#             "last_updated": row[5],
#         }
#         for row in result
#     ]
#     return departments_dict


# def formatted_designs(result):
#     designs_dict = [
#         {
#             "design_id": row[0],
#             "created_at": row[1],
#             "design_name": row[2],
#             "file_location": row[3],
#             "file_name": row[4],
#             "last_updated": row[5],
#         }
#         for row in result
#     ]
#     return designs_dict


# def formatted_payments(result):
#     payments_dict = [
#         {
#             "payment_id": row[0],
#             "created_at": row[1],
#             "last_updated": row[2],
#             "transaction_id": row[3],
#             "counterparty_id": row[4],
#             "payment_amount": row[5],
#             "currency_id": row[6],
#             "payment_type_id": row[7],
#             "paid": row[8],
#             "payment_date": row[9],
#             "company_ac_number": row[10],
#             "counterparty_ac_number": row[11],
#         }
#         for row in result
#     ]
#     return payments_dict


# def formatted_payments_types(result):
#     payments_dict = [
#         {
#             "payment_type_id": row[0],
#             "payment_type_name": row[1],
#             "created_at": row[2],
#             "last_updated": row[3],
#         }
#         for row in result
#     ]
#     return payments_dict


# def formatted_purchase_orders(result):
#     purchase_orders_dict = [
#         {
#             "purchase_order_id": row[0],
#             "created_at": row[1],
#             "last_updated": row[2],
#             "staff_id": row[3],
#             "counterparty_id": row[4],
#             "item_code": row[5],
#             "item_quantity": row[6],
#             "item_unit_price": row[7],
#             "currency_id": row[8],
#             "agreed_delivery_date": row[9],
#             "agreed_payment_date": row[10],
#             "agreed_delivery_location_id": row[11],
#         }
#         for row in result
#     ]
#     return purchase_orders_dict


# def formatted_sales_orders(result):
#     sales_orders_dict = [
#         {
#             "sales_order_id": row[0],
#             "created_at": row[1],
#             "last_updated": row[2],
#             "design_id": row[3],
#             "staff_id": row[4],
#             "counterparty_id": row[5],
#             "units_sold": row[6],
#             "unit_price": row[7],
#             "currency_id": row[8],
#             "agreed_delivery_date": row[9],
#             "agreed_payment_date": row[10],
#             "agreed_delivery_location_id": row[11],
#         }
#         for row in result
#     ]
#     return sales_orders_dict


# def formatted_staff(result):
#     staff_dict = [
#         {
#             "staff_id": row[0],
#             "first_name": row[1],
#             "last_name": row[2],
#             "department_id": row[3],
#             "email_address": row[4],
#             "created_at": row[5],
#             "last_updated": row[6],
#         }
#         for row in result
#     ]
#     return staff_dict


# def formatted_transactions(result):
#     transactions_dict = [
#         {
#             "transaction_id": row[0],
#             "transaction_type": row[1],
#             "sales_order_id": row[2],
#             "purchase_order_id": row[3],
#             "created_at": row[4],
#             "last_updated": row[5],
#         }
#         for row in result
#     ]
#     return transactions_dict


# # ---------------------- EXTRACT_LAMBDA --------------------------------------


# def execute_query(conn, query) -> str:
#     """
#     Runs a query
#     Arguments:
#     - conn: connection to the database
#     - query: query to perform in the database
#     Returns:
#     - All the data from the query
#     - Error in string format
#     """
#     try:
#         result = conn.run(query)
#         return result
#     except DatabaseError as e:
#         event_info = EventInfo(exception=e, resource="database", client="none")
#         event_info.log_error()
#         raise e
#         # return str(e)


# # TODO: Add to the error information about where the error took place,
# # give more information


# def extract_table(table_name: str, conn: Connection) -> list:
#     """
#     Action: Reads all data from a table in database totesys.
#     Arguments:
#     - table_name: name of the table
#     - conn: connection to the database
#     Returns: List of dictionaries with all the data from the table
#     """
#     # TODO: Figure out how to pick up just the data that is new
#     # TODO: avoid SQL injection
#     query = f"select * from {table_name}"
#     result = execute_query(conn, query)
#     if table_name == "address":
#         return formatted_addresses(result)
#     if table_name == "counterparty":
#         return formatted_counterparties(result)
#     if table_name == "currency":
#         return formatted_currencies(result)
#     if table_name == "department":
#         return formatted_departments(result)
#     if table_name == "design":
#         return formatted_designs(result)
#     if table_name == "payment_type":
#         return formatted_payments_types(result)
#     if table_name == "payment":
#         return formatted_payments(result)
#     if table_name == "purchase_order":
#         return formatted_purchase_orders(result)
#     if table_name == "sales_order":
#         return formatted_sales_orders(result)
#     if table_name == "staff":
#         return formatted_staff(result)
#     if table_name == "transaction":
#         return formatted_transactions(result)


# def save_data_to_json(filename: str, data: list) -> None:
#     """
#     Action: Write a json file with the data
#     Arguments:
#         filename: name of the json file
#         data: list containing dictionaries
#     """
#     with open(filename, "w", encoding="utf-8") as f:
#         json.dump(data, f, indent=4, cls=DateTimeEncoder)


# def write_to_s3(file_name, bucket, key):
#     """Helper to write material to S3."""
#     try:
#         s3 = boto3.resource("s3")
#         s3.Bucket(bucket).upload_file(file_name, key)
#         return True
#     except ClientError as c:
#         event_info = EventInfo(exception=c, resource="s3resource", client="boto3")
#         event_info.log_error()
#         return False


# def log_extraction_done():
#     event_info = EventInfo(
#         location="extract_lambda",
#         resource="lambda",
#         client="none",
#         reason="EXTRACTION SUCCESSFULLY DONE",
#     )
#     event_info.log_info()


# # db = connect_to_extract_db_cloud()


# def get_late_date_parameter():
#     ssm_client = boto3.client("ssm")
#     parameter_latest_date = ssm_client.get_parameter(Name="latest_date")
#     late_date_parameter = parameter_latest_date["Parameter"]["Value"]
#     datateime_str = datetime.datetime.strptime(
#         late_date_parameter, "%Y-%m-%d %H:%M:%S.%f"
#     )
#     return datateime_str


# def table_to_json(conn, table, where_date):
#     return conn.run(
#         f"select json_agg(row_to_json({table})) as payment_json from {table} where last_updated > ':{where_date}';",
#         where_date=where_date,
#     )[0][0]


# def get_table_names(conn):
#     data = conn.run(
#         """SELECT table_name
#         FROM information_schema.tables
#         WHERE table_schema = 'public'
#         AND table_type = 'BASE TABLE';"""
#     )
#     unwanted_tables = ["_prisma_migrations"]
#     return [i[0] for i in data if i[0] not in unwanted_tables]


# def get_latest_date(conn, tables):
#     newest = datetime.datetime(1990, 11, 3, 14, 20, 49, 962000)
#     for table in tables:
#         newest_table_time = conn.run(
#             f"select last_updated from {table} order by last_updated limit 1;"
#         )[0][0]
#         if newest_table_time > newest:
#             newest = conn.run(
#                 f"select last_updated from {table} order by last_updated limit 1;"
#             )[0][0]
#     return newest


# def update_date_parameter(newest):
#     ssm_client = boto3.client("ssm")
#     res = ssm_client.put_parameter(
#         Name="latest_date", Value=f"{newest}", Type="String", Overwrite=True
#     )


# def save_json_to_folder(table, latest_update, data):
#     if not os.path.exists(f"tmp/"):
#         os.mkdir(f"tmp/")
#     if not os.path.exists(f"tmp/data/"):
#         os.mkdir(f"tmp/data/")
#     if not os.path.exists(f"tmp/data/{table}"):
#         os.mkdir(f"tmp/data/{table}")
#     if not os.path.exists(
#         f"tmp/data/{table}/{latest_update.year}-{latest_update.strftime('%B')}/"
#     ):
#         os.mkdir(
#             f"tmp/data/{table}/{latest_update.year}-{latest_update.strftime('%B')}/"
#         )
#     file_name = f"tmp/data/{table}/{latest_update.year}-{latest_update.strftime('%B')}/{table}-{latest_update}.json"
#     with open(file_name, "w") as f:
#         f.write(json.dumps(data))
#     s3_client = boto3.client("s3")
#     try:
#         s3_client.upload_file(file_name, "test-extract-bucket-fe", file_name)
#     except FileNotFoundError:
#         logging.error(f"File {file_name} not found")
#     except NoCredentialsError:
#         logging.error(f"Credentials not correct")
#     except ClientError:
#         logging.error(f"Bucket does not exist or does not have access")


# def extract_lambda_handler(event, context):
#     """
#     Extracts data from database totesys, write the data into json files in the
#     S3 extract bucket

#     Arguments:
#         event: trigger of the lambda function
#         data: list containing dictionaries
#     """
#     db_conn = None

#     try:
#         # # https://docs.aws.amazon.com/AmazonS3/latest/userguide/ev-events.html
#         # s3_bucket_name = event["detail"]["bucket"]["name"]
#         # timestamp = str(int(datetime.timestamp(datetime.now())))

#         # file_path = os.path.realpath(__file__)

#         # table_names = [
#         #     "address",
#         #     "counterparty",
#         #     "currency",
#         #     "department",
#         #     "design",
#         #     "payment_type",
#         #     "payment",
#         #     "purchase_order",
#         #     "sales_order",
#         #     "staff",
#         #     "transaction",
#         # ]

#         # for table_name in table_names:
#         #     path_table_file = os.path.join(
#         #         os.path.dirname(file_path), f"{table_name}.json"
#         #     )
#         #     table_key = f"{table_name}/{table_name}_{timestamp}.json"
#         #     table_data = extract_table(table_name, db)
#         #     save_data_to_json(path_table_file, table_data)
#         #     write_to_s3(path_table_file, s3_bucket_name, table_key)

#         # log_extraction_done()
#         db_conn = connect_to_extract_db_cloud
#         table_names = get_table_names(db_conn())
#         latest_update = get_latest_date(db_conn(), table_names)
#         parameter_date = get_late_date_parameter()

#         if latest_update > parameter_date:
#             for table in table_names:
#                 json_table = table_to_json(db_conn(), table, parameter_date)
#             if len(json_table) > 0:
#                 save_json_to_folder(table, latest_update, json_table)
#             else:
#                 event_info = EventInfo(
#                     location="extract_lambda_handler",
#                     resource="database",
#                     client="db_conn",
#                     reason=f"There is no new data in {table}",
#                 )
#                 event_info.log_info()
#         else:
#             event_info = EventInfo(
#                 location="extract_lambda_handler",
#                 resource="database",
#                 client="db_conn",
#                 reason=f"There is no new data in {table}",
#             )
#         update_date_parameter(latest_update)
#         # newest = datetime.datetime(1990, 11, 3, 14, 20, 49, 962000)
#         # update_date_parameter(newest)

#         event_info = EventInfo(
#             location="extract_lambda_handler",
#             resource="lambda",
#             client="none",
#             reason=f"EXTRACTION SUCESSFULLY DONE",
#         )
#         event_info.log_info()
#     except Exception as e:
#         event_info = EventInfo(exception=e, resource="lambda_handler", client="none")
#         event_info.log_error()
#         raise e
#     finally:
#         if db_conn:
#             db_conn.close()


# # TODO:
# # REFACTOR


# if __name__ == "__main__":
#     event = {
#         "version": "0",
#         "id": "17793124-05d4-b198-2fde-7ededc63b103",
#         "detail-type": "Object Created",
#         "source": "aws.s3",
#         "account": "111122223333",
#         "time": "2021-11-12T00:00:00Z",
#         "region": "ca-central-1",
#         "resources": ["arn:aws:s3:::extract_bucket"],
#         "detail": {"version": "0"},
#         "bucket": {
#             "name": "test-extract-bucket-20240516151623472700000002",
#             "object": {
#                 "key": "example-key",
#                 "size": "5",
#                 "etag": "b1946ac92492d2347c6235b4d2611184",
#                 "version-id": "IYV3p45BT0ac8hjHg1houSdS1a.Mro8e",
#                 "sequencer": "617f08299329d189",
#             },
#             "request-id": "N4N7GDK58NMKJ12R",
#             "requester": "123456789012",
#             "source-ip-address": "1.2.3.4",
#             "reason": "PutObject",
#         },
#     }
#     extract_lambda_handler(event, "test")


import datetime
import json
import os
import boto3
import logging
from botocore.exceptions import NoCredentialsError, ClientError
from .connection_extract import connect_to_extract_db_cloud as db_conn
from pg8000 import DatabaseError


ssm_client = boto3.client("ssm", region_name="eu-west-2")
s3_client = boto3.client("s3", region_name="eu-west-2")


def get_table_names(conn):
    data = conn.run(
        """SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE';"""
    )
    unwanted_tables = ["_prisma_migrations"]
    return [i for i in data[0] if i not in unwanted_tables]


def get_latest_date(conn, tables):
    newest = datetime.datetime(1990, 11, 3, 14, 20, 49, 962000)
    for table in tables:
        newest_table_time = conn.run(
            f"select last_updated from {table} order by last_updated desc limit 1;"
        )[0][0]
        if newest_table_time > newest:
            newest = conn.run(
                f"select last_updated from {table} order by last_updated desc limit 1;"
            )[0][0]
    return newest


def get_latest_date_parameter(ssm_client=ssm_client):
    try:
        parameter_latest_date = ssm_client.get_parameter(Name="latest_date")
        late_date_parameter = parameter_latest_date["Parameter"]["Value"]

        datetime_str = datetime.datetime.strptime(
            late_date_parameter, "%Y-%m-%d %H:%M:%S.%f"
        )
    except ClientError as e:
        logging.error(f"{e}")
        if e.response["Error"]["Code"] == "ParameterNotFound":
            raise
    return datetime_str


# def table_to_json(conn, table, where_date):
#     res = conn.run(
#         f"select json_agg(row_to_json({table})) as payment_json from {table} where last_updated > ':{where_date}';",
#         where_date=where_date,
#     )[0][0]
#     return res


def table_to_json(conn, table, where_date):
    try:
        res = conn.run(
            f"select json_agg(row_to_json({table})) as payment_json from {table} where last_updated > ':{where_date}';",
            where_date=where_date,
        )[0][0]
        return res
    except DatabaseError as e:
        raise


def update_date_parameter(newest):
    # if parameter store empty
    ssm_client = boto3.client("ssm")
    res = ssm_client.put_parameter(
        Name="latest_date", Value=f"{newest}", Type="String", Overwrite=True
    )


def save_json_to_folder(table, latest_update, data, s3_client=s3_client):
    if not os.path.exists("/tmp/"):
        os.mkdir("/tmp/")
    if not os.path.exists("/tmp/data/"):
        os.mkdir("/tmp/data/")
    if not os.path.exists(f"/tmp/data/{table}"):
        os.mkdir(f"/tmp/data/{table}")
    if not os.path.exists(
        f"/tmp/data/{table}/{latest_update.year}-{latest_update.strftime('%B')}/"
    ):
        os.mkdir(
            f"/tmp/data/{table}/{latest_update.year}-{latest_update.strftime('%B')}/"
        )

    file_name = f"/tmp/data/{table}/{latest_update.year}-{latest_update.strftime('%B')}/{table}-{latest_update}.json"
    with open(file_name, "w") as f:
        f.write(json.dumps(data))

    try:
        s3_client.upload_file(
            file_name,
            "extraction-bucket-sorceress",
            f"{table}/{latest_update.year}-{latest_update.strftime('%B')}/{table}-{latest_update}.json",
        )
    except FileNotFoundError as e:
        logging.error(f"File {file_name} not found")
        raise e
    except NoCredentialsError as e:
        logging.error("Credentials not correct")
        raise e
    except ClientError as e:
        logging.error("Bucket does not exist or does not have access")
        raise e


def lambda_handler(event, context):
    try:
        table_names = get_table_names(db_conn())
        # print(f"==>> table_names: {table_names}")

        latest_date_from_db = get_latest_date(db_conn(), table_names)
        # print(f"==>> latest_date_from_db: {latest_date_from_db}")

        parameter_date = get_latest_date_parameter()
        # print(f"==>> parameter_date: {parameter_date}")
        if latest_date_from_db > parameter_date:
            for table in table_names:
                json_table = table_to_json(db_conn(), table, parameter_date)

                if json_table:
                    save_json_to_folder(table, latest_date_from_db, json_table)
                    logging.info(f"Table {table} saved")
                else:
                    logging.info(f"There is no new data in {table}")
        else:
            logging.info(f"No new data from {latest_date_from_db}")

        update_date_parameter(latest_date_from_db)
    except Exception as e:
        logging.error(f"Unable to complete database extraction{e}", exc_info=True)
        raise e
    # Whenever initially running this function, you need to uncomment these two lines, run it twice, obtain the data, and then commit those lines again.
    # newest = datetime.datetime(1990, 11, 3, 14, 20, 49, 962000)
    # update_date_parameter(newest)


# lambda_handler(1, 2)
