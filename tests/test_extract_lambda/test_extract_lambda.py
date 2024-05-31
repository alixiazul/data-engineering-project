import pytest
import os
from src.extract_lambda.extract_lambda import (
    get_table_names,
    get_latest_date_parameter,
    table_to_json,
    update_date_parameter,
    save_json_to_folder,
    get_latest_date,
    lambda_handler,
)
from pg8000 import DatabaseError
from unittest.mock import Mock
from moto import mock_aws
from botocore.exceptions import NoCredentialsError, ClientError
import boto3
import datetime
import json
from unittest.mock import Mock, patch
import logging


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
def s3_client(aws_credentials):
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


class TestGetTableNames:
    def test_return_list_of_strings(self, ssm_client, s3_client):
        mock_conn = Mock()
        mock_conn.run.return_value = [["1", "2", "3", "_prisma_migrations"]]
        res = get_table_names(mock_conn)
        assert all(type(i) == str for i in res)

    def test_return_list_without_prisma_migrations(self, ssm_client, s3_client):
        mock_conn = Mock()
        mock_conn.run.return_value = [["1", "2", "3", "_prisma_migrations"]]
        res = get_table_names(mock_conn)
        assert res == ["1", "2", "3"]


# class TestGetLatestDate:
#     def test_(self, ssm_client, s3_client):


class TestGetLatestDateParameter:
    def test_returns_parameter(self, ssm_client, s3_client):
        ssm_client.put_parameter(
            Name="latest_date",
            Value="2024-05-23 15:16:09.981000",
            Type="String",
            Overwrite=True,
        )
        res = get_latest_date_parameter(ssm_client=ssm_client)
        assert res == datetime.datetime(2024, 5, 23, 15, 16, 9, 981000)

    def test_wrong_parameter(self, ssm_client, s3_client):
        ssm_client.put_parameter(
            Name="latest_dat",
            Value="2024-05-23 15:16:09.981000",
            Type="String",
            Overwrite=True,
        )
        with pytest.raises(ClientError):
            res = get_latest_date_parameter(ssm_client=ssm_client)


class TestUpdateDateParameter:
    def test_update_date_parameter_from_store(self, ssm_client, s3_client):
        old_datetime = datetime.datetime(2020, 5, 23, 15, 00, 00, 199000)
        old_date_inserted = ssm_client.put_parameter(
            Name="latest_date",
            Value=str(old_datetime),
            Type="String",
            Overwrite=True,
        )

        get_old_parameters = ssm_client.get_parameter(Name="latest_date")

        new_datetime = datetime.datetime(2024, 5, 23, 19, 54, 58, 199000)
        update_date_parameter(new_datetime)

        get_new_parameters = ssm_client.get_parameter(Name="latest_date")

        updated_parameter_value = get_new_parameters["Parameter"]["Value"]
        assert old_datetime < datetime.datetime.fromisoformat(updated_parameter_value)


class TestTableToJson:
    def test_return_list_of_dicts(self):
        mock_conn = Mock()
        mock_conn.run.return_value = json.loads(
            """[[[
  {
    "transaction_id": 1,
    "transaction_type": "PURCHASE",
    "sales_order_id": null,
    "purchase_order_id": 2,
    "created_at": "2022-11-03T14:20:52.186",
    "last_updated": "2022-11-03T14:20:52.186"
  },
  {
    "transaction_id": 2,
    "transaction_type": "PURCHASE",
    "sales_order_id": null,
    "purchase_order_id": 3,
    "created_at": "2022-11-03T14:20:52.187",
    "last_updated": "2022-11-03T14:20:52.187"
  }]]]"""
        )

        expected = [
            {
                "transaction_id": 1,
                "transaction_type": "PURCHASE",
                "sales_order_id": None,
                "purchase_order_id": 2,
                "created_at": "2022-11-03T14:20:52.186",
                "last_updated": "2022-11-03T14:20:52.186",
            },
            {
                "transaction_id": 2,
                "transaction_type": "PURCHASE",
                "sales_order_id": None,
                "purchase_order_id": 3,
                "created_at": "2022-11-03T14:20:52.187",
                "last_updated": "2022-11-03T14:20:52.187",
            },
        ]

        res = table_to_json(mock_conn, "transaction", "2022-11-03T14:20:52.187")
        assert expected == res

    def test_table_to_json_throws_database_error(self):
        mock_conn = Mock()
        mock_conn.run.side_effect = DatabaseError()
        parameter_date = str(datetime.datetime(2020, 5, 23, 15, 00, 00, 199000))

        with pytest.raises(DatabaseError) as dbe:
            table_to_json(mock_conn, "transaction", parameter_date)


class TestSaveJsonToFolder:
    def test_file_not_found(self, s3_client):
        mock_s3_client = Mock()
        mock_s3_client.upload_file.side_effect = FileNotFoundError
        latest_update = datetime.datetime(2022, 11, 1)
        with pytest.raises(FileNotFoundError):

            save_json_to_folder("test", latest_update, "json", s3_client=mock_s3_client)

    def test_no_cretentials(self, s3_client):
        mock_s3_client = Mock()
        mock_s3_client.upload_file.side_effect = NoCredentialsError
        latest_update = datetime.datetime(2022, 11, 1)
        with pytest.raises(NoCredentialsError):

            save_json_to_folder("test", latest_update, "json", s3_client=mock_s3_client)

    def test_client_error(self, s3_client):
        mock_s3_client = Mock()
        mock_s3_client.upload_file.side_effect = ClientError(
            error_response={
                "Error": {
                    "Code": "ResourceNotFoundException",
                    "Message": "Bucket not found",
                }
            },
            operation_name="PutObjectBucket",
        )

        latest_update = datetime.datetime(2022, 11, 1)
        with pytest.raises(ClientError):

            save_json_to_folder("test", latest_update, "json", s3_client=mock_s3_client)


class TestGetLatestDate:
    def test_returns_datetime(self):
        mock_conn = Mock()
        mock_conn.run.return_value = [
            [datetime.datetime(1990, 11, 3, 14, 20, 49, 962000)]
        ]

        res = get_latest_date(mock_conn, "tables")
        assert isinstance(res, datetime.datetime)

    def test_newest_remains_unchanged(self):
        mock_conn = Mock()
        old_date_list = [[datetime.datetime(1990, 11, 3, 14, 20, 49, 962000)]]
        mock_conn.run.return_value = old_date_list
        res = get_latest_date(mock_conn, "tables")
        print(res)
        assert old_date_list[0][0] == res

    def test_get_latest_date_updates_newest(self):
        mock_conn1 = Mock()
        initial_date_list = [[datetime.datetime(1990, 11, 3, 14, 20, 49, 962000)]]
        mock_conn1.run.return_value = initial_date_list
        res1 = get_latest_date(mock_conn1, "tables")

        mock_conn2 = Mock()
        date_list_update = [[datetime.datetime(2024, 11, 3, 14, 20, 49, 962000)]]
        mock_conn2.run.return_value = date_list_update
        res2 = get_latest_date(mock_conn2, "tables")
        assert res2 > res1


# look at this later
class TestLambdaHandler:
    @pytest.fixture(scope="function")
    def secretsmanager_client(aws_credentials):
        with mock_aws():
            yield boto3.client("secretsmanager", region_name="eu-west-2")

    @patch("src.extract_lambda.extract_lambda.get_table_names")
    @patch("src.extract_lambda.extract_lambda.get_latest_date")
    @patch("src.extract_lambda.extract_lambda.get_latest_date_parameter")
    @patch("src.extract_lambda.extract_lambda.table_to_json")
    @patch("src.extract_lambda.extract_lambda.save_json_to_folder")
    @patch("src.extract_lambda.extract_lambda.update_date_parameter")
    @patch("src.extract_lambda.extract_lambda.db_conn")
    def test_handler_writes_json_files_to_s3(
        self,
        mock_db_conn,
        mock_update_date_parameter,
        mock_save_json_to_folder,
        mock_table_to_json,
        mock_get_latest_date_parameter,
        mock_get_latest_date,
        mock_get_table_names,
        s3_client,
        ssm_client,
        caplog,
    ):

        s3_client.create_bucket(
            Bucket="extraction-bucket-sorceress",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        ssm_client.put_parameter(
            Name="latest_date", Value="1999-05-24 00:00:00.000000", Type="String"
        )

        mock_get_latest_date.return_value = datetime.datetime(
            2022, 11, 3, 14, 20, 52, 186000
        )
        mock_get_latest_date_parameter.return_value = datetime.datetime(
            1999, 5, 24, 00, 00, 00, 000000
        )
        mock_table_to_json.side_effect = """[[
    {
        "transaction_id": 1,
        "transaction_type": "PURCHASE",
        "sales_order_id": null,
        "purchase_order_id": 2,
        "created_at": "2022-11-03T14:20:52.186",
        "last_updated": "2022-11-03T14:20:52.186"
    },
    {
        "transaction_id": 2,
        "transaction_type": "PURCHASE",
        "sales_order_id": null,
        "purchase_order_id": 3,
        "created_at": "2022-11-03T14:20:52.187",
        "last_updated": "2022-11-03T14:20:52.187"
    }]]"""

        with caplog.at_level(logging.INFO):
            lambda_handler({}, [])
            assert ["Foo"] == [rec.message for rec in caplog.records]


#     # @patch("src.extract_lambda.extract_lambda.get_table_names")
#     def test_no_new_data(self, caplog, aws_credentials):
#         # mock_get_table_names = Mock()
#         # print(mock_get_table_names)
#         # mock_get_table_names.side_effect = ["1"]
#         # patch_get_table_names = patch(
#         #     "src.extract_lambda.extract_lambda.get_table_names", return_value=["1"]
#         # )

#         mock_get_latest_date = Mock()
#         mock_get_latest_date.side_effect = datetime.datetime(
#             1991, 11, 3, 14, 20, 49, 962000
#         )

#         mock_get_latest_date_parameter = Mock()
#         mock_get_latest_date_parameter.side_effect = datetime.datetime(
#             1990, 11, 3, 14, 20, 49, 962000
#         )

#         mock_table_to_json = Mock()
#         mock_table_to_json.side_effect = """[[
#   {
#     "transaction_id": 1,
#     "transaction_type": "PURCHASE",
#     "sales_order_id": null,
#     "purchase_order_id": 2,
#     "created_at": "2022-11-03T14:20:52.186",
#     "last_updated": "2022-11-03T14:20:52.186"
#   },
#   {
#     "transaction_id": 2,
#     "transaction_type": "PURCHASE",
#     "sales_order_id": null,
#     "purchase_order_id": 3,
#     "created_at": "2022-11-03T14:20:52.187",
#     "last_updated": "2022-11-03T14:20:52.187"
#   }]]"""

#         mock_save_json_to_folder = Mock()
#         mock_save_json_to_folder.return_value = ["1"]
#         caplog.set_level(logging.INFO)
#         # patch_get_table_names.start()
#         with patch(
#             "src.extract_lambda.extract_lambda.get_table_names", side_effect=["1"]
#         ):
#             lambda_handler("event", "context")
#             # patch_get_table_names.stop()

#             with caplog.at_level(logging.INFO):
#                 assert "No new data from 2022-11-03 14:20:49.962000" in [
#                     rec.message for rec in caplog.records
#                 ]

###############################################################################################################

# @pytest.fixture(autouse=True)
# def connect_db():
#     db = connect_to_extract_db_cloud()
#     yield db
#     db.close()


# @pytest.fixture(scope="function")
# def s3(aws_credentials):
#     with mock_aws():
#         yield boto3.client("s3", region_name="eu-west-2")


# @pytest.fixture(scope="function")
# def aws_credentials():
#     """Mocked AWS Credentials for moto."""
#     os.environ["AWS_ACCESS_KEY_ID"] = "test"
#     os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
#     os.environ["AWS_SECURITY_TOKEN"] = "test"
#     os.environ["AWS_SESSION_TOKEN"] = "test"
#     os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


# def check_table_exists(table_name: str, connect_db):
#     query = (
#         "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE "
#         "table_name = :table_name)"
#     )
#     conn = connect_db
#     return conn.run(query, **{"table_name": table_name})


# class TestDatabaseErrors:
#     # TODO: Handling the errors, use patch.
#     # Think about how to pick up the error and act about it.
#     # EXAMPLE: connection to the database failed, why: credentials?
#     # @pytest.mark.skip
#     def test_execute_query_throws_database_error(self):
#         mock_conn = Mock()
#         mock_conn.run.side_effect = DatabaseError("Database error occurred")

#         with pytest.raises(DatabaseError) as dbe:
#             execute_query(mock_conn, "SELECT * FROM test_table")

#         assert isinstance(dbe.value, DatabaseError)

#     # @pytest.mark.skip
#     # def test_operational_error(self):
#     #     mock_conn = Mock()
#     #     mock_conn.side_effect = OperationalError("Database is down")

#     #     result = execute_query(mock_conn, "SELECT * FROM test_table")
#     #     print(str(result))
#     #     assert result == "Database is down"

#     # @pytest.mark.skip
#     # def test_programming_error(self):
#     #     mock_conn = Mock()
#     #     mock_cursor = Mock()
#     #     mock_conn.cursor.return_value = mock_cursor
#     #     mock_cursor.execute.side_effect = ProgrammingError(
#     #         "Syntax error in SQL query")

#     #     result = execute_query(mock_conn, "SELECT * FROM test_table")
#     #     assert result == "Syntax error in SQL query"

#     # def test_invalid_table_name_returns_false(self, connect_db):
#     #     result = check_table_exists("invalid_table_name", connect_db)
#     #     assert result == [[False]]


# class TestTableAddress:  #
#     TABLE_NAME = "address"

#     @pytest.mark.it("Address table exists in database totesys")
#     def test_address_table_exists(self, connect_db):
#         result = check_table_exists(self.TABLE_NAME, connect_db)
#         assert result == [[True]]

#     @pytest.mark.it("Address table has all expected column names")
#     def test_address_table_column_names(self, connect_db):
#         conn = connect_db
#         result = extract_table(self.TABLE_NAME, conn)
#         assert "address_id" in result[0]
#         assert "address_line_1" in result[0]
#         assert "address_line_2" in result[0]
#         assert "district" in result[0]
#         assert "city" in result[0]
#         assert "postal_code" in result[0]
#         assert "country" in result[0]
#         assert "phone" in result[0]
#         assert "created_at" in result[0]
#         assert "last_updated" in result[0]

#     @pytest.mark.it("Address table is not empty")
#     def test_address_table_is_not_empty(self, connect_db):
#         result = extract_table(self.TABLE_NAME, connect_db)
#         assert len(result) > 0

#     @pytest.mark.it("SAVE ADDRESS DATA TO JSON FILE IN THIS FORMAT")
#     def test_address_data_saved_to_json_file(self, connect_db):
#         filename = f"data/{self.TABLE_NAME}.json"
#         address_data = extract_table(self.TABLE_NAME, connect_db)
#         save_data_to_json(filename, address_data)
#         created_at = linecache.getline(filename, 11)
#         last_updated = linecache.getline(filename, 12)
#         assert "created_at" in created_at
#         assert "last_updated" in last_updated


# class TestTableCounterparty:
#     TABLE_NAME = "counterparty"

#     @pytest.mark.it("Counterparty table exists in database totesys")
#     def test_counterparty_table_exists(self, connect_db):
#         result = check_table_exists(self.TABLE_NAME, connect_db)
#         assert result == [[True]]

#     @pytest.mark.it("Counterparty table has all expected column names")
#     def test_counterparty_table_column_names(self, connect_db):
#         result = extract_table(self.TABLE_NAME, connect_db)
#         assert "counterparty_id" in result[0]
#         assert "counterparty_legal_name" in result[0]
#         assert "legal_address_id" in result[0]
#         assert "commercial_contact" in result[0]
#         assert "delivery_contact" in result[0]
#         assert "created_at" in result[0]
#         assert "last_updated" in result[0]

#     @pytest.mark.it("Counterparty table is not empty")
#     def test_counterparty_table_is_not_empty(self, connect_db):
#         result = extract_table(self.TABLE_NAME, connect_db)
#         assert len(result) > 0

#     @pytest.mark.it("SAVE COUNTERPARTY DATA TO JSON FILE IN THIS FORMAT")
#     def test_counterparty_data_is_saved_to_json_file(self, connect_db):
#         filename = f"data/{self.TABLE_NAME}.json"
#         counterparty_data = extract_table(self.TABLE_NAME, connect_db)
#         save_data_to_json(filename, counterparty_data)
#         created_at = linecache.getline(filename, 8)
#         last_updated = linecache.getline(filename, 9)
#         assert "created_at" in created_at
#         assert "last_updated" in last_updated


# class TestTableCurrency:
#     TABLE_NAME = "currency"

#     @pytest.mark.it("Currency table exists in database totesys")
#     def test_currency_table_exists(self, connect_db):
#         result = check_table_exists(self.TABLE_NAME, connect_db)
#         assert result == [[True]]

#     @pytest.mark.it("Currency table has all expected column names")
#     def test_currency_table_column_names(self, connect_db):
#         result = extract_table(self.TABLE_NAME, connect_db)
#         assert "currency_id" in result[0]
#         assert "currency_code" in result[0]
#         assert "created_at" in result[0]
#         assert "last_updated" in result[0]

#     @pytest.mark.it("Currency table is not empty")
#     def test_currency_table_is_not_empty(self, connect_db):
#         result = extract_table(self.TABLE_NAME, connect_db)
#         assert len(result) > 0

#     @pytest.mark.it("SAVE CURRENCY DATA TO JSON FILE IN THIS FORMAT")
#     def test_currency_data_is_saved_to_json_file(self, connect_db):
#         filename = f"data/{self.TABLE_NAME}.json"
#         currency_data = extract_table(self.TABLE_NAME, connect_db)
#         save_data_to_json(filename, currency_data)
#         created_at = linecache.getline(filename, 11)
#         last_updated = linecache.getline(filename, 12)
#         assert "created_at" in created_at
#         assert "last_updated" in last_updated


# class TestTableDepartment:
#     TABLE_NAME = "department"

#     @pytest.mark.it("Department table exists in database totesys")
#     def test_department_table_exists(self, connect_db):
#         result = check_table_exists(self.TABLE_NAME, connect_db)
#         assert result == [[True]]

#     @pytest.mark.it("Department table has all expected column names")
#     def test_currency_table_column_names(self, connect_db):
#         result = extract_table(self.TABLE_NAME, connect_db)
#         assert "department_id" in result[0]
#         assert "department_name" in result[0]
#         assert "location" in result[0]
#         assert "manager" in result[0]
#         assert "created_at" in result[0]
#         assert "last_updated" in result[0]

#     @pytest.mark.it("Department table is not empty")
#     def test_department_table_is_not_empty(self, connect_db):
#         result = extract_table(self.TABLE_NAME, connect_db)
#         assert len(result) > 0

#     @pytest.mark.it("SAVE DEPARTMENT DATA TO JSON FILE IN THIS FORMAT")
#     def test_department_data_is_saved_to_json_file(self, connect_db):
#         filename = f"data/{self.TABLE_NAME}.json"
#         department_data = extract_table(self.TABLE_NAME, connect_db)
#         save_data_to_json(filename, department_data)
#         created_at = linecache.getline(filename, 7)
#         last_updated = linecache.getline(filename, 8)
#         assert "created_at" in created_at
#         assert "last_updated" in last_updated


# class TestTableDesign:
#     TABLE_NAME = "design"

#     @pytest.mark.it("Design table exists in database totesys")
#     def test_design_table_exists(self, connect_db):
#         result = check_table_exists(self.TABLE_NAME, connect_db)
#         assert result == [[True]]

#     @pytest.mark.it("Design table has all expected column names")
#     def test_design_table_column_names(self, connect_db):
#         result = extract_table(self.TABLE_NAME, connect_db)
#         assert "design_id" in result[0]
#         assert "created_at" in result[0]
#         assert "design_name" in result[0]
#         assert "file_location" in result[0]
#         assert "file_name" in result[0]
#         assert "last_updated" in result[0]

#     @pytest.mark.it("Design table is not empty")
#     def test_design_table_is_not_empty(self, connect_db):
#         result = extract_table(self.TABLE_NAME, connect_db)
#         assert len(result) > 0

#     @pytest.mark.it("SAVE DESIGN DATA TO JSON FILE IN THIS FORMAT")
#     def test_design_data_is_saved_to_json_file(self, connect_db):
#         filename = f"data/{self.TABLE_NAME}.json"
#         design_data = extract_table(self.TABLE_NAME, connect_db)
#         save_data_to_json(filename, design_data)
#         created_at = linecache.getline(filename, 4)
#         last_updated = linecache.getline(filename, 8)
#         assert "created_at" in created_at
#         assert "last_updated" in last_updated


# class TestTablePayment:
#     TABLE_NAME = "payment"

#     @pytest.mark.it("Payment table exists in database totesys")
#     def test_payment_table_exists(self, connect_db):
#         result = check_table_exists(self.TABLE_NAME, connect_db)
#         assert result == [[True]]

#     @pytest.mark.it("Payment table has all expected column names")
#     def test_payment_table_column_names(self, connect_db):
#         result = extract_table(self.TABLE_NAME, connect_db)
#         assert "payment_id" in result[0]
#         assert "last_updated" in result[0]
#         assert "transaction_id" in result[0]
#         assert "counterparty_id" in result[0]
#         assert "payment_amount" in result[0]
#         assert "currency_id" in result[0]
#         assert "payment_type_id" in result[0]
#         assert "paid" in result[0]
#         assert "payment_date" in result[0]
#         assert "company_ac_number" in result[0]
#         assert "counterparty_ac_number" in result[0]

#     @pytest.mark.it("Payment table is not empty")
#     def test_payment_table_is_not_empty(self, connect_db):
#         result = extract_table(self.TABLE_NAME, connect_db)
#         assert len(result) > 0

#     @pytest.mark.it("SAVE PAYMENT DATA TO JSON FILE IN THIS FORMAT")
#     def test_design_data_is_saved_to_json_file(self, connect_db):
#         filename = f"data/{self.TABLE_NAME}.json"
#         payment_data = extract_table(self.TABLE_NAME, connect_db)
#         save_data_to_json(filename, payment_data)
#         created_at = linecache.getline(filename, 4)
#         last_updated = linecache.getline(filename, 5)
#         assert "created_at" in created_at
#         assert "last_updated" in last_updated


# class TestTablePaymentType:
#     TABLE_NAME = "payment_type"

#     @pytest.mark.it("Payment_type table exists in database totesys")
#     def test_payment_type_table_exists(self, connect_db):
#         result = check_table_exists(self.TABLE_NAME, connect_db)
#         assert result == [[True]]

#     @pytest.mark.it("Payment_type table has all expected column names")
#     def test_payment_type_table_column_names(self, connect_db):
#         result = extract_table(self.TABLE_NAME, connect_db)
#         assert "payment_type_id" in result[0]
#         assert "payment_type_name" in result[0]
#         assert "created_at" in result[0]
#         assert "last_updated" in result[0]

#     @pytest.mark.it("Payment_type table is not empty")
#     def test_payment_type_table_is_not_empty(self, connect_db):
#         result = extract_table(self.TABLE_NAME, connect_db)
#         assert len(result) > 0

#     @pytest.mark.it("SAVE PAYMENT_TYPE DATA TO JSON FILE IN THIS FORMAT")
#     def test_payment_type_data_is_saved_to_json_file(self, connect_db):
#         filename = f"data/{self.TABLE_NAME}.json"
#         payment_type_data = extract_table(self.TABLE_NAME, connect_db)
#         save_data_to_json(filename, payment_type_data)
#         created_at = linecache.getline(filename, 11)
#         last_updated = linecache.getline(filename, 12)
#         assert "created_at" in created_at
#         assert "last_updated" in last_updated


# class TestTablePurchaseOrder:
#     TABLE_NAME = "purchase_order"

#     @pytest.mark.it("Purchase_order table exists in database totesys")
#     def test_purchase_order_table_exists(self, connect_db):
#         result = check_table_exists(self.TABLE_NAME, connect_db)
#         assert result == [[True]]

#     @pytest.mark.it("Purchase_order table has all expected column names")
#     def test_purchase_order_column_names(self, connect_db):
#         result = extract_table(self.TABLE_NAME, connect_db)
#         assert "purchase_order_id" in result[0]
#         assert "created_at" in result[0]
#         assert "last_updated" in result[0]
#         assert "staff_id" in result[0]
#         assert "counterparty_id" in result[0]
#         assert "item_code" in result[0]
#         assert "item_quantity" in result[0]
#         assert "item_unit_price" in result[0]
#         assert "currency_id" in result[0]
#         assert "agreed_delivery_date" in result[0]
#         assert "agreed_payment_date" in result[0]
#         assert "agreed_delivery_location_id" in result[0]

#     @pytest.mark.it("Purchase_order table is not empty")
#     def test_purchase_order_table_is_not_empty(self, connect_db):
#         result = extract_table(self.TABLE_NAME, connect_db)
#         assert len(result) > 0

#     @pytest.mark.it("SAVE PURCHASE_ORDER DATA TO JSON FILE IN THIS FORMAT")
#     def test_purchase_order_is_saved_to_json_file(self, connect_db):
#         filename = f"data/{self.TABLE_NAME}.json"
#         purchase_order_data = extract_table(self.TABLE_NAME, connect_db)
#         save_data_to_json(filename, purchase_order_data)
#         created_at = linecache.getline(filename, 4)
#         last_updated = linecache.getline(filename, 5)
#         assert "created_at" in created_at
#         assert "last_updated" in last_updated


# class TestTableSalesOrder:
#     TABLE_NAME = "sales_order"

#     @pytest.mark.it("Sales_order table exists in database totesys")
#     def test_sales_order_table_exists(self, connect_db):
#         result = check_table_exists(self.TABLE_NAME, connect_db)
#         assert result == [[True]]

#     @pytest.mark.it("Sales_order table has all expected column names")
#     def test_sales_order_column_names(self, connect_db):
#         result = extract_table(self.TABLE_NAME, connect_db)
#         assert "sales_order_id" in result[0]
#         assert "created_at" in result[0]
#         assert "last_updated" in result[0]
#         assert "design_id" in result[0]
#         assert "staff_id" in result[0]
#         assert "counterparty_id" in result[0]
#         assert "units_sold" in result[0]
#         assert "unit_price" in result[0]
#         assert "currency_id" in result[0]
#         assert "agreed_delivery_date" in result[0]
#         assert "agreed_payment_date" in result[0]
#         assert "agreed_delivery_location_id" in result[0]

#     @pytest.mark.it("Sales_order table is not empty")
#     def test_sales_order_table_is_not_empty(self, connect_db):
#         result = extract_table(self.TABLE_NAME, connect_db)
#         assert len(result) > 0

#     @pytest.mark.it("SAVE SALES_ORDER DATA TO JSON FILE IN THIS FORMAT")
#     def test_sales_order_is_saved_to_json_file(self, connect_db):
#         filename = f"data/{self.TABLE_NAME}.json"
#         sales_order_data = extract_table(self.TABLE_NAME, connect_db)
#         save_data_to_json(filename, sales_order_data)
#         created_at = linecache.getline(filename, 4)
#         last_updated = linecache.getline(filename, 5)
#         assert "created_at" in created_at
#         assert "last_updated" in last_updated


# class TestTableStaff:
#     TABLE_NAME = "staff"

#     @pytest.mark.it("Staff table exists in database totesys")
#     def test_staff_table_exists(self, connect_db):
#         result = check_table_exists(self.TABLE_NAME, connect_db)
#         assert result == [[True]]

#     @pytest.mark.it("Staff table has all expected column names")
#     def test_staff_column_names(self, connect_db):
#         result = extract_table(self.TABLE_NAME, connect_db)
#         assert "staff_id" in result[0]
#         assert "first_name" in result[0]
#         assert "last_name" in result[0]
#         assert "department_id" in result[0]
#         assert "email_address" in result[0]
#         assert "created_at" in result[0]
#         assert "last_updated" in result[0]

#     @pytest.mark.it("Staff table is not empty")
#     def test_staff_table_is_not_empty(self, connect_db):
#         result = extract_table(self.TABLE_NAME, connect_db)
#         assert len(result) > 0

#     @pytest.mark.it("SAVE STAFF DATA TO JSON FILE IN THIS FORMAT")
#     def test_sales_order_is_saved_to_json_file(self, connect_db):
#         filename = f"data/{self.TABLE_NAME}.json"
#         staff_data = extract_table(self.TABLE_NAME, connect_db)
#         save_data_to_json(filename, staff_data)
#         created_at = linecache.getline(filename, 8)
#         last_updated = linecache.getline(filename, 9)
#         assert "created_at" in created_at
#         assert "last_updated" in last_updated


# class TestTableTransaction:
#     TABLE_NAME = "transaction"

#     @pytest.mark.it("Transaction table exists in database totesys")
#     def test_transaction_table_exists(self, connect_db):
#         result = check_table_exists(self.TABLE_NAME, connect_db)
#         assert result == [[True]]

#     @pytest.mark.it("Transaction table has all expected column names")
#     def test_transaction_column_names(self, connect_db):
#         result = extract_table(self.TABLE_NAME, connect_db)
#         assert "transaction_id" in result[0]
#         assert "transaction_type" in result[0]
#         assert "sales_order_id" in result[0]
#         assert "purchase_order_id" in result[0]
#         assert "created_at" in result[0]
#         assert "last_updated" in result[0]

#     @pytest.mark.it("Transaction table is not empty")
#     def test_transaction_table_is_not_empty(self, connect_db):
#         result = extract_table(self.TABLE_NAME, connect_db)
#         assert len(result) > 0

#     @pytest.mark.it("SAVE TRANSACTION DATA TO JSON FILE IN THIS FORMAT")
#     def test_transaction_is_saved_to_json_file(self, connect_db):
#         filename = f"data/{self.TABLE_NAME}.json"
#         transaction_data = extract_table(self.TABLE_NAME, connect_db)
#         save_data_to_json(filename, transaction_data)
#         created_at = linecache.getline(filename, 7)
#         last_updated = linecache.getline(filename, 8)
#         assert "created_at" in created_at
#         assert "last_updated" in last_updated


# @pytest.fixture
# def bucket(s3):
#     s3.create_bucket(
#         Bucket="extract_bucket",
#         CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
#     )


# class TestHandler:
#     BUCKET_NAME = "extract_bucket"

#     def test_extract_bucket_is_ready(self, s3, bucket):
#         bucket_list = s3.list_buckets()
#         # print("BUCKET LIST --->", bucket_list['Buckets'])
#         assert self.BUCKET_NAME in bucket_list["Buckets"][0]["Name"]

#     @pytest.mark.it("TestHandler: Handles unexpected exception")
#     @patch("src.extract_lambda.extract_lambda.write_to_s3")
#     def test_handler_handles_unexpected_exception(
#         self, mock_s3_writer, s3, bucket, caplog
#     ):
#         event = {}
#         mock_s3_writer.side_effect = ValueError
#         with pytest.raises(Exception):
#             extract_lambda_handler(event, [])

#     def test_write_to_s3_throws_filenotfound_error(self):
#         file_name = "invalid_address_table.json"
#         bucket_name = "test-extract-bucket-20240516151623472700000002"
#         key = "address/address.json"
#         with pytest.raises(FileNotFoundError):
#             write_to_s3(file_name, bucket_name, key)


# class TestGetSecret:

#     # Test 4: credentials not available
#     @patch("boto3.session.Session")
#     def test_get_secret_throws_nocredentialserror(self, mock_boto_session):
#         mock_client = Mock()
#         mock_client.get_secret_value.side_effect = NoCredentialsError()

#         mock_boto_session.return_value.client.return_value = mock_client

#         with pytest.raises(NoCredentialsError):
#             get_secret()

# Test 5: client error -> check in boto3.error exceptions
# @patch("boto3.session.Session")
# def test_get_secret_throws_clienterror_exception(self, mock_boto_session):
#     mock_client = Mock()
#     mock_client.get_secret_value.side_effect = ClientError(
#         error_response={
#             "Error": {
#                 "Code": "ResourceNotFoundException",
#                 "Message": "Secret not found",
#             }
#         },
#         operation_name="GetSecretValue",
#     )

#         mock_boto_session.return_value.client.return_value = mock_client
#         with pytest.raises(ClientError):
#             get_secret()

#     @patch("boto3.session.Session")
#     def test_get_secret_returns_secrets_correctly(self, mock_boto_session):
#         secret_value = json.dumps(
#             {
#                 "host": "test_host",
#                 "port": "test_port",
#                 "dbname": "test_db",
#                 "username": "test_user",
#                 "password": "test_password",
#             }
#         )
#         mock_value_response = {"SecretString": secret_value}
#         mock_client = Mock()
#         mock_client.get_secret_value.return_value = mock_value_response
#         mock_boto_session.return_value.client.return_value = mock_client
#         result = get_secret()

#         expected_result = {
#             "host": "test_host",
#             "port": "test_port",
#             "database": "test_db",
#             "username": "test_user",
#             "password": "test_password",
#         }

#         assert result == expected_result


# class TestErrorInfo:
#     def test_errorinfo_initiliases_correctly(self):
#         mock_exception = NoCredentialsError()
#         event_info = EventInfo(
#             exception=mock_exception, resource="secretsmanager", client="boto3"
#         )

#         assert event_info.location in os.path.basename(__file__)
#         assert event_info.resource == "secretsmanager"
#         assert event_info.client == "boto3"
#         assert event_info.reason == str(mock_exception)

#     def test_log_error(self):
#         mock_exception = Mock()
#         mock_exception.__str__ = Mock(return_value="a very important reason")

#         event_info = EventInfo(
#             exception=mock_exception, resource="test_resource", client="test_client"
#         )

#         with unittest.TestCase().assertLogs("extraction", level="ERROR") as cm:
#             event_info.log_error()

#         file_name = os.path.basename(__file__).removeprefix("test_")
#         expected_error_message = (
#             f"{{'location': '{file_name}', 'resource': 'test_resource', "
#             f"'client': 'test_client', 'reason': 'a very important reason'}}"
#         )
#         assert cm.output == [f"ERROR:extraction:{expected_error_message}"]

#     def test_log_info(self):
#         mock_exception = Mock()
#         mock_exception.__str__ = Mock(return_value="a very important reason")

#         event_info = EventInfo(
#             exception=mock_exception, resource="test_resource", client="test_client"
#         )

#         with unittest.TestCase().assertLogs("extraction", level="ERROR") as cm:
#             event_info.log_error()

#         file_name = os.path.basename(__file__).removeprefix("test_")
#         expected_error_message = (
#             f"{{'location': '{file_name}', 'resource': 'test_resource', "
#             f"'client': 'test_client', 'reason': 'a very important reason'}}"
#         )
#         assert cm.output == [f"ERROR:extraction:{expected_error_message}"]
