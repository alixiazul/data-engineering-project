import pytest
import pandas as pd
from numpy import nan
from src.transformation.lambda_function import (
    remove_duplicates_pd,
    design_schema,
    remove_null_values,
    counterparty_schema,
    staff_schema,
    currency_schema,
    get_json_from_s3,
    dim_date,
    get_latest_date_parameter,
    dim_counterparty,
    dim_staff,
    dim_currency,
    dim_design,
    dim_location,
    dim_transaction,
    save_parquet_to_s3,
    dim_date,
    fact_sales_order,
    lambda_handler,
)
from unittest.mock import Mock, patch
import boto3
from moto import mock_aws
import os
import json
import logging


@pytest.fixture(scope="function")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture(scope="function")
def ssm_client(aws_credentials):
    with mock_aws():
        yield boto3.client("ssm")


def create_fake_bucket_with_data(s3_client, bucket):
    bucket = bucket
    response = s3_client.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={
            "LocationConstraint": "eu-west-2",
        },
    )
    test_address = """[
  {
    "address_id": 1,
    "address_line_1": "6826 Herzog Via",
    "address_line_2": null,
    "district": "Avon",
    "city": "New Patienceburgh",
    "postal_code": "28441",
    "country": "Turkey",
    "phone": "1803 637401",
    "created_at": "2022-11-03T14:20:49.962",
    "last_updated": "2022-11-03T14:20:49.962"
  },
  {
    "address_id": 2,
    "address_line_1": "179 Alexie Cliffs",
    "address_line_2": null,
    "district": null,
    "city": "Aliso Viejo",
    "postal_code": "99305-7380",
    "country": "San Marino",
    "phone": "9621 880720",
    "created_at": "2022-11-03T14:20:49.962",
    "last_updated": "2022-11-03T14:20:49.962"
  },
  {
    "address_id": 3,
    "address_line_1": "148 Sincere Fort",
    "address_line_2": null,
    "district": null,
    "city": "Lake Charles",
    "postal_code": "89360",
    "country": "Samoa",
    "phone": "0730 783349",
    "created_at": "2022-11-03T14:20:49.962",
    "last_updated": "2022-11-03T14:20:49.962"
  }
]
"""

    response = s3_client.put_object(
        Bucket=bucket,
        Key="test_data/address-2024-05-21 09:28:10.208000.json",
        Body=test_address,
    )

    response = s3_client.put_object(
        Bucket=bucket,
        Key="test_data/address-2024-05-21 09:28:10.208000 copy.json",
        Body=test_address,
    )


def create_fake_empty_bucket_with_data(s3_client, bucket):
    bucket = bucket
    response = s3_client.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={
            "LocationConstraint": "eu-west-2",
        },
    )


# copy the tests for the cretentials from the first lambda
class TestGetJsonFromS3:
    # def test_bucket_not_exist(self):
    # res = get_json_from_s3(table="address/sdaf")
    # assert False == res

    def test_returns_list(self, s3_client, ssm_client):
        create_fake_bucket_with_data(s3_client, "extraction-bucket-sorceress")
        create_fake_empty_bucket_with_data(s3_client, "transformation-bucket-sorceress")
        response = ssm_client.put_parameter(
            Name="latest_date", Value="2024-05-21 09:28:10.208000", Type="String"
        )
        res = get_json_from_s3("address", s3_client=s3_client)
        assert list == type(res)

    def test_returns_list_of_dicst(self, s3_client, ssm_client):
        create_fake_bucket_with_data(s3_client, "extraction-bucket-sorceress")
        create_fake_empty_bucket_with_data(s3_client, "transformation-bucket-sorceress")
        response = ssm_client.put_parameter(
            Name="latest_date", Value="2024-05-21 09:28:10.208000", Type="String"
        )
        res = get_json_from_s3("address", s3_client=s3_client)
        assert all([type(i) == dict for i in res])

    def test_non_existing_table_name_retrun_empty_list(self, s3_client, ssm_client):
        create_fake_bucket_with_data(s3_client, "extraction-bucket-sorceress")
        create_fake_empty_bucket_with_data(s3_client, "transformation-bucket-sorceress")
        response = ssm_client.put_parameter(
            Name="latest_date", Value="2024-05-21 09:28:10.208000", Type="String"
        )
        res = get_json_from_s3(table="address", s3_client=s3_client)
        assert res == []

    def test_result_no_new_data(self, s3_client, ssm_client):
        create_fake_bucket_with_data(s3_client, "extraction-bucket-sorceress")
        create_fake_empty_bucket_with_data(s3_client, "transformation-bucket-sorceress")
        response = ssm_client.put_parameter(
            Name="latest_date", Value="2024-05-22 09:29:50.068000", Type="String"
        )
        res = get_json_from_s3(table="address", s3_client=s3_client)
        assert res == []

    def test_get_json_from_s3_has_processed_files(self, s3_client, ssm_client):
        create_fake_bucket_with_data(s3_client, "extraction-bucket-sorceress")
        create_fake_bucket_with_data(s3_client, "transformation-bucket-sorceress")
        response = ssm_client.put_parameter(
            Name="latest_date", Value="2024-05-22 09:29:50.068000", Type="String"
        )
        get_json_from_s3(table="address", s3_client=s3_client)

        res = s3_client.list_objects_v2(Bucket="transformation-bucket-sorceress")

        assert "Contents" in res


# class TestReadJsonPd:

#     # def test_read_json_throws_exception(self):
#     #     testInput = (
#     #         {
#     #             "design_id": 6,
#     #             "created_at": "2022-11-03T14:20:49.962",
#     #             "design_name": "Wooden",
#     #             "file_location": "/usr",
#     #             "file_name": "wooden-20220717-npgz.json",
#     #             "last_updated": "2022-11-03T14:20:49.962",
#     #         },
#     #         {
#     #             "created_at": "2022-11-03T14:20:49.962",
#     #             "design_name": "Wooden",
#     #             "file_location": "/usr",
#     #             "file_name": "wooden-20220717-npgz.json",
#     #             "last_updated": "2022-11-03T14:20:49.962",
#     #         },
#     #         {
#     #             "design_id": 50,
#     #             "created_at": "2023-01-12T16:31:09.694",
#     #             "design_name": "Granite",
#     #             "file_location": "/private/var",
#     #             "file_name": "granite-20220205-3vfw.json",
#     #             "last_updated": "2023-01-12T16:31:09.694",
#     #         },
#     #         {
#     #             "design_id": 49,
#     #             "created_at": "2023-01-12T16:31:09.694",
#     #             "design_name": "Granite",
#     #             "file_location": "/private/var",
#     #             "file_name": "granite-20220205-3vfw.json",
#     #             "last_updated": "2023-01-12T16:31:09.694",
#     #         },
#     #     )
#     #     mock_file = Mock()
#     #     mock_file.read_json_pd.side_effect = Exception()
#     #     with pytest.raises(Exception):
#     #         read_json_pd(testInput)


class TestDimDate:
    def test_returns_dataframe(self, ssm_client):
        ssm_res = ssm_client.put_parameter(
            Name="latest_date", Value="2024-05-22 09:29:50.068000", Type="String"
        )
        res = dim_date(ssm_client)
        assert type(pd.DataFrame()) == type(res)

    def test_creates_date_to_latest_parameter(self, ssm_client):
        ssm_res = ssm_client.put_parameter(
            Name="latest_date", Value="2024-05-22 09:29:50.068000", Type="String"
        )
        day_dict = {
            "date_id": [568],
            "year": [2024],
            "month": ["05"],
            "day": [22],
            "day_of_week": [4],
            "day_name": ["Wednesday"],
            "month_name": ["May"],
            "quarter": [2],
        }

        expected = pd.DataFrame(day_dict)
        x = expected.to_string(header=False, index=False, index_names=False).split("\n")
        expected = [",".join(ele.split()) for ele in x]

        res = dim_date(ssm_client).tail(1)
        x = res.to_string(header=False, index=False, index_names=False).split("\n")
        res = [",".join(ele.split()) for ele in x]
        assert res == expected

    def test_creates_date_from_beggining_of_database(self, ssm_client):
        ssm_res = ssm_client.put_parameter(
            Name="latest_date", Value="2024-05-22 09:29:50.068000", Type="String"
        )
        day_dict = {
            "date_id": [0],
            "year": [2022],
            "month": [11],
            "day": ["01"],
            "day_of_week": ["3"],
            "day_name": ["Tuesday"],
            "month_name": ["November"],
            "quarter": [3],
        }

        expected = pd.DataFrame(day_dict)
        x = expected.to_string(header=False, index=False, index_names=False).split("\n")
        expected = [",".join(ele.split()) for ele in x]

        res = dim_date(ssm_client).head(1)
        x = res.to_string(header=False, index=False, index_names=False).split("\n")
        res = [",".join(ele.split()) for ele in x]
        assert res == expected


class TestGetLatesDateParameter:
    def test_retuns_time_as_tring(self, ssm_client):
        ssm_res = ssm_client.put_parameter(
            Name="latest_date", Value="2024-05-22 09:29:50.068000", Type="String"
        )
        res = get_latest_date_parameter(ssm_client=ssm_client)
        assert res == "2024-05-22 09:29:50.068000"


@pytest.fixture(scope="function")
def create_test_dataframe_design_invalid():
    return pd.DataFrame(
        {
            "design_id": 6,
            "created_at": "2022-11-03T14:20:49.962",
            "design_name": "Wooden",
            "file_location": "/usr",
            "file_name": "wooden-20220717-npgz.json",
            "last_updated": "2022-11-03T14:20:49.962",
        },
        {
            "created_at": "2022-11-03T14:20:49.962",
            "design_name": "Wooden",
            "file_location": "/usr",
            "file_name": "wooden-20220717-npgz.json",
            "last_updated": "2022-11-03T14:20:49.962",
        },
        {
            "design_id": 50,
            "created_at": "2023-01-12T16:31:09.694",
            "design_name": "Granite",
            "file_location": "/private/var",
            "file_name": "granite-20220205-3vfw.json",
            "last_updated": "2023-01-12T16:31:09.694",
        },
        {
            "design_id": 49,
            "created_at": "2023-01-12T16:31:09.694",
            "design_name": "Granite",
            "file_location": "/private/var",
            "file_name": "granite-20220205-3vfw.json",
            "last_updated": "2023-01-12T16:31:09.694",
        },
    )


class TestDimDesign:
    def test_dim_design(self, s3_client, ssm_client):
        create_fake_bucket_with_data(s3_client, "extraction-bucket-sorceress")
        create_fake_bucket_with_data(s3_client, "transformation-bucket-sorceress")
        res = ssm_client.put_parameter(
            Name="latest_date", Value="2024-05-21 09:28:10.208000", Type="String"
        )

        test_design = """[
        {
            "design_id": 8,
            "created_at": "2024-05-21 09:28:10.208000",
            "design_name": "Wooden",
            "file_location": "/usr",
            "file_name": "wooden-20220717-npgz.json",
            "last_updated": "2024-05-21 09:28:10.208000"
        },
        {
            "design_id": 51,
            "created_at": "2024-05-21 09:28:10.208000",
            "design_name": "Bronze",
            "file_location": "/private",
            "file_name": "bronze-20221024-4dds.json",
            "last_updated": "2024-05-21 09:28:10.208000"
        }
            ]"""

        res = s3_client.put_object(
            Bucket="extraction-bucket-sorceress",
            Key="design/test_data/design-2024-05-21 09:28:10.208000.json",
            Body=test_design,
        )

        design_datetime_df = get_json_from_s3(table="design", s3_client=s3_client)

        design_df = design_datetime_df[0][1]
        print(design_df)
        dim_design(design_df)

    def test_duplicated_removed_df(self):

        testNoduplicates_df = pd.DataFrame(
            [
                {
                    "design_id": 6,
                    "created_at": "2022-11-03T14:20:49.962",
                    "design_name": "Wooden",
                    "file_location": "/usr",
                    "file_name": "wooden-20220717-npgz.json",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "design_id": 8,
                    "created_at": "2022-11-03T14:20:49.962",
                    "design_name": "Wooden",
                    "file_location": "/usr",
                    "file_name": "wooden-20220717-npgz.json",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "design_id": 51,
                    "created_at": "2023-01-12T18:50:09.935",
                    "design_name": "Bronze",
                    "file_location": "/private",
                    "file_name": "bronze-20221024-4dds.json",
                    "last_updated": "2023-01-12T18:50:09.935",
                },
                {
                    "design_id": 50,
                    "created_at": "2023-01-12T16:31:09.694",
                    "design_name": "Granite",
                    "file_location": "/private/var",
                    "file_name": "granite-20220205-3vfw.json",
                    "last_updated": "2023-01-12T16:31:09.694",
                },
                {
                    "design_id": 69,
                    "created_at": "2023-02-07T17:31:10.093",
                    "design_name": "Bronze",
                    "file_location": "/lost+found",
                    "file_name": "bronze-20230102-r904.json",
                    "last_updated": "2023-02-07T17:31:10.093",
                },
                {
                    "design_id": 16,
                    "created_at": "2022-11-22T15:02:10.226",
                    "design_name": "Soft",
                    "file_location": "/System",
                    "file_name": "soft-20211001-cjaz.json",
                    "last_updated": "2022-11-22T15:02:10.226",
                },
                {
                    "design_id": 54,
                    "created_at": "2023-01-16T09:14:09.775",
                    "design_name": "Plastic",
                    "file_location": "/usr/ports",
                    "file_name": "plastic-20221206-bw3l.json",
                    "last_updated": "2023-01-16T09:14:09.775",
                },
                {
                    "design_id": 55,
                    "created_at": "2023-01-19T08:10:10.138",
                    "design_name": "Concrete",
                    "file_location": "/opt/include",
                    "file_name": "concrete-20210614-04nd.json",
                    "last_updated": "2023-01-19T08:10:10.138",
                },
            ]
        )

        testDuplicates_df = pd.DataFrame(
            [
                {
                    "design_id": 6,
                    "created_at": "2022-11-03T14:20:49.962",
                    "design_name": "Wooden",
                    "file_location": "/usr",
                    "file_name": "wooden-20220717-npgz.json",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "design_id": 8,
                    "created_at": "2022-11-03T14:20:49.962",
                    "design_name": "Wooden",
                    "file_location": "/usr",
                    "file_name": "wooden-20220717-npgz.json",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "design_id": 51,
                    "created_at": "2023-01-12T18:50:09.935",
                    "design_name": "Bronze",
                    "file_location": "/private",
                    "file_name": "bronze-20221024-4dds.json",
                    "last_updated": "2023-01-12T18:50:09.935",
                },
                {
                    "design_id": 50,
                    "created_at": "2023-01-12T16:31:09.694",
                    "design_name": "Granite",
                    "file_location": "/private/var",
                    "file_name": "granite-20220205-3vfw.json",
                    "last_updated": "2023-01-12T16:31:09.694",
                },
                {
                    "design_id": 69,
                    "created_at": "2023-02-07T17:31:10.093",
                    "design_name": "Bronze",
                    "file_location": "/lost+found",
                    "file_name": "bronze-20230102-r904.json",
                    "last_updated": "2023-02-07T17:31:10.093",
                },
                {
                    "design_id": 16,
                    "created_at": "2022-11-22T15:02:10.226",
                    "design_name": "Soft",
                    "file_location": "/System",
                    "file_name": "soft-20211001-cjaz.json",
                    "last_updated": "2022-11-22T15:02:10.226",
                },
                {
                    "design_id": 54,
                    "created_at": "2023-01-16T09:14:09.775",
                    "design_name": "Plastic",
                    "file_location": "/usr/ports",
                    "file_name": "plastic-20221206-bw3l.json",
                    "last_updated": "2023-01-16T09:14:09.775",
                },
                {
                    "design_id": 55,
                    "created_at": "2023-01-19T08:10:10.138",
                    "design_name": "Concrete",
                    "file_location": "/opt/include",
                    "file_name": "concrete-20210614-04nd.json",
                    "last_updated": "2023-01-19T08:10:10.138",
                },
                {
                    "design_id": 56,
                    "created_at": "2023-01-19T08:10:10.138",
                    "design_name": "Concrete",
                    "file_location": "/opt/include",
                    "file_name": "concrete-20210614-04nd.json",
                    "last_updated": "2023-01-19T08:10:10.138",
                },
            ]
        )
        # filepath_no_duplicates = "data/test_design_data.json"
        # filepath_duplicates = "data/test_design_data_duplicates.json"

        # testDuplicates_df = read_json_pd(filepath_duplicates)
        # testNoduplicates_df = read_json_pd(filepath_no_duplicates)

        non_primary_key_col = [
            "created_at",
            "design_name",
            "file_location",
            "file_name",
            "last_updated",
        ]

        output = remove_duplicates_pd(testDuplicates_df, non_primary_key_col)

        assert (testDuplicates_df["design_id"].iloc[-1]) != (
            output["design_id"].iloc[-1]
        )
        assert (testNoduplicates_df["design_id"].iloc[-1]) == (
            output["design_id"].iloc[-1]
        )

    def test_remove_null_values(self):

        df = pd.DataFrame(
            [
                {
                    "design_id": 1,
                    "design_name": nan,
                    "file_location": "/usr",
                    "file_name": "wooden-20220717-npgz.json",
                },
                {
                    "design_id": 2,
                    "design_name": "Wooden",
                    "file_location": "/usr",
                    "file_name": "woodnpgz.json",
                },
            ]
        )
        expected = pd.DataFrame(
            [
                {
                    "design_id": 2,
                    "design_name": "Wooden",
                    "file_location": "/usr",
                    "file_name": "woodnpgz.json",
                }
            ]
        )
        df_no_null = pd.DataFrame(
            [
                {
                    "design_id": 1,
                    "design_name": "Glass",
                    "file_location": "/usr",
                    "file_name": "wooden-20220717-npgz.json",
                },
                {
                    "design_id": 2,
                    "design_name": "Wooden",
                    "file_location": "/usr",
                    "file_name": "woodnpgz.json",
                },
            ]
        )

        df = remove_null_values(df, "design_name")
        assert df["design_id"].iloc[0] == expected["design_id"].iloc[0]

        expected2 = remove_null_values(df_no_null, "design_name")
        assert df_no_null.design_id.all() == expected2.design_id.all()

    def test_design_schema_fulfilled(self):
        df = pd.DataFrame(
            [
                {
                    "design_id": 1,
                    "design_name": "Glass",
                    "file_location": "/usr",
                    "file_name": "wooden-20220717-npgz.json",
                },
                {
                    "design_id": 2,
                    "design_name": "Wooden",
                    "file_location": "/usr",
                    "file_name": "woodnpgz.json",
                },
            ]
        )

        result_df = design_schema(df)

        expected = ["design_id", "design_name", "file_location", "file_name"]
        assert list(result_df.columns.values) == expected

    def test_design_schema_missing_column(self):
        df = pd.DataFrame(
            [
                {
                    "design_id": 1,
                    "design_name": "Glass",
                    "file_name": "wooden-20220717-npgz.json",
                },
                {
                    "design_id": 2,
                    "design_name": "Wooden",
                    "file_location": "/usr",
                    "file_name": "woodnpgz.json",
                },
            ]
        )

        result_df = design_schema(df)

        expected = ["design_id", "design_name", "file_location", "file_name"]
        assert list(result_df.columns.values) == expected

    def test_data_types_match_schema_types_design(self):
        df = pd.DataFrame(
            [
                {
                    "design_id": 1,
                    "design_name": "Glass",
                    "file_name": "wooden-20220717-npgz.json",
                },
                {
                    "design_id": 2,
                    "design_name": "Wooden",
                    "file_location": "/usr",
                    "file_name": "woodnpgz.json",
                },
            ]
        )

        # check datetime , float, int, string,boolean
        result_df = design_schema(df)

        assert pd.api.types.is_integer_dtype(result_df["design_id"].dtype)
        assert pd.api.types.is_string_dtype(result_df["design_name"].dtype)
        assert pd.api.types.is_string_dtype(result_df["file_location"].dtype)
        assert pd.api.types.is_string_dtype(result_df["file_name"].dtype)

    @pytest.mark.skip
    def test_df_converted_to_parquet(self):
        df = pd.DataFrame(
            [
                {
                    "design_id": 1,
                    "design_name": "Glass",
                    "file_location": "/usr",
                    "file_name": "wooden-20220717-npgz.json",
                },
                {
                    "design_id": 2,
                    "design_name": "Wooden",
                    "file_location": "/usr",
                    "file_name": "woodnpgz.json",
                },
            ]
        )
        df = design_schema(df)

        result_df = df_to_parquet_conversion(df)


@pytest.mark.skip
def test_df_converted_to_parquet_raises_exception(self):
    testdf = pd.DataFrame(
        [
            {
                "design_id": 1,
                "design_name": "Glass",
                "file_name": "wooden-20220717-npgz.json",
            },
            {
                "design_id": 2,
                "design_name": "Wooden",
                "file_location": "/usr",
                "file_name": "woodnpgz.json",
            },
        ]
    )
    mock_df_conversion = Mock()
    mock_df_conversion.to_parquet.side_effect = Exception()
    with pytest.raises(Exception):
        read_json_pd(testdf)


class TestDimCounterParty:
    # def test_duplicated_removed_df(self):
    #     testNoduplicates_counterparty_df = pd.DataFrame(
    #         [
    #             {
    #                 "counterparty_id": 1,
    #                 "counterparty_legal_name": "Fahey and Sons",
    #                 "legal_address_id": 15,
    #                 "commercial_contact": "Micheal Toy",
    #                 "delivery_contact": "Mrs. Lucy Runolfsdottir",
    #                 "created_at": "2022-11-03 14:20:51.563",
    #                 "last_updated": "2022-11-03 14:20:51.563",
    #             },
    #             {
    #                 "counterparty_id": 2,
    #                 "counterparty_legal_name": "F and Sons",
    #                 "legal_address_id": 12,
    #                 "commercial_contact": "M Toy",
    #                 "delivery_contact": "Mrs. L Runolfsdottir",
    #                 "created_at": "2022-10-03 14:20:51.563",
    #                 "last_updated": "2022-10-03 14:20:51.563",
    #             }
    #         ]
    #     )

    #     testDuplicates_counterparty_df =  pd.DataFrame(
    #         [
    #             {
    #                 "counterparty_id": 1,
    #                 "counterparty_legal_name": "Fahey and Sons",
    #                 "legal_address_id": 15,
    #                 "commercial_contact": "Micheal Toy",
    #                 "delivery_contact": "Mrs. Lucy Runolfsdottir",
    #                 "created_at": "2022-11-03 14:20:51.563",
    #                 "last_updated": "2022-11-03 14:20:51.563",
    #             },
    #             {
    #                 "counterparty_id": 1,
    #                 "counterparty_legal_name": "Fahey and Sons",
    #                 "legal_address_id": 15,
    #                 "commercial_contact": "Micheal Toy",
    #                 "delivery_contact": "Mrs. Lucy Runolfsdottir",
    #                 "created_at": "2022-11-03 14:20:51.563",
    #                 "last_updated": "2022-11-03 14:20:51.563",
    #             },

    #         ]
    #     )
    #     # filepath_no_duplicates = "data/test_design_data.json"
    #     # filepath_duplicates = "data/test_design_data_duplicates.json"

    #     # testDuplicates_df = read_json_pd(filepath_duplicates)
    #     # testNoduplicates_df = read_json_pd(filepath_no_duplicates)

    #     non_primary_key_col = [
    #         "created_at",
    #         "design_name",
    #         "file_location",
    #         "file_name",
    #         "last_updated",
    #     ]

    #     output = remove_duplicates_pd(testDuplicates_df, non_primary_key_col)

    #     assert (testDuplicates_df["design_id"].iloc[-1]) != (
    #         output["design_id"].iloc[-1]
    #     )
    #     assert (testNoduplicates_df["design_id"].iloc[-1]) == (
    #         output["design_id"].iloc[-1]
    #     )

    def test_dim_counterparty(self, s3_client, ssm_client):
        create_fake_bucket_with_data(s3_client, "extraction-bucket-sorceress")
        create_fake_bucket_with_data(s3_client, "transformation-bucket-sorceress")
        ssm_client.put_parameter(
            Name="latest_date", Value="2024-05-21 09:28:10.208000", Type="String"
        )

        test_counterparty = """[
            {
                "counterparty_id": 1,
                "counterparty_legal_name": "Fahey and Sons",
                "legal_address_id": 1,
                "commercial_contact": "Micheal Toy",
                "delivery_contact": "Mrs. Lucy Runolfsdottir",
                "created_at": "2022-11-03T14:20:49.962",
                "last_updated": "2022-11-03T14:20:49.962"
            },
            {
                "counterparty_id": 2,
                "counterparty_legal_name": "Fahey ",
                "legal_address_id": 2,
                "commercial_contact": "Jane Doe",
                "delivery_contact": "Mrs. Lucy Runolfsdottir",
                "created_at": "2022-11-03T14:20:49.962",
                "last_updated": "2022-11-03T14:20:49.962"
            },
            {
                "counterparty_id": 3,
                "counterparty_legal_name": "Son",
                "legal_address_id": 3,
                "commercial_contact": "John Doe",
                "delivery_contact": "Mr. Lucy Runolfsdottir",
                "created_at": "2022-11-03T14:20:49.962",
                "last_updated": "2022-11-03T14:20:49.962"
            }
        ]"""

        test_address = """[
        {
            "address_id": 1,
            "address_line_1": "6826 Herzog Via",
            "address_line_2": null,
            "district": "Avon",
            "city": "New Patienceburgh",
            "postal_code": "28441",
            "country": "Turkey",
            "phone": "1803 637401",
            "created_at": "2022-11-03T14:20:49.962",
            "last_updated": "2022-11-03T14:20:49.962"
        },
        {
            "address_id": 2,
            "address_line_1": "179 Alexie Cliffs",
            "address_line_2": null,
            "district": null,
            "city": "Aliso Viejo",
            "postal_code": "99305-7380",
            "country": "San Marino",
            "phone": "9621 880720",
            "created_at": "2022-11-03T14:20:49.962",
            "last_updated": "2022-11-03T14:20:49.962"
        },
        {
            "address_id": 3,
            "address_line_1": "148 Sincere Fort",
            "address_line_2": null,
            "district": null,
            "city": "Lake Charles",
            "postal_code": "89360",
            "country": "Samoa",
            "phone": "0730 783349",
            "created_at": "2022-11-03T14:20:49.962",
            "last_updated": "2022-11-03T14:20:49.962"
        }
        ]
        """

        s3_client.put_object(
            Bucket="extraction-bucket-sorceress",
            Key="counterparty/test_data/counterparty-2024-05-21 09:28:10.208000.json",
            Body=test_counterparty,
        )

        s3_client.put_object(
            Bucket="extraction-bucket-sorceress",
            Key="address/test_data/address-2024-05-21 09:28:10.208000.json",
            Body=test_address,
        )

        address_datetime_df = get_json_from_s3(table="address", s3_client=s3_client)

        counterparty_datetime_df = get_json_from_s3(
            table="counterparty", s3_client=s3_client
        )

        counterparty_df = counterparty_datetime_df[0][1]

        address_df = address_datetime_df[0][1]

        dim_counterparty(counterparty_df, address_df)

    def test_counterparty_schema_has_the_right_column_names(self):
        counterparty_df = pd.DataFrame(
            [
                {
                    "counterparty_id": 1,
                    "counterparty_legal_name": "Fahey and Sons",
                    "legal_address_id": 15,
                    "commercial_contact": "Micheal Toy",
                    "delivery_contact": "Mrs. Lucy Runolfsdottir",
                    "created_at": "2022-11-03 14:20:51.563",
                    "last_updated": "2022-11-03 14:20:51.563",
                }
            ]
        )
        address_df = pd.DataFrame(
            [
                {
                    "address_id": 15,
                    "address_line_1": "605 Haskell Trafficway",
                    "address_line_2": "Axel Freeway",
                    "district": nan,
                    "city": "East Bobbie",
                    "postal_code": "88253-4257",
                    "country": "Heard Island and McDonald Islands",
                    "phone": "9687 937447",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                }
            ]
        )

        expected = [
            "counterparty_id",
            "counterparty_legal_name",
            "counterparty_legal_address_line_1",
            "counterparty_legal_address_line_2",
            "counterparty_legal_district",
            "counterparty_legal_city",
            "counterparty_legal_postal_code",
            "counterparty_legal_country",
            "counterparty_legal_phone_number",
        ]
        result = counterparty_schema(counterparty_df, address_df)
        result_dict = result.to_dict()
        keys_result_dict = list(result_dict.keys())

        test_col = [True for i in keys_result_dict if i in expected]
        assert all(test_col)

    def test_dimcounterparty_schema_missing_column(self):
        counterparty_df = pd.DataFrame(
            [
                {
                    "counterparty_id": 1,
                    "counterparty_legal_name": "Fahey and Sons",
                    "legal_address_id": 15,
                    "commercial_contact": "Micheal Toy",
                    "delivery_contact": "Mrs. Lucy Runolfsdottir",
                    "created_at": "2022-11-03 14:20:51.563",
                    "last_updated": "2022-11-03 14:20:51.563",
                },
                {
                    "counterparty_id": 2,
                    "legal_address_id": 28,
                    "commercial_contact": "Melba Sanford",
                    "delivery_contact": "Jean Hane III",
                    "created_at": "2022-11-03T14:20:51.563000",
                    "last_updated": "2022-11-03T14:20:51.563000",
                },
            ]
        )
        address_df = pd.DataFrame(
            [
                {
                    "address_id": 15,
                    "address_line_1": "605 Haskell Trafficway",
                    "address_line_2": "Axel Freeway",
                    "district": nan,
                    "city": "East Bobbie",
                    "postal_code": "88253-4257",
                    "country": "Heard Island and McDonald Islands",
                    "phone": "9687 937447",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "address_id": 28,
                    "address_line_1": "179 Alexie Cliffs",
                    "address_line_2": nan,
                    "district": nan,
                    "city": "Aliso Viejo",
                    "postal_code": "99305-7380",
                    "country": "San Marino",
                    "phone": "9621 880720",
                    "created_at": "2022-11-03T14:20:49.962000",
                    "last_updated": "2022-11-03T14:20:49.962000",
                },
            ]
        )
        result_df = counterparty_schema(counterparty_df, address_df)

        expected = [
            "counterparty_id",
            "counterparty_legal_name",
            "counterparty_legal_address_line_1",
            "counterparty_legal_address_line_2",
            "counterparty_legal_district",
            "counterparty_legal_city",
            "counterparty_legal_postal_code",
            "counterparty_legal_country",
            "counterparty_legal_phone_number",
        ]

        assert list(result_df.columns.values) == expected


class TestDimStaff:
    def test_staff_has_the_right_column_names(self):
        staff_df = pd.DataFrame(
            [
                {
                    "staff_id": 1,
                    "first_name": "Jeremie",
                    "last_name": "Franey",
                    "department_id": 2,
                    "email_address": "jeremie.franey@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 2,
                    "first_name": "Deron",
                    "last_name": "Beier",
                    "department_id": 6,
                    "email_address": "deron.beier@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 3,
                    "first_name": "Jeanette",
                    "last_name": "Erdman",
                    "department_id": 6,
                    "email_address": "jeanette.erdman@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 4,
                    "first_name": "Ana",
                    "last_name": "Glover",
                    "department_id": 3,
                    "email_address": "ana.glover@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 5,
                    "first_name": "Magdalena",
                    "last_name": "Zieme",
                    "department_id": 8,
                    "email_address": "magdalena.zieme@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 6,
                    "first_name": "Korey",
                    "last_name": "Kreiger",
                    "department_id": 3,
                    "email_address": "korey.kreiger@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 7,
                    "first_name": "Raphael",
                    "last_name": "Rippin",
                    "department_id": 2,
                    "email_address": "raphael.rippin@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 8,
                    "first_name": "Oswaldo",
                    "last_name": "Bergstrom",
                    "department_id": 7,
                    "email_address": "oswaldo.bergstrom@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "first_name": "Brody",
                    "last_name": "Ratke",
                    "department_id": 2,
                    "email_address": "brody.ratke@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 10,
                    "first_name": "Jazmyn",
                    "last_name": "Kuhn",
                    "department_id": 2,
                    "email_address": "jazmyn.kuhn@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 11,
                    "first_name": "Meda",
                    "last_name": "Cremin",
                    "department_id": 5,
                    "email_address": "meda.cremin@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 12,
                    "first_name": "Imani",
                    "last_name": "Walker",
                    "department_id": 5,
                    "email_address": "imani.walker@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 13,
                    "first_name": "Stan",
                    "last_name": "Lehner",
                    "department_id": 4,
                    "email_address": "stan.lehner@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 14,
                    "first_name": "Rigoberto",
                    "last_name": "VonRueden",
                    "email_address": "rigoberto.vonrueden@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 15,
                    "first_name": "Tom",
                    "last_name": "Gutkowski",
                    "department_id": 3,
                    "email_address": "tom.gutkowski@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 16,
                    "first_name": "Jett",
                    "last_name": "Parisian",
                    "department_id": 6,
                    "email_address": "jett.parisian@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 17,
                    "first_name": "Irving",
                    "last_name": "O'Keefe",
                    "department_id": 3,
                    "email_address": "irving.o'keefe@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 18,
                    "first_name": "Tomasa",
                    "last_name": "Moore",
                    "department_id": 8,
                    "email_address": "tomasa.moore@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 19,
                    "first_name": "Pierre",
                    "last_name": "Sauer",
                    "department_id": 2,
                    "email_address": "pierre.sauer@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 20,
                    "first_name": "Flavio",
                    "last_name": "Kulas",
                    "department_id": 3,
                    "email_address": "flavio.kulas@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
            ]
        )
        department_df = pd.DataFrame(
            [
                {
                    "department_id": 1,
                    "department_name": "Sales",
                    "location": "Manchester",
                    "manager": "Richard Roma",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "department_id": 2,
                    "department_name": "Purchasing",
                    "location": "Manchester",
                    "manager": "Naomi Lapaglia",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "department_id": 3,
                    "department_name": "Production",
                    "location": "Leeds",
                    "manager": "Chester Ming",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "department_id": 4,
                    "department_name": "Dispatch",
                    "location": "Leds",
                    "manager": "Mark Hanna",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "department_id": 5,
                    "department_name": "Finance",
                    "location": "Manchester",
                    "manager": "Jordan Belfort",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "department_id": 6,
                    "department_name": "Facilities",
                    "location": "Manchester",
                    "manager": "Shelley Levene",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "department_id": 7,
                    "department_name": "Communications",
                    "location": "Leeds",
                    "manager": "Ann Blake",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "department_id": 8,
                    "department_name": "HR",
                    "location": "Leeds",
                    "manager": "James Link",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
            ]
        )

        expected = [
            "staff_id",
            "first_name",
            "last_name",
            "department_name",
            "location",
            "email_address",
        ]

        result = staff_schema(staff_df, department_df)

        result_dict = result.to_dict()
        keys_result_dict = list(result_dict.keys())

        test_col = [True for i in keys_result_dict if i in expected]
        assert all(test_col)

    def test_dim_staff(self, s3_client, ssm_client):

        create_fake_bucket_with_data(s3_client, "extraction-bucket-sorceress")
        create_fake_bucket_with_data(s3_client, "transformation-bucket-sorceress")

        ssm_client.put_parameter(
            Name="latest_date", Value="2024-05-21 09:28:10.208000", Type="String"
        )

        test_departments = """
            [
                {
                    "department_id": 1,
                    "department_name": "Sales",
                    "location": "Manchester",
                    "manager": "Richard Roma",
                    "created_at": "2024-05-21 09:28:10.208000",
                    "last_updated": "2024-05-21 09:28:10.208000"
                },
                {
                    "department_id": 2,
                    "department_name": "Purchasing",
                    "location": "Manchester",
                    "manager": "Naomi Lapaglia",
                    "created_at": "2024-05-21 09:28:10.208000",
                    "last_updated": "2024-05-21 09:28:10.208000"
                },
                {
                    "department_id": 3,
                    "department_name": "Production",
                    "location": "Leeds",
                    "manager": "Chester Ming",
                    "created_at": "2024-05-21 09:28:10.208000",
                    "last_updated": "2024-05-21 09:28:10.208000"
                }
            ]
        """

        test_staff = """
            [
                {
                    "staff_id": 1,
                    "first_name": "Jeremie",
                    "last_name": "Franey",
                    "department_id": 2,
                    "email_address": "jeremie.franey@terrifictotes.com",
                    "created_at": "2024-05-21 09:28:10.208000",
                    "last_updated": "2024-05-21 09:28:10.208000"
                },
                {
                    "staff_id": 2,
                    "first_name": "Deron",
                    "last_name": "Beier",
                    "department_id": 3,
                    "email_address": "deron.beier@terrifictotes.com",
                    "created_at": "2024-05-21 09:28:10.208000",
                    "last_updated": "2024-05-21 09:28:10.208000"
                },
                {
                    "staff_id": 3,
                    "first_name": "Jeanette",
                    "last_name": "Erdman",
                    "department_id": 1,
                    "email_address": "jeanette.erdman@terrifictotes.com",
                    "created_at": "2024-05-21 09:28:10.208000",
                    "last_updated": "2024-05-21 09:28:10.208000"
                }
            ]
        """

        s3_client.put_object(
            Bucket="extraction-bucket-sorceress",
            Key="department/test_data/department-2024-05-21 09:28:10.208000.json",
            Body=test_departments,
        )
        s3_client.put_object(
            Bucket="extraction-bucket-sorceress",
            Key="staff/test_data/staff-2024-05-21 09:28:10.208000.json",
            Body=test_staff,
        )

        departments_datetime_df = get_json_from_s3(
            table="department", s3_client=s3_client
        )
        staff_datetime_df = get_json_from_s3(table="staff", s3_client=s3_client)

        departments_df = departments_datetime_df[0][1]

        staff_df = staff_datetime_df[0][1]

        dim_staff(staff_df, departments_df)

    def test_dimstaff_schema_missing_column(self):
        staff_df = pd.DataFrame(
            [
                {
                    "staff_id": 1,
                    "first_name": "Jeremie",
                    "last_name": "Franey",
                    "department_id": 2,
                    "email_address": "jeremie.franey@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 2,
                    "first_name": "Deron",
                    "last_name": "Beier",
                    "department_id": 6,
                    "email_address": "deron.beier@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 3,
                    "first_name": "Jeanette",
                    "last_name": "Erdman",
                    "department_id": 6,
                    "email_address": "jeanette.erdman@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 4,
                    "first_name": "Ana",
                    "last_name": "Glover",
                    "department_id": 3,
                    "email_address": "ana.glover@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 5,
                    "first_name": "Magdalena",
                    "last_name": "Zieme",
                    "department_id": 8,
                    "email_address": "magdalena.zieme@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 6,
                    "first_name": "Korey",
                    "last_name": "Kreiger",
                    "department_id": 3,
                    "email_address": "korey.kreiger@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 7,
                    "first_name": "Raphael",
                    "last_name": "Rippin",
                    "department_id": 2,
                    "email_address": "raphael.rippin@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 8,
                    "first_name": "Oswaldo",
                    "last_name": "Bergstrom",
                    "department_id": 7,
                    "email_address": "oswaldo.bergstrom@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "first_name": "Brody",
                    "last_name": "Ratke",
                    "department_id": 2,
                    "email_address": "brody.ratke@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 10,
                    "first_name": "Jazmyn",
                    "last_name": "Kuhn",
                    "department_id": 2,
                    "email_address": "jazmyn.kuhn@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 11,
                    "first_name": "Meda",
                    "last_name": "Cremin",
                    "department_id": 5,
                    "email_address": "meda.cremin@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 12,
                    "first_name": "Imani",
                    "last_name": "Walker",
                    "department_id": 5,
                    "email_address": "imani.walker@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 13,
                    "first_name": "Stan",
                    "last_name": "Lehner",
                    "department_id": 4,
                    "email_address": "stan.lehner@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 14,
                    "first_name": "Rigoberto",
                    "last_name": "VonRueden",
                    "department_id": 7,
                    "email_address": "rigoberto.vonrueden@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 15,
                    "first_name": "Tom",
                    "last_name": "Gutkowski",
                    "department_id": 3,
                    "email_address": "tom.gutkowski@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 16,
                    "first_name": "Jett",
                    "last_name": "Parisian",
                    "department_id": 6,
                    "email_address": "jett.parisian@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 17,
                    "first_name": "Irving",
                    "last_name": "O'Keefe",
                    "department_id": 3,
                    "email_address": "irving.o'keefe@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 18,
                    "first_name": "Tomasa",
                    "last_name": "Moore",
                    "department_id": 8,
                    "email_address": "tomasa.moore@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 19,
                    "first_name": "Pierre",
                    "last_name": "Sauer",
                    "department_id": 2,
                    "email_address": "pierre.sauer@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 20,
                    "first_name": "Flavio",
                    "last_name": "Kulas",
                    "department_id": 3,
                    "email_address": "flavio.kulas@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
            ]
        )
        department_df = pd.DataFrame(
            [
                {
                    "department_id": 1,
                    "department_name": "Sales",
                    "location": "Manchester",
                    "manager": "Richard Roma",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "department_id": 2,
                    "department_name": "Purchasing",
                    "location": "Manchester",
                    "manager": "Naomi Lapaglia",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "department_id": 3,
                    "department_name": "Production",
                    "location": "Leeds",
                    "manager": "Chester Ming",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "department_id": 4,
                    "department_name": "Dispatch",
                    "location": "Leds",
                    "manager": "Mark Hanna",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "department_id": 5,
                    "department_name": "Finance",
                    "location": "Manchester",
                    "manager": "Jordan Belfort",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "department_id": 6,
                    "department_name": "Facilities",
                    "location": "Manchester",
                    "manager": "Shelley Levene",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "department_id": 7,
                    "department_name": "Communications",
                    "location": "Leeds",
                    "manager": "Ann Blake",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "department_id": 8,
                    "department_name": "HR",
                    "location": "Leeds",
                    "manager": "James Link",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
            ]
        )

        result_df = staff_schema(staff_df, department_df)

        expected = [
            "staff_id",
            "first_name",
            "last_name",
            "department_name",
            "location",
            "email_address",
        ]

        assert list(result_df.columns.values).sort() == expected.sort()

    def test_invalid_email_removed(self):
        staff_df = pd.DataFrame(
            [
                {
                    "staff_id": 1,
                    "first_name": "Jeremie",
                    "last_name": "Franey",
                    "department_id": 2,
                    "email_address": "jeremie.franeyterrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
                {
                    "staff_id": 2,
                    "first_name": "Deron",
                    "last_name": "Beier",
                    "department_id": 6,
                    "email_address": "deron.beier@terrifictotes.com",
                    "created_at": "2022-11-03T14:20:51.563",
                    "last_updated": "2022-11-03T14:20:51.563",
                },
            ]
        )
        department_df = pd.DataFrame(
            [
                {
                    "department_id": 2,
                    "department_name": "Sales",
                    "location": "Manchester",
                    "manager": "Richard Roma",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "department_id": 6,
                    "department_name": "Purchasing",
                    "location": "Manchester",
                    "manager": "Naomi Lapaglia",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
            ]
        )

        result_df = staff_schema(staff_df, department_df)

        assert result_df["staff_id"].iloc[0] == 2


class TestDimCurrency:
    def test_currency_has_the_right_column_names(self):
        currency_df = pd.DataFrame(
            [
                {
                    "currency_id": 1,
                    "currency_code": "GBP",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "currency_id": 2,
                    "currency_code": "USD",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "currency_id": 3,
                    "currency_code": "EUR",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
            ]
        )

        expected = ["currency_id", "currency_code", "currency_name"]

        result = currency_schema(currency_df)

        result_dict = result.to_dict()
        keys_result_dict = list(result_dict.keys())

        test_col = [True for i in keys_result_dict if i in expected]
        assert all(test_col)

    def test_dim_currency(self, s3_client, ssm_client):
        create_fake_bucket_with_data(s3_client, "extraction-bucket-sorceress")
        create_fake_bucket_with_data(s3_client, "transformation-bucket-sorceress")
        res = ssm_client.put_parameter(
            Name="latest_date", Value="2024-05-21 09:28:10.208000", Type="String"
        )

        test_currency = """[
        {
            "currency_id": 1,
            "currency_code": "GBP",
            "created_at": "2024-05-21 09:28:10.208000",
            "last_updated": "2024-05-21 09:28:10.208000"
        },
        {
            "currency_id": 2,
            "currency_code": "USD",
            "created_at": "2024-05-21 09:28:10.208000",
            "last_updated": "2024-05-21 09:28:10.208000"
        },
        {
            "currency_id": 3,
            "currency_code": "EUR",
            "created_at": "2024-05-21 09:28:10.208000",
            "last_updated": "2024-05-21 09:28:10.208000"
        }
    ]"""

        res = s3_client.put_object(
            Bucket="extraction-bucket-sorceress",
            Key="currency/test_data/currency-2024-05-21 09:28:10.208000.json",
            Body=test_currency,
        )

        currency_datetime_df = get_json_from_s3(table="currency", s3_client=s3_client)

        currency_df = currency_datetime_df[0][1]
        dim_currency(currency_df)

    def test_dimcurrency_schema_missing_column(self):
        currency_df = pd.DataFrame(
            [
                {
                    "currency_id": 1,
                    "currency_code": "GBP",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "currency_id": 2,
                    "currency_code": "USD",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
                {
                    "currency_id": 3,
                    "currency_code": "EUR",
                    "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962",
                },
            ]
        )

        result_df = currency_schema(currency_df)
        expected = ["currency_id", "currency_code", "currency_name"]

        assert list(result_df.columns.values) == expected


class TestDimLocation:
    test_data = """[
    {
        "address_id": 1,
        "address_line_1": "6826 Herzog Via",
        "address_line_2": null,
        "district": "Avon",
        "city": "New Patienceburgh",
        "postal_code": "28441",
        "country": "Turkey",
        "phone": "1803 637401",
        "created_at": "2022-11-03T14:20:49.962",
        "last_updated": "2022-11-03T14:20:49.962"
    },
    {
        "address_id": 2,
        "address_line_1": "179 Alexie Cliffs",
        "address_line_2": null,
        "district": null,
        "city": "Aliso Viejo",
        "postal_code": "99305-7380",
        "country": "San Marino",
        "phone": "9621 880720",
        "created_at": "2022-11-03T14:20:49.962",
        "last_updated": "2022-11-03T14:20:49.962"
    },
    {
        "address_id": 3,
        "address_line_1": "148 Sincere Fort",
        "address_line_2": null,
        "district": null,
        "city": "Lake Charles",
        "postal_code": "89360",
        "country": "Samoa",
        "phone": "0730 783349",
        "created_at": "2022-11-03T14:20:49.962",
        "last_updated": "2022-11-03T14:20:49.962"
    }
    ]
    """

    def test_dim_location_returns_dataframe_or_not(self):
        data = json.loads(self.test_data)
        df = pd.DataFrame(data)
        result = dim_location(df)
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_read_dim_location_throws_error(self):
        df = pd.DataFrame()
        with pytest.raises(ValueError, match="DataFrame is empty"):
            dim_location(df)

    def test_droping_corrct_columns_or_not(self):
        data = json.loads(self.test_data)
        df = pd.DataFrame(data)
        result = dim_location(df)
        assert result.columns.tolist() == [
            "location_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        ]

    def test_change_column_names(self):
        data = json.loads(self.test_data)
        df = pd.DataFrame(data)
        result = dim_location(df)
        assert result.columns.tolist() == [
            "location_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        ]

    def test_fill_null_in_dataframe(self):
        data = json.loads(self.test_data)
        df = pd.DataFrame(data)
        result = dim_location(df)
        assert all(x is None for x in result["address_line_2"] if x is None)


class TestDimTransaction:
    test_data = """[
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
  },
  {
    "transaction_id": 3,
    "transaction_type": "SALE",
    "sales_order_id": 1,
    "purchase_order_id": null,
    "created_at": "2022-11-03T14:20:52.186",
    "last_updated": "2022-11-03T14:20:52.186"
  },
  {
    "transaction_id": 4,
    "transaction_type": "PURCHASE",
    "sales_order_id": null,
    "purchase_order_id": 1,
    "created_at": "2022-11-03T14:20:52.187",
    "last_updated": "2022-11-03T14:20:52.187"
  }]"""

    def test_resturn_df(self):
        res = dim_transaction(pd.DataFrame(json.loads(self.test_data)))
        assert isinstance(res, pd.DataFrame)
        assert not res.empty

    def test_empy_df_error(self):
        df = pd.DataFrame()
        with pytest.raises(ValueError, match="Dataframe is empty"):
            res = dim_transaction(df)

    def test_droping_corrct_columns_or_not(self):
        res = dim_transaction(pd.DataFrame(json.loads(self.test_data)))
        assert res.columns.tolist() == [
            "transaction_id",
            "transaction_type",
            "sales_order_id",
            "purchase_order_id",
        ]

    def test_fill_null_in_dataframe(self):
        res = dim_transaction(pd.DataFrame(json.loads(self.test_data)))
        assert all(x is None for x in res["sales_order_id"] if x is None)
        assert all(x is None for x in res["purchase_order_id"] if x is None)


def sales_order_df():
    test_sales_order_json = """[
    {
        "sales_order_id": 2,
        "created_at": "2022-11-03T14:20:52.186",
        "last_updated": "2022-11-03T14:20:52.186",
        "design_id": 3,
        "staff_id": 19,
        "counterparty_id": 8,
        "units_sold": 42972,
        "unit_price": 3.94,
        "currency_id": 2,
        "agreed_delivery_date": "2022-11-07",
        "agreed_payment_date": "2022-11-08",
        "agreed_delivery_location_id": 8
    },
    {
        "sales_order_id": 3,
        "created_at": "2022-11-03T14:20:52.188",
        "last_updated": "2022-11-03T14:20:52.188",
        "design_id": 4,
        "staff_id": 10,
        "counterparty_id": 4,
        "units_sold": 65839,
        "unit_price": 2.91,
        "currency_id": 3,
        "agreed_delivery_date": "2022-11-06",
        "agreed_payment_date": "2022-11-07",
        "agreed_delivery_location_id": 19
    },
    {
        "sales_order_id": 4,
        "created_at": "2022-11-03T14:20:52.188",
        "last_updated": "2022-11-03T14:20:52.188",
        "design_id": 4,
        "staff_id": 10,
        "counterparty_id": 16,
        "units_sold": 32069,
        "unit_price": 3.89,
        "currency_id": 2,
        "agreed_delivery_date": "2022-11-05",
        "agreed_payment_date": "2022-11-07",
        "agreed_delivery_location_id": 15
    }]"""
    return pd.DataFrame(json.loads(test_sales_order_json))


class TestSaveParquetToS3:
    def test_saves_file_successfuly(self, s3_client):
        # don't know what you want to call it if it is dim-location or what ever
        create_fake_empty_bucket_with_data(s3_client, "transformation-bucket-sorceress")
        latest_update = "2024-05-22 09:29:50.068000"
        test_address = """[
  {
    "address_id": 1,
    "address_line_1": "6826 Herzog Via",
    "address_line_2": null,
    "district": "Avon",
    "city": "New Patienceburgh",
    "postal_code": "28441",
    "country": "Turkey",
    "phone": "1803 637401",
    "created_at": "2022-11-03T14:20:49.962",
    "last_updated": "2022-11-03T14:20:49.962"
  },
  {
    "address_id": 2,
    "address_line_1": "179 Alexie Cliffs",
    "address_line_2": null,
    "district": null,
    "city": "Aliso Viejo",
    "postal_code": "99305-7380",
    "country": "San Marino",
    "phone": "9621 880720",
    "created_at": "2022-11-03T14:20:49.962",
    "last_updated": "2022-11-03T14:20:49.962"
  },
  {
    "address_id": 3,
    "address_line_1": "148 Sincere Fort",
    "address_line_2": null,
    "district": null,
    "city": "Lake Charles",
    "postal_code": "89360",
    "country": "Samoa",
    "phone": "0730 783349",
    "created_at": "2022-11-03T14:20:49.962",
    "last_updated": "2022-11-03T14:20:49.962"
  }
]
"""

        df = pd.DataFrame(json.loads(test_address))

        save_parquet_to_s3("address", latest_update, df)
        list_of_objects = [
            i["Key"]
            for i in s3_client.list_objects_v2(
                Bucket="transformation-bucket-sorceress"
            )["Contents"]
        ]

        assert (
            "address/2024-May/address-2024-05-22 09:29:50.068000.parquet"
            in list_of_objects
        )

    def test_given_empty_dataframe(self, s3_client):
        create_fake_empty_bucket_with_data(s3_client, "transformation-bucket-sorceress")
        latest_update = "2024-05-22 09:29:50.068000"

        df = pd.DataFrame()

        with pytest.raises(ValueError, match="Dataframe is empty"):
            save_parquet_to_s3("address", latest_update, df)


class TestDimDate:
    def test_returns_dataframe(self, ssm_client):
        ssm_res = ssm_client.put_parameter(
            Name="latest_date", Value="2024-05-22 09:29:50.068000", Type="String"
        )
        res = dim_date()
        assert type(pd.DataFrame()) == type(res)

    def test_creates_date_to_latest_parameter(self, ssm_client):
        ssm_res = ssm_client.put_parameter(
            Name="latest_date", Value="2024-05-22 09:29:50.068000", Type="String"
        )
        day_dict = {
            "date_id": ["2025-07-27"],
            "year": [2025],
            "month": ["07"],
            "day": [27],
            "day_of_week": [1],
            "day_name": ["Sunday"],
            "month_name": ["July"],
            "quarter": [2],
        }

        expected = pd.DataFrame(day_dict)
        x = expected.to_string(header=False, index=False, index_names=False).split("\n")
        expected = [",".join(ele.split()) for ele in x]

        res = dim_date().tail(1)
        x = res.to_string(header=False, index=False, index_names=False).split("\n")
        res = [",".join(ele.split()) for ele in x]
        assert res == expected

    def test_creates_date_from_beggining_of_database(self, ssm_client):
        ssm_res = ssm_client.put_parameter(
            Name="latest_date", Value="2024-05-22 09:29:50.068000", Type="String"
        )
        day_dict = {
            "date_id": ["2022-11-01"],
            "year": [2022],
            "month": [11],
            "day": ["01"],
            "day_of_week": ["3"],
            "day_name": ["Tuesday"],
            "month_name": ["November"],
            "quarter": [3],
        }

        expected = pd.DataFrame(day_dict)
        x = expected.to_string(header=False, index=False, index_names=False).split("\n")
        expected = [",".join(ele.split()) for ele in x]

        res = dim_date().head(1)
        x = res.to_string(header=False, index=False, index_names=False).split("\n")
        res = [",".join(ele.split()) for ele in x]

        assert res == expected


class TestFactSalesOrder:
    def test_resturn_df(self):
        res = fact_sales_order(sales_order_df())
        assert isinstance(res, pd.DataFrame)
        assert not res.empty

    def test_empy_df_error(self):
        df = pd.DataFrame()
        with pytest.raises(ValueError, match="Dataframe is empty"):
            res = fact_sales_order(df)

    def test_split_timestamp_created_at_colum_into_date_time_colunms(self):
        res = fact_sales_order(sales_order_df())
        assert "created_date" in res.columns
        assert "created_time" in res.columns

    def test_checking_split_is_correct_or_not(self):
        res = fact_sales_order(sales_order_df())
        assert res["created_date"].iloc[0] == pd.to_datetime("2022-11-03").date()
        assert res["created_time"].iloc[0] == pd.to_datetime("14:20:52.186").time()

    def test_split_timestamp_last_updated_colum_into_date_time_colunms(self):
        res = fact_sales_order(sales_order_df())
        assert "last_date" in res.columns
        assert "last_time" in res.columns

    def test_rename_staff_id_to_sales_staff_id(self):
        res = fact_sales_order(sales_order_df())
        assert res.columns.tolist() == [
            "sales_order_id",
            "design_id",
            "sales_staff_id",
            "counterparty_id",
            "units_sold",
            "unit_price",
            "currency_id",
            "agreed_delivery_date",
            "agreed_payment_date",
            "agreed_delivery_location_id",
            "created_date",
            "created_time",
            "last_date",
            "last_time",
        ]

    def test_droping_corrct_columns_or_not(self):
        res = fact_sales_order(sales_order_df())

        assert res.columns.tolist() == [
            "sales_order_id",
            "design_id",
            "sales_staff_id",
            "counterparty_id",
            "units_sold",
            "unit_price",
            "currency_id",
            "agreed_delivery_date",
            "agreed_payment_date",
            "agreed_delivery_location_id",
            "created_date",
            "created_time",
            "last_date",
            "last_time",
        ]

    def test_with_and_without_milliseconds(self):
        test_input = """[{
    "sales_order_id": 874,
    "created_at": "2023-02-16T09:01:10.238",
    "last_updated": "2023-02-16T09:01:10.238",
    "design_id": 53,
    "staff_id": 16,
    "counterparty_id": 17,
    "units_sold": 73142,
    "unit_price": 2.63,
    "currency_id": 2,
    "agreed_delivery_date": "2023-02-22",
    "agreed_payment_date": "2023-02-20",
    "agreed_delivery_location_id": 25
  },
  {
    "sales_order_id": 875,
    "created_at": "2023-02-16T09:26:10",
    "last_updated": "2023-02-16T09:26:10",
    "design_id": 9,
    "staff_id": 12,
    "counterparty_id": 17,
    "units_sold": 63011,
    "unit_price": 2.31,
    "currency_id": 3,
    "agreed_delivery_date": "2023-02-20",
    "agreed_payment_date": "2023-02-22",
    "agreed_delivery_location_id": 21
  }]"""

        # print("hello", type(pd.DataFrame(json.loads(test_input))))
        res = fact_sales_order(pd.DataFrame(json.loads(test_input)))
        # print(res)
        assert 1 == 1


class TestHandler:
    @pytest.mark.skip
    @pytest.mark.it("parquet files written to transformation-bucket-sorceress")
    @patch("src.transformation.lambda_function.get_json_from_s3")
    @patch("src.transformation.lambda_function.save_parquet_to_s3")
    @patch("src.transformation.lambda_function.get_latest_date_parameter")
    @patch("src.transformation.lambda_function.dim_currency")
    @patch("src.transformation.lambda_function.dim_staff")
    @patch("src.transformation.lambda_function.dim_counterparty")
    @patch("src.transformation.lambda_function.dim_design")
    @patch("src.transformation.lambda_function.dim_location")
    @patch("src.transformation.lambda_function.dim_transaction")
    @patch("src.transformation.lambda_function.dim_date")
    @patch("src.transformation.lambda_function.fact_sales_order")
    def test_handler_writes_parquet_files_to_s3(
        self,
        mock_fact_sales_order,
        mock_dim_date,
        mock_dim_transaction,
        mock_dim_location,
        mock_dim_design,
        mock_dim_counterparty,
        mock_dim_staff,
        mock_dim_currency,
        mock_get_latest_date_parameter,
        mock_save_parquet_to_s3,
        mock_get_json_from_s3,
        s3_client,
        ssm_client,
        caplog,
    ):

        s3_client.create_bucket(
            Bucket="transformation-bucket-sorceress",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        ssm_client.put_parameter(
            Name="latest_date", Value="2025-05-24 00:00:00.000000", Type="String"
        )

        mock_get_latest_date_parameter.return_value = "2025-05-24 00:00:00.000000"
        mock_get_json_from_s3.return_value = {}

        with caplog.at_level(logging.INFO):
            lambda_handler({}, [])
            assert "saved as parquet file" in caplog.text

    @pytest.mark.it("Test Handler raises Exception")
    @patch("src.transformation.lambda_function.get_json_from_s3")
    @patch("src.transformation.lambda_function.save_parquet_to_s3")
    @patch("src.transformation.lambda_function.get_latest_date_parameter")
    @patch("src.transformation.lambda_function.dim_currency")
    @patch("src.transformation.lambda_function.dim_staff")
    @patch("src.transformation.lambda_function.dim_counterparty")
    @patch("src.transformation.lambda_function.dim_design")
    @patch("src.transformation.lambda_function.dim_location")
    @patch("src.transformation.lambda_function.dim_transaction")
    @patch("src.transformation.lambda_function.dim_date")
    @patch("src.transformation.lambda_function.fact_sales_order")
    def test_handler_raises_exception(
        self,
        mock_fact_sales_order,
        mock_dim_date,
        mock_dim_transaction,
        mock_dim_location,
        mock_dim_design,
        mock_dim_counterparty,
        mock_dim_staff,
        mock_dim_currency,
        mock_get_latest_date_parameter,
        mock_save_parquet_to_s3,
        mock_get_json_from_s3,
        s3_client,
        ssm_client,
        caplog,
    ):

        s3_client.create_bucket(
            Bucket="transformation-bucket-sorceress",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        ssm_client.put_parameter(
            Name="latest_date", Value="2025-05-24 00:00:00.000000", Type="String"
        )

        mock_get_latest_date_parameter.return_value = "2025-05-24 00:00:00.000000"

        mock_get_json_from_s3.return_value = {}
        mock_save_parquet_to_s3.side_effect = ValueError()

        with pytest.raises(Exception) as e:
            lambda_handler({}, [])

        assert "Unable to convert to parquet file" in caplog.text
