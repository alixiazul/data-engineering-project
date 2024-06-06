import pandas as pd
import logging
import sys
import re
import json
import boto3
from datetime import datetime, timedelta
import awswrangler

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3", region_name="eu-west-2")
ssm_client = boto3.client("ssm", region_name="eu-west-2")

# ---dimension design----------
file_path = "data/test_design_data.json"
pd.set_option("display.max_columns", None)

# -----get parameter


def get_latest_date_parameter(ssm_client=ssm_client):
    parameter_latest_date = ssm_client.get_parameter(Name="latest_date")
    latest_date_parameter = parameter_latest_date["Parameter"]["Value"]
    return latest_date_parameter


# ------new json fx


def get_json_from_s3(table: str, s3_client=s3_client) -> list:
    extraction_bucket = s3_client.list_objects_v2(
        Bucket="extraction-bucket-sorceress",
        Prefix=table,
    )
    if "Contents" in extraction_bucket:
        extraction_bucket = [object["Key"] for object in extraction_bucket["Contents"]]

    transformation_bucket = s3_client.list_objects_v2(
        Bucket="transformation-bucket-sorceress", Prefix=table
    )
    if "Contents" in transformation_bucket:
        transformation_bucket = [
            object["Key"] for object in transformation_bucket["Contents"]
        ]

    convert = {
        "address": "dim_location",
        "counterparty": "dim_counterparty",
        "currency": "dim_currency",
        "department": "dim_staff",
        "design": "dim_design",
        "payment_type": "",
        "payment": "",
        "purchase_order": "",
        "sales_order": "facts_sales_order",
        "staff": "dim_staff",
        "transaction": "",
    }

    if table == "payment":
        extraction_bucket = [i for i in extraction_bucket if "payment_type" not in i]
        transformation_bucket = [
            i for i in transformation_bucket if "payment_type" not in i
        ]

    json_to_parquet = [
        [
            extract_ojb[-31:-5],
            pd.DataFrame(
                json.load(
                    s3_client.get_object(
                        Bucket="extraction-bucket-sorceress", Key=extract_ojb
                    )["Body"]
                )
            ),
        ]
        for extract_ojb in extraction_bucket
        if extract_ojb.replace(table, convert[table]).replace("json", "parquet")
        not in transformation_bucket
    ]

    if json_to_parquet:
        logging.info(f"There is new data in {table}")
        return json_to_parquet
    logging.info(f"There is no new data in {table}")
    return []


# -------remove duplicate rows
def remove_duplicates_pd(df: pd.DataFrame, non_primary_key_col: list):
    df = df.drop_duplicates(subset=non_primary_key_col)
    return df


# -------remove null values (not using)
def remove_null_values(df, col):
    df = df.dropna(subset=[col])
    return df


# -----DIMENSION TABLE: DESIGN


def design_schema(df):
    dim_design_schema = {
        "design_id": pd.Int64Dtype(),
        "design_name": pd.StringDtype(),
        "file_location": pd.StringDtype(),
        "file_name": pd.StringDtype(),
    }

    dim_design_df = pd.DataFrame(
        {
            column: pd.Series(dtype=column_type)
            for column, column_type in dim_design_schema.items()
        }
    )

    dim_design_df = pd.concat([dim_design_df, df], ignore_index="True")
    dim_design_df = dim_design_df.dropna()
    return dim_design_df


def dim_design(df):
    non_primary_key_col = [
        "created_at",
        "design_name",
        "file_location",
        "file_name",
        "last_updated",
    ]

    df = remove_duplicates_pd(df, non_primary_key_col)
    df = design_schema(df)

    return df


# -----DIMENSION TABLE: COUNTERPARTY


def counterparty_schema(counterparty_df: pd.DataFrame, address_df: pd.DataFrame):

    df = pd.merge(
        counterparty_df,
        address_df,
        how="left",
        left_on="legal_address_id",
        right_on="address_id",
    )

    # df = remove_null_values(df, "counterparty_id")

    df = df.drop(
        [
            "legal_address_id",
            "address_id",
            "created_at_x",
            "created_at_y",
            "last_updated_x",
            "last_updated_y",
            "commercial_contact",
            "delivery_contact",
        ],
        axis=1,
    )
    df = df.rename(
        columns={
            "address_line_1": "counterparty_legal_address_line_1",
            "address_line_2": "counterparty_legal_address_line_2",
            "district": "counterparty_legal_district",
            "city": "counterparty_legal_city",
            "postal_code": "counterparty_legal_postal_code",
            "country": "counterparty_legal_country",
            "phone": "counterparty_legal_phone_number",
        }
    )
    df = df.dropna(
        subset=[
            "counterparty_id",
            "counterparty_legal_name",
            "counterparty_legal_address_line_1",
            "counterparty_legal_city",
            "counterparty_legal_postal_code",
            "counterparty_legal_country",
            "counterparty_legal_phone_number",
        ],
        how="any",
    )

    return df


def dim_counterparty(counterparty_df, address_df):
    non_primary_key_col_counterparty = [
        "counterparty_legal_name",
        "legal_address_id",
        "commercial_contact",
        "delivery_contact",
        "created_at",
        "last_updated",
    ]
    non_primary_key_col_address = [
        "address_line_1",
        "address_line_2",
        "district",
        "city",
        "postal_code",
        "country",
        "phone",
        "created_at",
        "last_updated",
    ]

    counterparty_df = remove_duplicates_pd(
        counterparty_df, non_primary_key_col_counterparty
    )
    address_df = remove_duplicates_pd(address_df, non_primary_key_col_address)

    df = counterparty_schema(counterparty_df, address_df)

    return df


# -----DIMENSION TABLE: STAFF
def staff_schema(staff_df: pd.DataFrame, department_df: pd.DataFrame):
    df = pd.merge(
        staff_df,
        department_df,
        how="left",
        on="department_id",
    )

    df = df.drop(
        [
            "created_at_x",
            "created_at_y",
            "last_updated_x",
            "last_updated_y",
            "department_id",
            "manager",
        ],
        axis=1,
    )
    df = df.dropna(
        subset=[
            "staff_id",
            "first_name",
            "last_name",
            "department_name",
            "location",
            "email_address",
        ],
        how="any",
    )

    # Define a regular expression for email validation
    email_regex = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"

    # Function to validate email using regex
    def is_valid_email(email):
        return bool(re.match(email_regex, email)) if pd.notnull(email) else False

    # Apply email validation with vectorized approach (efficient)
    df = df[df["email_address"].apply(is_valid_email)]
    return df


def dim_staff(staff_df, departments_df):
    non_primary_key_col_staff = [
        "first_name",
        "last_name",
        "department_id",
        "email_address",
        "created_at",
        "last_updated",
    ]
    non_primary_key_col_department = [
        "department_name",
        "location",
        "manager",
        "created_at",
        "last_updated",
    ]

    staff_df = remove_duplicates_pd(staff_df, non_primary_key_col_staff)
    department_df = remove_duplicates_pd(departments_df, non_primary_key_col_department)

    df = staff_schema(staff_df, department_df)

    return df


# -----DIMENSION TABLE: CURRENCY
def currency_schema(currency_df: pd.DataFrame):
    df = currency_df.drop(
        ["created_at", "last_updated"],
        axis=1,
    )

    df["currency_name"] = ["pound sterling", "united states dollar", "euro"]

    df = df.dropna(subset=["currency_id", "currency_code", "currency_name"], how="any")

    return df


def dim_currency(df):
    non_primary_key_col = ["currency_code", "created_at"]

    df = remove_duplicates_pd(df, non_primary_key_col)
    df = currency_schema(df)

    return df


# -----DIMENSION TABLE: DATE
def dim_date():
    # if there is a file in dim date do not run this code

    day_dict = {
        "date_id": [],
        "year": [],
        "month": [],
        "day": [],
        "day_of_week": [],
        "day_name": [],
        "month_name": [],
        "quarter": [],
    }

    # 2022-11-01
    start_date = datetime(2022, 11, 1)
    # end_date = datateime_str
    # days_to_create = (end_date - start_date).days

    for i in range(1000):
        date = start_date + timedelta(days=i)
        day_dict["date_id"].append(
            datetime(
                int(date.strftime("%Y")),
                int(date.strftime("%m")),
                int(date.strftime("%d")),
            )
        )
        day_dict["year"].append(date.strftime("%Y"))
        day_dict["month"].append(date.strftime("%m"))
        day_dict["day"].append(date.strftime("%d"))
        day_dict["day_of_week"].append(int(date.strftime("%w")) + 1)
        day_dict["day_name"].append(date.strftime("%A"))
        day_dict["month_name"].append(date.strftime("%B"))
        day_dict["quarter"].append((int(date.strftime("%m")) - 1) // 3 + 1)

    dim_date_data = pd.DataFrame(day_dict)

    return dim_date_data


#     # transformation-bucket-sorceress


def dim_location(df):
    try:
        if df.empty:
            raise ValueError("DataFrame is empty")
        else:
            df = df.drop(["last_updated", "created_at"], axis=1)
            df = df.rename(columns={"address_id": "location_id"})
            df["address_line_2"].fillna("None")
            return df
    except Exception as e:
        logging.error(f"Error processing DataFrame: {e}")
        raise e


def dim_transaction(df):
    try:
        if df.empty:
            raise ValueError("Dataframe is empty")
        else:
            df = df.drop(["created_at", "last_updated"], axis=1)
            df["sales_order_id"].fillna("None")
            df["purchase_order_id"].fillna("None")
            return df
    except Exception as e:
        logging.error("Error processing in dataframe")
        raise e


def standardize_timestamp(ts):
    if "." not in ts:
        ts += ".000"
    return ts


def fact_sales_order(df):
    try:
        if df.empty:
            raise ValueError("Dataframe is empty")
        else:

            df["created_at"] = df["created_at"].apply(standardize_timestamp)
            df["last_updated"] = df["last_updated"].apply(standardize_timestamp)
            # this need to be turned into date time to use dt.time/dt.date
            df["created_at"] = pd.to_datetime(df["created_at"])
            df["created_date"] = pd.to_datetime(df["created_at"]).dt.date
            df["created_time"] = pd.to_datetime(df["created_at"]).dt.time
            df["last_updated"] = pd.to_datetime(df["last_updated"])
            df["last_date"] = pd.to_datetime(df["last_updated"]).dt.date
            df["last_time"] = pd.to_datetime(df["last_updated"]).dt.time
            df = df.rename(columns={"staff_id": "sales_staff_id"})
            df = df.drop(["last_updated", "created_at"], axis=1)
            return df
    except Exception as e:
        logging.error("Error processing in dataframe")
        raise e


def save_parquet_to_s3(table_name: str, latest_update: str, df: pd.DataFrame):
    if df.empty:
        raise ValueError("Dataframe is empty")
    date_str = datetime.strptime(latest_update, "%Y-%m-%d %H:%M:%S.%f")
    file_name = (
        f"s3://transformation-bucket-sorceress/{table_name}/"
        f"{date_str.year}-{date_str.strftime('%B')}/"
        f"{table_name}-{date_str}.parquet"
    )

    awswrangler.s3.to_parquet(
        df=df,
        path=file_name,
    )
    logging.info(
        f"File: {table_name}-{date_str}.parquet has been created successfully."
    )


def lambda_handler(event, context):
    try:
        # currency
        for latest_update, currency_df_json in get_json_from_s3("currency"):
            currency_df = dim_currency(currency_df_json)
            save_parquet_to_s3("dim_currency", latest_update, currency_df)

        # staff
        for staff_department_df_json in [
            [
                get_json_from_s3("staff"),
                get_json_from_s3("department"),
            ]
        ]:

            staff_df = dim_staff(
                staff_department_df_json[0][0][1], staff_department_df_json[1][0][1]
            )
            save_parquet_to_s3("dim_staff", staff_department_df_json[0][0][0], staff_df)

        # counterparty
        for counterparty_address_df_json in [
            [
                get_json_from_s3("counterparty"),
                get_json_from_s3("address"),
            ]
        ]:
            counterparty_df = dim_counterparty(
                counterparty_address_df_json[0][0][1],
                counterparty_address_df_json[1][0][1],
            )
            save_parquet_to_s3(
                "dim_counterparty",
                counterparty_address_df_json[0][0][0],
                counterparty_df,
            )

        # design
        for latest_update, design_df_json in get_json_from_s3("design"):
            design_df = dim_design(design_df_json)
            save_parquet_to_s3("dim_design", latest_update, design_df)

        # location
        for latest_update, location_df_json in get_json_from_s3("address"):
            location_df = dim_location(location_df_json)
            save_parquet_to_s3("dim_location", latest_update, location_df)

        # transaction
        for latest_update, transaction_df_json in get_json_from_s3("transaction"):
            transaction_df = dim_transaction(transaction_df_json)
            save_parquet_to_s3("dim_transaction", latest_update, transaction_df)

        # date
        date_df = dim_date()
        save_parquet_to_s3(
            "dim_date", str(dim_date()["date_id"].iloc[-1]) + ".000000", date_df
        )

        # sales
        for latest_update, sales_order_df_json in get_json_from_s3("sales_order"):
            sales_order_df = fact_sales_order(sales_order_df_json)
            save_parquet_to_s3("facts_sales_order", latest_update, sales_order_df)

    except Exception as e:
        logging.error(f"Unable to convert to parquet file: {e}", exc_info=True)
        raise e


lambda_handler(1, 2)
