import pandas as pd
import awswrangler
import pg8000
import boto3

s3_client = boto3.client("s3", region_name="eu-west-2")

# con = pg8000.connect(
#     "filipe", database="fake_facts_sales_order", password="mysecretword123"
# )


def db_con():
    user = "project_team_6"
    password = "5RcdUsiYSLcFKvA"
    host = "nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com"
    port = 5432  # Default PostgreSQL port
    database = "postgres"

    con = pg8000.connect(
        user=user, password=password, host=host, port=port, database=database
    )
    return con


def get_paquet_from(table):
    transformation_bucket_res = s3_client.list_objects_v2(
        Bucket="transformation-bucket-sorceress",
        Prefix=table,
    )

    transformation_bucket = [
        object["Key"] for object in transformation_bucket_res["Contents"]
    ]
    return transformation_bucket


def get_data_frame_from_parquet(path):
    if "dim_transaction/" in path:
        df = awswrangler.s3.read_parquet(
            path=f"s3://transformation-bucket-sorceress/{path}"
        )
        df.fillna(0, inplace=True)
        df["sales_order_id"] = df["sales_order_id"].astype("int32")
        df["purchase_order_id"] = df["purchase_order_id"].astype("int32")
        return df
    return awswrangler.s3.read_parquet(
        path=f"s3://transformation-bucket-sorceress/{path}"
    )


def get_used_file(s3_client=s3_client):
    return (
        s3_client.get_object(
            Bucket="warehouse-bucket-sorceress", Key="used_parquet_files.txt"
        )["Body"]
        .read()
        .decode("utf-8")
    )


def save_used_file(content, s3_client=s3_client):
    s3_client.put_object(
        Bucket="warehouse-bucket-sorceress", Key="used_parquet_files.txt", Body=content
    )


def dim_transaction_query():
    return """
    INSERT INTO dim_transaction (transaction_id, transaction_type, sales_order_id, purchase_order_id)
    VALUES (%s, %s, %s, %s)"""


def dim_date_query():
    return """
     INSERT INTO dim_date (date_id, year, month, day, day_of_week, day_name, month_name, quarter)
     VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""


def dim_counterparty_query():
    return """
     INSERT INTO dim_counterparty (counterparty_id, counterparty_legal_name, counterparty_legal_address_line_1, counterparty_legal_address_line_2, counterparty_legal_district, counterparty_legal_city, counterparty_legal_postal_code, counterparty_legal_country, counterparty_legal_phone_number)
     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""


def dim_currency_query():
    return """
     INSERT INTO dim_currency (currency_id, currency_code, currency_name)
     VALUES (%s, %s, %s)"""


def dim_design_query():
    return """
     INSERT INTO dim_design (design_id, design_name, file_location, file_name)
     VALUES (%s, %s, %s, %s)"""


def dim_location_query():
    return """
     INSERT INTO dim_location (location_id, address_line_1, address_line_2, district, city, postal_code, country, phone )
     VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""


def dim_staff_query():
    return """
     INSERT INTO dim_staff (staff_id, first_name, last_name, email_address, department_name, location)
     VALUES (%s, %s, %s, %s, %s, %s)"""


def put_df_into_warehouse(df, query, con):
    cursor = con.cursor()
    values = [tuple(row) for row in df.to_numpy()]

    chunck_size = 100000

    for i in range(0, len(values), chunck_size):
        chunck = values[i : i + chunck_size]
        try:
            cursor.executemany(query, chunck)
        # except pg8000.errors.IntegrityError as e:
        #     print(f"Integrity error in chunk starting at {i}: {e}")
        except Exception as e:
            print(f"Unexpected error in chunk starting at {i}: {e}")
    con.commit()
    cursor.close()
    con.close()


def facts_sales_order_query():
    return """
    INSERT INTO fact_sales_order (sales_order_id, design_id, sales_staff_id, counterparty_id, units_sold, unit_price, currency_id, agreed_delivery_date, agreed_payment_date, agreed_delivery_location_id, created_date, created_time, last_updated_date, last_updated_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    # loop for the fact table


# print([i for i in get_paquet_from("facts") if i not in content])


# fact_sales_order_insert_query = """
#     INSERT INTO fact_sales_order (sales_order_id, design_id, sales_staff_id, counterparty_id, units_sold, unit_price, currency_id, agreed_delivery_date, agreed_payment_date, agreed_delivery_location_id, created_date, created_time, last_updated_date, last_updated_time)
#     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""


def lambda_handler(event, context):

    content = get_used_file()

    files_to_add_to_the_warehouse = [
        i for i in get_paquet_from("dim") if i not in content
    ]

    for i in files_to_add_to_the_warehouse:
        if "dim_transaction/" in i:
            query = dim_transaction_query()
            print(i)
        elif "dim_date/" in i:
            query = dim_date_query()
        elif "dim_counterparty/" in i:
            query = dim_counterparty_query()
        elif "dim_currency/" in i:
            query = dim_currency_query()
        elif "dim_design/" in i:
            query = dim_design_query()
        elif "dim_location/" in i:
            query = dim_location_query()
        elif "dim_staff/" in i:
            query = dim_staff_query()

    con = db_con()
    df = get_data_frame_from_parquet(i)
    put_df_into_warehouse(df, query, con)
    content += f"\n{i}"
    save_used_file(content)

    for i in [i for i in get_paquet_from("facts") if i not in content]:
        if "facts_sales_order/" in i:
            query = facts_sales_order_query()

        con = db_con()
        df = get_data_frame_from_parquet(i)
        put_df_into_warehouse(df, query, con)
        content += f"\n{i}"
        save_used_file(content)
