import boto3
import json
import pandas as pd

s3_client = boto3.client("s3", region_name="eu-west-2")


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

    for i in json_to_parquet:
        print(type(i[0]), type(i[1]))

    if json_to_parquet:
        logging.info(f"There is new data in {table}")
        return json_to_parquet
    logging.info(f"There is no new data in {table}")
    return []
    # json_to_parquet = []
    # for extract_ojb in extraction_bucket:
    #     if (
    #         extract_ojb.replace(table, convert[table]).replace("json", "parquet")
    #         not in transformation_bucket
    #     ):
    #         res = s3_client.get_object(
    #             Bucket="extraction-bucket-sorceress", Key=extract_ojb
    #         )
    #         # print(json.load(res["Body"]))
    #         json_to_parquet.append(
    #             [extract_ojb[-31:-5], pd.DataFrame(json.load(res["Body"]))]
    #         )


get_json_from_s3("payment", s3_client)
