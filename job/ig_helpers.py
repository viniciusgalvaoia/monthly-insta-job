from datetime import datetime

import awswrangler as wr
import boto3
import pandas as pd
import requests


# Account audience_city,audience_country,audience_gender_age,audience_locale,online_followers
def get_lifetime_account_metrics(base, node, access_token):
    parameters = (
        f"/insights?metric=audience_city,audience_country,audience_gender_age,audience_locale,online_followers"
        f"&period=lifetime"
        f"&access_token={access_token}"
    )
    url = build_url(base, node, parameters)
    return request_data(url)


def get_token():
    token = ""
    return token


def build_url(base, node, parameters):
    return f"{base}{node}{parameters}"


def request_data(url):
    response = requests.get(url)
    data = response.json()
    return data


def format_folder_path(table_path: str, date: str, logger) -> str:
    """
        Formats the folder path, adding the year and month and returns the formatted folder path.

        Args:
            table_path (str): Query without year and month values.
            date (str): Date with year and month values to be extracted and included in the folder path.
        Returns:
            The formatted folder path, with year and month included.
        Raises:
            UnboundLocalError: in case that the date passed as a parameter is None
        Examples:
            ```python
            from job import __main__ as main
            main.format_folder_path("s3://pd-datalake-us-east-2-dev/bi_databases/MaterialRevenue", "2021-01-01")
            > 's3://pd-datalake-us-east-2-dev/bi_databases/MaterialRevenue/year=2021/month=1'
            ```
        """
    try:
        dt = datetime.strptime(date, "%Y-%m-%d")
    except ValueError as e:
        logger.info(e)
    return f"{table_path}/year={dt.year}/month={dt.month}/day={dt.day}"


def write_to_s3(df: pd.DataFrame, s3_path: str, local_config: dict, logger) -> dict:
    """
        Sends the passed dataframe to a specific bucket at s3.

        Args:
            df (pd.DataFrame): Pandas dataframe to save.
            s3_path (str): Folder path where the table will be saved.
            local_config (dict): Config dict to connect to athena and get S3 path.
        Returns:
            Dictionary with: ‘paths’: List of all stored files paths on S3.
                             ‘partitions_values’: Dictionary of partitions added with keys as S3 path locations
                              and values as a list of partitions values as str.
        Examples:
            ```python
            from job import __main__ as main
            main.write_to_s3(material_revenue_df,
                             "s3://pd-datalake-us-east-2-dev/bi_databases/MaterialRevenue",
                             config)
            ```
        """
    logger.info(f"SAVING RESULTS AT {s3_path}")
    boto3_session = boto3.Session(
        aws_access_key_id=local_config.get("AWS_ACCESS_KEY_ID_DATA_API"),
        aws_secret_access_key=local_config.get("AWS_SECRET_ACCESS_KEY_DATA_API"),
    )

    return wr.s3.to_parquet(
        df=df,
        path=s3_path,
        dtype={"Revenue": "decimal(5,2)"},  # used to force type of Revenue column
        mode="overwrite",
        dataset=True,
        boto3_session=boto3_session,
    )
