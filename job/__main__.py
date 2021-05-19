"""
Main module of the monthly-insta-job project.

"""

import configparser
import logging
import os
from datetime import datetime
from pprint import pprint

import pandas as pd
from ig_helpers import format_folder_path, get_lifetime_account_metrics, write_to_s3
from rich.logging import RichHandler
from rich.traceback import install

FORMAT = "[%(levelname)s] [%(message)s]"
logging.basicConfig(
    level=logging.INFO,
    format=FORMAT,
    datefmt="[%Y-%m-%d %H:%M:%S]",
    handlers=[RichHandler(show_level=False, show_path=False)],
)
install()
logger = logging.getLogger("main")


def process_lifetime_account_metrics(lifetime_metric: dict) -> pd.DataFrame:
    data = {
        lifetime_metric["name"].split("_", 1)[-1]: list(
            lifetime_metric["values"][0]["value"].keys()
        ),
        "amount": list(lifetime_metric["values"][0]["value"].values()),
    }
    return pd.DataFrame.from_dict(data)


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.realpath(".."), "setup.cfg"))

    logger.info("STARTING JOB")
    ig_user_id = config["GRAPH_API"]["IG_USER_ID"]
    base = config["GRAPH_API"]["BASE"]
    username = config["GRAPH_API"]["USERNAME"]
    access_token = config["GRAPH_API"]["ACCESS_TOKEN"]
    data_lake_bucket_name = config["S3"]["DATALAKE_BUCKET_NAME"]
    user_node = f"/{ig_user_id}"

    logger.info(f"GETTING DATA FROM INSTA: @{username}")
    date_str = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"REQUESTING LIFETIME METRICS ...")
    table_names = [
        "AudienceCity",
        "AudienceCountry",
        "AudienceGenderAge",
        "AudienceLocale",
    ]
    lifetime_account_metrics = get_lifetime_account_metrics(
        base, user_node, access_token
    )
    for idx, metric in enumerate(lifetime_account_metrics["data"]):
        if idx < len(table_names):
            lifetime_account_metrics_df = process_lifetime_account_metrics(metric)
            account_info_bucket_name = f"{data_lake_bucket_name}/{table_names[idx]}"
            write_to_s3(
                lifetime_account_metrics_df,
                format_folder_path(account_info_bucket_name, date_str, logger),
                config["AWS"],
                logger,
            )

    logger.info("END OF JOB")
