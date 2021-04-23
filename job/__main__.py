"""
Main module of the monthly-insta-job project.

"""

import logging

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


if __name__ == "__main__":
    logger.info("STARTING JOB")
    ig_user_id = 17841411237972805
    base = "https://graph.facebook.com/v9.0"
    user_node = f"/{ig_user_id}"
    access_token = ""
    username = "vinicius.py"
    logger.info(f"GETTING DATA FROM INSTA USERNAME: {username}")
    data_lake_bucket_name = "insta-vags-datalake-dev"
    config = {}
    date_str = "2021-01-01"

    logger.info(f"REQUESTING LIFETIME METRICS ...")
    lifetime_account_metrics = get_lifetime_account_metrics(
        base, user_node, access_token
    )
    logger.info(lifetime_account_metrics)
    lifetime_account_metrics_df = lifetime_account_metrics
    lifetime_account_metrics_bucket_name = f"{data_lake_bucket_name}/LifetimeMetrics"
    write_to_s3(
        lifetime_account_metrics_df,
        format_folder_path(lifetime_account_metrics_bucket_name, date_str, logger),
        config,
        logger,
    )

    logger.info("END OF JOB")
