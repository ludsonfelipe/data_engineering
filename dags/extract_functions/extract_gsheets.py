import polars as pl  # main
from io import BytesIO  # main
import requests
from requests import RequestException
import re
import logging  # main


# Configurar o logger
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
handler.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)


def convert_gsheet_hyperlink_to_downloadable_url(url: str) -> str:
    """
    Convert a Google Sheets URL to a downloadable URL.

    Applies regex to the input URL, extracts the spreadsheet_id and gid,
    and then constructs a new URL for downloading the CSV version of the sheet.

    Args:
        url (str): The original Google Sheets URL used to access the sheet in the browser.

    Returns:
        str: A downloadable URL to make requests or None if the URL is invalid.
    """
    try:
        logger.info(f"Starting conversion of Google Sheets URL: {url}")

        pattern = r"https:\/\/docs\.google\.com\/spreadsheets\/d\/([a-zA-Z0-9_-]+)\/edit\?gid=([0-9]+)"
        match = re.search(pattern, url)

        if match:
            spreadsheet_id, gid = match.groups()
            download_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
            logger.info(f"Successfully converted URL to: {download_url}")
            return download_url
        else:
            logger.error("URL format is incorrect for the regex pattern")
            raise ValueError("URL format is incorrect")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise


def read_an_url_and_return_content(url: str) -> bytes | None:
    """
    Makes a GET request to the specified URL and returns the content if successful.

    Args:
        url (str): The URL to send the GET request to.

    Returns:
        bytes: The content of the response, if the request is successful.
    """
    try:
        logger.info(f"Starting GET request to URL: {url}")
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        logger.info(f"Successfully retrieved content from URL: {url}")
        return r.content
    except RequestException:
        logger.error(f"Request failed for URL: {url} with exception: ", exc_info=True)
        return None
    finally:
        logger.info(f"Finished processing URL: {url}")


# vendas_url = convert_gsheet_hyperlink_to_downloadable_url(
#     "https://docs.google.com/spreadsheets/d/1c0xzgYouNi507q5bq6lGXKQJ4v67eHPn4VMoDuDx2-g/edit?gid=1750971763#gid=1750971763"
# )
# estoque_url = convert_gsheet_hyperlink_to_downloadable_url(
#     "https://docs.google.com/spreadsheets/d/1c0xzgYouNi507q5bq6lGXKQJ4v67eHPn4VMoDuDx2-g/edit?gid=0#gid=0"
# )

# df_vendas = pl.read_csv(BytesIO(get_gsheet(vendas_url)))
# df_estoque = pl.read_csv(BytesIO(get_gsheet(estoque_url)))

# print(df_vendas)
# print(df_estoque)
