import polars as pl
from io import BytesIO
import requests
from requests import RequestException
import re
import logging


# Config Logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def get_gsheet(url: str) -> bytes:
    """
    Makes a GET request to the specified URL and returns the content if successful.

    Args:
        url (str): The URL to send the GET request to.

    Returns:
        bytes: The content of the response, if the request is successful.
    """
    if not isinstance(url, str):
        logging.error("Input URL should be a string")
        return None
    
    logging.info(f'Starting request to {url}')
    try:
        logging.debug('Sending GET request')
        r = requests.get(url, timeout=10)

        logging.debug(f'Checking response status {r.status_code}')
        r.raise_for_status()
        return r.content
    except RequestException as e:
        logging.error(f'Error in response: {e}', exc_info=True)
    finally:
        logging.info(f'Finished request to {url}')

print(get_gsheet('wwww.seialsedlasldas.com'))

def convert_url(url: str) -> str | None:
    """
    Convert a Google Sheets URL to a downloadable URL.

    Applies regex to the input URL, extracts the spreadsheet_id and gid, 
    and then constructs a new URL for downloading the CSV version of the sheet.

    Args:
        url (str): The original Google Sheets URL used to access the sheet in the browser.

    Returns:
        str: A downloadable URL to make requests or None if the URL is invalid.
    """
    logging.info('Starting convert_url')
    if not isinstance(url, str):
        logging.error("Input URL should be a string")
        return None

    try:
        logging.debug("Applying regex to find matches")
        pattern = r"https:\/\/docs\.google\.com\/spreadsheets\/d\/([a-zA-Z0-9_-]+)\/edit\?gid=([0-9]+)"
        match = re.search(pattern, url)
        
        if match:
            logging.debug("Found matches for spreadsheet_id and gid")
            spreadsheet_id, gid = match.group(1), match.group(2)

            logging.debug("Checking if gid is valid")
            if gid.isdigit():
                logging.debug(f"Current values of spreadsheet_id, gid: {spreadsheet_id, gid}")
                return f'https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}'
            else:
                logging.error("gid is not valid. It should be a digit.")
                return None
        else:
            logging.error("URL doesn't match required pattern")
            return None
    except Exception:
        logging.exception(f"The following exception occurred while converting the URL")
        return None


vendas_url = convert_url("https://docs.google.com/spreadsheets/d/1c0xzgYouNi507q5bq6lGXKQJ4v67eHPn4VMoDuDx2-g/edit?gid=1750971763#gid=1750971763")
estoque_url = convert_url("https://docs.google.com/spreadsheets/d/1c0xzgYouNi507q5bq6lGXKQJ4v67eHPn4VMoDuDx2-g/edit?gid=0#gid=0")

df_vendas = pl.read_csv(BytesIO(get_gsheet(vendas_url)))
df_estoque = pl.read_csv(BytesIO(get_gsheet(estoque_url)))

print(df_vendas)
print(df_estoque)
