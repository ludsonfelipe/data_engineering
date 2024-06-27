import polars as pl
from io import BytesIO
import requests
from requests import RequestException
import re
import logging

# Config Logging
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def get_gsheet(url: str) -> bytes:
    """
    Makes a GET request to the specified URL and returns the content if successful.

    Args:
        url (str): The URL to send the GET request to.

    Returns:
        bytes: The content of the response, if the request is successful.
    """
    logging.info(f'Starting request to {url}')
    try:
        logging.info('Sending GET request')
        r = requests.get(url, timeout=10)

        logging.info('Checking response status')
        r.raise_for_status()
        return r.content
    except RequestException as e:
        logging.error(f'Error in response: {e}', exc_info=True)
    finally:
        logging.info(f'Finished request to {url}')

def convert_url(url):
    pattern = r"https:\/\/docs\.google\.com\/spreadsheets\/d\/([a-zA-Z0-9_-]+)\/edit\?gid=([0-9]+)"

    match = re.search(pattern, url)
    try:
        if match:
            spreadsheet_id = match.group(1)
            gid = match.group(2)
            return f'https://docs.google.com/spreadsheet/ccc?key={spreadsheet_id}&output=csv&gid={gid}'
    except logging.exception() as e:
        return logging.error("not found")
    

vendas_url = convert_url("https://docs.google.com/spreadsheets/d/1c0xzgYouNi507q5bq6lGXKQJ4v67eHPn4VMoDuDx2-g/edit?gid=1750971763#gid=1750971763")
estoque_url = convert_url("https://docs.google.com/spreadsheets/d/1c0xzgYouNi507q5bq6lGXKQJ4v67eHPn4VMoDuDx2-g/edit?gid=0#gid=0")

df_vendas = pl.read_csv(BytesIO(get_gsheet(vendas_url)))
df_estoque = pl.read_csv(BytesIO(get_gsheet(estoque_url)))

print(df_vendas)
print(df_estoque)

exit()
