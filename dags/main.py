import polars as pl
from io import BytesIO
import requests
import re
import logging


def get_gsheet(url: str) -> bytes:
    r = requests.get(url)
    return r.content

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

vendas_schema = {
    "Produto": pl.String,
    "Data": pl.String,
    "Sigla Loja": pl.String,
    "Venda": pl.String,
}

df_vendas = pl.read_csv(BytesIO(get_gsheet(vendas_url)))
print(df_vendas.to_pandas())

df_estoque = pl.read_csv(BytesIO(get_gsheet(estoque_url)))
print(df_estoque.to_pandas())

convert_url("assf")

exit()
