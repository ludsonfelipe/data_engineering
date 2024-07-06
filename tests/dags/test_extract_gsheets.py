import pytest
import re
from dags.extract_functions.extract_gsheets import convert_url, get_gsheet


@pytest.mark.parametrize(
    ("url,new_url"),
    [
        (
            "https://docs.google.com/spreadsheets/d/1c0xzgYouNi507q5bq6lGXKQJ4v67eHPn4VMoDuDx2-g/edit?gid=1750971763#gid=1750971763",
            "https://docs.google.com/spreadsheets/d/1c0xzgYouNi507q5bq6lGXKQJ4v67eHPn4VMoDuDx2-g/export?format=csv&gid=1750971763",
        ),
        (
            "https://docs.google.com/spreadsheets/d/1c0xzgYouNi507q5bq6lGXKQJ4v67eHPn4VMoDuDx2-g/edit?gid=0#gid=0",
            "https://docs.google.com/spreadsheets/d/1c0xzgYouNi507q5bq6lGXKQJ4v67eHPn4VMoDuDx2-g/export?format=csv&gid=0",
        ),
    ],
)
def test_convert_url_valid(url, new_url):
    result = convert_url(url)
    assert result == new_url


def test_convert_url_invalid_pattern():
    url = "www.example.com"
    result = convert_url(url)
    assert result is None
