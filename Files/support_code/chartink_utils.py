# chartink_utils.py

import requests
import pandas as pd
from bs4 import BeautifulSoup
import re

Charting_Link = "https://chartink.com/screener/"
Charting_url = 'https://chartink.com/screener/process'

class ChartinkFetchError(Exception):
    """Custom exception to handle errors during fetching data from Chartink."""
    pass


def fix_chartink_condition(text):
    """
    Fix common formatting issues in Chartink conditions copied from the website.
    - Removes extra spaces before parentheses.
    - Reduces multiple spaces to a single space.
    """
    text = re.sub(r'\s+\(', '(', text)  # remove space before '('
    text = re.sub(r'\s{2,}', ' ', text)  # replace multiple spaces with single space
    return text.strip()


def get_data_from_chartink(payload):
    """
    Fetches data from Chartink using the provided scan clause.
    Automatically fixes condition formatting before sending.
    """
    payload = fix_chartink_condition(payload)
    payload = {'scan_clause': payload}

    with requests.Session() as s:
        r = s.get(Charting_Link)
        soup = BeautifulSoup(r.text, "html.parser")
        csrf = soup.select_one("[name='csrf-token']")['content']
        s.headers['x-csrf-token'] = csrf
        r = s.post(Charting_url, data=payload)

        if r.status_code != 200:
            raise ChartinkFetchError(f"Failed to fetch data, status code: {r.status_code}")
        
        try:
            items = r.json()['data']
            df = pd.DataFrame(items)
        except (KeyError, ValueError) as e:
            raise ChartinkFetchError(f"Error processing response data: {e}")
        
    return df
