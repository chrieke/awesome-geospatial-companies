"""
Converts the companies Google sheet to the markdown formatting of the repository readme.
Due to the manual process of the information gathering on the companies & complex markdown formatting in the github
readme, the company data is managed in a Google sheet. This script automatically converts and formats it.

Add "--check-urls" to check for broken company website URLs.
"""

from typing import List
import requests
from requests.exceptions import RequestException
import argparse
import urllib3
import concurrent.futures
from dataclasses import dataclass

import pandas as pd
from tqdm import tqdm

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

parser = argparse.ArgumentParser(
    description="Convert the csv to markdown, optionally check the website urls via --check-urls"
)
parser.add_argument(
    "--check-urls",
    default=False,
    action="store_true",
    help="Check the company website urls.",
)
args = parser.parse_args()


@dataclass
class URLCheckResult:
    url: str
    status: str
    status_code: int = None
    error_message: str = None

def check_single_url(url: str) -> URLCheckResult:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    
    acceptable_codes = {200, 201, 202, 203, 301, 302, 303, 307, 308, 403, 429}
    
    if not url.startswith(('http://', 'https://')):
        url = f'https://{url}'
        
    try:
        response = requests.get(
            url, 
            headers=headers, 
            timeout=10,
            allow_redirects=True,
            verify=False
        )
        
        if response.status_code in acceptable_codes:
            return URLCheckResult(url=url, status="OK", status_code=response.status_code)
        else:
            return URLCheckResult(
                url=url, 
                status="ERROR", 
                status_code=response.status_code,
                error_message=f"Status Code: {response.status_code}"
            )
                
    except RequestException as e:
        if "ConnectTimeout" in str(e) or "ConnectionError" in str(e):
            return URLCheckResult(url=url, status="ERROR", error_message=str(e))
        return URLCheckResult(url=url, status="SKIP", error_message=str(e))

def check_urls(urls: List[str]):
    results = []
    
    # Use ThreadPoolExecutor for parallel processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {executor.submit(check_single_url, url): url for url in urls}
        
        for future in tqdm(concurrent.futures.as_completed(future_to_url), total=len(urls)):
            results.append(future.result())
    
    df_results = pd.DataFrame([
        {
            'URL': r.url,
            'Status': r.status,
            'Status Code': r.status_code,
            'Error': r.error_message
        } for r in results
    ])
    
    # Print summary
    print("\nURL Check Summary:")
    print("-----------------")
    print(f"Total URLs checked: {len(df_results)}")
    print(f"Successful: {len(df_results[df_results['Status'] == 'OK'])}")
    print(f"Errors: {len(df_results[df_results['Status'] == 'ERROR'])}")
    print(f"Skipped: {len(df_results[df_results['Status'] == 'SKIP'])}")
    
    # Print failed URLs
    if len(df_results[df_results['Status'] != 'OK']) > 0:
        print("\nFailed URLs:")
        print(df_results[df_results['Status'] != 'OK'].to_string(index=False))


def format_table(df):
    categories = {
        "Earth Observation": "🛰️",
        "GIS / Spatial": "🌎",
        "Climate": "☁️",
        "UAV / Aerial": "✈️",
        "Digital Farming": "🌿",
        "Webmap / Cartography": "🗺️",
        "Satellite Operator": "📡",
    }
    df = df.replace({"Category": categories})
    df["Company"] = df.apply(
        lambda x: f"[{x['Company']}]({x['Website']}){' ❗' if pd.notna(x['New']) else ''}",
        axis=1,
    )
    df["Focus"] = df["Category"] + " " + df["Focus"]

    gmaps_url = "https://www.google.com/maps/search/"
    df["Address"] = df.apply(
        lambda x: "".join(y + "+" for y in x["Address"].split(" ")), axis=1
    )
    df["Address"] = df.apply(
        lambda x: f"[📍 {x['City']}]({gmaps_url}{x['Address']})", axis=1
    )

    df["Size & City"] = df.apply(
        lambda x: f"**{x['Office Size'][0]}**{x['Office Size'][1:]} {x['Address']}",
        axis=1,
    )

    return df


def table_to_markdown(df):
    """
    Formatted pandas dataframe to markdown table string as in github Readme.
    """
    chapter_links = ""
    markdown_string = ""
    for country in sorted(df.Country.unique()):
        df_country = df[df["Country"] == country]
        df_country = df_country.drop(["Country"], axis=1)

        country_emoji = {
            "china": "cn",
            "france": "fr",
            "germany": "de",
            "italy": "it",
            "south_korea": "kr",
            "spain": "es",
            "turkey": "tr",
            "uae": "united_arab_emirates",
            "usa": "us",
            "russia": "ru",
            "japan": "jp",
            "bosnia and herzegovina": "bosnia_herzegovina",
        }
        flag_emoji = country.lower()
        flag_emoji = flag_emoji.replace(" ", "_")
        if flag_emoji in list(country_emoji.keys()):
            flag_emoji = country_emoji[flag_emoji]

        repo_link = "https://github.com/chrieke/awesome-geospatial-companies#"
        chapter_link = f"[:{flag_emoji}: {country}]({repo_link}{flag_emoji}-{country.lower().replace(' ', '-')})"
        chapter_links += f"{chapter_link} - "

        df_country = (
            df_country.groupby(["Company", "Focus"])["Size & City"]
            .apply(" <br /> ".join)
            .reset_index()
        )
        df_country = df_country[["Company", "Size & City", "Focus"]]
        df_country = df_country.rename(
            {"Company": f"Company ({df_country.shape[0]})"}, axis=1
        )

        markdown_string = (
            markdown_string
            + f"## :{flag_emoji}: {country} \n"
            + f"{df_country.to_markdown(index=False)} \n\n "
        )

    return chapter_links, markdown_string


df = pd.read_csv("awesome-geospatial-companies - Companies A-Z.csv")
print(f"Unique companies: {df['Focus'].nunique()}")

df = df.drop(["Notes (ex-name)"], axis=1)
# Print column name & row index of nan values
if df.loc[:, df.columns != "New"].isnull().values.any():
    for column in df.columns[df.columns != "New"]:
        if df[column].isnull().any():
            na_rows = df[column][df[column].isnull()].index.tolist()
            print(f"Column '{column}' has NaN at rows: {na_rows}")
    raise ValueError("Table contains NA values!!!")

if args.check_urls:
    check_urls(urls=df["Website"].values)

df = format_table(df=df)
df = df[["Company", "Size & City", "Focus", "Country"]]


chapter_links, markdown_string = table_to_markdown(df)
with open("Output.md", "w") as text_file:
    text_file.write(chapter_links + "\n\n" + markdown_string)
