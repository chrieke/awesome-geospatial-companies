"""
Converts the companies Google sheet to the markdown formatting of the repository readme.
Due to the manual process of the information gathering on the companies & complex markdown formatting in the github
readme, the company data is managed in a Google sheet. This script automatically converts and formats it.

Add "--check-urls" to check for broken company website URLs.
"""

from typing import List
import time
import requests
from requests.exceptions import RequestException
import argparse
import concurrent.futures
from dataclasses import dataclass

import pandas as pd
from tqdm import tqdm

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

    ok_codes = {200, 201, 202, 203, 301, 302, 303, 307, 308}
    warn_codes = {403, 429}
    retry_delays = [2, 5]

    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"
    elif url.startswith("http://"):
        url = url.replace("http://", "https://", 1)

    def _request(method="HEAD", verify=True):
        return requests.request(
            method, url, headers=headers, timeout=10, allow_redirects=True, verify=verify
        )

    last_error = None
    for attempt in range(len(retry_delays) + 1):
        if attempt > 0:
            time.sleep(retry_delays[attempt - 1])

        try:
            # Try HEAD first, fall back to GET on 405
            response = _request("HEAD")
            if response.status_code == 405:
                response = _request("GET")

            if response.status_code in ok_codes:
                return URLCheckResult(
                    url=url, status="OK", status_code=response.status_code
                )
            elif response.status_code in warn_codes:
                # For 429, respect Retry-After and retry
                if response.status_code == 429 and attempt < len(retry_delays):
                    retry_after = int(response.headers.get("Retry-After", retry_delays[attempt]))
                    time.sleep(max(0, retry_after - retry_delays[attempt]))
                    continue
                return URLCheckResult(
                    url=url,
                    status="WARN",
                    status_code=response.status_code,
                    error_message=f"Status Code: {response.status_code}",
                )
            else:
                last_error = URLCheckResult(
                    url=url,
                    status="ERROR",
                    status_code=response.status_code,
                    error_message=f"Status Code: {response.status_code}",
                )

        except requests.exceptions.SSLError as e:
            return URLCheckResult(
                url=url, status="WARN", error_message=f"SSL error: {e}"
            )

        except (requests.ConnectTimeout, requests.ConnectionError) as e:
            last_error = URLCheckResult(
                url=url, status="ERROR", error_message=str(e)
            )

        except RequestException as e:
            return URLCheckResult(url=url, status="SKIP", error_message=str(e))

    return last_error


def check_urls(urls: List[str]):
    results = []

    # Use ThreadPoolExecutor for parallel processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {executor.submit(check_single_url, url): url for url in urls}

        for future in tqdm(
            concurrent.futures.as_completed(future_to_url), total=len(urls)
        ):
            results.append(future.result())

    df_results = pd.DataFrame(
        [
            {
                "URL": r.url,
                "Status": r.status,
                "Status Code": r.status_code,
                "Error": r.error_message,
            }
            for r in results
        ]
    )

    # Print summary
    print("\nURL Check Summary:")
    print("-----------------")
    print(f"Total URLs checked: {len(df_results)}")
    print(f"Successful: {len(df_results[df_results['Status'] == 'OK'])}")
    print(f"Warnings: {len(df_results[df_results['Status'] == 'WARN'])}")
    print(f"Errors: {len(df_results[df_results['Status'] == 'ERROR'])}")
    print(f"Skipped: {len(df_results[df_results['Status'] == 'SKIP'])}")

    # Print warnings
    df_warn = df_results[df_results["Status"] == "WARN"]
    if len(df_warn) > 0:
        print("\nWarnings (site exists but may be blocking or has SSL issues):")
        print(df_warn.to_string(index=False))

    # Print failed URLs
    df_failed = df_results[df_results["Status"].isin(["ERROR", "SKIP"])]
    if len(df_failed) > 0:
        print("\nFailed URLs:")
        print(df_failed.to_string(index=False))


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
print(f"Unique companies: {df['Company'].nunique()}")

df = df.drop(columns=["Notes (ex-name)"])
df = df.rename(columns={"Unnamed: 1": "New"})

# Check for NaN values (excluding "New" column)
check_cols = [c for c in df.columns if c != "New"]
has_na = False
for column in check_cols:
    na_mask = df[column].isnull()
    if na_mask.any():
        has_na = True
        na_rows = df.loc[na_mask]
        print(f"\n⚠ Empty values in column '{column}' ({na_mask.sum()} rows):")
        for idx, row in na_rows.iterrows():
            company = row.get("Company", f"row {idx}")
            print(f"  Row {idx}: {company}")
if has_na:
    raise ValueError("Table contains empty values, see details above.")

if args.check_urls:
    check_urls(urls=df["Website"].values)

df = format_table(df=df)
df = df[["Company", "Size & City", "Focus", "Country"]]


chapter_links, markdown_string = table_to_markdown(df)
with open("Output.md", "w") as text_file:
    text_file.write(chapter_links + "\n\n" + markdown_string)
