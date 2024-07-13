from dataclasses import dataclass
from time import sleep
import feedparser
from parse_forms import parse_345
from bs4 import BeautifulSoup
import requests
from typing import Optional
import pandas as pd
from consts import user_agent, accepted_forms


def fetch_feed(form_type: str):
    url = feed_params(form_type=form_type, count=40)
    print(url)
    rss_dict = feedparser.parse(
        url,
        agent=user_agent,
    )
    if not isinstance(rss_dict, dict):
        print("Invalid rss feed")
        return
    entries = rss_dict["entries"]
    if not isinstance(entries, list):
        print("Rss feed missing entries")
        return
    print(len(entries))
    sleep(10)
    for entry in entries:
        title = entry["title"]
        starts_with_form = False
        for form in accepted_forms:
            if title.startswith(form + " "):
                starts_with_form = True
        if not starts_with_form:
            continue
        base_link = entry["link"]

        if base_link is None:
            continue
        link_request = requests.get(
            base_link, headers={"User-Agent": user_agent}
        ).content
        sleep(10)
        cleaned_request = BeautifulSoup(link_request, "html.parser")
        files_table = cleaned_request.find("table", class_="tableFile")
        if files_table is None:
            continue
        rows = files_table.find_all("tr")
        if len(rows) < 2:
            continue
        for row in rows[1:]:
            tds = row.find_all("td")
            if tds[3].string.startswith(form_type):
                # try:
                link = "https://www.sec.gov/" + tds[2].find("a").get("href")
                res = parse_345(link, form_type)
                with pd.option_context(
                    "display.max_rows", None, "display.max_columns", None
                ):
                    print(res)

                # except:
                #    print("failed")
                # break


def feed_params(
    form_type: Optional[str] = None,
    cik: Optional[str] = None,
    company: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    count: Optional[int] = None,
    start: Optional[int] = 0,
) -> str:
    return (
        "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK="
        + (cik if cik is not None else "")
        + "&type="
        + (form_type if form_type is not None else "")
        + "&company="
        + (company if company is not None else "")
        + ("&datea=" + start_date if start_date is not None else "")
        + ("&dateb=" + end_date if end_date is not None else "")
        + "&start="
        + (str(start) if start is not None else "")
        + "&count="
        + (str(count) if count is not None else "")
        + "&owner=include"
        + "&output=atom"
    )


@dataclass
class StockAction:
    title: str
    updated: str
