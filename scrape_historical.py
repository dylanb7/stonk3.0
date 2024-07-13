import json
import os
from consts import accepted_forms
from parse_forms import parse_345
from time import sleep
import pandas as pd


def scrape_historical():
    dirname = os.path.dirname(__file__)

    filename = os.path.join(dirname, "../submissions")

    iters = 0
    with os.scandir(filename) as entries:
        for entry in entries:
            iters += 1
            if entry.is_file and "submissions" not in entry.name:
                file = open(entry.path)
                json_file = json.load(file)
                if (
                    "filings" not in json_file
                    or "cik" not in json_file
                    or "recent" not in json_file["filings"]
                ):
                    continue
                entity_cik = json_file["cik"]
                filings_data = json_file["filings"]["recent"]
                if (
                    "form" not in filings_data
                    or "accessionNumber" not in filings_data
                    or "primaryDocument" not in filings_data
                ):
                    continue
                filed_forms = filings_data["form"]
                accession_numbers = filings_data["accessionNumber"]
                primary_documents = filings_data["primaryDocument"]
                for i in range(len(filed_forms)):
                    form = filed_forms[i]
                    if form not in accepted_forms:
                        continue
                    accession_number = accession_numbers[i].replace("-", "")
                    primary_document = primary_documents[i]
                    scrape_path = (
                        "https://www.sec.gov/Archives/edgar/data/"
                        + entity_cik
                        + "/"
                        + accession_number
                        + "/"
                        + primary_document
                    )
                    print("form" + form + " | path: " + scrape_path, end="\n\n")
                    res = parse_345(scrape_path, form)
                    sleep(10)
                    with pd.option_context(
                        "display.max_rows", None, "display.max_columns", None
                    ):
                        print(res)
                    print("\n\n\n")

            if iters > 2:
                break
