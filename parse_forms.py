import pandas as pd
from bs4 import BeautifulSoup
import requests
import re

from consts import user_agent


def parse_345(link, form):
    link_request = requests.get(link, headers={"User-Agent": user_agent})
    file_type = link_request.headers["content-type"]
    parsed_type = file_type.split(";")[0].split("/")[1]
    if parsed_type == "html":
        return parse_html(link_request.content, link, form)
    elif parsed_type == "xml":
        return parse_xml(link_request.content, link)
    print("unsupported format")


def parse_html(content: bytes, origin: str, form: str):

    def get_identity(identity_table):

        info_list = list()

        rows = identity_table.find_all("tr", recursive=False)
        if rows is None or len(rows) == 0:
            return pd.DataFrame()
        identity_cik_tds = rows[0].find_all("td", recursive=False)

        rpt_owner_field = identity_cik_tds[0].select_one("table > tr > td > a")
        if rpt_owner_field is not None:
            owner_cik = rpt_owner_field["href"].split("CIK=", 1)[1].strip()
            owner_name = rpt_owner_field.string.strip()
            info_list.append(["executiveCIK", owner_cik])
            info_list.append(["executiveName", owner_name])

        issuer_name_and_ticker = (
            identity_cik_tds[2] if form.startswith("3") else identity_cik_tds[1]
        )
        if issuer_name_and_ticker is not None:
            issuer_a_tag = issuer_name_and_ticker.find("a")
            if issuer_a_tag is not None:
                info_list.append(["firmName", issuer_a_tag.string.strip()])
                info_list.append(
                    ["firmCIK", issuer_a_tag["href"].split("CIK=", 1)[1].strip()]
                )
            issuer_ticker = issuer_name_and_ticker.find("span", class_="FormData")
            if issuer_ticker is not None:
                info_list.append(["firmTicker", issuer_ticker.string.strip()])

        report_period_td = (
            identity_cik_tds[1] if form.startswith("3") else rows[1].find("td")
        )
        if report_period_td is not None:
            date_span = report_period_td.find("span", class_="FormData")
            if date_span is not None:
                info_list.append(["periodOfReport", date_span.string.strip()])

        relationship_td = (
            rows[1].find("td") if form.startswith("3") else identity_cik_tds[2]
        )
        if relationship_td is not None:
            relationship_body = relationship_td.select_one(
                "table:nth-child(4) > tbody:nth-child(1)"
            )
            print(relationship_body)
            if relationship_body is not None:
                relationship_rows = relationship_body.find_all("tr")
                director_owner_tds = relationship_rows[0].find_all("td")
                if len(director_owner_tds) >= 4:
                    info_list.append(
                        ["isDirector", len(director_owner_tds[0].string.strip()) > 0]
                    )
                    info_list.append(
                        [
                            "isTenPercentOwner",
                            len(director_owner_tds[2].string.strip()) > 0,
                        ]
                    )
                officer_other_tds = relationship_rows[1].find_all("td")
                if len(officer_other_tds) >= 4:
                    info_list.append(
                        ["isOfficer", len(officer_other_tds[0].string.strip()) > 0]
                    )
                    info_list.append(
                        [
                            "isOther",
                            len(officer_other_tds[2].string.strip()) > 0,
                        ]
                    )
                position_descriptors = relationship_rows[2].find_all("td")
                if len(position_descriptors) >= 4:
                    officer_title = position_descriptors[1].string.strip()
                    if len(officer_title) > 0:
                        info_list.append(
                            [
                                "officerTitle",
                                officer_title,
                            ]
                        )
                    other_text = position_descriptors[3].string.strip()
                    if len(other_text) > 0:
                        info_list.append(
                            [
                                "otherText",
                                other_text,
                            ]
                        )

        date_of_original_submission = (
            rows[1].find_all("td")[1]
            if form.startswith("3")
            else rows[2].find_all("td")[0]
        )
        if date_of_original_submission is not None:
            field = date_of_original_submission.find("span", class_="FormData")
            if field is not None:
                info_list.append(
                    [
                        "dateOfOriginalSubmission",
                        field.string.strip(),
                    ]
                )

        dataDict = dict()
        for item in info_list:
            dataDict[item[0]] = item[1]

        data = pd.DataFrame.from_dict([dataDict])
        return data

    def get_transaction_row(row, derivative: bool):
        skip_code = "skip_me"

        def get_ordered_fields():
            if form.startswith("3"):
                if not derivative:
                    return [
                        "securityTitle",
                        "transactionShares",
                        "directOrIndirectOwnership",
                        "natureOfOwnership",
                    ]
                return [
                    "securityTitle",
                    "exerciseDate",
                    "expirationDate",
                    "underlyingSecurityTitle",
                    "underlyingSecurityShares",
                    "transactionPricePerShare",
                    "directOrIndirectOwnership",
                    "natureOfOwnership",
                ]
            elif form.startswith("4"):
                if not derivative:
                    return [
                        "securityTitle",
                        "transactionDate",
                        "exerciseDate",
                        "transactionCode",
                        "transactionFormType",
                        "transactionShares",
                        "transactionAcquiredDisposedCode",
                        "transactionPricePerShare",
                        "sharesOwnedFollowingTransaction",
                        "directOrIndirectOwnership",
                        "natureOfOwnership",
                    ]
                return [
                    "securityTitle",
                    "conversionOrExercisePrice",
                    "transactionDate",
                    skip_code,
                    "transactionCode",
                    "transactionFormType",
                    "securitiesAquired",
                    "securitiesDisposed",
                    "exerciseDate",
                    "expirationDate",
                    "underlyingSecurityTitle",
                    "underlyingSecurityShares",
                    "transactionPricePerShare",
                    "sharesOwnedFollowingTransaction",
                    "directOrIndirectOwnership",
                    "natureOfOwnership",
                ]
            return []

        ordered_fields = get_ordered_fields()

        info_transaction = list()
        tds = row.find_all("td", recursive=False)

        if len(tds) < len(ordered_fields):
            return []

        for i in range(len(ordered_fields)):
            field = ordered_fields[i]
            if field == skip_code:
                continue
            data = tds[i].find(
                "span",
                class_=(
                    "SmallFormData"
                    if form.startswith("4") and derivative
                    else "FormData"
                ),
            )
            if data is not None and len(data.string) > 0:
                info_transaction.append([field, data.string.strip()])

        return info_transaction

    def get_non_derivative_table(table):
        info_non_table = list()
        body = table.find("tbody")
        if body is None:
            return pd.DataFrame()
        table_rows = body.find_all("tr")
        for transaction in table_rows:
            transactionDict = dict()
            infoTransaction = get_transaction_row(transaction, derivative=False)

            for item in infoTransaction:
                transactionDict[item[0]] = item[1]

            transactionDict["table"] = "I: Non-Derivative Securities"
            info_non_table.append(pd.DataFrame.from_dict([transactionDict]))
        if len(info_non_table) > 0:
            data = pd.concat(info_non_table, sort=False, ignore_index=True)
        else:
            data = pd.DataFrame()
        return data

    def get_derivative_table(table):
        body = table.find("tbody")
        if body is None:
            return pd.DataFrame()
        table_rows = body.find_all("tr")
        info_table = list()
        for transaction in table_rows:
            infoTransaction = get_transaction_row(transaction, derivative=True)
            transactionDict = dict()

            for item in infoTransaction:
                transactionDict[item[0]] = item[1]

            transactionDict["table"] = "II: Derivative Securities"
            info_table.append(pd.DataFrame.from_dict([transactionDict]))

        if len(info_table) > 0:
            data = pd.concat(info_table, sort=False, ignore_index=True)
        else:
            data = pd.DataFrame()

        return data

    cleaned_page = BeautifulSoup(content, "html.parser")

    relevent_tables = cleaned_page.html.body.find_all("table", recursive=False)[1:-1]
    identity_data = get_identity(relevent_tables[0])
    data_non_table = get_non_derivative_table(relevent_tables[1])
    data_derivative_table = get_derivative_table(relevent_tables[2])
    identity_data["edgar.link"] = origin
    data_non_table["edgar.link"] = origin
    data_derivative_table["edgar.link"] = origin

    data = pd.concat(
        [data_non_table, data_derivative_table], sort=False, ignore_index=True
    )
    with pd.option_context("display.max_rows", None, "display.max_columns", None):
        print(identity_data)
        print(data)
    data = pd.merge(data, identity_data, on="edgar.link")

    footnotes = relevent_tables[-1].find_all("td", class_="FootnoteData")

    if footnotes is not None:
        footnotes_text = [note.string for note in footnotes]
        data["footnotes"] = "\n".join(footnotes_text)

    return data


def parse_xml(content: bytes, origin: str):

    def clean_text_xml(file):
        page = file.split("\\n")
        cleanedPage = ""
        counter = 0
        documentStart = re.compile("\<XML\>")
        documentEnd = re.compile("\<\/XML\>")
        for line in page:
            if counter == 0:
                if documentStart.search(line) is not None:
                    counter = counter + 1
                else:
                    continue
            else:
                if documentEnd.search(line) is not None:
                    counter = 0
                else:
                    cleanedPage = cleanedPage + line + " "

        cleanedPage = BeautifulSoup(cleanedPage, "xml")
        return cleanedPage

    def get_identity(text):

        info_list = list()

        issuer = text.find("issuer")
        if issuer is None:
            return pd.DataFrame()
        if issuer.find("issuerCik") is not None:
            info_list.append(["firmCIK", issuer.find("issuerCik").text.strip()])
        if issuer.find("issuerName") is not None:
            info_list.append(["firmName", issuer.find("issuerName").text.strip()])
        if issuer.find("issuerTradingSymbol") is not None:
            info_list.append(
                ["firmTicker", issuer.find("issuerTradingSymbol").text.strip()]
            )

        owner = text.find("reportingOwnerId")
        if owner.find("rptOwnerCik") is not None:
            info_list.append(["executiveCIK", owner.find("rptOwnerCik").text.strip()])
        if owner.find("rptOwnerName") is not None:
            info_list.append(["executiveName", owner.find("rptOwnerName").text.strip()])

        relationship = text.find("reportingOwnerRelationship")
        if relationship.find("isDirector") is not None:
            info_list.append(
                ["isDirector", relationship.find("isDirector").text.strip()]
            )
        if relationship.find("isOfficer") is not None:
            info_list.append(["isOfficer", relationship.find("isOfficer").text.strip()])
        if relationship.find("isTenPercentOwner") is not None:
            info_list.append(
                [
                    "isTenPercentOwner",
                    relationship.find("isTenPercentOwner").text.strip(),
                ]
            )
        if relationship.find("isOther") is not None:
            info_list.append(["isOther", relationship.find("isOther").text.strip()])
        if relationship.find("officerTitle") is not None:
            info_list.append(
                ["officerTitle", relationship.find("officerTitle").text.strip()]
            )
        if relationship.find("otherText") is not None:
            info_list.append(["otherText", relationship.find("otherText").text.strip()])

        if text.find("periodOfReport") is not None:
            info_list.append(
                ["periodOfReport", text.find("periodOfReport").text.strip()]
            )

        if text.find("dateOfOriginalSubmission") is not None:
            info_list.append(
                [
                    "dateOfOriginalSubmission",
                    text.find("dateOfOriginalSubmission").text.strip(),
                ]
            )

        dataDict = dict()
        for item in info_list:
            dataDict[item[0]] = item[1]

        data = pd.DataFrame.from_dict([dataDict])
        return data

    def get_transaction_row(transaction):
        info_transaction = list()

        if transaction.find("securityTitle") is not None:
            info_transaction.append(
                ["securityTitle", transaction.find("securityTitle").text.strip()]
            )
        if transaction.find("transactionDate") is not None:
            info_transaction.append(
                ["transactionDate", transaction.find("transactionDate").text.strip()]
            )
        if transaction.find("conversionOrExercisePrice") is not None:
            info_transaction.append(
                [
                    "conversionOrExercisePrice",
                    transaction.find("conversionOrExercisePrice").text.strip(),
                ]
            )

        if transaction.find("transactionCoding") is not None:
            trnsctnCoding = transaction.find("transactionCoding")
            if trnsctnCoding.find("transactionFormType") is not None:
                info_transaction.append(
                    [
                        "transactionFormType",
                        trnsctnCoding.find("transactionFormType").text.strip(),
                    ]
                )
            if trnsctnCoding.find("transactionCode") is not None:
                info_transaction.append(
                    [
                        "transactionCode",
                        trnsctnCoding.find("transactionCode").text.strip(),
                    ]
                )
            if trnsctnCoding.find("equitySwapInvolved") is not None:
                info_transaction.append(
                    [
                        "equitySwapInvolved",
                        trnsctnCoding.find("equitySwapInvolved").text.strip(),
                    ]
                )

        if transaction.find("transactionAmounts") is not None:
            transaction_amounts = transaction.find("transactionAmounts")
            if transaction_amounts.find("transactionShares") is not None:
                info_transaction.append(
                    [
                        "transactionShares",
                        transaction_amounts.find("transactionShares").text.strip(),
                    ]
                )
            if transaction_amounts.find("transactionPricePerShare") is not None:
                info_transaction.append(
                    [
                        "transactionPricePerShare",
                        transaction_amounts.find(
                            "transactionPricePerShare"
                        ).text.strip(),
                    ]
                )
            if transaction_amounts.find("transactionAcquiredDisposedCode") is not None:
                info_transaction.append(
                    [
                        "transactionAcquiredDisposedCode",
                        transaction_amounts.find(
                            "transactionAcquiredDisposedCode"
                        ).text.strip(),
                    ]
                )

        if transaction.find("exerciseDate") is not None:
            info_transaction.append(
                ["exerciseDate", transaction.find("exerciseDate").text.strip()]
            )
        if transaction.find("expirationDate") is not None:
            info_transaction.append(
                ["expirationDate", transaction.find("expirationDate").text.strip()]
            )

        if transaction.find("underlyingSecurity") is not None:
            transaction_underlying = transaction.find("underlyingSecurity")
            if transaction_underlying.find("underlyingSecurityTitle") is not None:
                info_transaction.append(
                    [
                        "underlyingSecurityTitle",
                        transaction_underlying.find(
                            "underlyingSecurityTitle"
                        ).text.strip(),
                    ]
                )
            if transaction_underlying.find("underlyingSecurityShares") is not None:
                info_transaction.append(
                    [
                        "underlyingSecurityShares",
                        transaction_underlying.find(
                            "underlyingSecurityShares"
                        ).text.strip(),
                    ]
                )

        if transaction.find("sharesOwnedFollowingTransaction") is not None:
            info_transaction.append(
                [
                    "sharesOwnedFollowingTransaction",
                    transaction.find("sharesOwnedFollowingTransaction").text.strip(),
                ]
            )
        if transaction.find("directOrIndirectOwnership") is not None:
            info_transaction.append(
                [
                    "directOrIndirectOwnership",
                    transaction.find("directOrIndirectOwnership").text.strip(),
                ]
            )
        if transaction.find("natureOfOwnership") is not None:
            info_transaction.append(
                [
                    "natureOfOwnership",
                    transaction.find("natureOfOwnership").text.strip(),
                ]
            )

        return info_transaction

    def get_non_derivative_table(text):
        table_non_derivative = text.find_all(
            re.compile(r"nonDerivativeTransaction|nonDerivativeHolding")
        )
        info_non_table = list()
        for transaction in table_non_derivative:
            transactionDict = dict()
            infoTransaction = get_transaction_row(transaction)

            for item in infoTransaction:
                transactionDict[item[0]] = item[1]

            transactionDict["table"] = "I: Non-Derivative Securities"
            info_non_table.append(pd.DataFrame.from_dict([transactionDict]))
        if len(info_non_table) > 0:
            data = pd.concat(info_non_table, sort=False, ignore_index=True)
        else:
            data = pd.DataFrame()
        return data

    def get_derivative_table(text):
        table_derivative = text.find_all(
            re.compile(r"derivativeTransaction|derivativeHolding")
        )
        info_table = list()
        for transaction in table_derivative:
            infoTransaction = get_transaction_row(transaction)
            transactionDict = dict()

            for item in infoTransaction:
                transactionDict[item[0]] = item[1]

            transactionDict["table"] = "II: Derivative Securities"
            info_table.append(pd.DataFrame.from_dict([transactionDict]))

        if len(info_table) > 0:
            data = pd.concat(info_table, sort=False, ignore_index=True)
        else:
            data = pd.DataFrame()

        return data

    link_request = str(content)
    text = clean_text_xml(link_request)
    identity_data = get_identity(text)
    data_non_table = get_non_derivative_table(text)
    data_derivative_table = get_derivative_table(text)
    identity_data["edgar.link"] = origin
    data_non_table["edgar.link"] = origin
    data_derivative_table["edgar.link"] = origin

    data = pd.concat(
        [data_non_table, data_derivative_table], sort=False, ignore_index=True
    )
    data = pd.merge(data, identity_data, on="edgar.link")

    if text.find("footnotes") is not None:
        data["footnotes"] = text.find("footnotes").text
    return data
