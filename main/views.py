# coding=utf-8
from django.shortcuts import render, redirect
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse
import gspread
from pytrends.request import TrendReq
from oauth2client.service_account import ServiceAccountCredentials
from django.conf import settings
from django.http.response import JsonResponse
import os
import json
import urllib.parse as pr
import re
import clicksend_client
from clicksend_client import SmsMessage
from clicksend_client.rest import ApiException
import ast

pytrend = TrendReq()


def index(request):
    # https: // bootsnipp.com / snippets / ZXKKD
    get_spreadsheet()
    return render(request, "main.html")


def get_spreadsheet():
    # based on https://www.twilio.com/blog/2017/02/an-easy-way-to-read-and-write-to-a-google-spreadsheet-in-python.html
    # read that file for how to generate the creds and how to use gspread to read and write to the spreadsheet

    # use creds to create a client to interact with the Google Drive API
    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

    try:
        json_creds = os.getenv("GOOGLE_SHEETS_CREDS_JSON")
        creds_dict = json.loads(json_creds)
    except:
        file_path = os.path.join(os.path.dirname(settings.BASE_DIR), "client-secret.json")
        print(file_path)
        with open(file_path) as f:
            creds_dict = json.load(f)
            print("creds: ", creds_dict)
    # creds_dict = json.loads(json_creds)
    creds_dict["private_key"] = creds_dict["private_key"].replace("\\\\n", "\n")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes)
    client = gspread.authorize(creds)

    # Find a workbook by url
    spread_url = "https://docs.google.com/spreadsheets/d/1wzFbaa6FE1EJOeLhsRtNfdjm1bztK2J72Kl2y76urUQ/edit#gid=0"
    spreadsheet = client.open_by_url(spread_url)
    # worksheet_list = spreadsheet.worksheets()
    sheet = spreadsheet.worksheet("Sheet1")

    # Extract and print all of the values
    # rows = sheet.get_all_records()
    ID_COLUMN = 1
    # STATUS_COLUMN = "A"
    payload = sheet.col_values(ID_COLUMN)
    print('list: ', payload)
    sendToTrends(payload)


def sendToTrends(payload):
    # Login to Google. Only need to run this once, the rest of requests will use the same session.
    pytrend.build_payload(kw_list=payload)
    # Interest Over Time
    interest_over_time_df = pytrend.interest_over_time()
    head = interest_over_time_df.head()
    print("head: ", head)


def sendToBigQeury():
    from google.cloud import bigquery

    import pandas

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    # table_id = "your-project.your_dataset.your_table_name"

    records = [
        {"title": u"The Meaning of Life", "release_year": 1983},
        {"title": u"Monty Python and the Holy Grail", "release_year": 1975},
        {"title": u"Life of Brian", "release_year": 1979},
        {"title": u"And Now for Something Completely Different", "release_year": 1971},
    ]
    dataframe = pandas.DataFrame(
        records,
        # In the loaded table, the column order reflects the order of the
        # columns in the DataFrame.
        columns=["title", "release_year"],
        # Optionally, set a named index, which can also be written to the
        # BigQuery table.
        index=pandas.Index(
            [u"Q24980", u"Q25043", u"Q24953", u"Q16403"], name="wikidata_id"
        ),
    )
    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=[
            # Specify the type of columns whose type cannot be auto-detected. For
            # example the "title" column uses pandas dtype "object", so its
            # data type is ambiguous.
            bigquery.SchemaField("title", bigquery.enums.SqlTypeNames.STRING),
            # Indexes are written if included in the schema by name.
            bigquery.SchemaField("wikidata_id", bigquery.enums.SqlTypeNames.STRING),
        ],
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )