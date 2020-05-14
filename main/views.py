# coding=utf-8
from django.shortcuts import render, redirect
import gspread
from pytrends.request import TrendReq
import pandas as pd
from google.cloud import bigquery
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
from django.conf import settings
import os
import json
from datetime import datetime, timedelta
import time
import random
pytrend = TrendReq()
bq_file_path = os.path.join(os.path.dirname(settings.BASE_DIR), "bigquery_client_secret.json")
# bq_client = bigquery.Client.from_service_account_json(bq_file_path)
with open(bq_file_path, 'r') as f:
    creds_dict = json.load(f)
credentials = service_account.Credentials.from_service_account_info(creds_dict)
bq_client = bigquery.Client(
    credentials=credentials,
    project=credentials.project_id,
)
scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

try:
    json_creds = os.getenv("GOOGLE_SHEETS_CREDS_JSON")
    creds_dict = json.loads(json_creds)
except:
    file_path = os.path.join(os.path.dirname(settings.BASE_DIR), "client-secret.json")
    print(file_path)
    with open(file_path) as f:
        creds_dict = json.load(f)
creds_dict["private_key"] = creds_dict["private_key"].replace("\\\\n", "\n")
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes)
client = gspread.authorize(creds)
test_spread_url = "https://docs.google.com/spreadsheets/d/1wzFbaa6FE1EJOeLhsRtNfdjm1bztK2J72Kl2y76urUQ/edit#gid=0"


def index(request):
    get_spreadsheet()
    return render(request, "main.html")


def sendToBQ(panda_df_3m):
    table_id_3m = 'corona.trends'
    job_config = bigquery.LoadJobConfig(schema=[
    ])
    job = bq_client.load_table_from_dataframe(
        panda_df_3m, table_id_3m, job_config=job_config)
    job.result()
    print("Loaded {} rows into :{}.".format(job.output_rows, table_id_3m))


def get_by_duration(results, row, data_headline, country, duration):
    pytrend.build_payload(kw_list=[row[data_headline[3]]], geo=country, timeframe=duration)
    df = pytrend.interest_over_time()
    df = df.rename(columns={row[data_headline[3]]: 'score'})
    if df.shape[1] == 2:
        df['vertical'] = row[data_headline[0]]
        df['category'] = row[data_headline[1]]
        df['sub_category'] = row[data_headline[2]]
        df['keyword_name'] = row[data_headline[3]]
        df['keyword_important'] = row[data_headline[4]]
        df['search_volume'] = row[data_headline[5]]
        df['time_stamp'] = datetime.now() + timedelta(hours=3)
        results.append(df[['vertical', 'category', 'sub_category', 'keyword_name', 'keyword_important',
                           'search_volume', 'score', 'time_stamp']])


def updateSheets(row_index):
    spreadsheet = client.open_by_url(test_spread_url)
    sheet = spreadsheet.worksheet("Sheet2")
    sheet.update_acell("A1", str(row_index))
    today = datetime.today().strftime('%Y-%m-%d')
    sheet.update_acell("B1", today)


def readIndex(index_sheet):
    # spreadsheet = client.open_by_url(test_spread_url)
    # sheet = spreadsheet.worksheet("Sheet2")
    try:
        val = index_sheet.get('A1')[0][0]
        print("a1 val: ", val)
        date = index_sheet.get('B1')[0][0]
        print("date: ", date)
        current_date = datetime.strptime(date, '%Y-%m-%d').date()
    except:
        val = None
        current_date = None
        print("didn't find date")
    return val, current_date


def getScoreAndSend(data, val, country='IL'):
    data_headline = data.columns
    print("data_headline: ", data_headline)
    for row_index, row in data.iterrows():
        if row_index == 0:
            print('row 0: ', row)
        if row_index > val:
            results_3m = []
            try:
                get_by_duration(results_3m, row, data_headline, country, duration='today 3-m')
            except:
                print("row index {} failed".format(row_index))
            try:
                results_3m_df = pd.concat(results_3m)
                print(results_3m_df.columns)
                sendToBQ(results_3m_df)
                rand_stop_time = random.randint(10, 20)
                print("stop for {} seconds".format(rand_stop_time))
                time.sleep(rand_stop_time)
            except:
                print("sending to BQ - index {} failed".format(row_index))
            updateSheets(row_index)


def getRand(minutesInADay, numberOfValues):
    mirvach = (minutesInADay/numberOfValues)*60
    print("mirvach: ", mirvach)
    mirvach_floor = (minutesInADay//numberOfValues)*60
    print("mirvach_floor: ", mirvach_floor)
    print(mirvach - mirvach_floor)
    rand_stop_time = random.randint(0, int(mirvach-mirvach_floor))
    print(rand_stop_time)
    timeToSleep = mirvach_floor + rand_stop_time
    return timeToSleep


def get_spreadsheet():
    # real_spread_url = 'https://docs.google.com/spreadsheets/d/1XFwPIiSq3k3FFksQ63dBZ_4gSfZOmGIVtBGbzSttkDI/edit?ts=5eabd9c9#gid=0'
    # spreadsheet = client.open_by_url(real_spread_url)
    # sheet = spreadsheet.worksheet("Data")

    spreadsheet = client.open_by_url(test_spread_url)
    sheet = spreadsheet.worksheet("Sheet1")
    index_sheet = spreadsheet.worksheet("Sheet2")

    # Extract and print all of the values
    data = sheet.get_all_values()
    numberOfValues = len(data)
    print("length: ", numberOfValues)
    headers = data.pop(0)
    df = pd.DataFrame(data, columns=headers)
    print(df.head())
    val, sheets_date = readIndex(index_sheet)
    now_date = datetime.today().date()
    if now_date == sheets_date:
        val = int(val)
        if numberOfValues > val:
            getScoreAndSend(df, val, country='IL')
        else:
            print("finished all rows")
    else:
        row_index = 0
        updateSheets(row_index)


    # minutesInADay = 24*60
    # sleepTime = getRand(minutesInADay, numberOfValues)

    # getScoreAndSend(df, country='IL')
    # panda_df = get_score_by_day(df, country='IL', duration='today 3-m')
    # print(panda_df.head())
    # panda_df.to_csv("trends.csv")
    # print("saved to csv")
    # table_id = 'corona.trends'
    #
    # # try:
    # #     panda_df.to_gbq(table_id, project_id="aviad-trends", if_exists='replace')
    # # except:
    # #     bq_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    # #     creds_dict = json.loads(bq_creds)
    # #     credentials = service_account.Credentials.from_service_account_info(creds_dict)
    # #     panda_df.to_gbq(table_id, project_id="aviad-trends", if_exists='replace', credentials=credentials)
    # #     # on dev
    # #     # bq_file_path = os.path.join(os.path.dirname(settings.BASE_DIR), "bigquery_client_secret.json")
    # #     #
    # #     # credentials = service_account.Credentials.from_service_account_file(bq_file_path)
    # #     # panda_df.to_gbq(table_id, project_id="aviad-trends", if_exists='replace', credentials=credentials)
    #
    # # on production
    # # try:
    # #     bq_client = bigquery.Client()
    # # except:
    # #     bq_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    # #     creds_dict = json.loads(bq_creds)
    # #     credentials = service_account.Credentials.from_service_account_info(creds_dict)
    # #     bq_client = bigquery.Client(
    # #         credentials=credentials,
    # #         project=credentials.project_id,
    # #     )
    # # on dev
    # # bq_file_path = os.path.join(os.path.dirname(settings.BASE_DIR), "bigquery_client_secret.json")
    # # bq_client = bigquery.Client.from_service_account_json(bq_file_path)
    # job_config = bigquery.LoadJobConfig(schema=[
    #     # bigquery.SchemaField(name="date", field_type="DATE"),
    #     # bigquery.SchemaField(name="vertical", field_type="STRING"),
    #     # bigquery.SchemaField(name="category", field_type="STRING"),
    #     # bigquery.SchemaField(name="sub_category", field_type="STRING"),
    #     # bigquery.SchemaField(name="keyword_name", field_type="STRING"),
    #     # bigquery.SchemaField(name="keyword_important", field_type="STRING"),
    #     # bigquery.SchemaField(name="search_volume", field_type="INTEGER"),
    #     # bigquery.SchemaField(name="score", field_type="INTEGER"),
    # ])
    # job = bq_client.load_table_from_dataframe(
    #     panda_df, table_id, job_config=job_config)
    # # Wait for the load job to complete.
    # # https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.WriteDisposition.html
    # job.result()
    # print("Loaded {} rows into :{}.".format(job.output_rows, table_id))


def get_score_by_day(data, country='IL', duration='today 3-m'):
    results = []
    data_headline = data.columns
    print("data_headline: ", data_headline)
    for index, row in data.iterrows():
        # pytrend = TrendReq()
        pytrend.build_payload(kw_list=[row[data_headline[3]]], geo=country, timeframe=duration)
        df = pytrend.interest_over_time()
        df = df.rename(columns={row[data_headline[3]]: 'score'})
        if df.shape[1] == 2:
            df['vertical'] = row[data_headline[0]]
            df['category'] = row[data_headline[1]]
            df['sub_category'] = row[data_headline[2]]
            df['keyword_name'] = row[data_headline[3]]
            df['keyword_important'] = row[data_headline[4]]
            df['search_volume'] = row[data_headline[5]]
            df['time_stamp'] = datetime.now()
            results.append(df[['vertical', 'category', 'sub_category', 'keyword_name', 'keyword_important',
                               'search_volume', 'score', 'time_stamp']])
    return pd.concat(results)


def callToPytrends(data, country='IL', duration='today 3-m'):
    results = []
    data_headline = data.columns
    print("data_headline: ", data_headline)
    for index, row in data.iterrows():
        # pytrend = TrendReq()
        pytrend.build_payload(kw_list=[row[data_headline[3]]], geo=country, timeframe=duration)
        df = pytrend.interest_over_time()
        df = df.rename(columns={row[data_headline[3]]: 'score'})
        if df.shape[1] == 2:
            df['vertical'] = row[data_headline[0]]
            df['category'] = row[data_headline[1]]
            df['sub_category'] = row[data_headline[2]]
            df['keyword_name'] = row[data_headline[3]]
            df['keyword_important'] = row[data_headline[4]]
            df['search_volume'] = row[data_headline[5]]
            df['time_stamp'] = datetime.now()
            results.append(df[['vertical', 'category', 'sub_category', 'keyword_name', 'keyword_important',
                               'search_volume', 'score', 'time_stamp']])
    return pd.concat(results)


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