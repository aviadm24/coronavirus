from apscheduler.schedulers.blocking import BlockingScheduler
import gspread
from pytrends.request import TrendReq
import pandas as pd
from google.cloud import bigquery
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
from django.conf import settings
import os
import json
import random
import time
from datetime import datetime, timedelta

pytrend = TrendReq()

# google BQ auth
try:
    bq_client = bigquery.Client()
except:
    bq_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    creds_dict = json.loads(bq_creds)
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    bq_client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )

# google sheets auth
scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
try:
    json_creds = os.getenv("GOOGLE_SHEETS_CREDS_JSON")
    creds_dict = json.loads(json_creds)
except:
    file_path = os.path.join(os.path.dirname(settings.BASE_DIR), "client-secret.json")
    with open(file_path) as f:
        creds_dict = json.load(f)
creds_dict["private_key"] = creds_dict["private_key"].replace("\\\\n", "\n")
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes)
client = gspread.authorize(creds)

real_spread_url = 'https://docs.google.com/spreadsheets/d/1XFwPIiSq3k3FFksQ63dBZ_4gSfZOmGIVtBGbzSttkDI/edit?ts=5eabd9c9#gid=0'
indexSheetName = 'Index'


def updateSheets(row_index):
    spreadsheet = client.open_by_url(real_spread_url)
    sheet = spreadsheet.worksheet(indexSheetName)
    sheet.update_acell("A1", str(row_index))
    today = datetime.today().strftime('%Y-%m-%d')
    sheet.update_acell("B1", today)


def readIndex(sheet):
    try:
        val = sheet.get('A1')[0][0]
        print("a1 val: ", val)
        date = sheet.get('B1')[0][0]
        print("date: ", date)
        current_date = datetime.strptime(date, '%Y-%m-%d').date()
    except:
        val = None
        current_date = None
        print("didn't find date")
    return val, current_date


def sendToBQ(panda_df_3m):
    table_id_3m = 'corona.trends3m'
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


def getScoreAndSend(data, val, country='IL'):
    data_headline = data.columns
    # print("data_headline: ", data_headline)
    for row_index, row in data.iterrows():
        if row_index >= val:
            results_3m = []
            try:
                get_by_duration(results_3m, row, data_headline, country, duration='today 3-m')
            except:
                print("row index {} failed".format(row_index))
            try:
                if results_3m != []:
                    results_3m_df = pd.concat(results_3m)
                    # print(results_3m_df.columns)
                    sendToBQ(results_3m_df)
                    rand_stop_time = random.randint(30, 50)
                    print("stop for {} seconds".format(rand_stop_time))
                    time.sleep(rand_stop_time)
            except:
                print("sending to BQ - index {} failed".format(row_index))
            try:
                updateSheets(row_index+1)  # adding 1 so it gets the next id in the next run
            except:
                print("cant update google sheets")


# def get_score_by_day(data, country='IL'):
#     results_3m = []
#     # results_7d = []
#     data_headline = data.columns
#     print("data_headline: ", data_headline)
#     for index, row in data.iterrows():
#         try:
#             get_by_duration(results_3m, row, data_headline, country, duration='today 3-m')
#         except:
#             print("row index {} failed".format(index))
#         # get_by_duration(results_7d, row, data_headline, country, duration='now 7-d')
#         rand_stop_time = random.randint(30, 90)
#         print("stop for {} seconds".format(rand_stop_time))
#         time.sleep(rand_stop_time)
#     return pd.concat(results_3m)


def get_spreadsheet():
    spreadsheet = client.open_by_url(real_spread_url)
    sheet = spreadsheet.worksheet("Data")

    # test_spread_url = "https://docs.google.com/spreadsheets/d/1wzFbaa6FE1EJOeLhsRtNfdjm1bztK2J72Kl2y76urUQ/edit#gid=0"
    # spreadsheet = client.open_by_url(test_spread_url)
    # sheet = spreadsheet.worksheet("Sheet1")

    # Extract and print all of the values
    data = sheet.get_all_values()
    numberOfValues = len(data)
    headers = data.pop(0)
    df = pd.DataFrame(data, columns=headers)
    # print(df.head())
    val = 0
    getScoreAndSend(df, val, country='IL')
    # # panda_df_7d.to_csv("trends.csv")
    # # print("saved to csv")
    # table_id_3m = 'corona.trends3m'
    # # table_id_7d = 'corona.trends7d'
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
    #
    # # Since string columns use the "object" dtype, pass in a (partial) schema
    # # to ensure the correct BigQuery data type.
    # try:
    #     bq_client = bigquery.Client()
    # except:
    #     bq_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    #     creds_dict = json.loads(bq_creds)
    #     credentials = service_account.Credentials.from_service_account_info(creds_dict)
    #     bq_client = bigquery.Client(
    #         credentials=credentials,
    #         project=credentials.project_id,
    #     )
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
    #     panda_df_3m, table_id_3m, job_config=job_config)
    # # Wait for the load job to complete.
    # job.result()
    # print("Loaded {} rows into :{}.".format(job.output_rows, table_id_3m))
    #
    # # job = bq_client.load_table_from_dataframe(
    # #     panda_df_7d, table_id_7d, job_config=job_config)
    # # # Wait for the load job to complete.
    # # job.result()
    # # print("Loaded {} rows into :{}.".format(job.output_rows, table_id_7d))

sched = BlockingScheduler()


# @sched.scheduled_job('cron', day_of_week='0-6', hour=2, minute=0)
@sched.scheduled_job('interval', id='trends', hours=8)
def timed_job():
    # get_spreadsheet()
    spreadsheet = client.open_by_url(real_spread_url)
    sheet = spreadsheet.worksheet("Data")
    index_sheet = spreadsheet.worksheet(indexSheetName)
    data = sheet.get_all_values()
    numberOfValues = len(data)
    headers = data.pop(0)
    df = pd.DataFrame(data, columns=headers)
    # print(df.head())
    val, sheets_date = readIndex(index_sheet)
    now_date = datetime.today().date()
    if now_date == sheets_date:
        val = int(val)
        if numberOfValues > val:
            getScoreAndSend(df, val, country='IL')
        else:
            print("finished all rows")
    else:
        val = 570
        getScoreAndSend(df, val, country='IL')


sched.start()
