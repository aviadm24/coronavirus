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

pytrend = TrendReq()


# def sendgrid_mail():
#     message = Mail(
#             from_email='from_email@example.com',
#             to_emails='aviadm24@gmail.com',
#             subject='Server stopped working',
#             html_content='<strong>and easy to do anywhere, even with Python</strong>')
#     try:
#         # sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
#         sg = SendGridAPIClient('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')
#         response = sg.send(message)
#         print(response.status_code)
#         print(response.body)
#         print(response.headers)
#     except Exception as e:
#         print(e)
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
            results.append(df[['vertical', 'category', 'sub_category', 'keyword_name', 'keyword_important',
                               'search_volume', 'score']])
        rand_stop_time = random.randint(15, 30)
        print("stop for {} seconds".format(rand_stop_time))
        time.sleep(rand_stop_time)
    return pd.concat(results)


def get_spreadsheet():
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

    # Find a workbook by url

    real_spread_url = 'https://docs.google.com/spreadsheets/d/1XFwPIiSq3k3FFksQ63dBZ_4gSfZOmGIVtBGbzSttkDI/edit?ts=5eabd9c9#gid=0'
    spreadsheet = client.open_by_url(real_spread_url)
    sheet = spreadsheet.worksheet("Data")

    # test_spread_url = "https://docs.google.com/spreadsheets/d/1wzFbaa6FE1EJOeLhsRtNfdjm1bztK2J72Kl2y76urUQ/edit#gid=0"
    # spreadsheet = client.open_by_url(test_spread_url)
    # sheet = spreadsheet.worksheet("Sheet1")

    # Extract and print all of the values
    data = sheet.get_all_values()
    headers = data.pop(0)
    df = pd.DataFrame(data, columns=headers)
    print(df.head())
    panda_df = get_score_by_day(df, country='IL', duration='today 3-m')
    print(panda_df.head())
    panda_df.to_csv("trends.csv")
    print("saved to csv")
    table_id = 'corona.trends'

    # try:
    #     panda_df.to_gbq(table_id, project_id="aviad-trends", if_exists='replace')
    # except:
    #     bq_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    #     creds_dict = json.loads(bq_creds)
    #     credentials = service_account.Credentials.from_service_account_info(creds_dict)
    #     panda_df.to_gbq(table_id, project_id="aviad-trends", if_exists='replace', credentials=credentials)
    #     # on dev
    #     # bq_file_path = os.path.join(os.path.dirname(settings.BASE_DIR), "bigquery_client_secret.json")
    #     #
    #     # credentials = service_account.Credentials.from_service_account_file(bq_file_path)
    #     # panda_df.to_gbq(table_id, project_id="aviad-trends", if_exists='replace', credentials=credentials)


    # Since string columns use the "object" dtype, pass in a (partial) schema
    # to ensure the correct BigQuery data type.
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
    # on dev
    # bq_file_path = os.path.join(os.path.dirname(settings.BASE_DIR), "bigquery_client_secret.json")
    # bq_client = bigquery.Client.from_service_account_json(bq_file_path)
    job_config = bigquery.LoadJobConfig(schema=[
        # bigquery.SchemaField(name="date", field_type="DATE"),
        # bigquery.SchemaField(name="vertical", field_type="STRING"),
        # bigquery.SchemaField(name="category", field_type="STRING"),
        # bigquery.SchemaField(name="sub_category", field_type="STRING"),
        # bigquery.SchemaField(name="keyword_name", field_type="STRING"),
        # bigquery.SchemaField(name="keyword_important", field_type="STRING"),
        # bigquery.SchemaField(name="search_volume", field_type="INTEGER"),
        # bigquery.SchemaField(name="score", field_type="INTEGER"),
    ])
    job = bq_client.load_table_from_dataframe(
        panda_df, table_id, job_config=job_config)
    # Wait for the load job to complete.
    job.result()
    print("Loaded {} rows into :{}.".format(job.output_rows, table_id))


sched = BlockingScheduler()


@sched.scheduled_job('cron', day_of_week='0-6', hour=15, minute=5)
def timed_job():
    # print('dir: ', os.getcwd())
    # print(__file__)
    # print(os.listdir(os.getcwd()))
    get_spreadsheet()
    # print('Time is: ', time)


sched.start()
