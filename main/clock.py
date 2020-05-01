from apscheduler.schedulers.blocking import BlockingScheduler
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import sched, time
from datetime import datetime, timedelta
from django.conf import settings
settings.configure()
import os
from django.core.cache import cache
import requests
import json
sent = False


def sendgrid_mail():
    message = Mail(
            from_email='from_email@example.com',
            to_emails='aviadm24@gmail.com',
            subject='Server stopped working',
            html_content='<strong>and easy to do anywhere, even with Python</strong>')
    try:
        # sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        sg = SendGridAPIClient('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')
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
    except Exception as e:
        print(e)


def check_time():
    r = requests.get("https://aviad2.herokuapp.com/get_time")
    # print(r.status_code)
    # print(r.content)
    t = json.loads(r.content)
    # print(t)
    sever_time = datetime.strptime(t['time'], '%b %d %Y %I:%M:%S')
    global sent
    # print("ping: ", sever_time)
    # print("now: ", datetime.now())
    delta = datetime.now() - sever_time - timedelta(hours=4, minutes=0)
    print('delta is: ', delta.seconds)
    if delta.seconds > 140:
        print('sent: ', sent)
        if sent == False and delta.seconds < 200:
            print('2sending mail +++++++++++++++++++++++=')
            sendgrid_mail()
            sent = True
            if delta.seconds > 100:
                sent = False

sched = BlockingScheduler()


@sched.scheduled_job('interval', seconds=10)
def timed_job():
    # print('dir: ', os.getcwd())
    # print(__file__)
    # print(os.listdir(os.getcwd()))
    check_time()
    # print('Time is: ', time)


sched.start()
# baseurl = settings.__dict__
# print(baseurl)
# base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# print(base)
# f = os.path.join(base, 'time.txt')
# with open(f, 'r') as fle:
#     print(fle.read())