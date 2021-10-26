import argparse
import csv
import datetime
import dateutil.relativedelta
from io import open
import mysql.connector
import os
import smtplib
import subprocess
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import yaml

parser = argparse.ArgumentParser(description='Query MemSQL and send a report.')
parser.add_argument('-q', dest='queryfile', help='full path of query file', default=None, required=True)
args = parser.parse_args()

def readYAML(path):
    #windows - uncomment 3 lines below
    curdir = os.getcwd()
    fullpath = os.path.join(curdir, 'query')
    fullpath = os.path.join(fullpath, path)

    #centos - uncomment line below
    #fullpath = os.path.join('/srv/scripts/memsql/query', path)
    config = ""
    with open(fullpath, 'r') as stream:
        config = yaml.safe_load(stream)
    return config


def runQuery(config):
    # TO DO - move to config.yaml
    conn = mysql.connector.connect(host='server_name', user='db_user', passwd='password', db='information_schema')
    cursor = conn.cursor(buffered=True)
    cursor.execute(query)

    columns = [i[0] for i in cursor.description]
    rows = cursor.fetchall()

    reportname = str(config.get('reportName')) + '_'+ datetime.date.today().strftime("%Y_%m_%d") + ".csv"

    # windows
    curdir = os.getcwd()
    fullpath = os.path.join(curdir, 'reports')
    reportfullpath = os.path.join(fullpath, reportname)

    # centos
    #reportfullpath = os.path.join('/srv/scripts/memsql/reports', reportname)

    f = open(reportfullpath, 'w', encoding='utf-8')
    report = csv.writer(f, lineterminator='\n')
    report.writerow(columns)
    report.writerows(rows)
    f.close()

    # add
    if config.get('encrypt') == 'y':
        passphrase = '-p' + config.get('pwd')
        newname = reportname.replace('.csv', '.zip')
        newname = os.path.join(fullpath, newname)
        cmd = ['7za', 'a', newname, reportfullpath, passphrase]

        sub = subprocess.Popen(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)

        emailReport(config, reportfullpath, newname)
    else:
        emailReport(config, reportfullpath, reportname)

def emailReport(config, reportfullpath, reportname):
    fromAddr = 'DoNotReply@domain.com'
    toAddr = config.get('to')
    ccAddr = config.get('cc')

    mailBody = reportname
    msg = MIMEMultipart()
    msg['From'] = fromAddr
    msg['To'] = ', '.join(toAddr)
    print(msg['To'])
    msg['CC'] = ', '.join(ccAddr)
    msg['Subject'] = config.get('subject')
    msg.attach(MIMEText(mailBody, 'plain'))

    attachment = open(reportfullpath, 'rb')
    attachPart = MIMEBase('application', 'octet-stream')
    attachPart.set_payload((attachment).read())
    encoders.encode_base64(attachPart)
    attachPart.add_header('Content-Disposition', "attachment; filename= " + reportname)
    msg.attach(attachPart)

    s = smtplib.SMTP('smpt server') 
    s.sendmail(fromAddr, toAddr, str(msg))
    s.quit()

def get_dates(config):
    today = datetime.date.today()
    range = config.get('reportRange')

    if range == 'previousDay':
        endDate = today
        startDate = endDate + dateutil.relativedelta.relativedelta(days=-1)
    elif range == 'previousWeek':
        week_start = config.get('reportWeekStart')
        dayweek = {
            'monday': 0,
            'tuesday': 1,
            'wednesday': 2,
            'thursday': 3,
            'friday': 4,
            'saturday': 5,
            'sunday': 6
        }[week_start.lower()]
        days = -1 * (today.weekday() - dayweek + 7)
        startDate = today + dateutil.relativedelta.relativedelta(days=days)
        endDate = startDate + dateutil.relativedelta.relativedelta(days=6)
    elif range == 'previousMonth':
        endDate = today.replace(day=1)
        startDate = endDate + dateutil.relativedelta.relativedelta(months=-1)
    else:
        endDate = '1900-01-01'
        startDate = '1900-01-02'
    return str(startDate), str(endDate)

##############################################################################################

config = readYAML(args.queryfile)
dates = get_dates(config)
query = config.get('query')
query = query.replace("@start", dates[0])
query = query.replace("@end", dates[1])

# run query and write results to csv file
rows = runQuery(config)
