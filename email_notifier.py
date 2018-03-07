import smtplib
import datetime as dt
import pandas as pd
import numpy as np

class email_notifier(object):
    def __init__(self,sender,subsystem,message,report_nm,report_dt,receivers,legend):
        self.sender = sender
        self.receivers = receivers
        self.message = message
        self.report_nm = report_nm
        self.subsystem = subsystem
        self.report_dt = report_dt
        self.legend = legend

    def send_run_notification(self):
        receivers = self.receivers.split(",")
        header = 'TO:' + str(receivers).strip('[]') + '\n' + 'From:' + self.sender + '\n' + \
            'Subject: ' + self.subsystem + ' Run Status Report - ' + str(self.report_dt) + '\n' + \
            'MIME-Version: 1.0' + '\n' + \
            'Content-type: text/html' + '\n'

        pretext = 'Hello Team, ' + '<br>' + '<br> Please find the ' + self.subsystem + ' run status report for the date ' + str(self.report_dt) + \
            'below. ' + '<br>'

        body = '<style> p {font-size: 10pt; font-family: Calibri; text-align: justify} </style> <p>' + pretext + '<br>' + self.legend + '</p>' + self.html_format_message()
        if self.html_format_message() == '':
            body = body + '<br>' + " No jobs ran on " + str(self.report_dt)

        message = header + body

        try:
            session = smtplib.SMTP('mailhost.jpmchase.net')
            session.sendmail(self.sender, receivers, message)
            session.starttls()
            session.quit()
            print "email sent"
        except Exception as err:
            print("An exception of type " + type(err).__name__ + "occured. Arguments:<br>" + str(err.args))


def html_format_message(self):
    html_msg = ''
    pd.set_output('display.max_colwidth', -1)
    for i,msg in enumerate(self.message):
        if msg:
            head = '<style> h3 {font-size: 15pt, font-family: Calibri;} </style> <h3> ' + self.report_nm[i] + ' </h3>'
            df = pd.DataFrame(msg, columns=msg[0].keys())
            df.index += 1
            if 'ExtractLagDays' in df:
                df['ExtractLagDays'].replace(np.nan, 'NA', regex=True)
            if 'ActualExtractDate' in df:
                df['ActualExtractDate'] = pd.to_datetime(df['ActualExtractDate'], format='%Y-%m-%d %H:%M:%S')
                df['ActualExtractDate'] = df['ActualExtractDate'].dt.strftime("%Y-%m-%d")
            if 'RunDateTime' in df:
                df['RunDateTime'] = pd.to_datetime(df['RunDateTime'], format='%Y-%m-%d %H:%M:%S')
                df['RunDateTime'] = df['RunDateTime'].dt.strftime("%Y-%m-%d %H:%M:%S %p")

            styles = [
                        dict(selector='thead th', props = [('background-color', 'lightgrey')]),
                        dict(selector='thead th', props=[('font-family', 'Calibri')]),
                        dict(selector='.col0', props=[('display', 'none')]),
                        dict(selector='th:first-child', props=[('display', 'none')]),
                        dict(selector='.row_heading, .blank', props=[('display', 'none')])
                      ]

            if 'ExpectedExtractDate' in df and 'ActualExtractDate' in df:
                html = (df.style \
                          .set_properties(**{'font-size': '9pt', 'font-family': 'Calibri', 'border-color' : 'black', 'background-color': 'lightgrey', 'text-align': 'center'}) \
                          .set_table_styles(styles) \
                          .apply(lambda x: np.where(x != df['ExpectedExtractDate'], 'color: red', 'color: green'), subset=['ActualExtractDate'])
                          .apply(lambda x: np.where(x != df['ExpectedExtractDate'], 'font-weight: bold', 'font-style: None'), subset=['ActualExtractDate'])
                          .render() \
                        )
            else:
                html = (df.style \
                        .set_properties(**{'font-size' : '9pt', 'font-family' : 'Calibri', 'border-color': 'black', 'background-color': 'lightgrey'}) \
                        .set_table_styles(styles) \
                        .render() \
                        )

            html_msg = html_msg + head + html
        return html_msg