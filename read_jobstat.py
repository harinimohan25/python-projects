import os, csv, sys, base64
from commands import getstatusoutput
from time import sleep
from pymysql.cursors import DictCursorMixin, Cursor
import pymysql
import configparser
from collections import OrderedDict

def get_passwd(arg_user, configpath):
    passwd = ''
    passwordRetrieved = False
    retries = 3
    arg_passwd_file = configpath.replace(os.path.basename(configpath),'ENV.scriptpwd.properties')
    epvaim_file = configpath.replace(os.path.basename(configpath),'ENV.epvaim.properties')
    data_reader = csv.reader(open(arg_passwd_file, 'rt'), delimiter=':', quotechar='"', quoting=csv.QUOTE_ALL)
    for rec in data_reader:
        if len(rec) != 0 and rec[0] == arg_user:
            passwd = base64.b64decode(rec[1]).replace('\n', '').replace('\r', '')
            if passwd <> '':
                passwordRetrieved = True

    if passwd == "":
        data_reader = csv.reader(open(epvaim_file, 'rt'), delimiter=':', quotechar='"', quoting=csv.QUOTE_ALL)

    for rec in data_reader:
        if len(rec) != 0 and rec[0].lower() == arg_user.lower():
            querystr, appid = rec[1].split("|")
            while not passwordRetrieved and retries <> 0:
                rtn, checkSync = getstatusoutput('/opt/CARKaim/sdk/clipasswordsdk getpassword ' +
                                                 '-p AppDescs.AppID=' + appid +
                                                 '-p Query="' + querystr + '" -o PasswordChangeInProcess')
                if checkSync == 'true':
                    sleep(1)
                    retries = retries - 1
                else:
                    rtn,passwd = getstatusoutput('/opt/CARKaim/sdk/clipasswordsdk getpassword ' +
                                                 '-p AppDescs.AppID=' + appid +
                                                 '-p Query="' + querystr + '" -o Password'
                                                )
                    passwordRetrieved = True

    if not passwordRetrieved:
        print("EPVAIM password is taking longer than expected")
        exit(99)
    elif password == "":
        print("Password for ID:" + arg_user + ", not found in properties file")
        exit(99)
    else:
        return passwd

class OrderedDictCursor(DictCursorMixin, Cursor):
    dict_type = OrderedDict

class read_jobstat(object):
    def __init__(self, configpath):
        config = ConfigParser.ConfigParser()
        config.read(configpath)
        db = { 'host' : config.get('mySQL','dbhost'),
               'port' : int(config.get('mySQL', 'port')),
               'dbuser' : config.get('mySQL', 'dbuser'),
               'dbpassword' : get_passwd(config.get('mySQL', 'dbuser'), configpath),
               'metadata_schema' : config.get('mySQL', 'schema'),
               'jobstat' : config.get('mySQL','jobstat'),
               'jobstat_rep_queries' : config.get('mySQL','jobstat_rep_queries'),
               'tbl_meta' : config.get('mySQL','tbl_meta')}
        self.db = db

    def read_table(self, report_dt):
        rep_list = []
        rep_name_list = []
        for idx,job_stat_request in enumerate(self.db['jobstat_rep_queries'].split("::")):
            if len(job_stat_query_report.split("->")) > 1:
                job_stat_query, report_name = job_stat_query_report.split("->")
            else:
                job_stat_query = job_stat_query_report.split("->")[0]
                report_name = "Report No " + str(idx+1)
            conn - pymysql.connect(host=self.db["host"], port=self.db['port'], user=self.db['dbuser'], passwd=self.db['dbpassword'], db=self.db['metadata_schema'])
            cursor = conn.cursor(OrderedDict)
            if '%s' in job_stat_query:
                cursor.execute(job_stat_query % (self.db['jobstat'], report_dt))
            else:
                s = filter(None, job_stat_query.strip().split(';'))
                for i in s:
                    cursor.execute(i.strip() + ';')

            ops_dict_list = cursor.fetchall()
            rep_list.append(report_name)
            rep_name_list.append(report_name)
            cursor.close()
            conn.close()
        return rep_list, rep_name_list
        
             }