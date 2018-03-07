import pymysql
import ConfigParser
import logging
from pymysql import DataError, ProgrammingError
from pprint import pprint
import base64, os, csv, sys
from commands import getstatusoutput
from time import sleep

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

class mySQLHandler():
    insert_ops_jobstat_sql = ("INSERT INTO <<DB_NAME>>.<<DB_TABLE>>("
                                "  feed,"
                                "  jobpid,"
                                "  filename,"
                                "  process_start,"
                                "  process_end,"
                                "  linecount,"
                                "  rowcount,"
                                "  filedate,"
                                "  status,"
                                "  message,"
                                "  raw_size,"
                                "  raw_replica_size,"
                                "  partition_col,"
                                "  file_end_dt,"
                                "  refined_count,"
                                "  )"
                                "  VALUE("
                                "  '%(feed)s',"
                                "  %(jobpid)d,"
                                "  '%(filename)s',"
                                "  '%(process_start)'"
                                "  '%(process_end)s'"
                                "  '%(linecount)s'"
                                "  '%(rowcount)s'"
                                "  '%(filedate)s'"
                                "  '%(status)s'"
                                "  '%(message)s'"
                                "  '%(raw_size)s'"
                                "  '%(raw_replica_size)s'"
                                "  '%(refined_size)s'"
                                "  '%(refined_replica_size)s'"
                                "  '%(partition_col)s'"
                                "  '%(file_end_dt)s'"
                                "  '%(refined_count)s'"
                                "   );"
                                "     ")

    update_ops_jobstat_sql = ("UPDATE <<DB_NAME>>.<<DB_TABLE>> "
                              " set filename='%(filename)s',"
                              "     process_start='%(process_start)s',"
                              "     process_end='%(process_end)s',"
                              "     linecount='%(linecount)s',"
                              "     rowcount='%(rowcount)s',"
                              "     filedate='%(filedate)s',"
                              "     status='%(status)s',"
                              "     message='%(message)s',"
                              "     raw_size='%(raw_size)s',"
                              "     raw_replica_size='%(raw_replica_size)s',"
                              "     refined_size='%(refined_size)s',"
                              "     refined_replica_size='%(refined_replica_size)s',"
                              "     partition_col='%(partition_col)s',"
                              "     file_end_dt='%(file_end_dt)s',"
                              "     refined_count='%(refined_count)s',"
                              "  WHERE (feed = '%(feed)s' AND jobpid = '%(jobpid)d' );"
                              "   " )

    def __init__(self, configpath):
        Config = ConfigParser.ConfigParser()
        Config.read(configpath)
        self.db = {'host': Config.get('mySQL', 'dbhost'),
                   'port': int(Config.get('mySQL', 'port'),
                   'dbuser': Config.get('mySQL', 'dbuser'),
                   'dbpassword': get_passwd(Config.get('mySQL', 'dbuser'), configpath),
                   'schema': Config.get('mySQL', 'schema'),
                   'jobstat': Config.get('mySQL', 'jobstat')
                  }

    def perform_DML(self, arg_action, arg_record):
        l_DML = self.insert_ops_jobstat_sql if arg_action == 'insert' else self.update_ops_jobstat_sql
        l_DML = l_DML.replace('<<DB_NAME>>', self.db['schema']).replace('<<DB_TABLE>>', self.db['jobstat'])
        l_DML = l_DML % arg_record

        try:
            conn = pymysql.connect(host=self.db['host'],
                                   port=self.db['port'],
                                   user=self.db['dbuser'],
                                   passwd=self.db['dbpassword'],
                                   db=self.db['schema']
                                   )
        except Exception as e:
            pprint("ERROR : mySQLHandler.py: The Exception during db.connect: " + str(e))

        cur = conn.cursor()
        try:
            cur.execute(l_DML)
        except ProgrammingError as e:
            conn.rollback()
            cur.close()
            conn.close()
            pprint("ERROR : mySQLHandler.py: The Exception during db.connect: " + str(e))
        except Exception as e:
            conn.rollback()
            cur.close()
            conn.close()
            pprint("ERROR : mySQLHandler.py: The Exception during db.connect: " + str(e))
        else:
            conn.commit()
        finally:
            cur.close()
            if conn.open:
                conn.close()

    def perform_DML2(self, dml_statement):
        try:
            conn = pymysql.connect(host=self.db['host'],
                                   port=self.db['port'],
                                   user=self.db['dbuser'],
                                   passwd=self.db['dbpassword'],
                                   db=self.db['schema'])
        except Exception as e:
            pprint("ERROR : mySQLHandler.py: The Exception during db.connect: " + str(e))

        cur = conn.cursor()
        try:
            cur.execute(dml_statement)
        except ProgrammingError as e:
            conn.rollback()
            cur.close()
            conn.close()
            pprint("ERROR : mySQLHandler.py: The Exception during db.connect: " + str(e))
        except Exception as e:
            conn.rollback()
            cur.close()
            conn.close()
            pprint("ERROR : mySQLHandler.py: The Exception during db.connect: " + str(e))
        else:
            conn.commit()
        finally:
            cur.close()
            if conn.open:
                conn.close()
