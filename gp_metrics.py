from __future__ import print_function
import sys, csv, os, base64, logging, pwd,
from time import time
from os.path import isfile
from getpass import getuser
from commands import getstatusoutput
from datetime import datetime
import pandas as pd
import utilities as util

class gp_metric(object):
    def __init__(self):
        self.GPMASTER = os.environ.get('GPMASTER', '<devservername>')
        self.GPPORT = os.environ.get('GPPORT', '')
        self.GPDB = os.environ.get('GPDB', '')
        self.GPUSER = os.environ.get('GPUSER', '')
        self.base_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + os.sep
        self.config = self.base_dir + 'common' + os.sep + 'ENV.scriptpwd.properties'
        self.pwd = self.getPwd(self.GPUSER.lower(), self.config)
        self.schema = 'dops'
        self.table = 'map_gp_metrics'

    def getPwd(self, arg_user, config_path):
        passwd = ''
        passwordRetrieved = False
        retries = 3
        arg_passwd_file = configpath.replace(os.path.basename(configpath), 'ENV.scriptpwd.properties')
        epvaim_file = configpath.replace(os.path.basename(configpath), 'ENV.epvaim.properties')
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
                        rtn, passwd = getstatusoutput('/opt/CARKaim/sdk/clipasswordsdk getpassword ' +
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


    def gp_cmd_func(self, in_sql):
        gp_cmd = "export PGPASSWORD=" + self.pwd.strip() + " && psql -h " + self.GPMASTER.lower() + " -U " + \
            self.GPUSER.lower() + " -p " + self.GPPORT + ' -d ' + self.GPDB + ' -c "' + in_sql + ';'
        return  gp_cmd

    def write_results(self, input, v_opt):
        v_list = []
        for i in input.replace(+, '|').replace('--', '').split('\n'):
            i_list = [j.strip() for j in i.replace('- ', '').split("|")]
            v_list.append(i_list)
            i_list = []
        o_list = [i for i in v_list[2:-3]]
        insert = ""
        row_counter = 0
        for v in o_list:
            t_list = str([str(x).strip("[]") for x in v]).strip("[]")
            if v_opt == 7:
                ins = """insert into """ + self.schema + "." + self.table + """(query_info, process_dt, entity_type, container_name, entity_name, size1, size2) values (%s);""" %(t_list)
            elif v_opt == 6:
                ins = """insert into """ + self.schema + "." + self.table + """(query_info, process_dt, entity_type, container_name, entity_name, size1) values (%s);""" % (
                t_list)
            elif v_opt == 5:
                ins = """insert into """ + self.schema + "." + self.table + """(query_info, process_dt, entity_type, container_name, size1) values (%s);""" % (
                    t_list)
            elif v_opt == 4:
                ins = """insert into """ + self.schema + "." + self.table + """(query_info, process_dt, entity_name, size1) values (%s);""" % (
                    t_list)
            logging.info("printing insert statement" + ins)
            inserts = inserts + ins
            row_counter = row_counter + 1
            logging.info("Row Counter is " + str(row_counter))

            if row_counter >= 250:
                logging.info('Inserting 250 rows batch')
                try:
                    run_cmd = self.gp_cmd_func(inserts)
                    self.run_shell_cmd(arg_shell_cmd=run_cmd)
                except:
                    logging.info('Error occured while inserting rows')
                    continue
                else:
                    logging.info('A batch of 250 rows have been completed successfuly')

                insert = ""
                row_counter = 0

        run_cmd = self.gp_cmd_func(inserts)
        return self.run_shell_cmd(arg_shell_cmd=run_cmd)

    class Msgcolors:
        HEADER = '\033[95m'
        OKBLUE = '\033[94m'
        OKGREEN = '\033[92m'
        WARNING = '\033[93m'
        FAIL = '\033[91m'
        ENDC = '\033[0m'
        BOLD = '\033[1m'
        UNDERLINE = '\033[4m'

    def run_shell_cmd(self, arg_shell_cmd):
        import subprocess
        try:
            print(self.Msgcolors.OKGREEN + 'Running : ' + arg_shell_cmd + self.Msgcolors.ENDC)
            p = subprocess.Popen(arg_shell_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
            (output, error) = p.communicate()
            rc = p.wait()
            if rc != 0 and error is not None:
                self.print_info("Output from Shell Execution " + output)
                logging.info("Output from Shell Execution " + output)
                util.abort_with_msg("Shell Exection with return code " + str(rc) + " and error " + str(error) )
            else:
                self.print_info(output)
                logging.info(output)
            return output
        except Exception as err:
            self.print_info("An exception of type " + type(err).__name__ + "occured. Arguments:" + str(err.args))

    def print_warn(self, msg):
        ts = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        print(self.Msgcolors.WARNING + "\n" + ts + ":\tWARNING: " + msg + "\n" + self.Msgcolors.ENDC)
        logging.warning(self.Msgcolors.WARNING + "\n" + ts + ":\tWARNING: " + msg + "\n" + self.Msgcolors.ENDC)

    def print_info(self, msg):
        ts = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        print(ts + ":\INFO: " + msg)
        logging.info(ts + ":\INFO: " + msg)

    def sql_cmds(self):
        sql_q0 = """ DELETE FROM """ + self.schema + "." + self.table + """ WHERE PROCESS_DT = CURRENT_DATE;"""
        q0_cmd = self.gp_cmd_func(sql_q0)
        q0_res = util.run_shell_cmd(q0_cmd)

        sql_q1 = """ SELECT 'DATABASE_SIZE' AS QUERY_TYPE, CURRENT_DATE AS PROCESS_DATE, DATNAME, PG_SIZE_PRETTY(PG_DATABASE_SIZE(DATNAME)) FROM 
        (SELECT DATNAME FROM PG_DATABASE WHERE UPPER(DATNAME) = 'DG') DB; """
        q1_cmd = self.gp_cmd_func(sql_q1)
        logging.info("q1_cmd : " + q1_cmd)
        q1_res = util.run_shell_cmd(q1_cmd)
        logging.info("q1_res : " + q1_res)
        q1_ret = self.write_results(q1_res, 4)
        logging.info("q1_ret : " + str(q1_ret))

        sql_q2 = """ select current_Date, autnspname, autrelname, autrelkind, auttoastoid from gp_toolkit.__gp_user_data_tables_readable rt """
        q2_cmd = self.gp_cmd_func(sql_q2)
        logging.info("q2_cmd : " + q2_cmd)
        q2_res = util.run_shell_cmd(q2_cmd)
        logging.info("q2_res : " + q2_res)
        q2_ret = self.write_results(q2_res, 4)
        logging.info("q2_ret : " + str(q2_ret))

        sql_q3 = """SELECT 'SCHEMA_SIZE' AS QUERY_TYPE, CURRENT_DATE, 'SCHEMA' AS ENTITY_TYPE, SCHEMA_NAME, SCHEMA_NAME, 
        PG_SIZE_PRETTY(SUM(TABLE_SIZE::BIGINT)::BIGINT) AS SIZE1, (SUM(TABLE_SIZE) / PG_DATABASE_SIZE(CURRENT_DATABASE())) * 100 AS SIZE2
        FROM (
            SELECT PG_CATALOG.PG_NAMESPACE.NSPNAME AS SCHEMA_NAME, 
                   PG_RELATION_SIZE(PG_CATALOG.PG_CLASS.OID) AS TABLE_SIZE
            FROM   PG_CATALOG.PG_CLASS
            JOIN   PG_CATALOG.PG_NAMESPACE ON RELNAMESPACE = PG_CATALOG.PG_NAMESPACE.OID
             ) T
        WHERE UPPER(SCHEMA_NAME) IN ('DM','DOPS','STAGE','BAK')
        GROUP BY SCHEMA_NAME
        """
        q3_cmd = self.gp_cmd_func(sql_q3)
        logging.info("q3_cmd : " + q3_cmd)
        q3_res = util.run_shell_cmd(q3_cmd)
        logging.info("q3_res : " + q3_res)
        q3_ret = self.write_results(q3_res, 7)
        logging.info("q3_ret : " + str(q3_ret))

        sql_q4 = """SELECT 'TABLE_SIZE_AND_PARTITIONS' AS QUERY_TYPE, CURRENT_DATE AS PROCESS_DT, SCHEMA_NAME, TABLENAME, 
                PG_SIZE_PRETTY(SUM(TABLE_SIZE::BIGINT)::BIGINT) AS SIZE1, SUM(NUM_PARTITIONS) AS NUM_PARTITIONS, SUM(ROW_CNT) AS NO_OF_ROWS
                FROM (
                    SELECT COALESCE(P.SCHEMANAME, N.NPSNAME) AS SCHEMANAME,
                           COALESCE(P.TABLENAME, C.RELNAME) AS TABLENAME,
                           1 AS NUM_PARTITIONS,
                           PG_RELATION_SIZE(N.NPSNAME || '.' || C.RELNAME) AS SIZE,
                           C.RELTUPLES::BIGINT AS ROW_CNT
                    FROM   PG_CATALOG.PG_CLASS C
                    INNER JOIN PG_NAMESAPCE N AS C.RELNAMSPACE = N.OID
                    LEFT JOIN PG_PARTITIONS P AS C.RELNAME = P.PARTITIONTABLENAME AND N.NSPNAME = P.PARTITIONSCHEMANAME
                    WHERE UPPER(N.NPSNAME IN ('DM','DOPS','STAGE','BAK') AND UPPER(P.SCHEMANAME) IN ('DM','DOPS','STAGE','BAK')
                ) Q
                GROUP BY 1,2,3,4
                """
        q4_cmd = self.gp_cmd_func(sql_q4)
        logging.info("q4_cmd : " + q4_cmd)
        q4_res = util.run_shell_cmd(q4_cmd)
        logging.info("q4_res : " + q4_res)
        q4_ret = self.write_results(q4_res, 7)
        logging.info("q4_ret : " + str(q4_ret))

        """
        COLUMNS: 
        CONTAINER NAME - THE NAME OF THE SCHEMA
        TABLE - THE NAME OF THE TABLE
        SIZE1 - TABLE SIZE - THE TOTAL SIZE THAT THIS TABLE TAKES
        SIZE2 - EXTERNAL SIZE - THE SIZE THAT RELATED OBJECTS OF THIS TABLE LIKE INDICES TAKE 
        """
        sql_q5 = """SELECT 'TABLE_SIZE_AND_EXTERNAL_SIZE' AS QUERY_TYPE, CURRENT_DATE AS PROCESS_DT, 'TABLE' AS ENTITY_TYPE, SCHEMANAME AS CONTAINTER_NAME,
                        RELNAME AS ENTITY_NAME,
                        PG_SIZE_PRETTY(PG_TOTAL_RELATION_SIZE(RELID)) AS SIZE1,
                        PG_SIZE_PRETTY(PG_TOTAL_RELATION_SIZE(RELID) - PG_RELATION_SIZE(RELID)) AS SIZE2,
                        PG_SIZE_PRETTY(SUM(TABLE_SIZE::BIGINT)::BIGINT) AS SIZE1, SUM(NUM_PARTITIONS) AS NUM_PARTITIONS, SUM(ROW_CNT) AS NO_OF_ROWS
                        FROM PG_CATALOG.PG_STATIO_USER_TABLES
                        WHERE UPPER(SCEHAMANAME) IN ('DM','DOPS','STAGE','DM')
                        ORDER BY PG_TOTAL_RELATION_SIZE(RELID) DESC;
                        """
        q5_cmd = self.gp_cmd_func(sql_q5)
        logging.info("q5_cmd : " + q5_cmd)
        q5_res = util.run_shell_cmd(q5_cmd)
        logging.info("q5_res : " + q5_res)
        q5_ret = self.write_results(q5_res, 7)
        logging.info("q5_ret : " + str(q5_ret))

if __name__ == '__main__':
    global base_dir
    global dir_sep

    dir_sep = '/'
    base_dir = os.environ.get('HOME')
    log_dir = os.path.expanduser('~') + dir_sep + 'log' + dir_sep
    log_file = log_dir + 'gp_metrics_' + datetime.now().strftime("%Y%m%d%H%M%S") + '.log'

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename=log_file)
    ts = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    logging.info("log_file is " + log_file)
    logging.info("Base Directory is " + base_dir)

    gp = gp_metric()
    logging.info(gp.sql_cmds())
    



















