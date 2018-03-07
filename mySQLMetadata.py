import pymysql
import ConfigParser
from utilities import abort_with_msg, print_info, print_warn
from datetime import datetime
import os, csv, sys, base64
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

class mySQLMetadata(object):
    select_table_metadata_sql = "SELECT * FROM %s WHERE SOURCESCHEMA = '%s' AND SOURCETBLNM = '%s' "
    select_column_metadata_sql = "SELECT * FROM %s WHERE SOURCESCHEMA = '%s' AND SOURCETBLNM = '%s'" \
                                 "AND IS_EXTRACTED='Y'" \
                                 " ORDER BY SEQ_NB"
    select_cal_metdata_sql = "SELECT substr(DAYNAME(DATE('%s') - INTERVAL LAG_DAYS DAY),1,3) AS DAY_OF_FROM_DT " \
                             ", DATE('%s') - INTERVAL LAG_DAYS DAY AS EFF_FROM_DT" \
                             ", RUN_ON_HOLIDAY " \
                             ", RUN_ON_MON " \
                             ", RUN_ON_TUE " \
                             ", RUN_ON_WED " \
                             ", RUN_ON_THU " \
                             ", RUN_ON_FRI " \
                             ", RUN_ON_SAT " \
                             ", RUN_ON_SUN " \
                             ", HOLIDAY_DT " \
                             ", HOLIDAY_DESC " \
                             " FROM METADATA_FOR_JOB_RUN LEFT JOIN HOLIDAY_CAL ON HOLIDAY_DT = DATE('%s') - INTERVAL LAG_DAYS_DAY " \
                             " WHERE SOURCESCHEMA = '%s' AND SOURCETBLNM = '%s' "

    select_ops_jobstat_sql = "SELECT * FROM '%s' where feed = '%s' and filedate = '%s' and file_end_dt = '%s' "

    select_ops_jobstat_max_sql = "SELECT max(file_end_dt) as max_end_dt from %s where feed='%s' AND status = 'COMPLETED SUCCESSFULLY'"

    select_dq_table_metadata_sql = "select entity_group_xref.group_nm, " \
                                   " entity_group_xref.sub_group_nm," \
                                   " entity_group_xref.filename," \
                                   " metadata_dq.src_qry_scr, " \
                                   " metadata_dq.tgt_qry_scr, " \
                                   " METADATA_FOR_TBL.db_type, " \ 
                                   " METADATA_FOR_TBL.db_server, " \
                                   " METADATA_FOR_TBL.db_user" \
                                   " from dops.metadata_dq metadata_dq, " \
                                   " dops.grouping grouping, " \
                                   " dops.entity_group_xref entity_group_xref, " \
                                   " dops.METADATA_FOR_TBL METADATA_FOR_TBL " \
                                   " where metadata_dq.sub_group_nm = entity_group_xref.sub_group_nm " \
                                   " and grouping.group_nm = and entity_group_xref.group_nm " \
                                   " and entity_group_xref.sourceschema = METADATA_FOR_TBL.sourceschema " \
                                   " and entity_group_xref.sourcetblnm = METADATA_FOR_TBL.sourcetblnm " \
                                   " and grouping.dq_chk_flg = 1 " \
                                   " and entity_group_xref.sub_group_nm is not null " \
                                   " and entity_group_xref.sub_group_nm <> '' " \
                                   " and entity_group_xref.group_nm = '%s' "

    select_ops_jobstat_linecnt_sql = "SELECT SUBSTRING_INDEX(GROUP_CONCAT(CAST(stat.linecount as CHAR) ORDER BY stat.process_end DESC), ',", 1)" \
                                     "AS linecount from dops.ops_jobstat stat where stat.feed = '%s' and stat.filedate = '%s' and stat.status = 'EXTRACT_COMPLETED' "

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

    def read_table_metadata(self, sourceDB, sourceTable):
        conn = pymysql.connect(host=self.db['host'], port=self.db['port'], user=self.db['user'], passwd=self.db['dbpassword'], db.self.db['metadata_schema'],
                                   cursorclass=pymysql.cursors.DictCursor)
        cursor = conn.cursor()
        cursor.execute(mySQLMetadata.select_table_metadata_sql % (self.db['tbl_meta'], sourceDB, sourceTable))
        result = cursor.fetchall()[0]
        if result is None:
            print("No data found for " + sourceDB + "," + sourceTable)
            sys.exit()
        tbl_metadata = {'src_db' : result['SOURCESCHEMA'].upper(),
                        'src_tbl': result['SOURCETBLNM'].upper(),
                        'rfrsh_rate': result['REFRESHRATE'].upper(),
                        'delta_col': result['DELTACOLUMN'].upper(),
                        'delta_col_typ': result['DELTACOLUMNTYPE'].upper(),
                        'delta_col_fmt': result['DELTACOLUMNFMT'].upper(),
                        'db_type': result['DB_TYPE'].upper(),
                        'db_server': result['DB_SERVER'].upper(),
                        'db_user': result['DB_USER'].upper(),
                        'extract_landing_dir': (result['EXTRACTLANDINGDIR']+'/' if result['EXTRACTLANDINGDIR'][-1:] != '/' else result['EXTRACTLANDINGDIR']),
                        'hdfs_basedir': result['HDFS_BASEDIR'].lower(),
                        'hdfs_extract_dir': result['HDFSEXTRACTDIR'].strip().lower(),
                        'stg_tbl_delimiter': result['DELIMITER'] if result['DELIMITER'] else '',
                        'tgt_tbl_bkt_clause': result['BUCKETINGCLAUSE'].upper(),
                        'stg_db': result['TGT_STG_DB'].upper(),
                        'tgt_db': result['TGT_DB'].upper(),
                        'hive_tbl_truncate': result['HIVE_TBL_TRUNCATE'].upper(),
                        'tgt_tbl': result['TRGTTBLNM'].upper(),
                        'hive_raw_retention': str(result['HIVE_RAW_RETENTION'] if result['HIVE_RAW_RETENTION'] else 0 ),
                        'hive_refined_retention': str(result['HIVE_REFINED_RETENTION'] if result['HIVE_REFINED_RETENTION'] else 0 ),
                        'hdfs_raw_dir': result['HIVE_RAW_DIR'].strip().lower(),
                        'hdfs_refined_dir': result['HIVE_REFINED_DIR'].strip().lower(),
                        'refresh_refined_DDL_DML': result['REFRESH_REFINED_DDL_DML'].upper() if result['REFRESH_REFINED_DDL_DML'].upper() else "Y",
                        'v1_support': result['V1SUPPORT'].upper(),
                        'jpmisdaf_num': result['JPMISDAF_NUM'].upper() if result['JPMISDAF_NUM'].upper() else '',
                        'source_sql_txt': result['SOURCE_SQL_TXT'].upper() if result['SOURCE_SQL_TXT'].upper() else '',
                        'split_by': result['SPLIT_BY'] if result['SPLIT_BY'] else '',
                        'file_format': result['RFND_FILE_FMT'] if result['RFND_FILE_FMT'] else ''
                        }
        cursor.close()
        conn.close()
        return  tbl_metadata


    def read_column_metadata(self, sourceDB, sourceTable):
        try:
            conn = pymysql.connect(host=self.db['host'], port=self.db['port'], user=self.db['user'], passwd=self.db['dbpassword'], db.self.db['metadata_schema'],
                                   cursorclass=pymysql.cursors.DictCursor)
            cursor = conn.cursor()
            cursor.execute(mySQLMetadata.select_column_metadata_sql % (self.db['col_meta'], sourceDB, sourceTable))
            col_metadata_dict_list = cursor.fetchall()
            cursor.close()
            conn.close()
            return  col_metadata_dict_list
        except Exception as err:
            abort_with_msg("An exception of type " + type(err).__name__ + "occurred. Arguments:\n" + str(err.args))

    def read_cal_metadata(self, sourceDB, sourceTable, from_dt):
        try:
            conn = pymysql.connect(host=self.db['host'], port=self.db['port'], user=self.db['user'], passwd=self.db['dbpassword'], db.self.db['metadata_schema'],
                                   cursorclass=pymysql.cursors.DictCursor)
            cursor = conn.cursor()
            cursor.execute(mySQLMetadata.select_cal_metdata_sql % (from_dt, from_dt, from_dt, sourceDB, sourceTable))
            cal_metadata_dict_list = cursor.fetchall()
            cursor.close()
            conn.close()
            return  cal_metadata_dict_list
        except Exception as err:
            abort_with_msg("An exception of type " + type(err).__name__ + "occurred. Arguments:\n" + str(err.args))

    def read_ops_jobstat(self, feed, from_dt, end_dt):
        try:
            conn = pymysql.connect(host=self.db['host'], port=self.db['port'], user=self.db['user'], passwd=self.db['dbpassword'], db.self.db['metadata_schema'],
                                   cursorclass=pymysql.cursors.DictCursor)
            cursor = conn.cursor()
            cursor.execute(mySQLMetadata.select_dq_table_metadata_sql % (self.db['jobstat'], feed, from_dt, end_dt))
            ops_jobstat_dict_list = cursor.fetchall()
            cursor.close()
            conn.close()
            return  ops_jobstat_dict_list
        except Exception as err:
            abort_with_msg("An exception of type " + type(err).__name__ + "occurred. Arguments:\n" + str(err.args))

    def read_ops_jobstat_max(self, feed):
        try:
            conn = pymysql.connect(host=self.db['host'], port=self.db['port'], user=self.db['user'], passwd=self.db['dbpassword'], db.self.db['metadata_schema'],
                                   cursorclass=pymysql.cursors.DictCursor)
            cursor = conn.cursor()
            cursor.execute(mySQLMetadata.select_dq_table_metadata_sql % (self.db['jobstat'], feed))
            ops_jobstat_dict_list = cursor.fetchall()
            cursor.close()
            conn.close()
            if ops_jobstat_dict_list[0]['max_end_dt'] is not None:
                return  ops_jobstat_dict_list[0]['max_end_dt'].strftime('%Y%m%d%H%M%S')
            else:
                return None
        except Exception as err:
            abort_with_msg("An exception of type " + type(err).__name__ + "occurred. Arguments:\n" + str(err.args))

    def read_dq_metadata(self, group_name):
        l_dq_metadata = []
        try:
            conn = pymysql.connect(host=self.db['host'], port=self.db['port'], user=self.db['user'], passwd=self.db['dbpassword'], db.self.db['metadata_schema'],
                                   cursorclass=pymysql.cursors.DictCursor)
            cursor = conn.cursor()
            cursor.execute(mySQLMetadata.select_dq_table_metadata_sql % (group_name))
            l_dq_metadata = cursor.fetchall()
            return l_dq_metadata
        except Exception as err:
            abort_with_msg("An exception of type " + type(err).__name__ + "occurred. Arguments:\n" + str(err.args))
        finally:
            cursor.close()
            if conn.open:
                conn.close()

    def read_job_stat_by_feed_busdt(self, feed_nm, bus_dt):
        l_job_stat_dic_list = []
        try:
            conn = pymysql.connect(host=self.db['host'], port=self.db['port'], user=self.db['dbuser'], passwd=self.db['dbpassword'], db=self.db['metadata_schema'],cursorclass=pymysql.cursors.DictCursor)
            cursor = conn.cursor()
            cursor.execute(mySQLMetadata.select_ops_jobstat_linecnt_sql %(feed_nm,bus_dt))
            l_job_stat_dic_list = cursor.fetchall()
            return l_job_stat_dic_list
        except Exception as err:
            print("An exception of type " + type(err).__name__ + "occurred. Arguments:\n" + str(err.args))
        finally:
            cursor.close()
            if conn.open:
                conn.close()

def lower_key(in_dict):
    if type(in_dict) is dict:
        out_dict = {}
        for key, item in in_dict.items():
            out_dict[key.lower()] = lower_key(item)
        return  out_dict
    elif type(in_dict) is list:
        return [lower_key(obj) for obj in in_dict]
    else:
        return in_dict