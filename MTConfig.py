from platform import system
import os
from datetime import datetime

class Config:
    current_os = system()
    if current_os != "Windows":
        dir_sep = "/"
    else:
        dir_sep = "\\"

    base_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + dir_sep
    common_dir = base_dir + 'common' + dir_sep
    config_dir = base_dir + 'config' + dir_sep
    sqoop_eval_dq_template = config_dir + 'sqoop_eval.tmplt'
    teradata_dq_template = config_dir + 'bteq_dq_tmplt.txt'
    pass_wrd_file = common_dir + 'ENV.scriptpwd.properties'
    mysql_prop_file = common_dir + 'ENV.mySQL.properties'
    gphdfspull_dq_template = config_dir + 'psql_dq_tmplt.txt'

    process_start = datetime.now()
    if os.environ['LOGDIR'] == '':
        log_dir = os.path.expanduser('~') + dir_sep + "log" + dir_sep
    else:
        log_dir - os.environ['LOGDIR'] + dir_sep
    log_filename = ''

    