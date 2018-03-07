import mySQLHandler
import re
from os.path import isfile
from MTConfig import Config
from utilities import *

class DataQuality:
    def __init__(self, db_type, db_server_details, db_user, dq_query_details, dq_template, dq_query_args_list, dq_result_file):
        self.db_type = db_type
        self.db_server = db_server_details
        self.db_user = db_user
        self.db_user_psw = get_passwd(arg_user=db_user.lower(), arg_passwd_file=Config.pass_wrd_file)
        self.dq_query_details = dq_query_details
        self.template = dq_template
        self.dq_result_file = dq_result_file
        self.dq_query_arg_list = dq_query_args_list

    def build_dq_query(self):
        dq_query = None
        try:
            if isfile(self.dq_query_details):
                with open(self.dq_query_details) as file_handle:
                    if dq_query is None:
                        dq_query = file_handle.read()
                        for key, value in self.dq_query_arg_list.items():
                            dq_query = dq_query.replace("<<" + key + ">>", value)
                    else:
                        for key, value in self.dq_query_arg_list.items():
                            if dq_query is None:
                               dq_query = self.dq_query_details.replace("<<" + key + ">>", value)
                            else:
                                dq_query = dq_query.replace("<<" + key + ">>", value)
            print dq_query
            return dq_query.strip("\n\r").strip("\n")
        except Exception as err:
            abort_with_msg("An exception of type " + type(err).__name__ + "occurred. Arguments:\n" + str(err.args))

    def isLogMessage(self,line):
        return False

    def create_dq_cmd(self):
        raise NotImplementedError

    def run_dq_cmd(self,dq_cmd):
        try:
            run_shell_cmd(arg_shell_cmd=dq_cmd)
        except Exception as err:
            abort_with_msg("An exception of type " + type(err).__name__ + "occurred. Arguments:\n" + str(err.args))

    def parse_output_result(self):
        l_list = []
        l_column_names = []
        l_column_values = []
        l_result_dict = {}
        try:
            with open(self.dq_result_file) as file_handle:
                content = file_handle.read()
                for line in content.splitlines():
                    if self.isLogMessage(line):
                        continue

                    if re.search('[0-9a-zA-Z]+', line):
                        l_list.append(re.sub("[\s]+","", line.strip(" |\n")))
            if len(l_list) != 2:
                if l_list[0] != "count":
                    raise RuntimeError("Invalid DQ Result. Expecting one entry for Column Names and one entry for stats")
            l_column_names = l_list[0].split('|')
            l_column_values = l_list[1].split('|')

            for val in range(len(l_column_names)):
                l_result_dict[l_column_names[val]] = l_column_values[val]
            return l_result_dict
        except Exception as err:
            abort_with_msg("An exception of type " + type(err).__name__ + "occurred. Arguments:\n" + str(err.args))
        finally:
            file_handle.close()

                













