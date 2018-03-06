from utilities import *
from dqbase import DataQuality
from MTConfig import Config

class DQSqoop(DataQuality):
    def create_dq_cmd(self):
        l_dq_template = self.dq_template
        l_tmp_db_user_psw_path = Config.log_dir + self.db_type + "_" + self.db_user + ".pfile"
        l_db_user_psw = self.db_user_psw

        try:
            dq_query = self.build_dq_query()
            if dq_query.endswith(';'):
                dq_query = dq_query[:-1]
            dq_query = '"' + dq_query + '"'
            if not isfile(l_dq_template):
                abort_with_msg("Template {} for creating Data Quality query not found.".format(l_dq_template))
            write_content(l_tmp_db_user_psw_path, l_db_user_psw, 'Y')
            l_file_password = "file://" + l_tmp_db_user_psw_path

            with open(l_dq_template) as file_read_handle:
                l_dq_cmd = file_read_handle.read().replace("<<CONNECTION_STRING>>",self.db_server)
                l_dq_cmd = l_dq_cmd.replace("<<DB_USER_NAME>>",self.db_user)
                l_dq_cmd = l_dq_cmd.replace("<<DB_PASSWORD>>", l_file_password)
                l_dq_cmd = l_dq_cmd.replace("<<SQOOP_QUERY>>", dq_query)
                if "ORACLE" in self.db_type:
                    l_dq_cmd = l_dq_cmd.replace("<<DB_DRIVE>>", "")
                    l_dq_cmd = l_dq_cmd.replace("<<DB_CONNECTION_PREFIX>>", "jdbc:oracle:thin:@")

                if "SYBASE" in self.db_type:
                    l_dq_cmd = l_dq_cmd.replace("<<DB_DRIVE>>", "--driver com.sybase.jdbc4.jdbc.SybDriver")
                    l_dq_cmd = l_dq_cmd.replace("<<DB_CONNECTION_PREFIX>>", "jdbc:sybase:Tds:")

                if "SQLSERVER" in self.db_type:
                    l_dq_cmd = l_dq_cmd.replace("<<DB_DRIVE>>", "--driver com.microsoft.sqlserver.jdbc.SQLServerDriver")
                    l_dq_cmd = l_dq_cmd.replace("<<DB_CONNECTION_PREFIX>>", "jdbc:sqlserver://")

                if self.db_type == 'ORACLE-SQOOP-TNS':
                    l_dq_cmd = l_dq_cmd.replace("<<MAPRED_JAVA_OPTS>>", "-D mapred.map.child.java.opts='-Doracle.net.tns_admin=.'")
                    l_dq_cmd = l_dq_cmd.replace("<<FILES_OPTION>>", "-files $TNS_ADMIN/tnsnames.ora,$TNS_ADMIN/sqlnet.ora")
                elif self.db_type == 'ORACLE-SQOOP' or self.db_type == 'SYBASE-SQOOP' or self.db_type == 'SQLSERVER-SQOOP' or self.db_type == 'ORACLE':
                    l_dq_cmd = l_dq_cmd.replace("<<MAPRED_JAVA_OPTS>>","" ).replace("<<FILES_OPTION>>","")

            l_dq_cmd = (l_dq_cmd).strip("\n") + " > " + self.dq_result_file
            return l_dq_cmd
        except Exception as err:
            abort_with_msg("An exception of type " + type(err).__name__ + "occured. Arguments:\n" + str(err.args))

    def isLogMessage(self, line):
        err_words = ['Warning: /opt/cloudera/parcels/CDH5.9.1-1.cdh5.9.1.p0.4/bin/../lib/sqoop/../accumulo does not exists! Accumulo imports will fail.', 'Please set $ACCUMULO_HOME to the root of your Accumulo installation.']
        for err_word in err_words:
            if line == err_word:
                return True
        return False
    