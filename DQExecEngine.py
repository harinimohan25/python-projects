from dqsqoop import DQSqoop
from dqteradata import DQTeradata
from dqhive import DQHive
from dqjobstat import DQJobStat
from MTConfig import Config
from utilities import *
from dqhdfspull import DQGphdfspull

class DQExec:
    def executedq(self, dq_metadata, db_type, query, dq_qry_args, dq_rslt_file):
        dqObj = None
        try:
            if db_type == "TERADATA":
                dqObj = self.teradataDQExecutor(dq_metadata, db_type, query, dq_qry_args, dq_rslt_file)
            elif db_type = "ORACLE-SQOOP":
                dqObj = self.sqoopDQExecutor(dq_metadata, db_type, query, dq_qry_args, dq_rslt_file)
            elif db_type = "ORACLE":
                dqObj = self.sqoopDQExecutor(dq_metadata, db_type, query, dq_qry_args, dq_rslt_file)
            elif db_type = "SYBASE-SQOOP":
                dqObj = self.sqoopDQExecutor(dq_metadata, db_type, query, dq_qry_args, dq_rslt_file)
            elif db_type = "HIVE":
                dqObj = self.hiveDQExecutor(dq_metadata, db_type, query, dq_qry_args, dq_rslt_file)
            elif db_type = "JOBSTAT":
                dqObj = self.jobstatDQExecutor(dq_metadata, db_type, query, dq_qry_args, dq_rslt_file)
            elif db_type = "GPHDFS-PULL":
                dqObj = self.gphdfspullDQExecutor(dq_metadata, db_type, query, dq_qry_args, dq_rslt_file)

            query = dqObj.create_dq_cmd()
            print "Exe Query: " + query

            dqObj.run_dq_cmd(query)
            return dqObj.parse_output_result()
        except Exception as err:
            abort_with_msg("An exception of type " + type(err).__name__ + "occurred. Arguments:\n" + str(err.args))

    def teradataDQExecutor(self,dq_md,db_type,query,dq_qry_args,dq_rslt_file):
        return DQTeradata(db_type,dq_md['db_server'], dq_md['db_user'], query, Config.teradata_dq_template, dq_qry_args, dq_rslt_file)

    def sqoopDQExecutor(self,dq_md,db_type,query,dq_qry_args,dq_rslt_file):
        return DQSqoop(db_type,dq_md['db_server'], dq_md['db_user'], query, Config.sqoop_eval_dq_template, dq_qry_args, dq_rslt_file)

    def hiveDQExecutor(self,dq_md,db_type,query,dq_qry_args,dq_rslt_file):
        return DQHive(db_type,dq_md['db_server'], dq_md['db_user'], query, None, dq_qry_args, dq_rslt_file)

    def jobstatDQExecutor(self, dq_md, db_type, query, dq_qry_args, dq_rslt_file):
        return DQHive(db_type, dq_md['db_server'], dq_md['db_user'], query, None, dq_qry_args, dq_rslt_file)

    def gphdfspullDQExecutor(self, dq_md, db_type, query, dq_qry_args, dq_rslt_file):
        return DQGphdfspull(db_type, dq_md['db_server'], dq_md['db_user'], query, Config.gphdfspull_dq_template, dq_qry_args, dq_rslt_file)
