from utilities import *
from dqbase import DataQuality
from MTConfig import Config
from mySQLMetdata import mySQLMetadata

class DQJobStat(DataQuality):
    def create_dq_cmd(self):
        return ""

    def run_dq_cmd(self,query):
        self.job_stat = self.read_ops_jobstat_busdate(self.dq_query_arg_list['FEED_NM'],self.dq_query_arg_list['FROM_DT'])

    def parse_output_result(self):
        if not self.job_stat or self.job_stat[0]['linecount'] is None:
            abort_with_msg("No Job Stat found for Feed [" + self.dq_query_arg_list['FEED_NM'] + "] , BusDate [" + self.dq_query_arg_list['FROM_DT'] + "]")
        return self.job_stat[0]

    def read_ops_jobstat_busdate(self, feed_nm, busdate):
        mysql_metadata = mySQLMetadata(Config.mysql_prop_file)
        return mysql_metadata.read_job_stat_by_feed_busdt(feed_nm,busdate)
