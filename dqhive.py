from utilities import *
from dqbase import DataQuality
class DQHive(DataQuality):
    def create_dq_cmd(self):
        try:
            dq_query = self.build_dq_query()
            l_dq_cmd = 'beeline -u $HIVEHOST -e "' + dq_query + '" --hiveconf mapred.job.queue.name=$HIVEQUEUE'
            l_dq_cmd = (l_dq_cmd).strip("\n") + " > " + self.dq_result_file
            return l_dq_cmd
        except Exception as err:
            abort_with_msg("An exception of type " + type(err).__name__ + "occured. Arguments:\n" + str(err.args))
            