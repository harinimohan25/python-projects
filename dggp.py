from utilities import *
form dqbase import DataQuality
from MTConfig import Config

class DQGP(DataQuality):
	def create_dq_cmd(self):
		l_temp_dq_query_file = Config.log_dir + datetime.now().strftime("%Y%m%d%H%M%S") + ".sql"
		try:
			dq_query = self.build_dq_query()
			write_content(l_temp_dq_query_file, dq_query, "Y")
			l_dq_cmd = "export PGPASSWORD=" + self.db_user_psw + " && psql -h " + self.db_server.lower() + " -U " + self.db_user.lower() + \
					   " -p " + os.environ['GPPORT'] + ' -e -f "' + l_temp_dq_query_file + '"'
			l_dq_cmd = (l_dq_cmd).strip("\n") + " > " + self.dq_result_file
			return l_dq_cmd
		except Exception as err:
			abort_with_msg("An exception of type " + type(err).__name__ + "occured. Arguments: "+ str(err.args))