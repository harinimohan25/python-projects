from utilities import *
form dqbase import DataQuality
from MTConfig import Config
from os.path import isfile

class DQGphdfspull(DataQuality):
	def create_dq_cmd(self):
		l_dq_tmplt = self.dq_template
		l_db_user_psw = self.db_user_psw
		try:
			if not isfile(l_dq_tmplt):
				abort_with_msg("Template {} for creating Data Quality query not found".format(l_dq_tmplt))
			#Calls Build DQ Validation Query
			l_dq_query = self.build_dq_query()
			with open(l_dq_tmplt) as file_read_handle:
				l_file_content = file_read_handle.read().replace("<<CONNECTION_STRING>>",self.db_server)
				l_file_content = l_file_content.replace("<<DB_USER_NAME>>",self.db_user)
				l_file_content = l_file_content.replace("<<DB_PASSWORD>>",l_db_user_psw)
				l_file_content = l_file_content.replace("<<SQL_OUTPUT>>",self.dq_result_file)
				l_file_content = l_file_content.replace("<<SQL_QUERY>>",l_dq_query)
			
			l_dq_cmd = l_file_content
			return l_dq_cmd
			
		except Exception as err:
			abort_with_msg("An exception of type " + type(err).__name__ + "occured. Arguments: "+ str(err.args))