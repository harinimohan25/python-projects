from platform import system
from timeit import default-timer as timer
from datetime import datetime,timedelta
from time import time
from mySQLMetadata import mySQLMetadata
from mySQLHandler import mySQLHandler
from PurgeData import PurgeData
from jobCalendarCheck import jobCalendarCheck
import re, csv, subprocess, os, logging, utilities, optparse
from lxml import etree
from commands imports getstatusoutput

# Coder: Harini Mohana Sundaram
# Subject: This code is to parse the xml and convert to a csv

class LocalValues:
	db_nm = ''
	tbl_nm = ''
	date_to_process = ''
	date_till_process = ''
	job_pid = 0
	process_start = datetime.now()
	log_filename = ''
	mysql_prop_file = ''
	rowcount = 0
	audit_tbl = 'ops_jobstat'
	filename = ''

def set_audit_action():
	if LocalValues.job_pid == 0:
		LocalValues.job_pid = int(round(time() * 1000))
		LocalValues.process_start = datetime.now()
		return 'insert'
	else:
		return 'update'

def audit_log(audit_action, arg_status_msg):
	l_process_end = datetime.now()
	utilities.GlobalValues.record = {'feed': LocalValues.tbl_nm,
				'jobpid': LocalValues.job_id,
				'filename' : LocalValues.filename,
				'process_start' : LocalValues.process_start,
				'process_end' : l_process_end,
				'linecount' : 0,
				'rowcount' : LocalValues.rowCount,
				'filedate' : LocalValues.date_to_process,
				'status': arg_status_msg,
				'message': '',
				'raw_size' : 0,
				'raw_replica_size' : 0,
				'refined_size' : 0,
				'refined_replica_size' : 0,
				'partition_col' : '',
				'file_end_dt' : LocalValues.date_to_process,
				'refined_count' : 0
			}
	utilities.GlobalValues.mySQLHandler_instance.perform_DML(arg_action=audit_action, arg_record=utilities.GlobalValues.record)

def checkRC(exitcode, message):
	ts = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
	logging.debug(ts + ": [[ERROR]] " + message)
	sys.exit(exitcode)
	
def missing_tag(vXMLData, vKeyCol):
	tag = vXMLData.find(vKeyCol)
	if tag is None:
		return True
	else:
		return False
	
def get_XML_data(vXMLData, vKeyCol, vTagList):
	if vKeyCol is not None:
		if missing_tag(vXMLData, vKeyCol):
			print("Missing Key Column in the XML: " + vXMLData.find(vKeyCol).text)
			return None
	info = []
	for tag in vTagList:
		node = vXMLData.find(tag)
		if node is not None and node.text:
			if tag == vKeyCol:
				info.append(node.text)
			else:
				info.append(node.text.encode("utf-8"))
		else:
			info.append("")
	return info

def run_shell_cmd(arg_shell_cmd, audit_action):
		try:
			ts = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
			logging.info(ts + ": [[INFO]] " + arg_shell_cmd)
			p=subprocess.Popen(arg_shell_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
			(output, error) = p.communicate()
			rc = p.wait()
			if rc != 0 and error is not None:
				logging.info(ts + ": [[INFO]] Output from Shell Execution: " + output)
				logging.debug(ts + ": [[ERROR]] Shell Execution fauled with return_code " + str(rc) + " and error " + str(error))
			else:
				logging.info(ts + ": [[INFO]] Output from Shell Execution: " + output)
			return rc
		except Exception as err:
			logging.debug(ts + ": [[ERROR]] An exception of type  " +  type(err).__name__ + "occurred. Arguments: " + str(err.args))
			audit_log(audit_action, 'Shell Command Failed: ' + arg_shell_cmd)
			checkRC(99, 'Shell Execution Failed')
			
def check_csv_cnt(output_filename):
	try:
		ts = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
		vCnt = getstatusoutput('wc -l ' + output_filename)
		if vCnt is None:
			logging.debug(ts + ":[[ERROR]] CSV Count is not returning" + str(vCnt))
		else:
			logging.info(ts + ":[[INFO]] CSV Count is : " + str(vCnt))
		return vCnt
	except Exception as err:
		logging.debug(ts + ": [[ERROR]] An Exception of type " + type(err).__name__ + "occurred. Arguments: " + str(err.args))
		audit_log(audit_action, 'CSV File ERror. Not Able to retrieve count  from the CSV file')
		checkRC(99,'CSV Check Count Failed')
		
def main():
	global base_dir
	global dir_sep
	start_time = timer()
	parser = optparse.OptionParser(usage="usage: %prog [options values]",
					version="%prog 2.0")
	parser.add_option('-f', '--FeedName',
			 help='Name of the Feed that needs to be executed',
			 dest=vFeedName)
	parser.add_option('-x', '--XMLName',
			 help='Name of the XML that needs to be parsed and converted to XML',
			 dest=vXMLName)
	parser.add_option('-s', '--StartDate',
			 help='Date used to create the CSV file',
			 dest='vFromDate',
			 default=None)
	parser.add_option('-k', '--KeyColumn',
			 help='Keycolumn based on which if a line needs to be written into CSV',
			 dest='vKeyCol',
			 default=None)
	parser.add_option('-l', '--OutputFileLocation',
			 help='Ouptut File Location ',
			 dest='vOutputDir',
			 default=None)
	parser.add_option('-r', '--RootTagName',
			 help='Root Tag Name where the search for the XML Tags should start. Give the most granular rooot tag from where the traversal should start',
			 dest='vRoot',
			 default=None)

	current_os = system()
	if current_os != "Windows":
		dir_sep = '/'
	else:
		dir_sep = '\\'
			    
	(opts, args) = parser.parse_args()
	base_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + dir_sep
	log_dir=LOGDIR
	if opts.vFeedName is None:
		parser.print_help()
		checkRC("99","Please provide Feed Name")
	if opts.vXMLName is None:
		parser.print_help()
		checkRC("99","Please provide XML File Name with Path")
	if opts.vFromDate is None:
		parser.print_help()
		checkRC("99","Please provide a From Date")
	if opts.vOutputDir is None:
		parser.print_help()
		checkRC("99","Please provide a Output Directory where the csv file should be placed")
	if opts.vRoot is None:
		parser.print_help()
		checkRC("99","Please provide a Root Tag Names else XML Parsong cannot be done")
	
	vOutputFileName = opts.vOutputDir + dir_sep + opts.vFeedName + "." + opts.vFromDate + ".csv"
	vLogFileName = log_dir + opts.vFeedName + "." + opts.vFromDate + "." + datetime.now().strftime("%Y%m%d%H%M%S") + ".log"
	logging.baseConfig(filename=vLogFileName, filemode='w', level=logging.DEBUG)
	logging.info("Logging Started")
	logging.info(datetime.now().strftime("%Y%m%d%H%M%S") + ": [[INFO]] Log FileName: " + vLogFileName)
	logging.info(datetime.now().strftime("%Y%m%d%H%M%S") + ": [[INFO]] XML Parsing Started)
	LocalValues.db_nm = "FILE"
	LocalValues.tbl_nm = opts.vFeedName.strip().upper()
	LocalValues.date_to_process = opts.vFromDate
	LocalValues.mysql_prop_file = base_dir + 'common' + dir_sep + 'ENV.mySQL.properties'
	LocalValues.process_start = datetime.now()
	print(datetime.now().strftime("%Y%m%d%H%M%S") + ": [[INFO]]	Prop File: " + LocalValues.mysql_prop_file)
	utilities.mySQLhdr = mySQLHandler(LocalValues.mysql_prop_file)
	utilities.GlobalValues.mySQLHandler_instance = mySQLHandler(LocalValues.mysql_prop_file)
	mysql_meta = mySQLMetadata(LocalValues.mysql_prop_file)
	col_meta = mysql_meta.read_column_metadata(sourceDB=LocalValues, sourceTable=LocalValues.tbl_nm) 
	audit_action = set_audit_action()
	audit_log(audit_action, "XML Parsubg Started")
	audit_action = set_audit_action()
	
	# Build the header list
	vTagList =  []
	for i, v in enumerate(col_meta):
		vTagList.append(v["SOURCECOLUMN"])
	if vTagList is None:
		audit_log(audit_action, 'Header TagList Creation Failed')
		logging.info(datetime.now().strftime("%Y%m%d%H%M%S") + ": [[ERROR]] Header TagList Creation Failed")
		checkRC("99","Header TagList Creation Failed")
	
	vOutData = []
	parser = etree.XMLParser(recover=True, remove_blank_text=True)
	root = etree.parse(opts.vXMLName, parser)
	print(str(type(root)) + ' ' + str(root))
	if root is None:
		audit_log(audit_action, "Parsing FAiled")
		logging.info(datetime.now().strftime("%Y%m%d%H%M%S") + ": [[ERROR]] Parsing FAiled")
		checkRC("99","XML Parsing Failed")
		
	# add field names by copying tag_list
	vRootTag = ".//" + opts.vRoot
	vTags = root.findall(vRootTag)
	for i in vTags:
		vXMLTags = get_XML_data(i,opts.vKeyCol,vTagList)
		if vXMLTags:
			vOutData.append(vXMLTags)
		else:
		audit_log(audit_action, "Parsing FAiled")
		logging.info(datetime.now().strftime("%Y%m%d%H%M%S") + ": [[ERROR]] Parsing FAiled")
		checkRC("99","XML Parsing Failed")

	logging.info(datetime.now().strftime("%Y%m%d%H%M%S") + ": [[INFO]] XML Parsing Completed")
	logging.info(datetime.now().strftime("%Y%m%d%H%M%S") + ": [[INFO]] Starting CSV File Write: " + vOutputFileName)
	
	# Writing to a CSV file
	vOutFile = open(vOutputFileName, "wb")
	csv_writer = csv.writer(vOutFile, quoting=csv.QUOTE_MINIMAL)
	for row in vOutData:
		csv_writer.writerow(row)
	vOutFile.close()
	logging.info(datetime.now().strftime("%Y%m%d%H%M%S") + ": [[INFO]] Complete CSV File WRite: " + vOutputFileName)
	logging.info(datetime.now().strftime("%Y%m%d%H%M%S") + ": [[INFO]] Checking if the csv has any data. If not abort the process")
	vCnt = check_csv_cnt(vOutputFileName)
	LocalValues.rowcount = vCnt[0]
	logging.info(datetime.now().strftime("%Y%m%d%H%M%S") + ": [[INFO]] Remove the XML File from Edge Node")
	run_shell_cmd("rm -f -r " + opts.vXMLName, audit_action)
	audit_log(audit_action, 'XML Parsing Completed')
	

if __name__ == '__main__':
	main()		
		
		