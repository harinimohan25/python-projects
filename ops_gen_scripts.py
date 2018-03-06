from platform import system
from timeit import default-timer as timer
from datetime import datetime,timedelta
from time import time
from mySQLMetadata import mySQLMetadata
from mySQLHandler import mySQLHandler
import os
import getparse, utilities, logging
from PurgeData import PurgeData
from jobCalendarCheck import jobCalendarCheck
import re
from framework_settings import *

class LocalValues:
	db_nm = ''
	tbl_nm = ''
	date_to_process = ''
	date_till_process = ''
	job_pid = 0
	process_start = datetime.now()
	log_filename = ''
	mysql_prop_file = ''
	src_row_count = 0
	rcvd_row_count = 0
	audit_tbl = 'ops_jobstat'
	raw_size = ''
	raw_replica_size = ''
	refined_size = ''
	refined_replica_size = ''
	filename = ''
	partition_col = ''
	refined_count = 0
	custom_extract = 'N'
	framework_version = '3.6.3'

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
				'linecount' : LocalValues.src_row_count,
				'rowcount' : LocalValues.rcvd_row_count,
				'filedate' : LocalValues.date_to_process,
				'status': arg_status_msg,
				'message': '',
				'raw_size' : LocalValues.raw_size,
				'raw_replica_size' : LocalValues.raw_replica_size,
				'refined_size' : LocalValues.refined_size,
				'refined_replica_size' : LocalValues.refined_replica_size,
				'partition_col' : LocalValues.partition_col,
				'file_end_dt' : LocalValues.date_till_process,
				'refined_count' : LocalValues.refined_count			
			}
	utilities.GlobalValues.mySQLHandler_instance.perform_DML(arg_action=audit_action, arg_record=utilities.GlobalValues.record)

def main():
	start_time = timer()
	parser = optparse.OptionParser(usage="usage: %prog [options values]",
					version="%prog 3.6.3")
	parser.add_option('-d', '--dbname',
			 help='Database of Table to be fork lifted into Hive',
			 dest=db_name)
	parser.add_option('-t', '--table',
			 help='Table name to be fork lifted into Hive',
			 dest=table_name)
	parser.add_option('-f', '--fromDate',
			 help='Optional: Data used to filter rows from source and also in name of extract file',
			 dest='fromDate',
			 default=None)
	parser.add_option('-e', '--endDate',
			 help='OptionalL End date used to filter rows from source for a range'
			      'Passing this fetches rows between start and end dates, inclusive of the dates passed',
			 dest='endDate'm
			 default=None)
	parser.add_option('-o', '--operator',
			 help='Optional: Operator to be used to filter rows. Possible values: >, >=, <, <=, =, <>'
			 dest='operator'
			 default='=')
	parser.add_option('-r', '--runSwitch',
			 help='Optional: Use this to run script with limited functionality.'
			      'Possible Values: G for Generate Scripts'
			      '\t\t\t E to execute Data Extraction Scripts'
			      '\t\t\t L to execute Load to Target Scripts'
			      '\t\t\t F to take complex query from SOURCE_SQL_TXT metadata'
			      '\t\t\t Q to execute Data Quality Check scripts'
			      '\t\t\t GELQ (default) does both of the above',
			 dest='runSwitch',
			 default='GEL')
	parser.add_option('-p','--persistExtract',
			  help='Optional: Used only for DEV or Debugging'
				'Set to N by default, will be set to Y only during dev calls to extract generated are not lost'
			  dest='persistExtract',
			  default=None)
	parser.add_option('-s', action='store_true',
			  help='Optional: Override the ops_jobstat status check',
			  dest='statOverride',
			  default=None)
	parser.add_option('-c',action='store_true',
			  help='Optional: Override for using HQLs from staging/deloy',
			  dest='useStage',
			  default=None)
	parser.add_option('-x','--extract',
			  help='Custom extract file to override generated extract feature - Requires a part from '
				'Command Line or from metadata column SOURCE_SQL_TEXT',
			  dest='extract_path',
			  default=None)
	parser.add_option('-m','--mysqlprop',
			  help='MySQL properties file override for switching to another set of meta tables',
			  dest='mysql_prop',
			 default=''),
	parser.add_option('-u','--usepostprocessor',
			  help='Post Processor Attachmnent, accepts a switch 'Y' for default'
			       'processing or accepts a dictionary of post processor switch and their values'
			  dest='postprocess_opts',
			  default='')
	parser.add_option('-l','--landingpath',
			  help='Custom Extract Landing path with filename to override generated extract landing path features. Requires a path from 
				command line argument',
			  dest='landing_path',
			  default='')

	current_os = system()
	if current_os != "Windows":
		dir_sep = '/'
	else:
		dir_sep = '\\'
			    
	(opts, args) = parser.parse_args()
	if opts.db_name is None or opts.table_name is None:
		parser.print_help()
		exit(-1)

	base_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + dir_sep
	base_fid_path = os.path.expanduser('-') + dir_sep
	log_dir=LOGDIR
	if not os.path.exists(log_dir):
		os.makedirs(log_dir)

	if opts.FromDate is not None:
		LocalValues.log_filename = log_dir + opts.db_name + '.' + opts.table_name + '.' + opts.fromDate + '.' + \
					datetime.now().strftime("%Y%m%d%H%M%S") + ".log"
	else:
		LocalValues.log_filename = log_dir + opts.db_name + '.' + opts.table_name + '.' + datetime.now().strftime("%Y%m%d") + '.' + \
					datetime.now().strftime("%Y%m%d%H%M%S") + ".log"   

	logging.baseConfig(filename=LocalValues.log_filename, filemode='w', level=logging.INFO)
	logging.info("Logging Started")
	utilities.print_info("framework version: " + LocalValues.framework_version)
	utilities.print_info("Log file from the current run: " + LocalValues.log_filename)
	utilities.print_info("Base Directory is " + base_dir)
	utilities.print_info("Current OS: " + current_os)
	
	if opts.db_name is None:
		utilities.abort_with_msg("Please provide Source Database Name")
	if opts.table_name is None:
		utilities.abort_with_msg("Please provide Source Table/View Name")
	if opts.fromDate is None:
		utilities.print_info("No Date Provided. Job Calendar will be used")
		
	operators_to_check = [">",">=", "<", "<=", "=", "<>"]
	if opts.operator != '':
		if not any(oper in opts.operator for oper in operators_to_check):
			parser.print_help()
			utilities.abort_with_msg("Not a valid operator to build a condition")
			
	legal_switch = ["G", "GE", "GEL", "GL", "GQ", "Q", "GLQ", "GELQ", "GF", "GEF", "GELF", "GLF", "GQF", "GELQF", "GFE", "GEFL", "GEFLQ", "GEQLF", "GFEL", "P"]
	if opts.runSwitch.upper() not in legal_switch:
		parser.print_help()
		utilities.abort_with_msg("Not a valid runSwitch. Valid Switch combinations: " + str(legal_switch))
	
	if "G" not in opts.runSwitch and opts.runSwitch is not None and "jds_user" not in opts.db_name.lower():
		parser.print_help()
		utilities.abort_with_msg("Cannot execute Extraction, Load or Quality check without script generation." +
								 "Include G in switch")
								 
	LocalValues.db_nm = opts.db_name.strip().upper()
	LocalValues.tbl_nm = opts.table_name.strip().upper()
	if opts.mysql_prop == '':
		LocalValues.mysql_prop_file = base_dir + 'common' + dir_sep + 'ENV.mySQL.properties'
	else:
		LocalValues.mysql_prop_file = opts.mysql_prop
	LocalValues.process_start = datetime.now()
	utilities.mySQLhdr = mySQLHandler(LocalValues.mysql_prop_file)
	utilities.GlobalValues.mySQLHandler_instance = mySQLHandler(LocalValues.mysql_prop_file)
	utilities.GlobalValues.epvaim_file = base_dir + 'common' + dir_sep + 'ENV.epvaim.properties'
	
	if "jds_user" not in LocalValues.db_nm.lower():
		mysql_meta = mySQLMetadata(LocalValues.mysql_prop_file)
		tbl_meta = mysql_meta.read_table_metadata(sourceDB=LocalValues.db_nm, sourceTable=LocalValues.tbl_nm)
		col_meta = mysql_meta.read_column_metadata(sourceDB=LocalValues.db_nm, sourceTable=LocalValues.tbl_nm)
		
	if "P" in opts.runSwitch:
		LocalValues.date_to_process = opts.fromDate
		mysql_meta = mySQLMetadata(LocalValues.mysql_prop_file)
		tbl_meta = mysql_meta.read_table_metadata(sourceDB=LocalValues.db_nm, sourceTable=LocalValues.tbl_nm)
		src_folder_path = normalize_path(tbl_meta['src_tbl']).lower()
		tgt_folder_path = normalize_path(tbl_meta['extract_landing_dir'])
		processed_folder_list = get_folder_list(src_folder_path)
		move_raw_folder(processed_folder_list, tgt_folder_path)
		tgt_folder_list = get_tgt_folder_info(tgt_folder_path, processed_folder_list)
		tgt_final_folder_list = move_csv_files_sub_folder(tgt_folder_list)
		opts.runSwitch = "Q"
		gen_ext_hql(tgt_final_folder_list, base_dirm mysql_meta, tbl_meta, dir_sepm LocalValues, opts)
		retention(tgt_final_folder_list, tgt_folder_path)
		exit(-1)
		
	temp_path = ''
	if "F" not in opts.runSwitch:
		if opts.extract_path.strip().lower() == 'meta' and len(tbl_meta['source_sql_txt'].strip()) < 5:
			utilities.abort_with_msg("Custom Source Path missing in meta. Its required if -x switch is used')
		elif opts.extract_path.strip().lower() != 'meta':
			if base_fid_path in opts.extract_path.strip():
				temp_path = opts.extract_path.strip()
			else:
				temp_path = base_fid_path + opts.extract_path.strip()
			if not os.path.exists(temp_path):
				utilities.print_warn("Custom Extract path passed from argument does not exists. will check in meta path")
				if not os.path.exists(tbl_meta['source_sql_txt'].strip()):
					utilities.abort_with_msg("Custom Extract file doesnt exist either in argument or meta")
				else:
					temp_path = tbl_meta['source_sql_txt'].strip()
			tbl_meta['source_sql_txt'] = temp_path.strip()
		elif opts.extract_path.strip().lower() == 'meta' and len(tbl_meta['source_sql_txt'].strip()) > 5:
			if base_fid_path in tbl_meta['source_sql_txt'].strip()
				temp_path = tbl_meta['source_sql_txt'].strip()
			else:
				temp_path = base_fid_path + tbl_meta['source_sql_txt'].strip()
			if not os.path.exists(temp_path):
				utilities.abort_with_msg("Customer Extract path from meta does not exists")
			tbl_meta['source_sql_txt'] =  temp_path.strip()
		if temp_path !=  base_fid_path and temp_path != '':
			LocalValues.custom_extract = 'Y'
			utilities.print_info("Custom Extract is in use")
			
	if opts.fromDate is None:
		if opts.endDate is not None:
			utilities.abort_with_msg("End Date not accepted without from date")
		
		end_date = 'datetime.today().strftime("%Y%m%d')
		job_cal_check = jobCalendarCheck(end_date, LocalValues.db_nm, LocalValues.tbl_nm, LocalValues.mysql_prop_file)
		runSwitch, eff_end_date = job_cal_check.job_calendar_check()
		last_good_date = mysql_meta.read_opts_jobstat_max(feed=LocalValues.tbl_nm)
		if last_good_date is None:
			utilities.print_warn("Last run date has returned to None. So will be using the date from Calendar : " + eff_end_date)
			last_good_date = eff_end_date
			new_from_date = (datetime.strptime(last_good_date,'%Y%m%d')).strftime('%Y%m%d')
		else:
			new_from_date = (datetime.strptime(last_good_date, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
		LocalValues.date_to_process = new_from_date
		LocalValues.date_till_process = eff_end_date
		
		if eff_end_date == new_from_date:
			if run_switch == 'N':
				utilities.print_warn("Job will not run for effective date: " + str(new_from_date))
				audit_action = set_audit_action()
				audit_log(audit_action, "JOB NOT SCHEDULED TO RUN FOR " + eff_end_date)
				exit(-1)
		if new_from_date > eff_end_date:
			utilities.print_warn("Job will not run when from_date " + new_from_date + " is greater than end date " + eff_end_date)
			audit_action = set_audit_action()
			audit_log(audit_action, "JOB ABORTED BECASE FROM DATE " + new_from_date + ' GREATER THAN END DATE ' + eff_end_date)
			exit(-1)
			
		opts.fromDate = new_from_date
		opts.endDate = eff_end_date
		utilities.print_info("Run Switch: " + run_switch + " Effective From Date: " + str(opts.fromDate))
		utilities.print_info("Run Switch: " + run_switch + " Effective End Date: " + str(opts.endDate))
		
	LocalValues.date_to_process = opts.fromDate
	LocalValues.date_till_process = opts.endDate
	
	if opts.statOverride is None:
		ops_jobstat = mysql_meta.read_opts_jobstat(feed=LocalValues.tbl_nm, 
												   from_dt=LocalValues.date_to_process,
												   end_dt=LocalValues.date_till_process
												  )
		for x in ops_jobstat:
			if x["status"] == "COMPLETED SUCCESSFULLY":
				utilities.print_info("Already the extract for the date is completed")
				audit_action = set_audit_action
				audit_log(audit_action, 'GRACEFUL EXIT PERFORMED')
				exit(0)
	else:
		utilities.print_info("JobStat override switch is on. Bypassing Jobstat Check for the Extract and filedate")
		
	for key, val in sorted(tbl_meta.items()):
		utilities.print_info("\t\t" + key + " : " + str(val))
		
	utilities.print_info("SOURCECOLNM" + "\t" + "RAW_COL_INHIVE" + "\t" +  RFND_COL_INHIVE + "\t" + RFND_COL_TYPE_INHIVE + "\t" + "LENGTH" + "\t" + "IS_EXTRACTED" + "\t" + \
						 "IS_ENCRYPTED" + "\t" + "IS_PARTITIONED" + "\t" + "SEQ_NB")
	for row in col_meta:
		utilities.print_info(row["SOURCECOLNM"] + "\t" + row["RAW_COL_INHIVE"] + "\t" + row["RFND_COL_INHIVE"] + "\t" +  row["RFND_COL_TYPE_INHIVE"] + "\t" + row["LENGTH"] + "\t" + 
							 row["IS_EXTRACTED"] + "\t" +  row["IS_ENCRYPTED"] + "\t" + row["IS_PARTITIONED"] + "\t" +  str(row["SEQ_NB"]))
	ext_time = 0
	LocalValues.job_pid = 0
	if 'FILE' in tbl_meta["db_type"]:
		if opts.landing_path.strip() != '':
			tbl_meta["extract_landing_dir"] = opts.landing_path.strip()
		tbl_meta["extract_landing_dir"] = tbl_meta["extract_landing_dir"][:-1] if tbl_meta["extract_landing_dir"][:-1] == '/' else tbl_meta['extract_landing_dir']
		
	FormattedString = re.search(r'<<(.+?)>>',tbl_meta["extract_landing_dir"])
	if FormattedString:
		formatteddate = datetime.strptime(opts.fromDate, '%Y%m%d').strftime(FormattedString.group(1))
		formatted_string = tbl_meta["extract_landing_dir"]
		tbl_meta["extract_landing_dir"] = tbl_meta["extract_landing_dir"].replace(FormattedString.group(), formatteddate)
	
	if "G" in opts.runSwitch:
		if tbl_meta['delta_col']:
			l_extract_filter = utilities.build_extract_filter(arg_tbl_meta=tbl_meta,
															  arg_from_val=LocalValues.date_to_process,
															  arg_to_val=LocalValues.date_till_process,
															  arg_operator=opts.operator,
															  run_switch=opts.runSwitch)
		else:
			utilities.print_warn("No Condition Built for extract. Placeholder condition of 1=1 will be used")
			l_extract_filter = "1 = 1"
		
		helper_dict = utilities.gen_script_from_tmplt(arg_base_dir=base_dir,
													  arg_tbl_meta=tbl_meta,
													  arg_col_meta=col_meta,
													  arg_dir_sep=dir_sep,
													  arg_xtract_filter=l_extract_filter,
													  arg_from_val=LocalValues.date_to_process,
													  arg_to_val=LocalValues.date_till_process,
													  arg_operator-opts.operator,
													  run_switch=opts.runSwitch,
													  arg_usestage=opts.useStage,
													  custom_extract=LocalValues.custom_extract)

		gen_time = timer()
		utilities.print_info("Time take to generate scripts " + str(gen_time - start_time) + " sec")
		v_part_col = (helper_dict["ptn_col_list"][0] + '_PTN').lower() if len(helper_dict["ptn_col_list"]) > 0 else None
		LocalValues.partition_col = '_PTN'.join(helper_dict["ptn_col_list"])+'_PTN' if len(helper_dict["ptn_col_list"]) > 0 else ''
		audit_action = set_audit_action()
		audit_log(audit_action, 'GENERATE EXTRACT SCRIPT')
	if '_RAW' in helper_dict["raw_hdfs_partition_location"].upper():
		v_dirpath=tbl_meta["hdfs_raw_dir"] + dir_sep + tbl_meta["tgt_tbl"].lower() + "_raw"
	else:
		v_dirpath=tbl_meta["hdfs_raw_dir"] + dir_sep + tbl_meta["tgt_tbl"].lower()
													  
	if "E" in opts.runSwitch:
		audit_action = set_audit_action()
		audit_log(audit_action, "START EXECUTE SOURCE DB EXTRACT SCRIPT")
		LocalValues.filename, LocalValues.src_row_count, LocalValues.rcvd_row_count = utilities.run_extract(arg_tbl_meta=tbl_meta,
																											arg_log_dir=log_dir,
																											arg_passwd_file=base_dir + 'common' + dir_sep + 'ENV.scriptpwd.properties',
																											arg_date_for_extract=LocalValues.date_to_process,
																											arg_date_for_extract_end=LocalValues.date_till_process,
																											log_filename = LocalValues.log_filename,
																											arg_helper_dict=helper_dict)
		if tbl_meta["v1_support"] == "Y":
			exit(0)
		
		if LocalValues.filename != '' and opts.persistExtract == 'N':
			if "TDCH" not in tbl_meta['db_type'] and "SQOOP" not in tbl_meta['db_type'] and 'FILE-HDFS' not in tbl_meta['db_type'] and 'GPHDFS-PULL' not in tbl_meta['db_type']:
				utilities.run_shell_cmd("rm -f " + LocalValues.filename)
		
		if opts.runSwitch == 'GE' or opts.runSwitch == 'GEF':
			LocalValues.raw_size, LocalValues.raw_replica_size = utilities.check_hdfs_space(arg_hdfs_base_path=v_dirpath, from_dt=opts.fromDate)
			utilities.print_info("Raw Size: " + LocalValues.raw_size)
			utilities.print_info("Raw Replica Size: " + LocalValues.raw_replica_size)
			if tbl_meta["hive_raw_retention"].strip() != '' and tbl_meta["hive_raw_retention"].strip() != '0':
				purgeraw = PurgeData(tbl_meta["hdfs_raw_dir"] + dir_sep + tbl_meta["tgt_tbl"].lower() + "_raw",
									 LocalValues.date_to_process,
									 tbl_meta["hive_raw_retention"],
									 tbl_meta["stg_db"],
									 tbl_meta["tgt_tbl] + "_raw"
				purgeraw.purge_ptn()
				
		ext_time = timer()
		utilities.print_info("Time taken to extract from Source " + str(ext_time - gen_time) + " sec")
		audit_log(audit_action, 'Completed Execute source db script')
		
		if tbl_meta['db_type'] == 'TERADATA':
			utilities.del_in_hdfs(tbl_meta["hdfs_basedir"] + '/' + tbl_meta["hdfs_extract_dir"] + "/" + os.path.basename(LocalValues.filename))
			
	if "L" in opts.runSwitch:
		audit_action = set_audit_action()
		utilities.run_hql(arg_script=helper_dict["ins_hive_rfnd_tbl"],
						  arg_mode="f",
						  arg_param=' --hiveconf inputsrcdt='" + opts.fromDate + "'"
						 )
						 
		if tbl_meta["hive_raw_retention"].strip() != '-1' and "SQOOP" not in tbl_meta["db_type"].upper():
			utilities.hdfs_compress(input_hdfs_dir=helper_dict["raw_hdfs_partition_location"])
		
		LocalValues.refined_count = utilities.get_count_hive_table(db=tbl_meta['tgt_db'],table=tbl_meta['tgt_tbl'],
																   partition_col=LocalValues.partition_col,
																   from_dt=LocalValues.date_to_process,
																   end_dt=LocalValues.date_till_process,
																   query=None,
																   tbl_truncate=tbl_meta["hive_tbl_truncate"])
		if LocalValues.refined_count == '0' and LocalValues.rcvd_row_count != '0':
			utilities.print_warn("Refined Count check returned 0. dEfaulting to RowCount")
			LocalValues.refined_count = LocalValues.rcvd_row_count
			utilities.print_info("Default count from Refined: " + str(LocalValues.refined_count))
			
		LocalValues.raw_size, LocalValues.raw_replica_size = utilities.check_hdfs_space(arg_hdfs_base_path=v_dirpath, from_dt=opts.fromDate)
		utilities.print_info("Raw size: " + LocalValues.raw_size)
		utilities.print_info("Raw Replication Size: " + LocalValues.raw_replica_size)
		LocalValues.refined_size, LocalValues.refined_replica_size = utilities.check_hdfs_space(arg_hdfs_base_path=tbl_meta["hdfs_refined_dir"] + dir_sep + tbl_meta["tgt_tbl"].lower(),
																								from_dt=opts.fromDate,
																								tbl_truncate=tbl_meta["hive_tbl_truncate"]
																							    )
		utilities.print_info("Refined size: " + LocalValues.refined_size)
		utilities.print_info("Refined Replication Size: " + LocalValues.refined_replica_size)
																								
						 
		if tbl_meta["hive_raw_retention"].strip() != '' and tbl_meta["hive_raw_retention].strip() != '0':
			purgeraw =  PurgeData(tbl_meta["hdfs_raw_dir"] + dir_sep + tbl_meta["tgt_tbl"].lower() + "_raw",
								  LocalValues.date_to_process,
								  tbl_meta["hive_raw_retention"],
								  tbl_meta["stg_db"],
								  tbl_meta["tgt_tbl"] + "_raw"
								 )
			purgeraw.purge_ptn()
		
		if tbl_meta["hive_refined_retention"].strip() != '' and tbl_meta["hive_refined_retention"].strip() != 0 \
			and tbl_meta["hive_tbl_truncate"] != 'P':
			purgerefined = PurgeData(tbl_meta["hdfs_refined_dir"] + dir_sep + tbl_meta["tgt_tbl"].lower(),
									LocalValues.date_to_process,
									tbl_meta["hive_refined_retention"],
									tbl_meta["tgt_db"],
									tbl_meta["tgt_tbl"]
									)
			purgeraw.purge_ptn() 
		audit_log(audit_action, "Completed Loading data to Hive')
		load_time = timer()
		utilities.print_info("Time take to load to hive refined table" + str(load_time - ext_time) + " secs")
		utilities.print_info("Time taken to generate, extract and load to hive " + str(load_time - start_time) + " secs")
		
	if "Q" in opts.runSwitch:
		utilities.run_dq(arg_tbl_meta=tbl_meta,
						 arg_log_dir=log_dir,
						 from_date=opts.fromDate,
						 end_date=opts.endDate,
						 part_col=v_part_col,
						 which_table="both"
						)
		qual_time = timer()
		utilities.print_info("Time taken to generate till quality check " + str(qual_time - start_time) + " secs")				 
	
	if opts.postprocess_opts.strip() != '':
		post_time_begin = timer()
		utilities.print_info(" PostProcessor is active")
		utilities.run_post_processory(postprocess_opts=opts.postprocess_opts, 
									  tbl_meta-tbl_meta,
									  from_dt=LocalValues.date_to_process,
									  end_dt=LocalValues.date_till_process)
		post_time_end = timer()
		utilities.print_info("Time taken from Postprocess: " + str(post_time_end - post_time_begin) + " secs")
		utilities.print_info("Time taken to run till Postprocess: " + str(post_time_end - start_time) + " secs")
	
	if opts.runSwitch != 'G' and opts.runSwitch != 'GF':
		audit_action = 'update'
		audit_log(audit_action, 'COMPLETED SUCCESSFULLY')
		

if __name__ == '__main__':
	main()
	