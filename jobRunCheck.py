from mySQLMetadata import mySQLMetadata
from datetime import datetime
from sys import exit
from utilities import abort_with_msg, print_info, print_warn

class jobCalendarCheck:
    def __init__(self, from_dt, sourceDB, sourceTable, mysql_prop_file):
        self.from_dt = from_dt
        self.sourceDB = sourceDB
        self.sourceTable = sourceTable
        self.mysql_prop_file = mysql_prop_file

    def retrieve_cal_meta(self):
        mysql_meta = mySQLMetadata(self.mysql_prop_file)
        cal_meta = mysql_meta.read_cal_metadata(sourceDB=self.sourceDB,
                                                sourceTable=self.sourceTable,
                                                from_dt=self.from_dt)
        return cal_meta

    def job_calendar_check(self):
        cal_meta = self.retrieve_cal_meta()
        if len(cal_meta) == 0:
            abort_with_msg("No Calendar Metadata Available")
        elif len(cal_meta) > 1:
            abort_with_msg("No Calendar Metadata Available")

        cal_meta = cal_meta[0]
        run_switch = 'N'

        if cal_meta["DAY_OF_FROM_DT"] == 'Mon' and cal_meta["RUN_ON_MON"] == "Y":
            run_switch = 'Y'
        elif cal_meta["DAY_OF_FROM_DT"] == 'Tue' and cal_meta["RUN_ON_TUE"] == "Y":
            run_switch = 'Y'
        elif cal_meta["DAY_OF_FROM_DT"] == 'Wed' and cal_meta["RUN_ON_WED"] == "Y":
            run_switch = 'Y'
        elif cal_meta["DAY_OF_FROM_DT"] == 'Thu' and cal_meta["RUN_ON_THU"] == "Y":
            run_switch = 'Y'
        elif cal_meta["DAY_OF_FROM_DT"] == 'Fri' and cal_meta["RUN_ON_FRI"] == "Y":
            run_switch = 'Y'
        elif cal_meta["DAY_OF_FROM_DT"] == 'Sat' and cal_meta["RUN_ON_SAT"] == "Y":
            run_switch = 'Y'
        elif cal_meta["DAY_OF_FROM_DT"] == 'Sun' and cal_meta["RUN_ON_SUN"] == "Y":
            run_switch = 'Y'

        if cal_meta["HOLIDAY_DT"] is not None:
            if cal_meta["RUN_ON_HOLIDAY"] == "Y":
                run_switch = 'Y'
            else:
                run_switch = 'N'

        return  run_switch, datetime.strptime(str(cal_meta["EFF_FROM_DT"]),'%Y-%m-%d').strftime("%Y%m%d")
    

