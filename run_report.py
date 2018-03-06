import os, optparse
from email_notifier import email_notifier
from read_jobstat import read_jobstat
from datetime import datetime

def main():
    parser = optparse.OptionParser(usage="usage: %prog [options values]", version="%prog 1.0")
    parser.add_option('-f', '--from',
                      help='Sender Email ID',
                      dest='sender')
    parser.add_option('-t','--to',
                      help='Receiver Email ID',
                      dest='receivers')
    parser.add_option('-s','--subsystem',
                      help='Report subsystem name like Clickfox, TDD',
                      dest='subsystem')
    parser.add_option('-d','--date',
                      help='date for which the report would be pulled'
                            'Passing this fetches rows between start and end dates, inclusive of the dates passed',
                      dest='date',
                      default=None)
    parser.add_option('-c','--config',
                      help='MySQL config properties file',
                      dest='config',
                      default=None)
    parser.add_option('-l', '--legend',
                      help='Give if required status report column legend to make it more understandable',
                      dest='legend',
                      default=None)

    current_os = system()
    if current_os != 'Windows':
        dir_sep = "/"
    else:
        dir_sep = "\\"

    (opts, args) = parser.parse_args()
    base_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + dir_sep

    if (opts.sender is None) or (opts.receivers is None) or (opts.subsystem is None):
        print("See below config options")
        parser.print_help()
        print("Exiting")
        exit(-1)
    if opts.config is None:
        opts.config = base_dir + 'common' + dir_sep + 'ENV.mySQL.properties'
    if opts.date is None:
        opts.date = datetime.today().strftime('%Y-%m-%d')

    try:
        job = read_jobstat(opts.config)
        retrieve_rows, report_name = job.read_table(opts.date)
        if (len(retrieve_rows) > 1 and (len(retrieve_rows[0]) == 0)):
            print("Nothing returned from table: " + job.db['jobstat'])
            print('Exiting')
            exit(-1)
        else:
            notifier = email_notifier(opts.sender, opts.subsystem, retrieve_rows, report_name, opts.date, opts.receivers, opts.legend)
            notifier.send_run_notification()
    except Exception as err:
        print("An exception of type " + type(err).__name__ + "occurred. Arguments:\n" + str(err.args))

if __name__ == "__main__":
    main()
