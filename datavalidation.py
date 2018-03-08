from utilities import *
from __future__ import print_function
from pyspark import SparkConf, SparkContent
from pyspark.sql import HiveContext, SQLContext
import sys
import utilities
import optparse
from pprint import pprint
import math

def main():
    parser = optparse.OptionParser(usage="usage: %prog [options values]",
                                   version="%prog 1.0")
    parser.add_option('-d', '--dbnane',
                      help="Database of Table to be fork lifted into Hive ",
                      dest='db_name')
    parser.add_option('-t', '--table_name',
                      help="Table name to be fork lifted into Hive",
                      dest='table_name')
    parser.add_option('-f', '--from_date',
                      help="Date used to filter rows from source and also in names of extract file",
                      dest='from_date',
                      default="")
    parser.add_option('-f', '--from_date',
                      help="Date used to filter rows from source and also in names of extract file",
                      dest='from_date',
                      default="")


def exit_mode(rc,sc):
    sc.stop()
    if rc == 1:
        sys.exit(-1)
    else:
        sys.exit(0)

def raw_table_df(hc,db_name,table_name,from_date):
    if len(from_date) != 0:
        query="select * from " + db_name + "." + table_name + " where " + "loaddate" + " = " + from_date
    else:
        query="select * from " + db_name + "." + table_name
    print("raw_query : " + query)
    df=hc.sql(query)
    return df

def refined_table_df(hc,db_name, table_name, part_col, from_date, end_date):
    if len(part_col) == 0:
        query="select * from " + db_name + "." + table_name
    else:
        if len(end_date) != 0:
            query="select * from " + db_name + "." + table_name + ' where ' + part_col + ' between ' + from_date + ' and ' + end_date
        else:
            query='select * from ' + db_name + "." + table_name + " where " + part_col + " = " + from_date
    print(query)
    df = hc.sql(query)
    return df

def status_check(sc,hc,df,table_name):
    rec_count = df.count()
    utilities.print_info('Record Count for tables "%s" is "%d" ' %(table_name, rec_count))
    exprs_fact = [((count_null(c) / rec_count) * 100.alias(c) for c in df.columns)]
    stats = df.agg(*exprs_fact)
    stats_p = stats.toPandas()
    for index, row in stats_p.iterrows():
        null_count_dict=row.to_dict()

    utilities.print_info('Null percentage for columns in table "%s" ' %table_name)
    pprint(null_count_dict)
    return (rec_count, null_count_dict)

def count_null(c):
    return sum(col(c).isNull().cast("integer")).alias(c)

def dict_compare(d1, d2):
    d1_keys = set(d1.keys())
    d2_keys = set(d2.keys())
    intersect_keys = d1_keys.intersection(d2_keys)
    added = d1_keys - d2_keys
    removed = d2_keys - d1_keys
    null_defer = {o : (int(d1[o], int(d2[o]))) for o in intersect_keys if int(d1[o]) != int(d2[o]) and math.fabs(d1[o] -d2[o]) > 1.0}
    same = {o: (int(d1[o]), int(d2[o])) for o in intersect_keys if int(d1[o]) == 100}
    full_null = {o : int(d2[o]) for o in intersect_keys if int(d2[o]) == 100}
    return added, removed, null_defer, same, full_null

if __name__ == "__main__":
    main()


