import xml.etree.ElementTree as ET
import collections
import sys
from os.path import isfile

def parseXML(xmlfile):
    tree = ET.parse(xmlfile)
    root = tree.getroot()
    holder = collections.OrderedDict()
    incond = ''
    outcond = ''
    for item in root.findall('./SMART_FOLDER/JOB'):
        jobname = item.attrib['JOBNAME']
        for child in item:
            if child.tag == 'INCOND':
                incond = child.attrib['NAME']
            if child.tag == 'OUTCOND':
                outcond = child.attrib['NAME']
        holder[jobname] = (incond, outcond)
    return holder

def validator(job_in_out):
    if job_in_out.__class__ == 'collections.OrderedDict':
        print("cannot validate input")
        exit(-1)
    else:
        print("Validation begins")
    job_num = []
    fail = False
    for jobname, conds in job_in_out.items():
        if conds[0] != '' and 'PL-' not in conds[0] and "-OK" not in conds[0]:
            print "Invalid IN Condition : Naming standards violation " + conds[0]
            fail=True
        if 'PL-' + jobname + '-OK' != conds[1]:
            print "Invalide OUT condition - mismatch with Job NAme: " + conds[1]
            fail=True
        job_num.append(int(jobname.split('-')[0][-3:]))
    job_num = list(set(job_num))
    for i in range(min(job_num), max(job_num)+1):
        if i not in job_num:
            print "ControlM job numbers missed" + str(i)
    if fail:
        print "Serious error present. Aborting!!!"
        exit(-1)

def main(filepath):
    if not isfile(filepath):
        print "File to be validated not found"
        exit(-1)
    ret_values = parseXML(xmlfile=filepath)
    validator(ret_values)

if __name__ == '__main__':
    print "XML to be validated : " + sys.argv[1]
    main(sys.argv[1])


