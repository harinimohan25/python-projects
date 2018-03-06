import re, sys
for line in sys.stdin:
    print re.sub(r'\xdf','"', re.sub(r'\xfe','\001',re.sub(r'\r', '', re.sub(r'"', '',line)))),