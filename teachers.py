# Script to take values from PowerSchool and put them into a csv file for upload to Clever
# basically just a big SQL query, the results are massaged a tiny bit to get the email and a few other fields format
# then output one teacher per line to the teachers.csv file which is then uploaded to the Clever SFTP server

# importing module
import pysftp  # used to connect to the Clever sftp site and upload the file
import sys  # needed for  non-scrolling display
import os  # needed to get system variables which have the PS IP and password in them
import oracledb # needed to connect to the PowerSchool database (oracle database)
from datetime import datetime

un = 'PSNavigator'  # PSNavigator is read only, PS is read/write
pw = os.environ.get('POWERSCHOOL_DB_PASSWORD') # the password for the PSNavigator account
cs = os.environ.get('POWERSCHOOL_PROD_DB') # the IP address, port, and database name to connect to

#set up sftp login info, stored as environment variables on system
sftpUN = os.environ.get('CLEVER_SFTP_USERNAME')
sftpPW = os.environ.get('CLEVER_SFTP_PASSWORD')
sftpHOST = os.environ.get('CLEVER_SFTP_ADDRESS')
cnopts = pysftp.CnOpts(knownhosts='known_hosts') #connection options to use the known_hosts file for key validation

print("Username: " + str(un) + " |Password: " + str(pw) + " |Server: " + str(cs))
print("SFTP Username: " + str(sftpUN) + " |SFTP Password: " + str(sftpPW) + " |SFTP Server: " + str(sftpHOST)) #debug so we can see what sftp info is being used

# create the connecton to the database
with oracledb.connect(user=un, password=pw, dsn=cs) as con:
    with con.cursor() as cur:  # start an entry cursor
        with open('teacher_log.txt', 'w') as log:
            with open('Teachers.csv', 'w') as output:  # open the output file
                print("Connection established: " + con.version)
                print('"Teacher_id","Teacher_number","State_teacher_id","School_id","First_name","Middle_name","Last_name","Teacher_email","Active","Title","Username"',file=output)  # print out header row
                print('"Teacher_id","Teacher_number","State_teacher_id","School_id","First_name","Middle_name","Last_name","Teacher_email","Active","Title","Username"',file=log)
                # get the overall user info (non-school specific) for all users in the current school, filtering to only those who have an email filled in to avoid "fake" accounts like test/temp staff
                cur.execute('SELECT dcid, teachernumber, SIF_StatePrid, homeschoolid, first_name, middle_name, last_name, email_addr, title, teacherloginid FROM users WHERE email_addr IS NOT NULL ORDER BY dcid')
                users = cur.fetchall()
                for user in users:
                    try:
                        print(user)
                        print(user, file=log)
                    except Exception as er:
                        print(f'ERROR on {user[1]}: {er}')
                        print(f'ERROR on {user[1]}: {er}', file=log)
                
                