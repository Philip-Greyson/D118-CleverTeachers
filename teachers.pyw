# Script to take values from PowerSchool and put them into a csv file for upload to Clever
# basically just a big SQL query, the results are massaged a tiny bit to get the email and a few other fields format
# then output one teacher per line to the teachers.csv file which is then uploaded to the Clever SFTP server

# importing module
import pysftp  # used to connect to the Clever sftp site and upload the file
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

print("PS Username: " + str(un) + " |PS Password: " + str(pw) + " |PS Server: " + str(cs))
print("SFTP Username: " + str(sftpUN) + " |SFTP Password: " + str(sftpPW) + " |SFTP Server: " + str(sftpHOST)) #debug so we can see what sftp info is being used

IGNORED_NAMES = ['ADMIN', 'TECH', 'PLUGIN', 'SUB', 'TEST', 'TEACHER']

# create the connecton to the database
with oracledb.connect(user=un, password=pw, dsn=cs) as con:
    with con.cursor() as cur:  # start an entry cursor
        with open('teacher_log.txt', 'w') as log:
            with open('Teachers.csv', 'w') as output:  # open the output file
                print("Connection established: " + con.version)
                print('"Teacher_id","Teacher_number","State_teacher_id","School_id","First_name","Middle_name","Last_name","Teacher_email","Title","Username"',file=output)  # print out header row
                print('"Teacher_id","Teacher_number","State_teacher_id","School_id","First_name","Middle_name","Last_name","Teacher_email","Title","Username"',file=log)
                # get the overall user info (non-school specific) for all users in the current school, filtering to only those who have an email filled in to avoid "fake" accounts like test/temp staff
                cur.execute('SELECT dcid, teachernumber, SIF_StatePrid, homeschoolid, first_name, middle_name, last_name, email_addr, title, teacherloginid FROM users WHERE email_addr IS NOT NULL AND homeschoolid <> 136 AND homeschoolid <> 2 AND homeschoolid <> 0 ORDER BY dcid')
                users = cur.fetchall()
                for user in users:
                    try:
                        print(user)
                        # print(user, file=log)
                        enabled = False # set their enabled flag to be false by default, will be set to true later if they have an active school
                        # store the info in variables for better readability so its obvious what we pass later on
                        uDCID = str(user[0]) # get the unique DCID for that user
                        teacherNum = str(user[1])
                        stateID = str(user[2]) if user[2] else '' # get their state teacher number if it exists, or just a blank string if not
                        homeschool = str(user[3])
                        firstName = str(user[4])
                        middleName = str(user[5]) if user[5] else ''
                        lastName = str(user[6])
                        email = str(user[7]).lower() # convert email in PS to lowercase to ignore any capital letters in it
                        title = str(user[8]) if user[8] else ''
                        loginID = str(user[9]) if user[9] else ''
                        staffID = '' # reset staffID to blank each time, will be populated below if relevant
                        if firstName.upper() in IGNORED_NAMES or lastName.upper() in IGNORED_NAMES:  # check if their first or last names match one of our ignored terms
                            print(f'WARN: Found user {firstName} {lastName} - {email} that has a name in the ignored list, they will be skipped')
                            print(f'WARN: Found user {firstName} {lastName} - {email} that has a name in the ignored list, they will be skipped', file=log)
                            continue  # skip to the next iteration of the loop, aka the next user
                        # next do a query for their schoolstaff entries that are active, they have one per building they have teacher access in with different info
                        cur.execute('SELECT id, status FROM schoolstaff WHERE users_dcid = ' + uDCID + ' AND status=1')
                        schoolStaff = cur.fetchall()
                        if schoolStaff: # if we get results back from the query, the staff member has at least 1 active school
                            if len(schoolStaff) == 1: # if there is only one school result, they previously were in clever but had a "bad" non-unique ID
                                staffID = str(schoolStaff[0][0])
                            print(f'"{uDCID}","{teacherNum}","{stateID}","{homeschool}","{firstName}","{middleName}","{lastName}","{email}","{title}","{loginID}"')
                            print(f'"{uDCID}","{teacherNum}","{stateID}","{homeschool}","{firstName}","{middleName}","{lastName}","{email}","{title}","{loginID}"', file=log)
                            print(f'"{uDCID}","{teacherNum}","{stateID}","{homeschool}","{firstName}","{middleName}","{lastName}","{email}","{title}","{loginID}"', file=output)
                        else: # if they have no active buildings, they should not be included in output file
                            print(f'INFO: {email} has no active buildings, skipping')
                            print(f'INFO: {email} has no active buildings, skipping',file=log)

                    except Exception as er:
                        print(f'ERROR on {user[1]}: {er}')
                        print(f'ERROR on {user[1]}: {er}', file=log)

            # connect to the Clever SFTP server using the login details stored as environement variables
            with pysftp.Connection(sftpHOST, username=sftpUN, password=sftpPW, cnopts=cnopts) as sftp:
                print('SFTP connection established')
                print('SFTP connection established', file=log)
                # print(sftp.pwd) # debug, show what folder we connected to
                # print(sftp.listdir())  # debug, show what other files/folders are in the current directory
                # sftp.put('Teachers.csv')  # upload the file onto the sftp server
                print("Student sync file plraced on remote server")
                print("Student sync file placed on remote server", file=log)
                
                