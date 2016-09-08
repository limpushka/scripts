#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Backup script Made for Abbyy-LS

import os
import argparse
import logging
import datetime
#import urlparse
import subprocess
import zipfile
import shutil
from pymongo import MongoClient,MongoReplicaSetClient

logging.basicConfig(level=logging.INFO)

#Key options for script launch 
parser = argparse.ArgumentParser(description='Backup schedule options - Monthly,Weekly,Daily')
parser.add_argument('--monthly', '-m', action="store_true", help='Option for Monthly Backup')
parser.add_argument('--weekly', '-w', action="store_true", help='Option for Weekly Backup')
parser.add_argument('--daily', '-d', action="store_true", help='Option for Daily Backup')
 
args = parser.parse_args()

#Check our arguments

if args.monthly:
    work_dir = "/datadrive/opt/mongodbbackup/work"
    storage_dir = "/datadrive/opt/mongodbbackup/storage/monthly"
    max_backups = 2    
elif args.weekly:
    work_dir = "/datadrive/opt/mongodbbackup/work"
    storage_dir = "/datadrive/opt/mongodbbackup/storage/weekly"
    max_backups = 4    
elif args.daily:
    work_dir = "/datadrive/opt/mongodbbackup/work"
    storage_dir = "/datadrive/opt/mongodbbackup/storage/daily"
    work_dir = "d:/Development/Mongodb/work/"
    storage_dir = "d:/Development/Mongodb/storage/daily"
    max_backups = 2  
pid = os.getpid()

def check_pid(pid):        
    """ Check for the existence of a pid. """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True
check_pid(pid)

#db auth credentials
db_login = "admin"
db_pass = "abbyy231*"

#Connect to mongodb and Get all Database names
db_conn = MongoClient('localhost',27017)
#db_names=db_conn.database_names()
#db_conn = MongoReplicaSetClient('localhost', 'admin',replicaSet='testreplset01',
#   read_preference=ReadPreference.SECONDARY_PREFERRED)
db_conn.admin.authenticate(db_login,db_pass)
db_names=db_conn.database_names()




class MongoDB():

    def __init__(self,db_names):
        for x in db_names:
            if x != "local" and x !="admin":
		self.db_name = x
                print "Mongo BD Object for Backup Database Name %s " % self.db_name
                self.run_mongobackup(self.db_name)
                self.run_mongocleanup(self.db_name)

    def run_mongobackup(self,db_name):
        self.now = datetime.datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
        #CLeanup work_dir folder.
        #os.remove(work_dir)

        #print os.path.dirname(work_dir)
        #cleanup_folder = subprocess.check_output(
            #[
                #'rm',
                #'-rf',
                #'%s','os.path.dirname(work_dir)'])
        #logging.info(cleanup_folder)
        print "Running mongodump for %s current date %s " % (db_name,self.now)
        backup_output = subprocess.check_output(
                    [
                        'mongodump',
                        '-u', '%s' % db_login,
                        '-p', '%s' % db_pass,
                        '--authenticationDatabase','%s' %'admin',
                        '-d', '%s' % self.db_name,
                        #'--port', '%s' % port,
                        '-o', '%s' % work_dir
	            ])
        logging.info(backup_output)

        #Ziping the result
        archive_name = self.db_name + '.'+ self.now
        source_name = work_dir + self.db_name
        archive_path = os.path.join(storage_dir,self.db_name)

        #Check if backup directory exists
        if os.path.exists(archive_path):
            print "Backup Directory Exists"
        else:
            os.mkdir(os.path.join(storage_dir + self.db_name))

        with zipfile.ZipFile(os.path.join(archive_path,"%s.zip" % (archive_name)), "w", zipfile.ZIP_DEFLATED) as zf:
            abs_src = os.path.abspath(source_name)
            arcname = absname[len(abs_src) + 1:]
            print 'zipping %s as %s' % (os.path.join(dirname, filename),
                                           arcname)
            zf.write(absname, arcname)
            print "End zip dump and saving zip files to storage"




    def run_mongocleanup(self,db_name):
        archive_path = os.path.join(storage_dir,self.db_name)
        a = []
	while (len(a) > max_backups):
	    a.sort()
	    filetodel = a[0]
	    del a[0]
	    print "File to remove: %s" % filetodel
	    os.remove(os.path.join(archive_path,filetodel))
	    print "File %s deleted" % filetodel
	print "CLeanup for DB %s Done" %self.db_name
	print "We dont need to cleanup"
	print "Count for Backup zip files in Backup Directory: %d" %len(a)	



try:
    MongoDB(db_names)
except AssertionError, msg:
    logging.error(msg)




