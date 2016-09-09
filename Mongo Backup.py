#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Backup script Made for Abbyy-LS

import sys
import time
import os
import argparse
import logging
import datetime
import subprocess
import zipfile
import shutil
import psutil
import zc.lockfile
from pymongo import MongoClient

logging.basicConfig(format=u'%(levelname)-8s [%(asctime)s]  %(message)s',datefmt='%m/%d/%Y %H:%M:%S' ,filename='d:\Development\Python\log\mongo-backup.log',level=logging.INFO)


#Check if file locked and exits
lockfile = "d:/Development/Mongodb/Mongolock.lock"
if os.path.exists(lockfile):
    logging.error("Another instance of this script is Running")
    sys.exit("Another instance of this script is Running")
else:
    lock = zc.lockfile.LockFile(lockfile,content_template='{pid}; {hostname}')

#Key options for script launch 
parser = argparse.ArgumentParser(description='Backup schedule options - Monthly,Weekly,Daily')
parser.add_argument('--monthly', '-m', action="store_true", help='Option for Monthly Backup')
parser.add_argument('--weekly', '-w', action="store_true", help='Option for Weekly Backup')
parser.add_argument('--daily', '-d', action="store_true", help='Option for Daily Backup')
 
args = parser.parse_args()

#Check our key arguments

if args.monthly:
    #work_dir = "/datadrive/opt/mongodbbackup/work/"
    #storage_dir = "/datadrive/opt/mongodbbackup/storage/montlhy"
    work_dir = "d:/Development/Mongodb/work/"
    storage_dir = "d:/Development/Mongodb/storage/monthly/"
    max_backups = 2
    logging.info("Starting monthly MongoDB backup" )
elif args.weekly:
    #work_dir = "/datadrive/opt/mongodbbackup/work/"
    #storage_dir = "/datadrive/opt/mongodbbackup/storage/weekly"
    work_dir = "d:/Development/Mongodb/work/weekly/"
    storage_dir = "d:/Development/Mongodb/storage/weekly/"
    max_backups = 4    
    logging.info("Starting weekly MongoDB backup" )
elif args.daily:
    #work_dir = "/datadrive/opt/mongodbbackup/work/"
    #storage_dir = "/datadrive/opt/mongodbbackup/storage/daily"
    work_dir = "d:/Development/Mongodb/work/"
    storage_dir = "d:/Development/Mongodb/storage/daily/"
    min_backups = 10
    logging.info("Starting daily MongoDB backup" )
else:
    work_dir = "d:/Development/Mongodb/work/"
    storage_dir = "d:/Development/Mongodb/storage/monthly/"
    max_backups = 2
    logging.info("Starting monthly MongoDB backup")



#Switch replica to single

#Connect to mongodb and Get all Database names
db_conn = MongoClient('localhost',27017)
db_names=db_conn.database_names()

#DB auth credentials
#db_login="admin"
#db_pass="abbyy231*"




class MongoDB():
           
    def __init__(self,db_names):    
        b = []
        for x in db_names:
            if x != "local" and x !='admin':
                self.db_name = x
                #print "Mongo BD Object for Backup Database Name %s " % self.db_name
                #check free disk space
                disk_space = psutil.disk_usage(storage_dir)
                if (disk_space.percent < 85):
                    self.run_mongobackup(self.db_name)               
                else:
                    self.run_mongocleanup(self.db_name)
        
        
                
    def run_mongobackup(self,db_name):
        self.now = datetime.datetime.today().strftime('%Y-%m-%d-%H-%M-%S')
        
        logging.info("Running mongodump for DB: %s " % (db_name))
        backup_output = subprocess.check_output(
                    [
                        'mongodump',
                        #'-u', '%s' % db_login,
                        #'-p', '%s' % db_pass,
                        #'--authenticationDatabase','%s' %admin,
                        '-d', '%s' % self.db_name,
                        #'--port', '%s' % port,
                        '-o', '%s' % work_dir
                    ])
        #logging.info(backup_output)
        
        #Ziping the result
        archive_name = self.db_name + '.'+ self.now
        source_name = work_dir + self.db_name
        archive_path = os.path.join(storage_dir,self.db_name)
        
        #Check if backup directory exists
        if not os.path.exists(archive_path):
            os.makedirs(os.path.join(storage_dir,self.db_name))
        
            
            
        with zipfile.ZipFile(os.path.join(archive_path,"%s.zip" % (archive_name)), "w", allowZip64 = True) as zf:
            abs_src = os.path.abspath(source_name)
            for dirname, subdirs, files in os.walk(abs_src):
                for filename in files:
                    absname = os.path.abspath(os.path.join(dirname, filename))
                    arcname = absname[len(abs_src) + 1:]
                    zf.write(absname, arcname)
            logging.info("End zip dump for DB: %s and saving zip file to %s " %(self.db_name,archive_path))   
        
        
        
    def run_mongocleanup(self,db_name):
        archive_path = os.path.join(storage_dir,self.db_name)
        a = []
        for files in os.listdir(archive_path):
            a.append(files)
       
        while (len(a) > max_backups):
            a.sort()
            filetodel = a[0]
            del a[0]
            #print "File to remove: %s" % filetodel
            os.remove(os.path.join(archive_path,filetodel))
            #print "File %s deleted" % filetodel
            logging.info("Not enough free disk space. Cleanup process started.Files to Del %s" %filetodel)
            
            #print "CLeanup for DB %s Done" %self.db_name
        #logging.info("Cleanup for Backup zip files in %s Backup Directory: %d" %(self.db_name, len(a))
                
   
    logging.info("Backup Done Successfully")


try:
    MongoDB(db_names)
except AssertionError, msg:
    logging.error(msg)

# Unclock and delete temp file
lock.close()
os.remove(lockfile)
    
    