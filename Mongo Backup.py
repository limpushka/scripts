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
#Unlock and delete lock file.
def un_lock():
    lock.close()
    os.remove(lockfile)

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
    max_backups = 100
    logging.info("Starting daily MongoDB backup" )
else:
    work_dir = "d:/Development/Mongodb/work/"
    storage_dir = "d:/Development/Mongodb/storage/daily/"
    max_backups = 100
    logging.info("Starting monthly MongoDB backup")



#Switch replica to single

#Connect to mongodb and Get all Database names
db_conn = MongoClient('localhost',27017)
db_names=db_conn.database_names()
#if db_conn == True:
    #db_names=db_conn.database_names()
#else:
    #logging.error("Could not connect to MongoDB Instance.Check if service is running")
    #sys.exit("Could not connect to MongoDB Instance.Check Mongodb service")
#DB auth credentials
#db_login="admin"
#db_pass="abbyy231*"




class MongoDB():
           
    def __init__(self,db_names):              
        for x in db_names:
            if x != "local":
                self.db_name = x              
                self.mongo_backup(self.db_name)
                self.mongo_clean_up(self.db_name)                              
          
                
    def mongo_backup(self,db_name):
        self.now = datetime.datetime.today().strftime('%Y-%m-%d-%H-%M-%S')
        
        logging.info("Running mongodump for DB: %s " % (db_name))
        backup_output = subprocess.check_output(  # Run Mongodump for each Database
                    [
                        'mongodump',
                        #'-u', '%s' % db_login,
                        #'-p', '%s' % db_pass,
                        #'--authenticationDatabase','%s' %admin,
                        '-d', '%s' % self.db_name,
                        #'--port', '%s' % port,
                        '-o', '%s' % work_dir
                    ])
        
        archive_name = self.db_name + '.'+ self.now
        source_name = work_dir + self.db_name
        archive_path = os.path.join(storage_dir,self.db_name)
        
        # Check if backup directory exists
        if not os.path.exists(archive_path):
            os.makedirs(os.path.join(storage_dir,self.db_name))
        
            
            
        with zipfile.ZipFile(os.path.join(archive_path,"%s.zip" % (archive_name)), "w", allowZip64 = True) as zf:  # Ziping the result
            abs_src = os.path.abspath(source_name)
            for dirname, subdirs, files in os.walk(abs_src):
                for filename in files:
                    absname = os.path.abspath(os.path.join(dirname, filename))
                    arcname = absname[len(abs_src) + 1:]
                    zf.write(absname, arcname)
            logging.info("End zip dump for DB: %s and saving zip file to %s " %(self.db_name,archive_path))
        logging.info("Backup Done Successfully")
        
    def mongo_clean_up(self,db_name):
            archive_path = os.path.join(storage_dir,self.db_name)
            a = []
            for files in os.listdir(archive_path):
                a.append(files)
               
            while (len(a) > max_backups):
                a.sort()
                filetodel = a[0]
                del a[0]
                os.remove(os.path.join(archive_path,filetodel))
                logging.info("There are too many backup files. Starting cleanup process. File %s was deleted %s from directory %s" %(filetodel,archive_path))
            logging.info("Cleanup Done for Backup zip files in %s Backup Directory: %d" %(self.db_name, len(a)))       
        
        
def disk_clean_up(db_names): # Delete old zip backup files when disk space is less than 85%
    cleanup_dir = "d:/Development/Mongodb/storage/daily/"
    for x in db_names:
        if x != 'local':
            cleanup_path = os.path.join(cleanup_dir,x)
            if not os.path.exists(cleanup_path):
                continue
            a = []
            for files in os.listdir(cleanup_path):
                a.append(files)
                a.sort()
                filetodel = a[0]
                del a[0]
                os.remove(os.path.join(cleanup_path,filetodel))
                logging.info("Not enough free disk space. Cleanup process started.File to Del %s" %filetodel)        
       
                   
   
disk_space = psutil.disk_usage(storage_dir)
while (disk_space.percent <= 85):
    disk_clean_up(db_names)

try:
    MongoDB(db_names)
except AssertionError, msg:
    logging.error(msg)

# Unclock and delete temp file
un_lock()

    
    