#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Backup script Made for Abbyy-LS
# Script connects to MongoDB, gets all db names, then for each database except "local" performs mongodump and archive's the result  to our backup storage directory.
# Script checks our storage directory and if free disk space is less than 15% - performs Clean up.

import sys
import os
import argparse
import logging
import datetime
import subprocess
import zipfile
import psutil
import zc.lockfile
import shutil
from pymongo import MongoClient

logging.basicConfig(format=u'%(levelname)-8s [%(asctime)s]  %(message)s', datefmt='%m/%d/%Y %H:%M:%S', filename='d:\Development\Python\log\mongo-backup.log', level=logging.INFO)

# Check if file locked and exits
lockfile = "d:/Development/Mongodb/Mongolock.lock"
if os.path.exists(lockfile):
    logging.error("Another instance of this script is Running")
    sys.exit("Another instance of this script is Running")
else:
    lock = zc.lockfile.LockFile(lockfile, content_template='{pid}; {hostname}')

# Unlock and delete lock file.
def un_lock():
    lock.close()
    os.remove(lockfile)

# Key options for script launch
parser = argparse.ArgumentParser(description='Backup schedule options - Monthly,Weekly,Daily')
parser.add_argument('--monthly', '-m', action="store_true", help='Option for Monthly Backup')
parser.add_argument('--weekly', '-w', action="store_true", help='Option for Weekly Backup')
parser.add_argument('--daily', '-d', action="store_true", help='Option for Daily Backup')
 
args = parser.parse_args()

# Check our key arguments

if args.monthly:
    # work_dir = "/datadrive/opt/mongodbbackup/work/"
    # storage_dir = "/datadrive/opt/mongodbbackup/storage/montlhy"
    work_dir = "d:/Development/Mongodb/work/"
    storage_dir = "d:/Development/Mongodb/storage/monthly/"
    max_backups = 2
    logging.info("Starting monthly MongoDB backup" )
elif args.weekly:
    # work_dir = "/datadrive/opt/mongodbbackup/work/"
    # storage_dir = "/datadrive/opt/mongodbbackup/storage/weekly"
    work_dir = "d:/Development/Mongodb/work/weekly/"
    storage_dir = "d:/Development/Mongodb/storage/weekly/"
    max_backups = 4    
    logging.info("Starting weekly MongoDB backup" )
elif args.daily:
    # work_dir = "/datadrive/opt/mongodbbackup/work/"
    # storage_dir = "/datadrive/opt/mongodbbackup/storage/daily"
    work_dir = "d:/Development/Mongodb/work/"
    storage_dir = "d:/Development/Mongodb/storage/daily/"
    max_backups = 100
    logging.info("Starting daily MongoDB backup" )
else:
    work_dir = "d:/Development/Mongodb/work/"
    storage_dir = "d:/Development/Mongodb/storage/daily/"
    max_backups = 100
    logging.info("Starting daily MongoDB backup")

# Switch Mongo replica to single and reverse
def switch_mongo(src, dst):
    try:
        stop_check = subprocess.check_call(
            [
                'service',
                'mongod',
                'stop'
            ])
    except subprocess.CalledProcessError as e:
        if e.returncode !=0:
            logging.error("Failed To Stop mongodb service.Check log Output %s and ReturnCode %s" % (e.output, e.returncode))
            sys.exit("Failed To Stop mongodb service.Check log %s and ReturnCode" % (e.output))
    #logging_info(stop_check)
    os.remove(dst)
    shutil.copyfile(src, dst)
    try:
        start_check = subprocess.check_call(
        [
            'service',
            'mongod',
            'start'
        ])
    except subprocess.CalledProcessError as e:
            if e.returncode !=0:
                logging.error("Failed To Stop mongodb service.Check log Output %s and ReturnCode %s" % (e.output, e.returncode))
                sys.exit("Failed To Stop mongodb service.Check log %s and ReturnCode" % (e.output))    
    #logging_info(restart_check)

#switch_mongo('/etc/mongod.conf.single','/etc/mongod.conf')

# Connect to mongodb and Get all Database names
db_conn = MongoClient('localhost', 27017)
db_names=db_conn.database_names()

# DB auth credentials
# db_login="admin"
# db_pass="abbyy231*"

class MongoDB(object):
    def __init__(self, db_names):
        for x in db_names:
            if x != "local":
                self.db_name = x              
                self.mongo_backup(self.db_name)
                self.mongo_clean_up(self.db_name)
    def mongo_backup(self, db_name):
        self.now = datetime.datetime.today().strftime('%Y-%m-%d-%H-%M-%S')
        
        logging.info("Running mongodump for DB: %s " % (db_name))
        backup_output = subprocess.check_output(  # Run Mongo Dump for each Database
                    [
                        'mongodump',
                        # '-u', '%s' % db_login,
                        # '-p', '%s' % db_pass,
                        # '--authenticationDatabase','%s' %'admin',
                        '-d', '%s' % self.db_name,
                        # '--port', '%s' % port,
                        '-o', '%s' % work_dir
                    ])
        
        logging.info(backup_output)
        archive_name = self.db_name + '.' + self.now
        source_name = work_dir + self.db_name
        archive_path = os.path.join(storage_dir, self.db_name)
        
        # Check if backup directory exists
        if not os.path.exists(archive_path):
            os.makedirs(os.path.join(storage_dir, self.db_name))
        
        zip_name = os.path.join(archive_path,"%s.zip" % archive_name)
            
        with zipfile.ZipFile(zip_name, "w", allowZip64=True) as zf:  # Ziping the result
            abs_src = os.path.abspath(source_name)
            for dirname, subdirs, files in os.walk(abs_src):
                for filename in files:
                    absname = os.path.abspath(os.path.join(dirname, filename))
                    arcname = absname[len(abs_src) + 1:]
                    zf.write(absname, arcname)
            logging.info("End zip dump for DB: %s and saving zip file to %s " % (self.db_name, archive_path))
        logging.info("Backup Done Successfully")
        
    def mongo_clean_up(self, db_name):
            archive_path = os.path.join(storage_dir, self.db_name)
            a = []
            for files in os.listdir(archive_path):
                a.append(files)
               
            while len(a) > max_backups:
                a.sort()
                filetodel = a[0]
                del a[0]
                os.remove(os.path.join(archive_path,filetodel))
                logging.info("Start cleanup process. File %s was deleted from directory %s" % (filetodel, archive_path))
                logging.info("Cleanup Done for Backup zip files in %s Backup Directory: %d" % (self.db_name, len(a)))
        
        
def disk_clean_up(db_names):  # Delete old zip backup files when disk space is less than 15%
    cleanup_dir = "d:/Development/Mongodb/storage/daily/"
    for x in db_names:
        if x != 'local':
            cleanup_path = os.path.join(cleanup_dir, x)
            if not os.path.exists(cleanup_path):
                continue
            a = []
            for files in os.listdir(cleanup_path):
                a.append(files)
                a.sort()
                if len(a) == 0:
                    break
                filetodel = a[0]
                del a[0]
                os.remove(os.path.join(cleanup_path, filetodel))
                logging.info("Not enough free disk space. Cleanup process started.File to Del %s" % filetodel)

disk_space = psutil.disk_usage(storage_dir)
while disk_space.percent <= 85:
    disk_clean_up(db_names)
    
        
        

try:
    MongoDB(db_names)
except AssertionError, msg:
    logging.error(msg)

# Unlock and delete temp file
un_lock()

#switch_mongo('/etc/mongod.conf.replica','/etc/mongod.conf')