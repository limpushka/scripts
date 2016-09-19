#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Backup script Made for Abbyy-LS
# Script connects to MongoDB, gets all db names, then for each database except "local" performs mongodump and archive's the result  to our backup storage directory.
# Script checks our storage directory and if free disk space is less than 15% - performs Disk Clean up.

import sys
import os
import time
import argparse
import logging
import datetime
import subprocess
import zipfile
import psutil
import zc.lockfile
from shutil import copyfile, rmtree
from pymongo import MongoClient

logging.basicConfig(format=u'%(levelname)-8s [%(asctime)s]  %(message)s', datefmt='%m/%d/%Y %H:%M:%S', filename='/var/log/mongo-backup.log', level=logging.INFO)

work_dir = "/datadrive/opt/mongodbbackup/work/"
cleanup_dir = "/datadrive/opt/mongodbbackup/storage/daily"
mongodb_conf = "/etc/mongod.conf"
lockfile = "/tmp/mongo-backup.lock"

# Check if  directory exists? otherwise creates it
def check_dir(path):
    if not os.path.exists(path):  
        os.makedirs(path) 

# Key options for script launch
parser = argparse.ArgumentParser(description='Backup schedule options - Monthly,Weekly,Daily')
parser.add_argument('--monthly', '-m', action="store_true", help='Option for Monthly Backup')
parser.add_argument('--weekly', '-w', action="store_true", help='Option for Weekly Backup')
parser.add_argument('--daily', '-d', action="store_true", help='Option for Daily Backup')
                                     
args = parser.parse_args()

# Checking input arguments
if args.monthly:
    storage_dir = "/datadrive/opt/mongodbbackup/storage/monthly"
    check_dir(storage_dir)  
    max_backups = 2
    logging.info("Starting monthly MongoDB backup")
elif args.weekly:
    storage_dir = "/datadrive/opt/mongodbbackup/storage/weekly"
    check_dir(storage_dir)
    max_backups = 4    
    logging.info("Starting weekly MongoDB backup")
elif args.daily:
    storage_dir = "/datadrive/opt/mongodbbackup/storage/daily"
    check_dir(storage_dir)   
    max_backups = 1000
    logging.info("Starting daily MongoDB backup")    
    
# Unlock and delete lock file.
def un_lock():
    lock.close()
    os.remove(lockfile)

# Switch Mongod replica to single and reverse
def switch_to_single():
    logging.info("Start switching Mongod to single instance. Stopping service")
    try:
        stop_check = subprocess.check_call(
        [
            'service',
            'mongod',
            'stop'
        ])
    except subprocess.CalledProcessError as e:
            if e.returncode !=0:
                logging.error("Failed To Stop Mongod service. Check log. ReturnCode is %s" % e.returncode)
                sys.exit("Failed To Stop Mongod service.Check log. ReturnCode is %s" % e.returncode)
    
    
    os.remove(mongodb_conf)
    logging.info("Copying Mongod config")
    copyfile('/etc/mongod.conf.single', mongodb_conf)
    logging.info("Starting Mongod service")
    try:
        start_check = subprocess.check_call(
        [
            'service',
            'mongod',
            'start'
        ])
    except subprocess.CalledProcessError as e:
            if e.returncode !=0:
                logging.error("Failed To Stop Mongod service. Check log. ReturnCode is %s" % e.returncode)
                sys.exit("Failed To Stop Mongod service. Check log. ReturnCode is %s" % e.returncode)
    logging.info("Switching Mongod to single instance ended successfully.")    
    
# Switch Mongodb single to replica set
def switch_to_replica():
    logging.info("Start switching Mongod to replica set. Stopping service")
    try:
        stop_check = subprocess.check_call(
        [
            'service',
            'mongod',
            'stop'
        ])
    except subprocess.CalledProcessError as e:
            if e.returncode !=0:
                logging.error("Failed To Stop Mongod service. Check log. ReturnCode is %s" % e.returncode)
                sys.exit("Failed To Stop Mongod service.Check log %s and ReturnCode" % (e.output))
    
    os.remove(mongodb_conf)
    logging.info("Copying Mongod config")
    copyfile('/etc/mongod.conf.replica', mongodb_conf)
    logging.info("Starting Mongod service")
    try:
        start_check = subprocess.check_call(
        [
            'service',
            'mongod',
            'start'
        ])
    except subprocess.CalledProcessError as e:
            if e.returncode !=0:
                logging.error("Failed to Start Mongod service. Check log. ReturnCode is %s" % e.returncode)
                sys.exit("Failed to Stop Mongod service. Check log %s and ReturnCode" % (e.output))
    logging.info("Switching Mongod to replica ended successfully.")

class MongoDB:
    mongodb_list = []

    def __init__(self):
        self.db_name = db_name
        self.dumptime = datetime.datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
        self.mongodb_list.append(self)
             
    def mongo_backup(self):
        self.dumptime = datetime.datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
        logging.info("Running mongodump for DB: %s , dumptime: %s" % (self.db_name, self.dumptime))
        try:
            backup_output = subprocess.check_call(  # Run Mongodump for each Database
                    [
                        'mongodump',
                        '-d', '%s' % self.db_name,
                        '-o', '%s' % work_dir
                    ])
        except subprocess.CalledProcessError as e:
                    logging.error("Failed to run mongodump. Output Error %s" % e.output)
                    sys.exit("Failed to run mongodump. Output Error %s" % e.output)        
        logging.info("Mongodump for DB: %s ended Successfully" % self.db_name)        
        
    def mongo_zip_result(self):
        
        archive_name = self.db_name + '.' + self.dumptime
        source_name = work_dir + self.db_name
        archive_path = os.path.join(storage_dir, self.db_name)

        check_dir(archive_path)

        zip_name = os.path.join(archive_path, "%s.zip" % archive_name)
        logging.info("Start zipping dump for DB: %s. Archive zip file name %s " % (self.db_name, archive_name))
        
        with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as zf:  # Zipping the result
            abs_src = os.path.abspath(source_name)
            for dirname, subdirs, files in os.walk(abs_src):
                for filename in files:
                    absname = os.path.abspath(os.path.join(dirname, filename))
                    arcname = absname[len(abs_src) + 1:]
                    zf.write(absname, arcname)
            logging.info("End zip dump for DB: %s and saving zip file %s to %s " % (self.db_name, archive_name, archive_path))
            logging.info("Zipping for %s Done Successfully" %archive_name)

    def mongo_clean_up(self):
            archive_path = os.path.join(storage_dir, self.db_name)
            a = []

            check_dir(archive_path)  
                
            for files in os.listdir(archive_path):  
                a.append(files)                

            while len(a) > max_backups:
                a.sort()
                filetodel = a[0]
                del a[0]
                os.remove(os.path.join(archive_path,filetodel))
                logging.info("Starting cleanup process. File %s was deleted from directory %s" % (filetodel, archive_path))
                logging.info("Cleanup Done. Total files:%d in Backup Directory %s" % (len(a), self.db_name))
                

def disk_clean_up(db_name):  # Delete old archive backup files when free disk space is less than 15%
    logging.info("Starting disk_clean_up function for %s" % db_name)
    cleanup_path = os.path.join(cleanup_dir, db_name)
    a = []
    for files in os.listdir(cleanup_path):
        a.append(files)
        
    while len(a) > 2 :
        a.sort()
        filetodel = a[0]
        del a[0]
        os.remove(os.path.join(cleanup_path, filetodel))
        logging.info("Not enough free disk space. Cleanup process started.File to Del %s" % filetodel)
    
                    
def check_backup_count(db_names):
    result = False
    b = {}
    for db_name in db_names:
        if db_name != "local":
            cleanup_path = os.path.join(cleanup_dir, db_name)
            for files in os.listdir(cleanup_path):
                a = []
                a.append(files)
                if len(a) > 2 :
                    b[db_name] =  True
                else:
                    b[db_name] = False
    
    
    for key in b:
        if b[key] == True:
            result = True
        else:
            result = False
    return result
            
               


"""Script run start's here"""

# Check, if file is locked and exits, if true
if os.path.exists(lockfile):
    logging.error("Another instance of this script is Running")
    sys.exit("Another instance of this script is Running")
else:
    lock = zc.lockfile.LockFile(lockfile, content_template='{pid}; {hostname}')

# Start cleaning working directory
logging.info("CLeaning working directory")
if os.path.exists(work_dir):
    rmtree(work_dir) # Remove all files in work_dir                                        
                                    
# Switch Mongod to single 
switch_to_single()

# Connect to Mongodb. Get list of all database names
db_conn = MongoClient('localhost', 27017)
db_names = db_conn.database_names()

# Checks free disk space and cleans storage directory  if disk usage is higher than 85%
disk_space = psutil.disk_usage(storage_dir)
if disk_space.percent >= 85:
    try:
        for db_name in db_names:
            if db_name != "local":
                cleanup_path = os.path.join(cleanup_dir, db_name)
                if not os.path.exists(cleanup_path):
                    continue
                else:
                    disk_clean_up(db_name)
    except AssertionError, msg:
        logging.error(msg)
#else :
    #logging.error("Disk cleanup failed. Nothing to delete.")
    #sys.exit("Disk cleanup failed. Nothing to delete.")
        
for db_name in db_names:
    if db_name != "local":
        try:
            db_name = MongoDB()
            db_name.mongo_backup() 
        except AssertionError, msg:
            logging.error(msg)

# Swiching to single
switch_to_replica()

for db_name in MongoDB.mongodb_list:
    try:
        db_name.mongo_zip_result()
        db_name.mongo_clean_up()
    except AssertionError, msg:
        logging.error(msg)
            
# Unlocking and deleting temp file
un_lock()

# Final Message
logging.info("All task's for current backup schedule done.")