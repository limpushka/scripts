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
from shutil import copyfile, rmtree, copytree, move
from shlex import split
from pymongo import MongoClient


work_dir = "/datadrive/opt/mongodbbackup/work/"
cleanup_dir = "/datadrive/opt/mongodbbackup/storage/daily"
fresh_backup_dir = "/datadrive/opt/mongodbbackup/fresh/"
mongodb_conf = "/etc/mongod.conf"
lockfile = "/tmp/mongo-backup.lock"
logfile = "/var/log/mongodb/mongo-backup.log"
backup_time = datetime.datetime.today().strftime('%Y-%m-%d_%H-%M-%S')

logging.basicConfig(format=u'%(levelname)-8s [%(asctime)s]  %(message)s', datefmt='%m/%d/%Y %H:%M:%S-%Z', filename = logfile , level=logging.INFO)

# Check if  directory exists? otherwise creates it
def check_dir(path):
    if not os.path.exists(path):  
        os.makedirs(path) 

# Check disk space usage
def get_disk_space():
    disk_space = psutil.disk_usage(storage_dir)
    return disk_space.percent

# Remove unpacked backup for extra fast mongorestoring
def move_backup():
    logging.info("Start moving fresh backup")
    check_dir(fresh_backup_dir)
    d = []
    for dirname in os.listdir(fresh_backup_dir):
        d.append(dirname)
    if len(d) == 2:
        d.sort()
        dirtodel = d[0]
        del d[0]
        rmtree(os.path.join(fresh_backup_dir,dirtodel))        
        logging.info("%s Deleted from fresh backup directory" % dirtodel)
    fresh_dir = os.path.join(fresh_backup_dir, backup_time)   
    move(work_dir,fresh_dir)
    
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
else:
    logging.info("Please specify key arguments.--monthly - Option for Monthly Backup,--weekly - Option for Weekly Backup , -daily - Option for Daily Backup")
    sys.exit("Please specify key arguments.--monthly - Option for Monthly Backup,--weekly - Option for Weekly Backup , -daily - Option for Daily Backup")    

# Unlock and delete lock file.
def un_lock():
    lock.close()
    os.remove(lockfile)

# Switch Mongod replica to single and reverse
def switch_to_single():
    logging.info("Start switching Mongod to single instance. Stopping service")
    backup_time = datetime.datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
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
                logging.error("Failed To Start Mongod service. Check log. ReturnCode is %s" % e.returncode)
                sys.exit("Failed To Start Mongod service. Check log. ReturnCode is %s" % e.returncode)
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
                sys.exit("Failed to Start Mongod service. Check log %s and ReturnCode" % (e.output))
    logging.info("Switching Mongod to replica ended successfully.")

class MongoDB:
    mongodb_list = []
    

    def __init__(self):
        self.db_name = db_name
        self.mongodb_list.append(self)
             
    def mongo_backup(self):
        logging.info("Running mongodump for DB: %s, dumptime: %s" % (self.db_name, backup_time))
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
        
        archive_name = self.db_name + '_' + backup_time
        source_name = work_dir + self.db_name
        archive_path = os.path.join(storage_dir, self.db_name)

        check_dir(archive_path)

        zip_name = os.path.join(archive_path, "%s.zip" % archive_name)
        tar_name = os.path.join(archive_path, "%s.tar.lz4" % archive_name)
        logging.info("Start zipping dump for DB: %s. Archive zip file name %s " % (self.db_name, archive_name))
        
        
        #tar cvf - folderABC | lz4 > folderABC.tar.lz4
        os.chdir(work_dir)
        tar_cmd = ['tar', 'cvf', '-', '%s' % self.db_name] 
        lz4_cmd = ['lz4', '>', '%s' % tar_name] 
        tar = subprocess.Popen(split(tar_cmd), stdout=subprocess.PIPE)
        lz4 = subprocess.Popen(split(lz4_cmd), stdin=tar.stdout, stdout=subprocess.PIPE)
        output = lz4.communicate()[0]        
        try:
            tar = subprocess.Popen(split(tar_cmd), stdout=subprocess.PIPE)
            lz4 = subprocess.Popen(split(lz4_cmd), stdin=tar.stdout, stdout=subprocess.PIPE)
            output = lz4.communicate()[0]              
            
            #zip_from_shell = subprocess.check_call(  # Run tar+lz4 for Db dump
                    #[
                        #'tar',
                        #'cvf',
                        #'-',
                        #'%s' % self.db_name,
                        #'|',
                        #'lz4',
                        #'>',
                        #'%s'  % tar_name
                    
                        
                    #])
        except subprocess.CalledProcessError as e:
                            logging.error("Failed to run zip. Output Error %s" % e.output)
                            sys.exit("Failed to run zip. Output Error %s" % e.output)
                            
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
        
    if len(a) > 6 :
        a.sort()
        filetodel = a[0]
        del a[0]
        os.remove(os.path.join(cleanup_path, filetodel))
        logging.info("Not enough free disk space. Cleanup process started.File to Del %s" % filetodel)
    else :
        logging.error("Disk cleanup failed. Nothing to delete.")
        un_lock()
        sys.exit("Disk cleanup failed. Nothing to delete.")
                    

"""Script run start's here"""

# Check, if file is locked and exits, if true
if os.path.exists(lockfile):
    logging.info("Another instance of this script is Running")
    sys.exit("Another instance of this script is Running")
else:
    lock = zc.lockfile.LockFile(lockfile, content_template='{pid}; {hostname}')

# Start cleaning working directory
logging.info("Cleaning working directory")
if os.path.exists(work_dir):
    rmtree(work_dir) # Remove all files in work_dir                                        
                                    
# Switch Mongod to single 
switch_to_single()

# Connect to Mongodb. Get list of all database names
db_conn = MongoClient('localhost', 27017)
db_names = db_conn.database_names()
 
# Checks free disk space and cleans storage directory  if disk usage is higher than 85%
while get_disk_space() >= 85:
    try:
        for db_name in db_names:
            cleanup_path = os.path.join(cleanup_dir, db_name)
            if not os.path.exists(cleanup_path):
                continue
            else:
                disk_clean_up(db_name)
    except AssertionError, msg:
        logging.error(msg)
        

        
for db_name in db_names:
    if db_name != 'et_api':
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

#Copy unpacked dump files to do extrafast mongorestoring 
move_backup()

# Final Message
logging.info("All task's for current backup schedule done.")