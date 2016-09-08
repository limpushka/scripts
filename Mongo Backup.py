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
import psutil
from pymongo import MongoClient,MongoReplicaSetClient

logging.basicConfig(format='%(levelname)s:%(message)s',filename='d:\Development\Python\log\mongo-backup.log',level=logging.INFO)


#Key options for script launch 
parser = argparse.ArgumentParser(description='Backup schedule options - Monthly,Weekly,Daily')
parser.add_argument('--monthly', '-m', action="store_true", help='Option for Monthly Backup')
parser.add_argument('--weekly', '-w', action="store_true", help='Option for Weekly Backup')
parser.add_argument('--daily', '-d', action="store_true", help='Option for Daily Backup')
 
args = parser.parse_args()

#Check our arguments

if args.monthly:
    #work_dir = "/datadrive/opt/mongodbbackup/work/"
    #storage_dir = "/datadrive/opt/mongodbbackup/storage/montlhy/"
    work_dir = "d:/Development/Mongodb/work/"
    storage_dir = "d:/Development/Mongodb/storage/monthly/"
    max_backups = 2    
elif args.weekly:
    #work_dir = "/datadrive/opt/mongodbbackup/work/"
    #storage_dir = "/datadrive/opt/mongodbbackup/storage/weekly/"
    work_dir = "d:/Development/Mongodb/work/weekly/"
    storage_dir = "d:/Development/Mongodb/storage/weekly/"
    max_backups = 4    
elif args.daily:
    #work_dir = "/datadrive/opt/mongodbbackup/work/"
    #storage_dir = "/datadrive/opt/mongodbbackup/storage/daily/"
    work_dir = "d:/Development/Mongodb/work/"
    storage_dir = "d:/Development/Mongodb/storage/daily/"
    max_backups = 2    
else:
    work_dir = "d:/Development/Mongodb/work/"
    storage_dir = "d:/Development/Mongodb/storage/monthly/"
    max_backups = 2    

#check free disk space
disk_space = psutil.disk_usage(storage_dir)
if (disk_space.percent > 85):
    print ("Not enough free disk space\n")
    
#pid = os.getpid()
#def check_pid(pid):        
    #""" Check for the existence of a pid. """
    #try:
        #os.kill(pid, 0)
    #except OSError:
        #return False
    #else:
        #return True
#check_pid(pid)


#Connect to mongodb and Get all Database names
db_conn = MongoClient('localhost',27017)
db_names=db_conn.database_names()

#DB auth credentials
#db_login="admin"
#db_pass="abbyy231*"




class MongoDB():
    get_start_time = datetime.datetime.today().strftime('%Y-%m-%d-%H-%M-%S')    
    logging.info("%s Starting MongoDB Backup" % (get_start_time)) 
       
    def __init__(self,db_names):    
        for x in db_names:
            if x != "local" and x !='admin':
                self.db_name = x
                print "Mongo BD Object for Backup Database Name %s " % self.db_name
                self.run_mongobackup(self.db_name)
                self.run_mongocleanup(self.db_name)
        
        
                
    def run_mongobackup(self,db_name):
        self.now = datetime.datetime.today().strftime('%Y-%m-%d-%H-%M-%S')
        #CLeanup work_dir folder.
        #os.remove(work_dir)
        
        #print os.path.dirname(work_dir)
        #cleanup_folder = subprocess.check_output(
            #[
                #'rm',
                #'-rf',
                #'%s','os.path.dirname(work_dir)'])
        #logging.info(cleanup_folder)
        #print "Running mongodump for %s current date %s " % (db_name,self.now)
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
        
            
            
        with zipfile.ZipFile(os.path.join(archive_path,"%s.zip" % (archive_name)), "w", zipfile.ZIP_DEFLATED) as zf:
            abs_src = os.path.abspath(source_name)
            for dirname, subdirs, files in os.walk(abs_src):
                for filename in files:
                    absname = os.path.abspath(os.path.join(dirname, filename))
                    arcname = absname[len(abs_src) + 1:]
                    #print 'zipping %s as %s' % (os.path.join(dirname, filename),
                    #                           arcname)
                    zf.write(absname, arcname)
                    #print "End zip dump and saving zip files to storage"
                         
        
        
        
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
            
            
            #print "CLeanup for DB %s Done" %self.db_name
        #print "Count for Backup zip files in %s Backup Directory: %d" %(self.db_name, len(a))
get_end_time = datetime.datetime.today().strftime('%Y-%m-%d-%H-%M-%S')    
logging.info("%s Backup Done Successfully" %(get_end_time))
  
     
              
try:
    MongoDB(db_names)
except AssertionError, msg:
    logging.error(msg)



    
    