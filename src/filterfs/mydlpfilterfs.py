#!/usr/bin/env python
# Copyright (c) 2012 Ozgur Batur
# License GPLv3, see http://www.gnu.org/licenses/gpl.html#content

from __future__ import with_statement

import os
import shutil
import logging

from errno import *
from os.path import realpath
from sys import argv, exit, stdin
from threading import Lock, Thread
from socket import socket
from logging.handlers import SysLogHandler

from mydlpfuse import FUSE, FuseOSError, Operations,\
        LoggingMixIn, fuse_get_context

TMP_PATH = "/var/cache/mydlpep/wcache"
SAFE_MNT_PATH = "/mnt/mydlpep/safemount"

class ActiveFile():

    def __init__(self, path, context, fh):
        self.path = path
        self.cpath = TMP_PATH + path
        self.context = context
        self.changed = False
        self.fh = fh
        self.cfh = 0
        self.mode = 0
        self.flags = 0

    def to_string(self):
        uid, gid, pid = self.context
        return ("uid:" + str(uid) + " gid:" + str(gid) + " pid:" + 
                str(pid) +  " path:" + self.path + " cpath:" + self.cpath +
                " fh:" + str(self.fh) + " cfh:" + str(self.cfh) +
                " changed :" + str(self.changed))
   

class SeapClient():

    def __init__(self, server, port):
        self.server = server
        self.port = port
        self.sock = socket()
        self.sock.connect((server, port))

    def send(message):
        self.sock.sendall(message + "\n")
        response = self.sock.recv(1024).strip()
        return response

    def allow_write_by_path(self, path, userpath, context):
        uid, guid, pid = context
        response = send("BEGIN")
        if not response.startswith("OK"):
            return True

        opid = response.split()[1]
        response = send("SETPROP " + opid + " filename=" + userpath)
        if not response.startswith("OK"):
            return True

        #This is not required in linux client
        #self.sock.sendall("SETPROP " + opid + " burn_after_reading=true\n")
        #response = self.sock.recv(1024).strip()
        #if not response.startswith("OK"):
        #   return True

        p = os.popen("getent passwd  |awk -v val=" + str(uid) + 
                     " -F \":\" '$3==val{print $1}'")
        username = p.readline()
        p.close()
        response = send("SETPROP " + opid + " user=" + username)
        if not response.startswith("OK"):
            return True

        response = send("PUSHFILE " + opid + " " + path)
        response = self.sock.recv(1024).strip()
        if not response.startswith("OK"):
            return True

        response = send("END " + opid)
        if not response.startswith("OK"):
            return True

        response = send("ACLQ " + opid)
        if not response.startswith("OK"):
            return True
        send("DESTROY " + opid)
        if response.split()[1] == "block":
            return False
        else:
            return True


class MyDLPFilter(LoggingMixIn, Operations):

    def __init__(self, root):
        self.root = realpath(root)
        self.rwlock = Lock()
        self.files = {}
        self.seap =  SeapClient("127.0.0.1" , 9099)
        logger.info("Started on " + self.root)
        logger.info("Connected to SEAP server 127.0.0.1:9099")

    def __call__(self, op, path, *args):
        return super(MyDLPFilter, self).__call__(op, self.root + path, *args)

    def access(self, path, mode):
        if not os.access(path, mode):
            raise FuseOSError(EACCES)

    chmod = os.chmod
    chown = os.chown

    def create(self, path, mode):
        context = fuse_get_context()
        fh = os.open(path, os.O_WRONLY | os.O_CREAT, mode)
        if not fh in self.files:
            active_file = ActiveFile(path, context, fh)
            active_file.mode = mode
            active_file.flags = os.O_WRONLY | os.O_CREAT
            self.files.update({fh: active_file})
        logger.debug("create "+ self.files[fh].to_string())
        return fh

    def destroy(self, private_data):
        logger.info("stopped filter mounted on " + mount_point)
        
    def flush(self, path, fh):
        context = fuse_get_context()
        if fh in self.files:
            active_file = self.files[fh] 
            if active_file.changed:
                retval = os.fsync(active_file.cfh)
                try:
                    text = open(active_file.cpath, "r").read()
                    if not self.seap.allow_write_by_path(active_file.cpath,
                                                         active_file.path, 
                                                         context):
                        logger.info("block flush to " + active_file.path)
                        retval = -EACCES
                    else:
                        shutil.copy2(active_file.cpath, active_file.path)
                        logger.debug("flush changed file " +
                                     active_file.to_string())
                except (IOError, os.error) as why:
                    errors.append((active_file.cpath, 
                                   active_file.cpath, 
                                   str(why)))
                    logger.error("flush error" + errors)
                active_file.changed = False
                return retval
            else:
                logger.debug("flush unchanged " + active_file.to_string())
                return os.fsync(active_file.fh)
        else:
            logger.error("flush error EBADF fh:", active_file.fh)  
            return -EBADF

    def fsync(self, path, datasync, fh):
        context = fuse_get_context()
        logger.debug("fync context:", context, " path: ", path)
        if fh in self.files:
            active_file = self.files[fh] 
            if active_file.changed:
                retval = os.fsync(active_file.cfh)
                try:
                    text = open(active_file.cpath, "r").read()
                    if not self.seap.allow_write_by_path(active_file.cpath,
                                                         active_file.path, 
                                                         context):
                    #if text.find("block") != -1:
                        logger.info("block sync to " + active_file.path)
                        retval = -EACCES
                    else:
                        shutil.copy2(active_file.cpath, active_file.path)
                        logger.info("fsync changed  file " +
                                    active_file.to_string())
                except (IOError, os.error) as why:
                    errors.append((active_file.cpath, 
                                   active_file.cpath, 
                                   str(why)))
                    logger.error("fsync error" + errors)     
                active_file.changed = False
                return retval
            else:
                logger.debug("fsync unchanged " + active_file.to_string())
                return os.fsync(active_file.fh)
        else:
            logger.error("fsync error EBADF fh:", active_file.fh)
            return -EBADF

    def getattr(self, path, fh=None):
        st = os.lstat(path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
            'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    getxattr = None

    def link(self, target, source):
        return os.link(source, target)

    listxattr = None
    mkdir = os.mkdir
    mknod = os.mknod

    def open(self, path, flags):
        context = fuse_get_context()
        fh = os.open(path, flags)
        logger.debug("open context:", context, " path: ", path)
        if not (context, path) in self.files:
            active_file = ActiveFile(path, context, fh)
            active_file.flags = flags
            print "open new file " + active_file.to_string()
            self.files.update({fh: active_file})
        return fh

    #todo need to add sth here for apps read files 
    #after writing before flushing
    def read(self, path, size, offset, fh):
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.read(fh, size)

    def readdir(self, path, fh):
        uid, guid, pid  = fuse_get_context()
        return ['.', '..'] + os.listdir(path)

    readlink = os.readlink

    def release(self, path, fh):
        del self.files[fh]
        return os.close(fh)

    def rename(self, old, new):
        uid, guid, pid  = fuse_get_context()
        return os.rename(old, new)

    rmdir = os.rmdir

    def statfs(self, path):
        stv = os.statvfs(path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail',
             'f_bfree', 'f_blocks', 'f_bsize', 'f_favail', 'f_ffree',
             'f_files', 'f_flag', 'f_frsize', 'f_namemax'))

    def symlink(self, target, source):
        return os.symlink(source, target)

    def truncate(self, path, length, fh=None):
        with open(path, 'r+') as f:
            f.truncate(length)

    unlink = os.unlink
    utimens = os.utime

    def write(self, path, data, offset, fh):
        context = fuse_get_context()
        if fh in self.files:
            active_file = self.files[fh]
            if active_file.changed == False:
                active_file.changed = True 
                active_file.cpath = TMP_PATH + path               
                if not os.path.exists(os.path.dirname(active_file.cpath)):
                    os.makedirs(os.path.dirname(active_file.cpath))
                try:
                    shutil.copy2(path, active_file.cpath)
                    logger.debug("write copy on change from " + path + " to "
                                 + active_file.to_string())
                    
                except (IOError, os.error) as why:
                    print str(why)
                if active_file.mode !=0:
                    active_file.cfh = os.open(active_file.cpath,
                                              active_file.flags, 
                                              active_file.mode)
                else:
                    active_file.cfh = os.open(active_file.cpath, 
                                              active_file.flags)
            with self.rwlock:
                logger.debug("write to cpath " + active_file.to_string())
                os.lseek(active_file.cfh, offset, 0)
                return os.write(active_file.cfh, data)
        else:
            logger.error("write error EBADF fh:" + fh)
            return -EBADF 


def start_fuse(mount_point, safe_point):
    os.system("mkdir -p " + safe_point)
    os.system("mount --bind " + mount_point + " " + safe_point)
    fuse = FUSE(MyDLPFilter(safe_point), mount_point, foreground=True, 
                nonempty=True, allow_other=True)
    

if __name__ == '__main__':
    logger = logging.getLogger()
    handler = SysLogHandler(address = '/dev/log', 
                            facility = SysLogHandler.LOG_LOCAL6)
    formatter = logging.Formatter('MyDLP filterfs: %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    
    if len(argv) != 2:
        print('usage: %s  <mountpoint>' % argv[0])
        logger.error("Incorrect parameters")
        exit(1)
    
    mount_point = argv[1]
    safe_point = SAFE_MNT_PATH + mount_point
    
    logger.info("Starting MyDLP filterfs on " + mount_point)
    logger.debug("Safe mount on " + safe_point)
    logger.debug("Temp path on " + TMP_PATH)
    start_fuse(mount_point, safe_point)
    os.system("umount "  + safe_point) 
    os.system("rm -rf " + safe_point)
    os.system("rm -rf " + TMP_PATH)


