#!/usr/bin/env python
# Copyright (c) 2012 Ozgur Batur
# License GPLv3, see http://www.gnu.org/licenses/gpl.html#content

from __future__ import with_statement

import os
import shutil
import logging
import time

from errno import *
from os.path import realpath
from sys import argv, exit, stdin
from threading import Lock, Thread
from socket import socket
from logging.handlers import SysLogHandler

from mydlpfuse import FUSE, FuseOSError, Operations,\
        LoggingMixIn, fuse_get_context

import subprocess
import signal
import quopri
import tempfile
import pwd

TMP_PATH = "/var/tmp/mydlp"
SAFE_MNT_PATH = "/var/tmp/mydlpep/safemount"

class ActiveFile():

    def __init__(self, path, context, fh):
        self.path = path
        self.cpath = None
        self.context = context
        self.changed = False
        self.fh = fh
        self.cfh = 0
        self.mode = 0
        self.flags = 0

    def create_cpath(self):
        if self.cpath is None:
            cpath_handle, self.cpath = tempfile.mkstemp(".tmp", "mydlpep-", TMP_PATH)
            os.close(cpath_handle)

    def duplicate_cpath(self):
        if self.cpath is None:
            return None
        cpath2_handle, cpath2 = tempfile.mkstemp(".tmp", "mydlpep-", TMP_PATH)
        os.close(cpath2_handle)
        os.remove(cpath2)
        os.link(self.cpath, cpath2)
        return cpath2

    def cleanup_cpath(self):
        if self.cpath is not None:
            try:
                os.remove(self.cpath)
            except (IOError, os.error) as why:
                errors = str(why) + " " + self.to_string() 
                logger.error("cleanup_cpath error" + errors)     
            self.cpath = None
   
    def cpath_to_string(self):
        if self.cpath is None:
            return "None"
        return self.cpath

    def to_string(self):
        uid, gid, pid = self.context
        return ("uid:" + str(uid) + " gid:" + str(gid) + " pid:" + 
                str(pid) +  " path:" + self.path + " cpath:" + self.cpath_to_string() +
                " fh:" + str(self.fh) + " cfh:" + str(self.cfh) +
                " changed :" + str(self.changed))
   

class SeapClient():

    def __init__(self, server, port):
        self.server = server
        self.port = port
        self.sock = socket()
        self.sock.settimeout(145)
        self.sock.connect((self.server, self.port))

    def send(self, message):
        logger.debug("try to send " + message)
        for try_count in range(3):        
            try:
                logger.debug("try count " + str(try_count))
                self.sock.sendall(message + "\r\n")
                response = self.sock.recv(1024).strip()
                logger.debug("<" + message + ">")
                logger.debug("[" + response + "]")
                if response == "":
                    self.sock = socket()
                    self.sock.settimeout(10)
                    self.sock.connect((self.server, self.port))
                else:
                    return response
            except IOError as why:
                logger.error("Seap send IOError" + str(why))
                print "Seap send IOError" + str(why)
                #time.sleep(2)
                self.sock = socket()
                self.sock.settimeout(10)
                self.sock.connect((self.server, self.port))

        logger.error("Seap send failed to server" + self.server + ":" +
                     str(self.port))
        return ""

    def allow_write_by_path(self, path, userpath, context):
        logger.debug("allow_write_by_path " + userpath)
        try:
            uid, guid, pid = context
            response = self.send("BEGIN")
            if not response.startswith("OK"):
                print "Return true from aclq. Begin: " + response
                return True
            
            userpathdir, userpathbase = os.path.split(userpath)
            opid = response.split()[1]
            response = self.send("SETPROP " + opid + " filename=" + quopri.encodestring(userpathbase))
            if not response.startswith("OK"):
                print "Return true from aclq. Setprop1: " + response
                return True

            response = self.send("SETPROP " + opid + " destination=" + quopri.encodestring(userpath))
            if not response.startswith("OK"):
                print "Return true from aclq. Setprop2: " + response
                return True

            response = self.send("SETPROP " + opid + " burn_after_reading=true")
            if not response.startswith("OK"):
                print "Return true from aclq. Setprop3: " + response
                return True
            
            user_tuple = pwd.getpwuid(uid)
            username = user_tuple.pw_name

            response = self.send("SETPROP " + opid + " user=" + username.strip())
            if not response.startswith("OK"):
                print "Return true from aclq. Setprop4: " + response
                return True
            
            response = self.send("PUSHFILE " + opid + " " + quopri.encodestring(path))
            if not response.startswith("OK"):
                print "Return true from pushfile. Response: " + response
                return True
            
            response = self.send("END " + opid)            
            if not response.startswith("OK"):
                print "Return true from end. Response: " + response
                return True
            
            response = self.send("ACLQ " + opid)
            if not response.startswith("OK"):
                print "Return true from aclq. Response: " + response
                return True
            self.send("DESTROY " + opid)
            print response.split()[1]
            if response.split()[1] == "block":
                return False
            else:
                return True
        except (IOError, OSError) as why:
            logger.error("allow_write_by_path " + str(why) )

class MyDLPFilter(LoggingMixIn, Operations):

    def __init__(self, mount, root):
        self.mount = realpath(mount)
        self.root = realpath(root)
        self.rwlock = Lock()
        self.files = {}
        self.seap =  SeapClient("127.0.0.1" , 9099)
        logger.info("Started on " + self.root)
        logger.info("Connecting to SEAP server 127.0.0.1:9099")

    def __call__(self, op, path, *args):
        return super(MyDLPFilter, self).__call__(op, self.root + path, *args)

    def access(self, path, mode):
        if not os.access(path, mode):
            raise FuseOSError(EACCES)

    chmod = os.chmod
    chown = os.chown

    def create(self, path, mode):
        context = fuse_get_context()
        try:
            fh = os.open(path, os.O_WRONLY | os.O_CREAT, mode)
        except:
            return -EBADF            
        if not fh in self.files:
            active_file = ActiveFile(path, context, fh)
            active_file.mode = mode
            active_file.flags = os.O_WRONLY | os.O_CREAT
            self.files.update({fh: active_file})
        logger.debug("create "+ self.files[fh].to_string())
        return fh

    def destroy(self, private_data):
        logger.info("stopped filter mounted on " + mount_point)

    def get_real_path(self, path):
        if path.startswith(self.root):
            return self.mount + path[len(self.root):]
        return path

    def handle_flush_sync(self, fh, context):
        if fh in self.files:
            active_file = self.files[fh] 
            if active_file.changed:
                retval = os.fsync(active_file.cfh)
                os.close(active_file.cfh)
                try:
                    #text = open(active_file.cpath, "r").read()
                    tmpcpath = active_file.duplicate_cpath()
                    if tmpcpath is None:
                        logger.error("tmpcpath is None using cpath" + active_file.cpath)
                        tmpcpath = active_file.cpath
                    if not self.seap.allow_write_by_path(tmpcpath,
                                                        self.get_real_path(active_file.path), 
                                                         context):
                       logger.info("block flush to " + active_file.path)
                       retval = -EACCES
                    else:
                        shutil.copy2(active_file.cpath, active_file.path)
                        logger.debug("flush changed file " +
                                     active_file.to_string())
                except (IOError, os.error) as why:
                    errors = str(why) + " " + active_file.to_string() 
                    logger.error("flush error " + errors)
                finally:
                    active_file.cleanup_cpath()
                active_file.changed = False
                return retval
            else:
                logger.debug("flush unchanged " + active_file.to_string())
                return os.fsync(active_file.fh)
        else:
            logger.error("flush error EBADF fh:", active_file.fh)  
            return -EBADF
        
    def flush(self, path, fh):
        logger.debug("flush "+ self.files[fh].to_string())
        print "FLUSH is called. Path: " + path
        context = fuse_get_context()
        return self.handle_flush_sync(fh, context)

    def fsync(self, path, datasync, fh):
        logger.debug("fsync "+ self.files[fh].to_string())
        print "fsync: "+ self.files[fh].to_string()
        context = fuse_get_context()
        return self.handle_flush_sync(fh, context)

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
        if not fh in self.files:
            active_file = ActiveFile(path, context, fh)
            active_file.flags = flags
            logger.debug("open new file " + active_file.to_string())
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
        print "RELEASE IS CALLED. Path: " + path
        del self.files[fh]
        return os.close(fh)

    def rename(self, old, new):
        if not new.startswith(self.root):
            new = self.root + new
        logger.debug("rename: " + old + " " + new)  
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
        #print "WRITE is called. Path: " + path
        context = fuse_get_context()
        if fh in self.files:
            active_file = self.files[fh]
            if active_file.changed == False:
                active_file.changed = True 
                active_file.create_cpath()
                if not os.path.exists(os.path.dirname(active_file.cpath)):
                    os.makedirs(os.path.dirname(active_file.cpath))
                try:
                    shutil.copy2(path, active_file.cpath)
                    logger.debug("write copy on change from " + path + " to "
                                 + active_file.to_string())
                    
                except (IOError, os.error) as why:
                    logger.error("write exception " + str(why))
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
    try:
        if not os.path.exists(safe_point):
            os.makedirs(safe_point)
    except OSError as e:
        logger.debug("mkdir execution failed: " + e.strerror)
        print "mkdir execution failed: " + e.strerror

    try:
        retcode = subprocess.call("/bin/mount --bind " + mount_point + " " + safe_point, shell=True)
        if retcode == 0:
            logger.debug("Mount bind process returned: " + str(retcode))
            print "Mount bind process returned: " + str(retcode)
        else:
            logger.debug("MOUNT BIND Process terminated by signal: " + str(retcode))
            print "MOUNT BIND Process terminated by signal: " + str(retcode)
    except OSError as e:
        logger.debug("mount bind execution failed: " + e.strerror)
        print "mount bind execution failed: " + e.strerror
    
    fuse = FUSE(MyDLPFilter(mount_point, safe_point), mount_point, foreground=True, 
                nonempty=True, allow_other=True)

def remove_old_safe_mount(path):
    if os.path.isdir(path):
        for f in os.listdir(path):
            abs_path = os.path.join(path, f)
            remove_old_safe_mount(abs_path)
        os.rmdir(path)
    else:
        os.remove(path)


signal_mount = None
signal_safemount = None

def set_signal_globals(mount, safemount):
    global signal_mount
    global signal_safemount
    signal_mount = mount
    signal_safemount = safemount

def signal_handler(signal, frame):
    logger.debug("terminating")
    print "Signal handler is called"
    if signal_mount is not None:
        try:
            retcode = subprocess.call("/bin/umount -l " + signal_mount, shell=True)
            if retcode == 0:
                logger.debug("Umount process of mount point returned: " + str(retcode))
                print "Unmount process of mount point returned: " + str(retcode)
            else:
                logger.debug("Unmount Process of mount point terminated by signal: " + str(retcode))
                print "Unmount Process of mount point terminated by signal: " + str(retcode)
        except OSError as e:
            logger.debug("Unmount of mount point execution failed: " + e.strerror)
    if signal_safemount is not None:
        try:
            retcode = subprocess.call("/bin/umount -l " + signal_safemount, shell=True)
            if retcode == 0:
                remove_old_safe_mount(signal_safemount)
                logger.debug("Umount process of safe mount point returned: " + str(retcode))
                print "Unmount process of safe mount point returned: " + str(retcode)
            else:
                logger.debug("Unmount Process of safe mount point terminated by signal: " + str(retcode))
                print "Unmount Process of safe mount point terminated by signal: " + str(retcode)
        except OSError as e:
            logger.debug("Unmount of safemount point execution failed: " + e.strerror)
    exit(0)
    
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
    safe_point = SAFE_MNT_PATH + realpath(mount_point)
    
    logger.debug("Starting MyDLP filterfs on " + mount_point)
    logger.debug("Safe mount on " + safe_point)
    logger.debug("Temp path on " + TMP_PATH)
#    os.system("/bin/rmdir --ignore-fail-on-non-empty " + safe_point)
    if os.path.exists(safe_point):
        if os.path.ismount(safe_point):
            try:
                retcode = subprocess.call("/bin/umount -l " + safe_point, shell=True)
                if retcode == 0:
                    logger.debug("Umount process of safe mount point returned: " + str(retcode))
                    print "Unmount process of safe mount point returned: " + str(retcode)
                else:
                    logger.debug("Unmount Process of safe mount point terminated by signal: " + str(retcode))
                    print "Unmount Process of safe mount point terminated by signal: " + str(retcode)
            except OSError as e:
                logger.debug("Unmount of safemount point execution failed: " + e.strerror)
 
        remove_old_safe_mount(safe_point)
    set_signal_globals(realpath(mount_point), safe_point)
    signal.signal(signal.SIGINT, signal_handler)
    start_fuse(mount_point, safe_point)
    signal_handler(signal.SIGINT, None)


