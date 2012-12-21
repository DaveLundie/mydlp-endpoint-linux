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

import quopri
import tempfile
import pwd

import pyudev
import signal
from time import sleep
from subprocess import Popen

class PartitionTracer():
    def __init__(self, root_device):
        self.root_device = root_device
        self.processes = dict()
        self.mounts = dict()
        self.safemounts = dict()
        self.filters = dict()

    def loop(self):
        while True:
            self.poll_mounts()
            self.poll_safemounts()
            self.poll_filters()
            sleep(1)

    def poll_mounts(self):
        new_mounts = dict()
        for line in open('/proc/mounts'):
            if line.startswith('/dev/' + self.root_device):
                parts = line.split()
                if parts[1].startswith('/var/tmp/mydlpep/safemount'):
                    continue
                new_mounts[parts[0]] = parts[1]
        self.handle_mount_change(new_mounts)

    def handle_mount_change(self, new_mounts):
        for dev in self.mounts.keys():
            if not new_mounts.has_key(dev):
                # means that dev has been umounted
                self.handle_umount(dev)
            elif (self.mounts[dev] != new_mounts[dev]):
                # means that dev has been umounted and remounted
                self.handle_umount(dev)
                self.handle_mount(dev, new_mounts[dev])
        for dev in new_mounts.keys():
            if not self.mounts.has_key(dev):
                # means that dev has been newly mounted
                self.handle_mount(dev, new_mounts[dev])

        # update mounts
        self.mounts = new_mounts

    def poll_safemounts(self):
        new_safemounts = dict()
        for line in open('/proc/mounts'):
            if line.startswith('/dev/' + self.root_device):
                parts = line.split()
                if parts[1].startswith('/var/tmp/mydlpep/safemount'):
		    new_safemounts[parts[0]] = parts[1]
        self.handle_safemount_change(new_safemounts)

    def handle_safemount_change(self, new_safemounts):
        for dev in self.safemounts.keys():
            if not new_safemounts.has_key(dev):
                # means that dev has been umounted
                self.handle_umount(dev)
            elif (self.safemounts[dev] != new_safemounts[dev]):
                # means that dev has been umounted and remounted
                self.handle_umount(dev)

        # update safemounts
        self.safemounts = new_safemounts

    def poll_filters(self):
        new_filters = dict()
        for line in open('/proc/mounts'):
            if line.startswith('MyDLPFilter'):
                parts = line.split()
		new_filters[parts[1]] = parts[0]
        self.handle_filter_change(new_filters)

    def handle_filter_change(self, new_filters):
        for dev in self.filters.keys():
            if not new_filters.has_key(dev):
                # means that dev has been umounted
                self.handle_umount_path(dev)

        # update filters
        self.filters = new_filters

    def handle_umount_path(self, path):
        for dev, devpath in self.mounts.items():
            if devpath == path:
                self.handle_umount(dev)
                return
        logger.debug('handle_umount_path failed, cannot find path in filters ' + path)

    def handle_umount(self, dev):
        logger.debug("deattaching " + dev)
        if not self.processes.has_key(dev):
            logger.debug('handle_umount failed, can not find a process for ' + dev)
            return
        p = self.processes[dev]
        p.send_signal(signal.SIGINT)
        del self.processes[dev]

    def handle_mount(self, dev, path):
        logger.debug("attaching " + dev + " " + path)
        p = Popen(['/usr/share/mydlp-endpoint-linux/libexec/mydlpfilterfs.py', path])
        if self.processes.has_key(dev):
            logger.debug('a process for ' + dev + ' already exists, overwriting')
        self.processes[dev] = p

if __name__ == '__main__':
    logger = logging.getLogger()
    handler = SysLogHandler(address = '/dev/log', 
                            facility = SysLogHandler.LOG_LOCAL6)
    formatter = logging.Formatter('MyDLP mountfs: %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    
    if len(argv) != 2:
        print('usage: %s  <trace_device>' % argv[0])
        logger.error("Incorrect parameters")
        exit(1)
    
    trace_device = argv[1]
    
    logger.debug("Tracing mounts on " + trace_device)

    t = PartitionTracer(trace_device)
    t.loop()

