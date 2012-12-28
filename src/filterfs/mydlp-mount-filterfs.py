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

import signal
from time import sleep
from subprocess import Popen

import subprocess, fcntl, os, sys, errno

from gevent import socket

class PartitionTracer():
    def __init__(self, root_device):
        self.root_device = root_device
        self.processes = dict()
        self.mounts = dict()
        self.safemounts = dict()
        self.filters = dict()
        self.udisksp = None
        self.continue_loop = True

    def get_continue_loop(self):
        return self.continue_loop

    def start_udisks(self):
        self.udisksp = subprocess.Popen(
            '/usr/bin/udisks --monitor', shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)

        fcntl.fcntl(self.udisksp.stdin, fcntl.F_SETFL, os.O_NONBLOCK)
        fcntl.fcntl(self.udisksp.stdout, fcntl.F_SETFL, os.O_NONBLOCK)
        self.udisksp.stdin.close()

        while True:
            try:
                line = self.udisksp.stdout.readline()
                if not line or line == '':
                    break
                sleep(0.1)
                self.poll()
            except IOError:
                ex = sys.exc_info()[1]
                if ex[0] != errno.EAGAIN:
                    raise
                else:
                    sys.exc_clear()

            try:
                socket.wait_read(self.udisksp.stdout.fileno())
            except KeyboardInterrupt:
                sys.exc_clear()
                self.cleanup_mounts()

    def cleanup_mounts(self):
        self.continue_loop = False
        self.udisksp.terminate()
        self.cleanup_mounts1(0, 0)

    def cleanup_mounts1(self, lenm, lens):
        self.poll()
        lenm0 = len(self.mounts)
        lens0 = len(self.safemounts)

        if lenm0 == lenm and lens0 == lens:
            exit(0)

        if len(self.mounts) > 0:
            for dev in self.mounts.keys():
                self.handle_umount(dev)
            self.cleanup_mounts1(lenm0, lens0)
        elif len(self.safemounts) > 0:
            for dev in self.safemounts.keys():
                self.handle_umount(dev)
            self.cleanup_mounts1(lenm0, lens0)


    def stop_udisks(self):
        if self.udisksp is not None:
            self.udisksp.stdout.close()
            self.udisksp.send_signal(signal.SIGINT)

    def poll(self):
            self.poll_mounts()
            self.poll_safemounts()
            self.poll_filters()

    def is_dev(self, string):
        return string.startswith('/dev/')

    def is_root_dev(self, string):
        return string.startswith('/dev/' + self.root_device)

    def is_safemount(self, string):
        return string.startswith('/var/tmp/mydlpep/safemount')

    def is_filter(self, string):
        return string.startswith('MyDLPFilter')

    def poll_mounts(self):
        new_mounts = dict()
        for line in open('/proc/mounts'):
            if self.is_dev(line):
                parts = line.split()
                if self.is_safemount(parts[1]):
                    continue
                else:
                    new_mounts[parts[0]] = parts[1]
        self.handle_mount_change(new_mounts)

    def handle_mount_change(self, new_mounts):
        for dev in self.mounts.keys():
            if self.is_root_dev(dev):
                if not new_mounts.has_key(dev):
                    # means that dev has been umounted
                    self.handle_umount(dev)
                elif (self.mounts[dev] != new_mounts[dev]):
                    # means that dev has been umounted and remounted
                    self.handle_umount(dev)
                    self.handle_mount(dev, new_mounts[dev])
        for dev in new_mounts.keys():
            if self.is_root_dev(dev):
                if not self.mounts.has_key(dev):
                    # means that dev has been newly mounted
                    self.handle_mount(dev, new_mounts[dev])

        # update mounts
        self.mounts = new_mounts

    def poll_safemounts(self):
        new_safemounts = dict()
        for line in open('/proc/mounts'):
            if self.is_dev(line):
                parts = line.split()
                if self.is_safemount(parts[1]):
		    new_safemounts[parts[0]] = parts[1]
        self.handle_safemount_change(new_safemounts)

    def handle_safemount_change(self, new_safemounts):
        for dev in self.safemounts.keys():
            if self.is_root_dev(dev):
                if not new_safemounts.has_key(dev):
                    # means that dev has been umounted
                    self.handle_umount(dev)
                elif (self.safemounts[dev] != new_safemounts[dev]):
                    # means that dev has been umounted and remounted
                    self.handle_umount(dev)

        # update safemounts
        self.safemounts = new_safemounts
        # check sanity
        self.handle_safemount_sanity()

    def handle_safemount_sanity(self):
        for dev in self.safemounts.keys():
            if not self.mounts.has_key(dev):
                self.handle_umount(dev)

    def poll_filters(self):
        new_filters = dict()
        for line in open('/proc/mounts'):
            if self.is_filter(line):
                parts = line.split()
		new_filters[parts[1]] = parts[0]
        self.handle_filter_change(new_filters)

    def handle_filter_change(self, new_filters):
        for path in self.filters.keys():
            if not new_filters.has_key(path):
                # means that dev has been umounted
                self.handle_umount_path(path)

        # update filters
        self.filters = new_filters
        # check sanity
        self.handle_filter_sanity()

    def handle_filter_sanity(self):
        for filter_path in self.filters.keys():
            for dev, dev_path in self.mounts.items():
                if filter_path == dev_path:
                    if not self.safemounts.has_key(dev):
                        logger.debug("handle_filters_sanity " + dev + " not sane")
                        self.handle_hard_umount_path(filter_path)
                    return
            self.handle_hard_umount_path(filter_path)

    def handle_hard_umount_path(self, path):
        logger.debug("hard umount after 100ms " + path)
        sleep(0.1)
        os.system("/bin/umount " + path)


    def handle_umount_path(self, path):
        for dev, devpath in self.mounts.items():
            if self.is_root_dev(dev) and devpath == path:
                self.handle_umount(dev)
                return
        logger.debug('handle_umount_path failed, cannot find path in filters ' + path)

    def handle_umount(self, dev):
        logger.debug("deattaching after 100ms " + dev)
        sleep(0.1)
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

signal_t = None

def set_signal_globals(t):
    global signal_t
    signal_t = t

def signal_handler(signal, frame):
    logger.debug("terminating")
    if signal_t is not None:
        logger.debug("terminating")
        t.cleanup_mounts()
        t.stop_udisks()
    exit(0)

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
    set_signal_globals(t)
    signal.signal(signal.SIGINT, signal_handler)
    while t.get_continue_loop():
	try:
            t.start_udisks()
        finally: 
            t.stop_udisks()
    signal_handler(signal.SIGINT, None)

