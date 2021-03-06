#!/usr/bin/env python

# Copyright (c) 2012 Ozgen Muzac <ozgen@mydlp.com>
# License GPLv3, see http://www.gnu.org/licenses/gpl.html#content

import sys
import tempfile
import os
import logging
from sys import argv
from socket import socket
from logging.handlers import SysLogHandler

TMP_PATH = "/var/tmp"

class DaemonClient():

	def __init__(self, server, port, job_id, user_name, printer_info, file_name):
		self.server = server
		self.port = port
		self.job_id = job_id
		self.printer_info = printer_info
		self.file_name = file_name
		self.user_name = user_name
		self.sock = socket()
		self.sock.settimeout(147)
		self.sock.connect((self.server, self.port))

	def send(self, message):
		self.sock.sendall(message + "\n")
		response = self.sock.recv(1024).strip()
		return response

	def send_to_daemon(self, file_path):
		try:
			message = "file_path: " + file_path
			response = self.send(message)
			logger.debug("file_path response: %s" %response)
			if not response.startswith("OK"):
				return True
			
			message = "user_name: " + self.user_name
			response = self.send(message)
			logger.debug("user_name response: %s" %response)
			if not response.startswith("OK"):
				return True		

			message = "printer_info: " + self.printer_info
			response = self.send(message)
			logger.debug("printer_info response: %s" %response)
			if not response.startswith("OK"):
				return True		

			message = "file_name: " + self.file_name
			response = self.send(message)
			logger.debug("file_name response: %s" %response)
			if not response.startswith("OK"):
				return True		

			message = "job_id: " + str(self.job_id)
			response = self.send(message)
			logger.debug("job_id response: %s" %response)
			if response.startswith("OK"):
				return True
			else:
				return False
		finally:
			if self.sock is not None:
				self.sock.close()
	

def start_transfering(job_id, user_name, printer_info, file_name):
	try:
		daemon_client = DaemonClient("127.0.0.1", 9100, job_id, user_name, printer_info, file_name)
		fd, path = tempfile.mkstemp(".tmp", "mydlpprnt-", TMP_PATH)
		logger.debug("Tmp file path: %s" %path)
		f = sys.stdin
		text = f.read()
		fout = open(path, "w+b")
		fout.write(text)
		fout.close()
		os.close(fd)
		response = daemon_client.send_to_daemon(path)
		if response:
			sys.stdout.write(text)
	except:
		e = sys.exc_info()[0]
		logger.error("error occurred when sending to main daemon: %s" % e)

if __name__ == '__main__':
	logger = logging.getLogger()
	handler = SysLogHandler(address = '/dev/log',
	facility = SysLogHandler.LOG_LOCAL6)
	formatter = logging.Formatter('MyDLP print-filter: %(levelname)s %(message)s')
	handler.setFormatter(formatter)
	logger.addHandler(handler)
	logger.setLevel(logging.DEBUG)

	printer_info = os.environ["PRINTER"]
	jobId = int(argv[1])
	user_name = argv[2]
	file_name = argv[3]
	logger.debug(argv[0]+" "+argv[1]+" "+argv[2]+" "+argv[3]+" "+argv[4]+" "+argv[5])
	start_transfering(jobId, user_name, printer_info, file_name)
