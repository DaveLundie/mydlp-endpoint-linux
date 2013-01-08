#!/usr/bin/env python

# Copyright (c) 2012 Ozgen Muzac <ozgen@mydlp.com>
# License GPLv3, see http://www.gnu.org/licenses/gpl.html#content

import socket
import cups
import signal
import sys

from threading import Thread

class SeapClient():
	def __init__(self, server, port):
		self.server = server
		self.port = port
		self.sock = socket.socket()
		self.sock.settimeout(145)
		self.sock.connect((self.server, self.port))

	def send(self, message):
		for try_count in range(3):
			try:
				self.sock.sendall(message+"\r\n")
				response = self.sock.recv(1024).strip()
				return response
			except IOError as why:
				self.sock = socket()
				self.sock.settimeout(10)
				self.sock.connect((self.server, self.port))
		return ""
		
	def acl_query(self, file_path, user_name, file_name, printer_info):
		try:
			print "file_name: " + file_name
			print "printer_info: " + printer_info
			response = self.send("BEGIN")
			if not response.startswith("OK"):
				return True
			opid = response.split()[1]
			response = self.send("SETPROP " + opid + " filename=" + file_name)
			if not response.startswith("OK"):
				return True

			response = self.send("SETPROP " + opid + " printerName=" + printer_info) #TODO: should be used printer name
			if not response.startswith("OK"):
				return True

			response = self.send("SETPROP " + opid + " burn_after_reading=true")
			if not response.startswith("OK"):
				return True

			response = self.send("SETPROP " + opid + " user=" + user_name)
			if not response.startswith("OK"):
				return True

			response = self.send("PUSHFILE " + opid + " " + file_path)
			if not response.startswith("OK"):
				return True

			response = self.send("END " + opid)
			if not response.startswith("OK"):
				return True

			response = self.send("ACLQ " + opid)
			self.send("DESTROY " + opid)
			print response.split()[1]
			if response.split()[1] == "block":
				return False
			else:
				return True
		except (IOError, OSError) as why:
			print "An error occured in aclq"

		

class DaemonAgent(Thread):
	def __init__(self, conn, addr):
		self.conn = conn
		self.claddr = addr
		self.seap_client = SeapClient("127.0.0.1", 9099)
		Thread.__init__(self)

	def run(self):
		print "Thread Started"
		inp = self.conn.recv(1024).strip()
		while inp:
			inp_arr = inp.split()
			if inp_arr[0] == "file_path:":
				file_path = inp_arr[1]
				self.conn.send("OK\n")
				inp = self.conn.recv(1024).strip()
			elif inp_arr[0] == "printer_info:":
				printer_info = inp_arr[1]
				self.conn.send("OK\n")
				inp = self.conn.recv(1024).strip()
			elif inp_arr[0] == "file_name:":
				file_name = inp_arr[1]
				self.conn.send("OK\n")
				inp = self.conn.recv(1024).strip()
			elif inp_arr[0] == "user_name:":
				user_name = inp_arr[1]
				self.conn.send("OK\n")
				inp = self.conn.recv(1024).strip()
			elif inp_arr[0] == "job_id:":
				job_id = inp_arr[1]
				response = self.seap_client.acl_query(file_path, user_name, file_name, printer_info)
				if response:
					self.conn.send("OK\n")
				else:
					self.conn.send("BLOCK\n")
					my_cups = cups.Connection()
					my_cups.cancelJob(int(job_id))
					exit()
				self.conn.close()
				break
			elif inp_arr[0] == "added:":
				print "new printer added"
				self.conn.send("OK\n")
				inp = self.conn.recv(1024).strip()
			else:
				inp = self.conn.recv(1024).strip()
		

def signal_handler(signal, frame):
	s.close()
	sys.exit(0)


HOST = '127.0.0.1'
PORT = 9100
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((HOST, PORT))
s.listen(1)
signal.signal(signal.SIGINT, signal_handler)
connections = []

while True:
	conn, addr = s.accept()
	conn.settimeout(147)
	print 'Connected by', addr
	a = DaemonAgent(conn, addr)
	a.start()
	connections.append(conn)
