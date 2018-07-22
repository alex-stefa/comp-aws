#!/usr/bin/python

from __future__ import with_statement
import sys
import socket
import time
import traceback
import pickle
import struct
import httplib
import threading
import subprocess
import logging
import random
import zlib
import os
import signal


# Customizable params:

DEBUG = False

#---------------------------------------------------------------------------------------------------------------------

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level = logging.DEBUG,
    filename = 'log_client_%s.log' % (str(random.randrange(1000)) if DEBUG else socket.gethostname()),
    filemode = 'w'
)

console = logging.StreamHandler()
console.setLevel(logging.DEBUG if DEBUG else logging.INFO)
console.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
logging.getLogger('').addHandler(console)

def enum(**enums):
    return type('Enum', (), enums)

Commands = enum(REFRESH=0x1, SHUTDOWN=0x2, HELLO=0x4, PLANETLAB=0x8, OK=0x16, ERROR=0x32, STATUS=0x64)

def time2str(time):
    return '%02dm%02ds.%d' % (
        int(time) / 60,
        int(time) % 60,
        int((time - int(time)) * 10)
    )

#---------------------------------------------------------------------------------------------------------------------

"""
Data sent by a client in a normal exchange:

data = {
    'records': [
        {
            'ec2host1': {
                'times': {
                    'req1': [start_time, conn_duration, get_duration],
                    'req2': [start_time, conn_duration, get_duration],
                    'reqN': [start_time, conn_duration, get_duration]
                },
                'trace': [start_time, stdout]
            },
            'ec2host2': {
                'times': {
                    'req1': [start_time, conn_duration, get_duration],
                    'req2': [start_time, conn_duration, get_duration],
                    'reqN': [start_time, conn_duration, get_duration]
                },
                'trace': [start_time, stdout]
            },
            'ec2hostM': {
                'times': {
                    'req1': [start_time, conn_duration, get_duration],
                    'req2': [start_time, conn_duration, get_duration],
                    'reqN': [start_time, conn_duration, get_duration]
                },
                'trace': [start_time, stdout]
            }
        },
    
        // one record for each request interval (15min)
        // if the server cannot be contacted at the end of every interval,
        // new records are added to the 'records' array
    ]
}
"""
        
class Probe():
    def __init__(self, ec2host, req_names, clock_offset, timeout=120):
        self.record = { 'times': dict(), 'trace': [0, ''] } 
        self.ec2host = ec2host
        self.req_names = req_names
        self.clock_offset = clock_offset
        self.conn_timeout = float(timeout) / len(req_names) / 2.0
        self.proc = None
        
    def do_probe(self):
        logging.debug('Starting probe for %s..' % self.ec2host)
        hostname = self.ec2host.split('|')[0] #if DEBUG else self.ec2host
        for req_name in self.req_names:
            try:
                start_time = time.time()
                #conn = httplib.HTTPConnection(hostname, timeout=self.conn_timeout)
                conn = httplib.HTTPConnection(hostname)
                conn_time = time.time()
                if req_name[0] != '/':
                    req_name = '/' + req_name
                conn.request('GET', req_name)
                response = conn.getresponse()
                if response.status == httplib.OK:
                    data = response.read()
                    now_time = time.time()
                    self.record['times'][req_name] = [
                        start_time - self.clock_offset, 
                        conn_time - start_time, 
                        now_time - conn_time
                    ]
                    logging.debug('Got http://%s%s %d bytes in %.3fs' % 
                        (self.ec2host, req_name, len(data), now_time - start_time))
                else:
                    logging.debug('ERROR: %s returned %d (%s) for %s' % 
                        (self.ec2host, response.status, response.reason, req_name))
                conn.close()
            except:
                logging.debug('ERROR: error requesting http://%s%s \n%s' % (self.ec2host, req_name, traceback.format_exc()))
        try:
            self.record['trace'][0] = time.time() - self.clock_offset
            self.proc = subprocess.Popen(['tracepath', '-n', hostname.split(':')[0]], stdout=subprocess.PIPE)
        except:
            logging.debug('ERROR: could not complete tracepath command \n%s' % traceback.format_exc())
            
    def end(self):
        if self.proc is not None:
            if self.proc.poll() is None:
                try:
                    #self.proc.terminate()
                    os.kill(self.proc.pid, signal.SIGTERM)
                except:
                    logging.debug('ERROR: failed stopping tracepath \n%s' % traceback.format_exc())
            else:
                try:
                    self.record['trace'][1] = self.proc.communicate()[0]
                except:
                    logging.debug('ERROR: failed getting tracepath output \n%s' % traceback.format_exc())
        return self.record

#---------------------------------------------------------------------------------------------------------------------

""""
SERVER <-> PLANETLAB_NODE protocol:

    SERVER          CLIENT
    ------          ------
                    PLANETLAB
                    greeting
    OK
    node_info
                    records
    OK
    <close>
                    <clear_records>
                    <close>            
"""

class Node():
    def __init__(self, sock_handler):
        self.handler = sock_handler
        self.id = None
        self.running = False
        self.records = list()
        self.probes = None
        self.probe_timers = None
        self.reset_timer = None
        self.last_probing = None
        self.hostname = socket.gethostname()
        
    def __do_hello(self):
        logging.debug('Attempting to register at server at %s..' % self.handler.str_address)
        self.id = None
        self.slot = None
        self.delta = None
        self.req_int = None
        self.ec2hosts = None
        self.req_names = None
        self.clock_offset = None
        self.handler.connect()
        if not self.handler.connected:
            return
        self.handler.send_command(Commands.HELLO)
        greeting = {
            'hostname': self.hostname
        }
        self.handler.send_object(greeting)
        code = self.handler.recv_command()
        if code == Commands.OK:
            reply = self.handler.recv_object()
            if reply is not None:
                logging.debug('Received %s' % reply)
                self.id = reply['id']
                self.slot = reply['slot']
                self.delta = reply['delta']
                self.req_int = reply['req_int']
                self.ec2hosts = reply['ec2hosts']
                self.req_names = reply['req_names']
                self.clock_offset = time.time() - reply['now']
        else:
            if code == Commands.ERROR:
                logging.info('ERROR: server returned ERROR code after sending HELLO message')
        self.handler.close()
                                
    def __do_exchange(self):
        logging.debug('Starting update protocol..')
        self.handler.connect()
        if not self.handler.connected:
            return
        self.handler.send_command(Commands.PLANETLAB)
        greeting = {
            'id': self.id,
            'slot': self.slot,            
            'hostname': self.hostname
        }
        self.handler.send_object(greeting)
        code = self.handler.recv_command()
        if code == Commands.OK:
            reply = self.handler.recv_object()
            if reply is not None:
                logging.debug('Received %s' % reply)
                self.id = reply['id']
                self.slot = reply['slot']
                self.delta = reply['delta']
                self.req_int = reply['req_int']
                self.ec2hosts = reply['ec2hosts']
                self.req_names = reply['req_names']
                self.clock_offset = time.time() - reply['now']
                data = {
                    'records': self.records
                }
                self.handler.send_object(data)
                code = self.handler.recv_command()
                if code == Commands.OK:
                    self.records = list()
                else:
                    if code == Commands.ERROR:
                        logging.info('ERROR: server returned ERROR code after sending data')
        else:
            if code == Commands.ERROR:
                logging.info('ERROR: server returned ERROR code after greeting')
        self.handler.close()
    
    def __setup(self):
        if not self.running:
            return
        now = time.time() - self.clock_offset
        last_setup = self.last_probing
        self.last_probing = now
        init_delay = self.req_int - (float(int(now * 1000) % int(self.req_int * 1000)) / 1000) + self.delta
        while init_delay >= self.req_int:
            init_delay -= self.req_int
        if last_setup is None:
            logging.debug('Probes will start in %s' % time2str(init_delay))
        else:
            logging.debug('Setting up probes..')
            if (now + init_delay - last_setup) > self.req_int:
                init_delay -= self.req_int
        time_per_probe = float(self.req_int) / len(self.ec2hosts)
        self.probes = [Probe(ec2host, self.req_names, self.clock_offset, time_per_probe) 
                        for ec2host in self.ec2hosts]
        self.probe_timers = [threading.Timer(max(0, init_delay + time_per_probe * i), probe.do_probe)
                        for i, probe in enumerate(self.probes)]
        for probe_timer in self.probe_timers:
            probe_timer.start()
        self.reset_timer = threading.Timer(max(0, init_delay + self.req_int), self.__reset)
        self.reset_timer.start()
        
    def __reset(self):
        if not self.running:
            return
        record = dict()
        for probe_timer in self.probe_timers:
            probe_timer.cancel()
        for probe in self.probes:
            record[probe.ec2host] = probe.end()
        self.records.append(record)
        threading.Thread(target=self.__do_exchange).start()
        self.__setup()

    def register(self):
        while True:
            self.__do_hello()
            if self.id is None:
                time.sleep(10)
            else:
                break

    def start(self):
        if self.id is None:
            logging.info('ERROR: node be must registered to server before collecting data')
            return
        if self.running:
            logging.info('ERROR: client already running')
            return
        self.running = True
        self.__setup()
        
    def stop(self):
        self.running = False
        if self.reset_timer is not None:
            self.reset_timer.cancel()
            self.reset_timer = None
        if self.probe_timers is not None:
            for probe_timer in self.probe_timers:
                probe_timer.cancel()
            self.probe_timers = None
        if self.probes is not None:
            record = dict()
            for probe in self.probes:
                record[probe.ec2host] = probe.end()
            self.records.append(record)
            self.probes = None
        self.last_probing = None
        self.handler.close()
    
#---------------------------------------------------------------------------------------------------------------------

class SocketHandler():
    def __init__(self, _server_ip, _server_port):
        self.server_ip = _server_ip
        self.server_port = _server_port
        self.connected = False
        self.str_address = '%s:%d' % (self.server_ip, self.server_port)
        
    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.sock.connect((self.server_ip, self.server_port))
            self.connected = True
        except:
            logging.debug('ERROR: could not connect to server at %s \n%s' % (self.str_address, traceback.format_exc()))
       
    def close(self):
        """ force close client socket """
        try:
            self.connected = False
            self.sock.shutdown()
            self.sock.close()
        except:
            pass
                    
    def send_object(self, obj):
        """ write an object to client socket as a pickle string """
        if not self.connected:
            return
        try:
            pickle_str = zlib.compress(pickle.dumps(obj))
            netw_str = struct.pack('!I%ds' % len(pickle_str), len(pickle_str), pickle_str)
            self.sock.sendall(netw_str)
        except:
            logging.debug('ERROR: socket write to %s failed \n%s' % (self.str_address, traceback.format_exc()))
            self.close()
            
    def recv_object(self):
        """ reads a pickled string from the client socket and decodes it into an object """
        if not self.connected:
            return None
        len_size = struct.calcsize('!I')
        bytes = self.sock.recv(len_size)
        if len(bytes) != len_size:
            logging.debug('ERROR: server %s closed socket before data size was read' % self.str_address)
            self.close()
            return None
        len_val = struct.unpack('!I', bytes)[0]
        len_struct = struct.calcsize('%ds' % len_val)
        val_struct = ''
        while len(val_struct) < len_struct:
            bytes = self.sock.recv(4096)
            if len(bytes) == 0:
                logging.debug('ERROR: server %s closed socket before pickle data was read' % self.str_address)
                self.close()
                return None
            val_struct += bytes
        try:
            pickle_str = struct.unpack('%ds' % len_val, val_struct)[0]
            return pickle.loads(zlib.decompress(pickle_str))
        except:
            logging.debug('ERROR: server %s sent invalid pickle data \n%s' % (self.str_address, traceback.format_exc()))
            self.close()
        return None
         
    def send_command(self, command):
        """ sends a 1-byte command to the client socket """
        if not self.connected:
            return
        sent = self.sock.send(chr(command))
        if sent != 1:
            logging.debug('ERROR: socket send command to %s failed' % self.str_address)
            self.close()

    def recv_command(self):
        """ reads a 1-byte command from the client socket """
        if not self.connected:
            return None
        command = self.sock.recv(1)
        if len(command) == 0:
            logging.debug('ERROR: server %s closed socket before command was read' % self.str_address)
            self.close()
            return None
        return ord(command[0])
        
#---------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":

    if len(sys.argv) != 3:
        logging.info(' Usage: ./awsclient.py <server-ip> <server-port>\n')
        if not DEBUG:
            sys.exit(1)

    ip_addr = sys.argv[1] if not DEBUG else 'localhost'
    port = int(sys.argv[2]) if not DEBUG else 8080
    
    node = Node(SocketHandler(ip_addr, port))
    
    try:
        node.register()
        node.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
     
    logging.info('Shutting down..')
    node.stop()

