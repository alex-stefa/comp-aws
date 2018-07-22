#!/usr/bin/python2.7

from __future__ import print_function
import os
import sys
import errno
import traceback
import time
import datetime
import socket
import threading
import SocketServer
import uuid
import json
import pickle
import struct
import logging
import zlib


# Customizable params:

DEBUG = False # print debug messages or not
hosts_file = 'ec2hosts.txt' # filename containing EC2 hostnames or IPs
req_file = 'requests.txt' # filename containing names of files to request from EC2 servers
req_int = 15 * 60 # request interval in seconds for Planet-Lab nodes
data_dir = 'data/' # folder where all measurement data is collected
dead_int = 3600 # if a node has not been seen for an hour, it is considered dead

#---------------------------------------------------------------------------------------------------------------------

# Globals:

ec2hosts = None # list of EC2 hostnames or IPs (port is always 80)
req_names = None # list of file names to be requested from EC2 instances
slots = [None] * 1024 # time slots allocated to nodes to even out request making times during each 'req_int' period

#---------------------------------------------------------------------------------------------------------------------

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level = logging.DEBUG, 
    filename = 'log_server.log',
    filemode = 'w'
)

console = logging.StreamHandler()
console.setLevel(logging.DEBUG if DEBUG else logging.INFO)
console.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
logging.getLogger('').addHandler(console)

def enum(**enums):
    return type('Enum', (), enums)

Commands = enum(REFRESH=0x1, SHUTDOWN=0x2, HELLO=0x4, PLANETLAB=0x8, OK=0x16, ERROR=0x32, STATUS=0x64)

def ensure_path(path):
    """ creates directory tree if it does not exist """
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

def refresh_config():
    """ re-read EC2 hostnames and request names from file """
    global ec2hosts
    global req_names
    logging.info('Reading EC2 hostnames from \'%s\'..' % hosts_file)
    try:
        with open(hosts_file, 'r') as f:
            ec2hosts = [line.strip() for line in f.readlines() if len(line.strip()) > 0]
        logging.info('Read %d EC2 hostnames:' % len(ec2hosts))
        for i, host in enumerate(ec2hosts):
            logging.info('\t#%d %s' % (i+1, host))
    except:
        logging.info('ERROR: Error reading ec2 host file \'%s\' \n%s' % (hosts_file, traceback.format_exc()))
    logging.info('Reading request names from \'%s\'..' % req_file)
    try:
        with open(req_file, 'r') as f:
            req_names = [line.strip() for line in f.readlines() if len(line.strip()) > 0]
        logging.info('Read %d request names:' % len(req_names))
        for i, name in enumerate(req_names):
            logging.info('\t#%d %s' % (i+1, name))
    except:
        logging.info('ERROR: Error reading request names file \'%s\' \n%s' % (req_file, traceback.format_exc()))

#---------------------------------------------------------------------------------------------------------------------

"""
'node' dictionary structure:
    id = unique identifier
    address = remote ip address
    last_seen = timestamp when last connected
    slot = index in slots[]
    delta = assigned wait time before making requests (= slot2time(slot))
    hostname = socket.gethostname() on PlanetLab node
"""

def filter_dead():
    """ remove nodes from 'slots[]' which haven't connected to the server in a long time """
    global slots
    logging.debug('Filtering dead nodes..')
    now = time.time()
    slots = [node if (node is not None) and (now - node['last_seen'] < dead_int) else None
                for node in slots]

def find_node(id):
    for node in slots:
        if node is not None:
            if node['id'] == id:
                return node
    return None

def get_new_slot():
    step = len(slots)
    while True:
        curr = 0
        while True:
            i = int(curr + step / 2.0)
            if i >= len(slots):
                break
            if slots[i] is None:
                return i
            curr += step
        if step <= 1:
            return None
        step /= 2.0 

def slot2time(slot):
    return slot * req_int / float(len(slots))
    
def time2slot(time):
    return time * len(slots) / float(req_int)
    
def time2str(time):
    return '%02dm%02ds.%d' % (
        int(time) / 60,
        int(time) % 60,
        int((time - int(time)) * 10)
    )
    
#---------------------------------------------------------------------------------------------------------------------

def store_data(node, data):
    try:
        logging.debug('New data (%d records) from s%d' % (len(data['records']), node['slot']))
        shortname = node['hostname']
        if shortname is None or len(shortname) < 1:
            shortname = node['id']
        filename = os.path.join(data_dir, shortname + '.txt')
        mode = 'a' if os.path.isfile(filename) else 'w'
        with open(filename, mode) as f:
            if mode == 'w':
                f.write('%s\n%s\n%s\n' % (node['id'], node['hostname'], node['address']))
            for record in data['records']:
                f.write(json.dumps(record) + '\n')
    except:
        logging.info('ERROR: failed writing to data file \'%s\' \n%s' % (filename, traceback.format_exc()))

#---------------------------------------------------------------------------------------------------------------------

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass

class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler): # could use SocketServer.StreamRequestHandler
    start_time = time.time() # time when server started

    def assign_slot(self, hostname):
        """ assigns a new node and assigns it in a position in 'slots[]' """
        slot = get_new_slot()
        if slot is None:
            logging.info('ERROR: No time slot available for %s', self.str_address)
            self.send_command(Commands.ERROR)
            self.close_client_socket()
            return None
        else:
            slots[slot] = {
                'id': str(uuid.uuid4()),
                'address': self.str_address,
                'last_seen': time.time(),
                'slot': slot,
                'delta': slot2time(slot),
                'hostname': hostname
            }
            return slots[slot]
            
    def close_client_socket(self):
        """ force close client socket """
        try:
            self.request.shutdown()
            self.request.close()
        except:
            pass
                    
    def send_object(self, obj):
        """ write an object to client socket as pickle string """
        try:
            pickle_str = zlib.compress(pickle.dumps(obj))
            netw_str = struct.pack('!I%ds' % len(pickle_str), len(pickle_str), pickle_str)
            self.request.sendall(netw_str)
        except:
            logging.debug('ERROR: socket write to %s failed \n%s' % (self.str_address, traceback.format_exc()))
            self.close_client_socket()
            
    def recv_object(self):
        """ reads a pickle string from the client socket and decodes it into an object """
        len_size = struct.calcsize('!I')
        bytes = self.request.recv(len_size)
        if len(bytes) != len_size:
            logging.debug('ERROR: client %s closed socket before data size was read' % self.str_address)
            self.close_client_socket()
            return None
        len_val = struct.unpack('!I', bytes)[0]
        len_struct = struct.calcsize('%ds' % len_val)
        val_struct = ''
        while len(val_struct) < len_struct:
            bytes = self.request.recv(4096)
            if len(bytes) == 0:
                logging.debug('ERROR: client %s closed socket before pickle data was read' % self.str_address)
                self.close_client_socket()
                return None
            val_struct += bytes
        try:
            pickle_str = struct.unpack('%ds' % len_val, val_struct)[0]
            return pickle.loads(zlib.decompress(pickle_str))
        except:
            logging.debug('ERROR: client %s sent invalid pickle data \n%s' % (self.str_address, traceback.format_exc()))
            self.close_client_socket()
        return None
         
    def send_command(self, command):
        """ sends a 1-byte command to the client socket """
        sent = self.request.send(chr(command))
        if sent != 1:
            logging.debug('ERROR: socket send command to %s failed' % self.str_address)
            self.close_client_socket()

    def recv_command(self):
        """ reads a 1-byte command from the client socket """
        command = self.request.recv(1)
        if len(command) == 0:
            logging.debug('ERROR: client %s closed socket before command was read' % self.str_address)
            self.close_client_socket()
            return None
        return ord(command[0])


    def handle(self):
        """ main server socket handle method """
        
        # pretty print client address
        self.str_address = '%s:%d' % (self.client_address[0], self.client_address[1])

        # identify the connection purpose
        command = self.recv_command()
        if command is None:
            return

        # command to reload the ec2 hosts file and request names file
        if command == Commands.REFRESH:
            logging.debug('Received REFRESH from %s' % self.str_address)
            self.send_command(Commands.OK)
            self.close_client_socket()
            refresh_config()
            return
            
        # command to shutdown server
        if command == Commands.SHUTDOWN:
            logging.debug('Received SHUTDOWN from %s' % self.str_address)
            self.send_command(Commands.OK)
            self.close_client_socket()
            self.server.init_shutdown = True
            return 
        
        # command to list PlanetLab nodes
        if command == Commands.STATUS:
            logging.debug('Received STATUS from %s' % self.str_address)
            self.send_command(Commands.OK)
            self.close_client_socket()
            nodes_str = ' %-3s %-45s %-4s %-7s %-8s %s\n' % (
                '#', 'HOSTNAME', 'SLOT', 'DELTA', 'SEEN', 'ADDR')
            now = time.time()
            count = 0
            for node in slots:
                if node is not None:
                    count += 1
                    nodes_str += ' %2d. %-45s %4d %7.3f %s %s\n' % (
                        count,
                        node['hostname'],
                        node['slot'],
                        node['delta'],
                        time2str(now - node['last_seen']),
                        node['address']
                   )
            start_ago = now - ThreadedTCPRequestHandler.start_time
            start_ago_time = datetime.timedelta(days=start_ago/(3600*24), seconds=start_ago%(3600*24))
            next_delta = req_int - float(int(now * 1000) % int(req_int * 1000)) / 1000
            logging.info("""
            
Server started on:            %s (%s ago)
Current server time:          %s
Next synchronization cycle:   %s (in %s)
Known PlanetLab nodes:
%s
""" % (
                time.ctime(ThreadedTCPRequestHandler.start_time), 
                str(start_ago_time),
                time.ctime(now),
                time.ctime(now + next_delta),
                time2str(next_delta),
                nodes_str))
            return
            
        # a node is connecting to the server for the first time    
        if command == Commands.HELLO:
            logging.debug('Received HELLO from %s' % self.str_address)
            greeting = self.recv_object()
            if greeting is None:
                return
            node = self.assign_slot(greeting['hostname'])
            if node is None:
                return
            reply = {
                'id': node['id'],
                'slot': node['slot'],
                'delta': node['delta'],
                'req_int': req_int,
                'now': time.time(),
                'ec2hosts': ec2hosts,
                'req_names': req_names
            }
            self.send_command(Commands.OK)
            self.send_object(reply)
            self.close_client_socket()
            return
                
        # client sends data collected since last connection and server replies as above
        if command == Commands.PLANETLAB:
            logging.debug('Received PLANETLAB from %s' % self.str_address)
            greeting = self.recv_object()
            if greeting is None:
                return
            node = find_node(greeting['id'])
            if node is None: # if server does not know about node add it to 'slots[]'
                slot = greeting['slot']
                if slots[slot] is None: # restore node to same slot if it has been removed from 'slots[]'
                    node = {
                       'id': greeting['id'],
                       'address': self.str_address,
                       'last_seen': time.time(),
                       'slot': slot,
                       'delta': slot2time(slot),
                       'hostname': greeting['hostname']
                    }
                    slots[slot] = node
                else: # assign an empty slot, but with the same id
                    node = self.assign_slot(greeting['hostname'])
                    if node is None:
                        return
                    node['id'] = greeting['id'] 
            else: # server knows about node; just update last seen time
                node['last_seen'] = time.time()
            reply = {
                'id': node['id'],
                'slot': node['slot'],
                'delta': node['delta'],
                'req_int': req_int,
                'now': time.time(),
                'ec2hosts': ec2hosts,
                'req_names': req_names
            }
            self.send_command(Commands.OK)
            self.send_object(reply)
            data = self.recv_object()
            if data is None:
                return
            self.send_command(Commands.OK)
            self.close_client_socket()
            store_data(node, data)
            return

#---------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":

    if len(sys.argv) != 3:
        logging.info(' Usage: ./awsserver.py <external-ip> <port>\n')
        if not DEBUG:
            sys.exit(1)

    ip_addr = sys.argv[1] if not DEBUG else 'localhost'
    ip_addr = '168.7.23.167'
    port = int(sys.argv[2]) if not DEBUG else 8080
    
    ensure_path(data_dir)

    refresh_config()
    
    server = ThreadedTCPServer((ip_addr, port), ThreadedTCPRequestHandler)
    server.allow_reuse_address = True
    server.init_shutdown = False
    
    ip_addr, port = server.server_address
    
    logging.info('Starting awsping server on %s:%d..' % (ip_addr, port))

    # Start a thread with the server -- that thread will then start one
    # more thread for each request
    server_thread = threading.Thread(target=server.serve_forever)
    # Exit the server thread when the main thread terminates
    server_thread.daemon = True
    server_thread.start()
    
    logging.info('Server loop running in thread: %s' % server_thread.name)
    
    sec_count = 0
    try:
        while True:
            time.sleep(1)
            sec_count += 1
            if sec_count == 60:
                sec_count = 0
                filter_dead()
            if server.init_shutdown:
                break
    except KeyboardInterrupt:
        pass
     
    logging.info('Shutting down..')
    server.shutdown()
    
    





