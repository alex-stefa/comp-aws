#!/usr/bin/python2.7

from __future__ import print_function
import sys
import socket


#---------------------------------------------------------------------------------------------------------------------

def enum(**enums):
    return type('Enum', (), enums)

Commands = enum(REFRESH=0x1, SHUTDOWN=0x2, HELLO=0x4, PLANETLAB=0x8, OK=0x16, ERROR=0x32, STATUS=0x64)

#---------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":

    if len(sys.argv) == 1:
        print('Usage: ./awscommand.py <REFRESH | SHUTDOWN | STATUS> [<server-ip> <server-port>]\n')
        sys.exit(1)

    command = sys.argv[1]
    server_ip = 'localhost'
    server_port = 8080
    
    if len(sys.argv) >= 3:
        server_ip = sys.argv[2]
    if len(sys.argv) >= 4:
        server_port = int(sys.argv[3])

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((server_ip, server_port))
        if command == 'REFRESH':
            sock.sendall(chr(Commands.REFRESH))
        if command == 'SHUTDOWN':
            sock.sendall(chr(Commands.SHUTDOWN))
        if command == 'STATUS':
            sock.sendall(chr(Commands.STATUS))
        code = ord(sock.recv(1).strip()[0])
        received = '<UNKNOWN>'
        if code == Commands.OK:
            received = 'OK'
        if code == Commands.ERROR:
            received = 'ERROR'
            
    finally:
        sock.close()

    print("Sent:     {}".format(command))
    print("Received: {}".format(received))



