#!/usr/bin/python2.7

from __future__ import print_function
import xmlrpclib

if __name__ == "__main__":
    
    plc_api = xmlrpclib.ServerProxy('https://www.planet-lab.org/PLCAPI/',allow_none=True)

    auth = { 
        'AuthMethod' : 'password',
        'Username' : 'username@rice.edu',
        'AuthString' : 'p@ssw0rd'
    }

    slice_name='rice_comp529'

    # the slice's node ids
    node_ids = plc_api.GetSlices(auth, slice_name, ['node_ids'])[0]['node_ids']

    # get hostname for these nodes
    slice_nodes = plc_api.GetNodes(auth, node_ids, ['hostname'])
    
    print('Got %d nodes in %s:' % (len(slice_nodes), slice_name))
    
    for node in slice_nodes:
        print(node['hostname'])






