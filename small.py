#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

from disktools import read_block, write_block
import logging

import disktools

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

def unload(func):
    def f(*args, **kwargs):
        ans = func(*args, **kwargs)
        return ans
    return f


class Memory():
    def __init__(self):
        self.files = {}
        self.data = defaultdict(bytes)
        self.fd = 0
        now = int( time( ))
        self.files['/'] = dict(
                st_mode = (S_IFDIR | 0o755),
                st_ctime = now,
                st_mtime = now,
                st_atime = now,
                st_nlink = 2)
        self.diskId = 0
        self.bitmap = bytearray([0]*12)
        #print("+++++",self.printX())
        self.writeDisk()

    def writeDisk(self):
        print(self.files['/']['st_mode']) 
        print(self.files['/']['st_ctime'])
        print(self.files['/']['st_mtime'])
        print(self.files['/']['st_atime'])
        print(self.files['/']['st_nlink'])

        byte_mode = disktools.int_to_bytes(self.files['/']['st_mode'],2)
        byte_ctime = disktools.int_to_bytes(self.files['/']['st_ctime'],4)
        byte_mtime = disktools.int_to_bytes(self.files['/']['st_mtime'],4)
        byte_atime = disktools.int_to_bytes(self.files['/']['st_atime'],4)
        byte_nlink = disktools.int_to_bytes(self.files['/']['st_nlink'],1)
        size = disktools.int_to_bytes( 30,2 )
        print (byte_mode,"++", byte_ctime,"++", byte_mtime,"++", byte_atime,"++", byte_nlink)
        
        print (byte_mode.__class__)
        empty = bytearray([0]*1)

        data = empty + byte_mode + byte_ctime + byte_mtime + byte_atime + byte_nlink + size + self.bitmap
        print (self.diskId)
        disktools.write_block(self.diskId, data)
    
    @unload
    def printX(self):
        print("print x was called")
        return "printx return"

if __name__ == '__main__':

    """
    Variables for testing
    """
    Num_block = 16


    print("===Excuting small.py===")

    #Reading
    for i in range(Num_block):
        print("Block ",i," : ", disktools.read_block(i) )



    #Writing
    for i in range(Num_block):
        msg = bytearray([0]*64)
        disktools.write_block(i, msg)

    
    print ("\n===After writing===\n")

    Memory()

    #Reading
    for i in range(Num_block):

        print("Block ",i," : ", disktools.read_block(i) )

    print ("==== Tring to decode ====")
    data = disktools.read_block(0)
    print ("Byte length = ", len(data))

    print (data[1:3], "++", data[3:7], "++", data[7:11], "++", data[11:15], "++", data[15:16])
    print( disktools.bytes_to_int(data[1:3]))
    print( disktools.bytes_to_int(data[3:7]))
    print( disktools.bytes_to_int(data[7:11]))
    print( disktools.bytes_to_int(data[11:15]))
    print( disktools.bytes_to_int(data[15:16]))
    print("Extra data ->", data[16:64])
    print(data[16:17])
    print(data[16] == 0)
    print(data[17] == 0)

    name = "Hello"
    byte_name = bytearray(name.encode())
    empty = bytearray([0]*(16-len(name)))

    print(name, byte_name+empty)
    

