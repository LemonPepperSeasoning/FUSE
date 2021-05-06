#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

import disktools

if not hasattr(__builtins__, 'bytes'):
    bytes = str

ROOT_BLOCK_ID = 0
NUM_BLOCKS = 16

#def getDataFromDisk(func):
#    def f(*args, **kwargs):
#        try:
#            returnVal = func(*args, **kwargs)
#            return returnVal
#        except Exception as e:
#            raise e    
#    return f


def getDiskData(func):
    def f(*args, **kwargs):
        """ 
        args[0] : self
        args[1] : path (usually)
        args[3...n] : *this is pretty random
        """
        #print(args[0].files[args[1]])
        #print(args[1])
        #print(args[0].files)
        if args[1] in args[0].files:
            if not 'st_mode' in  args[0].files[args[1]] :
                #read from blockkk
                data = disktools.read_block(args[0].files[args[1]]['block_id'])
                 
                args[0].files[args[1]] = dict(
                        st_mode = disktools.bytes_to_int(data[1:3]),
                        st_ctime = disktools.bytes_to_int(data[3:7]),
                        st_mtime = disktools.bytes_to_int(data[7:11]),
                        st_atime = disktools.bytes_to_int(data[11:15]),
                        st_nlink = disktools.bytes_to_int(data[15:16]),
                        size = disktools.bytes_to_int(data[16:18]))

        returnVal = func(*args, **kwargs)
        return returnVal
    return f


def writeMetaDataOnDisk(func):
    def f(*args, **kwargs):
        returnVal = func(*args, **kwargs)
        #print(args[1])
        #print ("writing to disk :", args[0].files[args[1]]['st_ctime'])
        byte_mode = disktools.int_to_bytes(args[0].files[args[1]]['st_mode'],2)         
        byte_ctime = disktools.int_to_bytes(args[0].files[args[1]]['st_ctime'],4)        
        byte_mtime = disktools.int_to_bytes(args[0].files[args[1]]['st_mtime'],4)        
        byte_atime = disktools.int_to_bytes(args[0].files[args[1]]['st_atime'],4)        
        byte_nlink = disktools.int_to_bytes(args[0].files[args[1]]['st_nlink'],1)        
        byte_b_id = disktools.int_to_bytes(args[0].files[args[1]]['block_id'],1)        
        byte_size = disktools.int_to_bytes(args[0].files[args[1]]['size'],2)        
        data = bytearray([0]*1) + byte_mode + byte_ctime + byte_mtime + byte_atime + byte_nlink+byte_size

        print("Data :",data,", BlockIndex :",args[0].files[args[1]]['block_id'])
        disktools.write_block(args[0].files[args[1]]['block_id'], data) 
        


        name = args[1][1:len(args[1])]
        empty = bytearray([0]*(16-len(name)))
        ab = (bytearray(name.encode()) + empty + byte_b_id ) 
        
       
        cData = disktools.read_block(0)
        if (cData[0] != '\00'):
            newData = distktools.read_block(1)
            cData += newData
        
        cSize = args[0].files['/']['size']
        if (cSize > 64):
            i_page = int( cSize / 64)
            print(i_page)
        
        nData = cData[0:16]+disktools.int_to_bytes(cSize+16,2) +cData[18:cSize] + ab
        print(nData)
        print(len(nData),"++",cSize+17)
        args[0].files['/']['size'] = cSize+17
        

        block_id_counter = 0
        while ( len(nData) > 64 ):
            print(nData[0])

            chunk = nData[0:64]
            disktools.write_block(block_id_counter, chunk)
            block_id_counter += 1
            nData = bytearray([0]) + nData[64:len(nData)]
            if (block_id_counter > 3):
                print ("METABLOCK OVER THE LIMIT")

        disktools.write_block( block_id_counter, nData)

        return returnVal
    return f

def writeDataOnDisk(func):
    def f(*args, **kwargs):
        returnVal = func(*args, **kwargs)
        print(args[0].data[args[1]])
        return returnVal
    return f


class Memory(LoggingMixIn, Operations):

    def __init__(self):
        indexBlock = ROOT_BLOCK_ID
        rootMetaData = disktools.read_block(indexBlock)
        self.bitmap = [0,1,2,3]
        nextPage = rootMetaData[0:1]
        print("Next page :", nextPage, ":", nextPage == '\x00')
        while (nextPage != '\x00' and indexBlock<4 ):
            print("Reading New page")
            nextData = disktools.read_block(indexBlock+1)
            nextPage = nextData[0:1]
            rootMetaData += nextPage[1:len(nextData)]

        self.files = {}
        self.data = defaultdict(bytes)
        self.fd = 0
        self.files['/'] = dict(
                st_mode = disktools.bytes_to_int(rootMetaData[1:3]),
                st_ctime = disktools.bytes_to_int(rootMetaData[3:7]),
                st_mtime = disktools.bytes_to_int(rootMetaData[7:11]),
                st_atime = disktools.bytes_to_int(rootMetaData[11:15]),
                st_nlink = disktools.bytes_to_int(rootMetaData[15:16]),
                size = disktools.bytes_to_int(rootMetaData[16:18])
                )
        print("file size :",self.files['/']['size']) 
        
        index = 18
        while(rootMetaData[index] != 0):
            if(index + 17 > len(rootMetaData)):
                print("Major issue ! Not reading enough block")

            name ="/"+ rootMetaData[index : index+16].decode().rstrip('\x00')
            print(name, len(name))
            print(rootMetaData[index+16 : index+17])
            b_id = disktools.bytes_to_int(rootMetaData[index+16 : index+17])
            self.files[name] = dict(
                    block_id = b_id)
            self.bitmap.append(b_id) #the block is no longer free.
            index += 17

    
    def chmod(self, path, mode):
        self.files[path]['st_mode'] &= 0o770000
        self.files[path]['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid):
        self.files[path]['st_uid'] = uid
        self.files[path]['st_gid'] = gid

    @writeMetaDataOnDisk
    def create(self, path, mode):
        b_id = -1
        for i in range(NUM_BLOCKS):
            if i not in self.bitmap:
                self.bitmap.append(i)
                b_id = i
                break
        
        if b_id == -1:
            print("Out of block !. This should not happen")
            raise IOError("out of block")
        
        now = int(time())
        self.files[path] = dict(
            st_mode=(S_IFREG | mode),
            st_nlink=1,
            st_size=0,
            st_ctime=now,
            st_mtime=now,
            st_atime=now,
            block_id = b_id,
            size = 0
            )
        self.fd += 1
        return self.fd

    @getDiskData
    def getattr(self, path, fh=None):
        if path not in self.files:
            raise FuseOSError(ENOENT)

        return self.files[path]

    def getxattr(self, path, name, position=0):
        attrs = self.files[path].get('attrs', {})

        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        attrs = self.files[path].get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        self.files[path] = dict(
            st_mode=(S_IFDIR | mode),
            st_nlink=2,
            st_size=0,
            st_ctime=time(),
            st_mtime=time(),
            st_atime=time())

        self.files['/']['st_nlink'] += 1

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        return self.data[path][offset:offset + size]

    def readdir(self, path, fh):
        return ['.', '..'] + [x[1:] for x in self.files if x != '/']

    def readlink(self, path):
        return self.data[path]

    def removexattr(self, path, name):
        attrs = self.files[path].get('attrs', {})

        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        self.data[new] = self.data.pop(old)
        self.files[new] = self.files.pop(old)

    def rmdir(self, path):
        # with multiple level support, need to raise ENOTEMPTY if contains any files
        self.files.pop(path)
        self.files['/']['st_nlink'] -= 1

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        self.files[target] = dict(
            st_mode=(S_IFLNK | 0o777),
            st_nlink=1,
            st_size=len(source))

        self.data[target] = source

    @writeDataOnDisk
    def truncate(self, path, length, fh=None):
        # make sure extending the file fills in zero bytes
        self.data[path] = self.data[path][:length].ljust(
            length, '\x00'.encode('ascii'))
        self.files[path]['st_size'] = length

    def unlink(self, path):
        self.data.pop(path)
        self.files.pop(path)

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        self.files[path]['st_atime'] = atime
        self.files[path]['st_mtime'] = mtime

    @writeDataOnDisk
    def write(self, path, data, offset, fh):
        self.data[path] = (
            # make sure the data gets inserted at the right offset
            self.data[path][:offset].ljust(offset, '\x00'.encode('ascii'))
            + data
            # and only overwrites the bytes that data is replacing
            + self.data[path][offset + len(data):])
        self.files[path]['st_size'] = len(self.data[path])
        return len(data)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('mount')
    args = parser.parse_args()
    print("====== MOUNTING ======")

    logging.basicConfig(level=logging.CRITICAL)
    fuse = FUSE(Memory(), args.mount, foreground=True)
