#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import os
import pwd #for getting the username from uid
import disktools

if not hasattr(__builtins__, 'bytes'):
    bytes = str

ROOT_BLOCK_ID = 0
NUM_BLOCKS = 16


#Gets the meta data for the root directory. (from block 0, because thats where i store it)
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
                 
                #args[0].files[args[1]] = dict(
                #        st_mode = disktools.bytes_to_int(data[1:3]),
                #        st_ctime = disktools.bytes_to_int(data[3:7]),
                #        st_mtime = disktools.bytes_to_int(data[7:11]),
                #        st_atime = disktools.bytes_to_int(data[11:15]),
                #        st_nlink = disktools.bytes_to_int(data[15:16]),
                #        size = disktools.bytes_to_int(data[16:18]))
                args[0].files[args[1]].update({
                    'st_mode' : disktools.bytes_to_int(data[1:3]),
                    'st_ctime' : disktools.bytes_to_int(data[3:7]),
                    'st_mtime' : disktools.bytes_to_int(data[7:11]),
                    'st_atime' : disktools.bytes_to_int(data[11:15]),
                    'st_nlink' : disktools.bytes_to_int(data[15:16]),
                    'st_size':  disktools.bytes_to_int(data[16:18]),
                    'st_uid' : disktools.bytes_to_int(data[18:22]),
                    'st_gid' : disktools.bytes_to_int(data[22:24]),
                    })
        
                print("length :",disktools.bytes_to_int(data[16:18]))
        returnVal = func(*args, **kwargs)
        return returnVal
    return f

# Reads the data from disk. (data, not metadata)
def openDisk(func):
    def f(*args, **kwargs):
        if ( not args[1] in args[0].data.keys()):

            print ("OPEN DISK data: ",args[0].data[args[1]])
            b_id = args[0].files[args[1]]['block_id']
            size = args[0].files[args[1]]['st_size']
            # 22 is the off set. (first 22 of the block is metadata)
            
            total_data = disktools.read_block(b_id)
            b_id = disktools.bytes_to_int(total_data[0:1])
            size_2_read = size - 24
            while (size_2_read >= 63):
                block_data = disktools.read_block(b_id)
                total_data += block_data[1:63]
                b_id = disktools.bytes_to_int( block_data[0:1] )
                size_2_read -= 63

            if(b_id != 0):
                block_data = disktools.read_block(b_id)
                total_data += block_data[1: size_2_read]
            
            args[0].data[ args[1] ] = total_data[24:len(total_data)].decode().encode('ascii')
            #if (size+24 > 64):
            #    data1 = disktools.read_block(b_id)
            #    data2 = disktools.read_block(disktools.bytes_to_int(data1[0:1]))
            #    args[0].data[args[1]] = ( data1[24:64] + data2[1:size-64+24+1] ).decode().encode('ascii')
            #else:
            #    data1 = disktools.read_block(b_id)
            #    args[0].data[args[1]] = ( data1[24:size+24] ).decode().encode('ascii')
        #print("Data we read : ", args[0].data[args[1]].decode())
        returnVal = func(*args, **kwargs)
        print("==== OPEN CALLED ===")
        return returnVal
    return f

# File metadata.
# gets called when we user creats new file. 
# write metadata for a file & update root file. (adding the filename & block_id)
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
        byte_size = disktools.int_to_bytes(args[0].files[args[1]]['st_size'],2)        
        byte_uid = disktools.int_to_bytes(args[0].files[args[1]]['st_uid'],4)        
        byte_gid = disktools.int_to_bytes(args[0].files[args[1]]['st_gid'],2)        
        
        data = bytearray([0]*1) + byte_mode + byte_ctime + byte_mtime + byte_atime + byte_nlink+byte_size + byte_uid + byte_gid
        print("Data :",data,", BlockIndex : ",disktools.bytes_to_int(byte_b_id))
        disktools.write_block(args[0].files[args[1]]['block_id'], data) 
        
        
        
        """
        UPDATE THE ROOT BLOCK INFORMATION
        """
        name = args[1][1:len(args[1])]
        empty = bytearray([0]*(16-len(name)))
        byte_name = (bytearray(name.encode()) + empty + byte_b_id ) 
        print ("block index :",disktools.bytes_to_int(byte_b_id))
        cData = disktools.read_block(0)
        
        cSize = args[0].files['/']['st_size']
        print("CSIZE : ",cSize)
        if (cSize > 64):
            numToRead = int(cSize / 64)
            for i in range(1, numToRead+1):
                cData += disktools.read_block(i)

        bitmap = args[0].bitmap
        nData = cData[0:16]+disktools.int_to_bytes(cSize+17,2) + bitmap +cData[30:cSize] + byte_name
        
        print("NDATA : ",nData, "length :",len(nData))
        print(len(nData),"++",cSize+17)
        args[0].files['/']['st_size'] = cSize+17
        
        b_counter = 0
        while (len(nData) > (b_counter+1)*64 ):
            disktools.write_block(b_counter, nData[b_counter*64 : (b_counter+1)*64])
            b_counter += 1
        
        disktools.write_block( b_counter, nData[b_counter*64 : len(nData)])    

        #if (len(nData)>64):
        #    disktools.write_block(0, nData[0:64])
        #    disktools.write_block(1, nData[64:len(nData)])
        #else:
        #    disktools.write_block(0, nData[0:len(nData)])
        
        print("READ BLOCK 0 :", disktools.read_block(0))

        return returnVal
    return f

def writeDataOnDisk(func):
    def f(*args, **kwargs):
        print("== WRITE DATA ON DISK ==")
        print(args[0].files[args[1]]['st_size'])
        returnVal = func(*args, **kwargs)

        #Get the block id.
        #Write it to block
        
        print(args[0].files[args[1]]['st_size'])
        print(args[0].bitmap)
        
        b_id = args[0].files[args[1]]['block_id']
        metaData = disktools.read_block(b_id)
   
        data = bytearray(args[0].data[args[1]].encode('ascii'))
        args[0].files[args[1]]['st_size'] = len(data)
        
        metaData = metaData[0:16] + disktools.int_to_bytes(len(data),2)+metaData[18:24]

        w_data = metaData + data

        w_data = w_data[1:len(w_data)] #Removing the first index, which represents the next block id.
        while ( len(w_data) > 63 ):
            
            new_b_id = -1
            for i in range( len(args[0].bitmap)):
                if args[0].bitmap[i] == 0:
                    args[0].bitmap[i] = 1
                    new_b_id = i+4
                    break
            if new_b_id == -1:
                raise IOError ("Out of blocks to write !")
            
            disktools.write_block( b_id, disktools.int_to_bytes(new_b_id, 1)+w_data[0:63])
            b_id = new_b_id
            w_data = w_data[63: len(w_data)]

        disktools.write_block(b_id, disktools.int_to_bytes(0,1) + w_data[0:len(w_data)])


        #if (len(w_data) > 64):
        #    new_b_id = -1
        #    print("BITMAP =====> :",args[0].bitmap, ": ",len(args[0].bitmap), ":", args[0].bitmap[0])
        #    for i in range(len(args[0].bitmap)):
        #        if args[0].bitmap[i] == 0:
        #            args[0].bitmap[i] = 1
        #            new_b_id = i+4
        #            break

        #    if new_b_id == -1:
        #        print("Out of block !. This should not happen")
        #        raise IOError("out of block")

        #   disktools.write_block(b_id, disktools.int_to_bytes(i,1) + w_data[1:64])
        #    disktools.write_block(new_b_id, w_data[64: len(w_data)])
        #else:
        #    disktools.write_block(b_id, w_data[0:len(w_data)])
        
        return returnVal
            
    return f

def eraseFileDisk(func):
    def f(*args, **kwargs):

        # UPDATE THE ROOT BLOCK
        rootMetaData = disktools.read_block(ROOT_BLOCK_ID)
        cSize = args[0].files['/']['st_size']
        print("CSIZE : ",cSize)
        if (cSize > 64):
            numToRead = int(cSize / 64)
            for i in range(1, numToRead+1):
                rootMetaData += disktools.read_block(i)
        print(len(rootMetaData)) 
        index = 30
        while(rootMetaData[index] != 0 ):
            if(index +17 > len(rootMetaData)):
                print("Major issue ! Not reading enough block")

            name ="/"+ rootMetaData[index : index+16].decode().rstrip('\x00')
            print(name, rootMetaData[index+16 : index+17], ", boolean : ",name == args[1])
            if (name == args[1]):
                b_id = disktools.bytes_to_int(rootMetaData[index+16 : index+17])
                rootMetaData = rootMetaData[0:index]+rootMetaData[index+17:len(rootMetaData)]
            index += 17
            if (index >= len(rootMetaData)):
                    break
        print("Root block size : ", cSize, " -> ", len(rootMetaData)) 
        args[0].files['/']['st_size'] = len(rootMetaData)
        rootMetaData = rootMetaData[0:16] + disktools.int_to_bytes(len(rootMetaData),2)+ rootMetaData[18:len(rootMetaData)]
        b_counter = 0
        while (len(rootMetaData) > (b_counter+1)*64 ):
            disktools.write_block(b_counter, rootMetaData[b_counter*64 : (b_counter+1)*64])
            b_counter += 1
        
        disktools.write_block( b_counter, bytearray([0]*64))
        disktools.write_block( b_counter, rootMetaData[b_counter*64 : len(rootMetaData)])
 
        
        # UPDATE bitmap and erase data
        print("DELETING THIS BLOCK :",b_id)
        u_data = disktools.read_block(b_id)
        disktools.write_block(b_id, bytearray([0]*64))
        args[0].bitmap[b_id - 4] = 0
        b_id = disktools.bytes_to_int( u_data[0:1] )
        while( b_id != 0 ):
            print("DELETING THIS BLOCK :",b_id)
            u_data = disktools.read_block(b_id)
            disktools.write_block(b_id, bytearray([0]*64))
            args[0].bitmap[b_id - 4] = 0
            b_id = disktools.bytes_to_int( u_data[0:1] )


        func(*args, **kwargs)
        
    return f

class Memory(LoggingMixIn, Operations):

    def __init__(self):
        rootMetaData = disktools.read_block(ROOT_BLOCK_ID)
        print(rootMetaData) 
        self.bitmap = rootMetaData[18:30]
        self.files = {}
        self.data = defaultdict(bytes)
        self.fd = 0
        self.files['/'] = dict(
                st_mode = disktools.bytes_to_int(rootMetaData[1:3]),
                st_ctime = disktools.bytes_to_int(rootMetaData[3:7]),
                st_mtime = disktools.bytes_to_int(rootMetaData[7:11]),
                st_atime = disktools.bytes_to_int(rootMetaData[11:15]),
                st_nlink = disktools.bytes_to_int(rootMetaData[15:16]),
                st_size = disktools.bytes_to_int(rootMetaData[16:18])
                )
        print("file size :",self.files['/']['st_size']) 
        print("Bitmap :",self.bitmap)
        print("Bitmap len: ",len(self.bitmap))
        print("Boolean : ", self.bitmap[0] == 1)
        #rootFileSize = self.files['/']['st_size']
        if (self.files['/']['st_size'] > 64):
            numToRead = int(self.files['/']['st_size'] / 64)
            print ("print num read :",numToRead)
            for i in range(1, numToRead+1):
                newData = disktools.read_block(i)
                rootMetaData += newData
 
        print(len(rootMetaData))
        index = 30
        while(rootMetaData[index] != 0 ):
            if(index +17 > len(rootMetaData)):
                print("Major issue ! Not reading enough block")

            name ="/"+ rootMetaData[index : index+16].decode().rstrip('\x00')
            print(name, rootMetaData[index+16 : index+17])
            b_id = disktools.bytes_to_int(rootMetaData[index+16 : index+17])
            self.files[name] = dict(
                    block_id = b_id)
            print("BLOCK IS FOR Loading FILES :", b_id)
            self.bitmap[b_id - 4] = 1 #the block is no longer free.
            index += 17
            if (index >= len(rootMetaData)):
                    break

    
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
        for i in range(len(self.bitmap)):
            if self.bitmap[i] == 0:
                self.bitmap[i] = 1
                b_id = i+4
                break
        print("CHOOSEN Block_id: ",b_id)
        
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
            st_uid = os.getuid(),
            st_gid = os.getgid()
            )
        print("+++++++++ UID : ",os.getuid(), " : ", os.getgid())
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
    
    @openDisk
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

    
    def truncate(self, path, length, fh=None):
        print("===== Truncate() called ===========")
        # make sure extending the file fills in zero bytes
        self.data[path] = self.data[path][:length].ljust(
            length, '\x00'.encode('ascii'))
        self.files[path]['st_size'] = length

    @eraseFileDisk
    def unlink(self, path):
        if (path in self.data.keys()):
            self.data.pop(path)
        self.files.pop(path)

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        self.files[path]['st_atime'] = atime
        self.files[path]['st_mtime'] = mtime

    @writeDataOnDisk
    def write(self, path, data, offset, fh):
        print("== Write --:", path in self.data.keys())
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

    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(Memory(), args.mount, foreground=True)
