#!/usr/bin/python3
#
# packet.py
#
# Definition for all the packet-related constants and classes in EasyDB
#

import struct
import math
from .exception import *

# request commands
INSERT = 1
UPDATE = 2
DROP = 3
GET = 4
SCAN = 5
EXIT = 6

# response codes
OK = 1
NOT_FOUND = 2
BAD_TABLE = 3
BAD_QUERY = 4
TXN_ABORT = 5
BAD_VALUE = 6
BAD_ROW = 7
BAD_REQUEST = 8
BAD_FOREIGN = 9
SERVER_BUSY = 10
UNIMPLEMENTED = 11

# column types
NULL = 0
INTEGER = 1
FLOAT = 2
STRING = 3
FOREIGN = 4


# operator types
class operator:
    AL = 1  # everything
    EQ = 2  # equal
    NE = 3  # not equal
    LT = 4  # less than
    GT = 5  # greater than
    LE = 6  # you do not have to implement the following two
    GE = 7


# Request Function Family
# Function 0. Request Function
def request(sock, command, table_nr=0):
    # sending struct request to server
    buf = struct.pack("!ii", command, table_nr)
    sock.send(buf)


# Function 1. Request to Insert a row
def request_insert(sock, values, index, types):
    # 1.1 Pack Request Struct
    buf = struct.pack("!ii", INSERT, index)

    # 1.2 Pack Row Struct
    # 1.2.1 Pack Count
    buf += struct.pack("!i", len(values))

    # 1.2.2 Pack value
    for (value, type_val) in zip(values, types):
        if type_val == INTEGER:
            # since we define an 'int' as 8 bytes, so we actually use 'long long' below
            buf += struct.pack("!iiq", type_val, 8, value)
        elif type_val == FLOAT:
            # since we define a 'float' as 8 bytes, so we actually use 'double' below
            buf += struct.pack("!iid", type_val, 8, value)
        elif type_val == STRING:
            size = math.ceil(len(value) / 4) + len(value)
            buf += struct.pack("!ii" + str(size) + "s", type_val, size, value.encode('ascii'))
        else:  # foreign
            # let's hope the id field is 8 bytes
            buf += struct.pack("!iiq", type_val, 8, value)

    # Send Request to Server
    sock.send(buf)


# Function 2. Request to Update a row
def request_update(sock, pk, values, version, index, types):
    # 2.1 Pack Request
    buf = struct.pack("!ii", UPDATE, index)

    # 2.2 Pack key
    if version is not None:  # activate atomic update
        buf += struct.pack("!qq", pk, version)
    else:  # atomic update is not activated
        buf += struct.pack("!qq", pk, 0)

    # 2.3 Pack Row
    # 2.3.1 Pack Count
    buf += struct.pack("!i", len(values))

    # 2.3.2 Pack value
    for (value, type_val) in zip(values, types):
        if type_val == INTEGER:
            buf += struct.pack("!iiq", type_val, 8, value)
        elif type_val == FLOAT:
            buf += struct.pack("!iid", type_val, 8, value)
        elif type_val == STRING:
            size = math.ceil(len(value) / 4) + len(value)
            buf += struct.pack("!ii" + str(size) + "s", type_val, size, value.encode('ascii'))
        else:  # foreign
            buf += struct.pack("!iiq", type_val, 8, value)

    # sending struct request to server
    sock.send(buf)


# Function 3. Request to drop a row
def request_drop(sock, index, pk):
    # sending struct request to server
    buf = struct.pack("!iiq", DROP, index, pk)
    sock.send(buf)


# Function 4. Request to get
def request_get(sock, index, pk):
    buf = struct.pack("!iiq", GET, index, pk)
    sock.send(buf)


# Function 5. Request to scan
def request_scan(sock, tb_idx, op, col_num, val, col_type):
    # Append buf with "request", "table index", "column index", and "operator"
    buf = struct.pack("!ii", SCAN, tb_idx)
    buf += struct.pack("!ii", col_num, op)
    if op == operator.AL:
        buf += struct.pack("!ii", 0, 0)
    elif col_type == INTEGER:
        buf += struct.pack("!iiq", INTEGER, 8, val)
    elif col_type == FLOAT:
        buf += struct.pack("!iid", FLOAT, 8, val)
    elif col_type == STRING:
        size = math.ceil(len(val) / 4) + len(val)
        buf += struct.pack("!ii" + str(size) + "s", STRING, size, val.encode('ascii'))
    else:
        buf += struct.pack("!iiq", FOREIGN, 8, val)
    sock.send(buf)


# Response Function Family
# Function 0. Response Function
def response(sock):
    # expecting struct response, which is 4 bytes
    buf = sock.recv(4)
    return struct.unpack("!i", buf)[0]


# Function 1. Response to Insert Request
def response_insert(sock):
    # 1.1 Receive Response Code
    buf = sock.recv(4)
    code, = struct.unpack("!i", buf)  # comma used to only receive first 4 bytes

    # 1.2 Receive Key if code is ok
    if code is OK:
        buf = sock.recv(8+8)
        return struct.unpack('!qq', buf)
    elif code is BAD_FOREIGN:
        raise InvalidReference("Unexpected code %d during insert()" % code)


# Function 2. Response Function to update
def response_update(sock):
    # 2.1 Receive Response Code
    buf = sock.recv(4)
    code, = struct.unpack("!i", buf)

    # 2.2 Receive version if code is ok
    if code is OK:
        buf = sock.recv(8)
        return struct.unpack('!q', buf)[0]
    elif code is TXN_ABORT:
        raise TransactionAbort("Unexpected code %d during update()" % code)
    elif code is BAD_FOREIGN:
        raise InvalidReference("Unexpected code %d during update()" % code)
    elif code is NOT_FOUND:
        raise ObjectDoesNotExist("Unexpected code %d during update()" % code)


# Function 3. Response Function to drop
def response_drop(sock):
    # 3.1 Receive
    buf = sock.recv(4)
    code, = struct.unpack("!i", buf)

    if code is NOT_FOUND:
        raise ObjectDoesNotExist("Unexpected code %d during drop()" % code)


# Function 4. Response to Get
def response_get(sock):
    # Receive Response Code
    buf = sock.recv(4)
    code = struct.unpack("!i", buf)[0]

    if code is OK:
        buf = sock.recv(8)
        version = struct.unpack("!q", buf)[0]
        buf = sock.recv(4)
        row_count = struct.unpack("!i", buf)[0]
        value_list = []
        for i in range(row_count):
            # Grab value type
            buf = sock.recv(4)
            curr_val_type = struct.unpack("!i", buf)[0]
            # Grab value size
            buf = sock.recv(4)
            curr_val_size = struct.unpack("!i", buf)[0]
            # Grab data itself according to different data type
            buf = sock.recv(curr_val_size)
            if curr_val_type == INTEGER:
                curr_val_buf = struct.unpack("!q", buf)[0]
            elif curr_val_type == FLOAT:
                curr_val_buf = struct.unpack("!d", buf)[0]
            elif curr_val_type == STRING:
                curr_val_buf = struct.unpack("!" + str(curr_val_size) + "s", buf)[0]
                curr_val_buf = curr_val_buf.decode("ascii")
                curr_val_buf = curr_val_buf.replace("\x00", "")
            else:  # foreign
                curr_val_buf = struct.unpack("!q", buf)[0]
            value_list.append(curr_val_buf)
        ret_tuple = value_list, version
        return ret_tuple
    elif code is NOT_FOUND:
        raise ObjectDoesNotExist


# Function 5: Response to Scan
def response_scan(sock):
    # 3.1 Receive
    buf = sock.recv(4)
    code = struct.unpack("!i", buf)[0]

    if code is OK:
        buf = sock.recv(4)
        count = struct.unpack("!i", buf)[0]
        ans = []
        for i in range(count):
            buf = sock.recv(8)
            ret_id = struct.unpack("!q", buf)[0]
            ans.append(ret_id)
        return ans
    elif code is BAD_QUERY:
        raise PacketError

