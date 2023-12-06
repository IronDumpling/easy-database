#!/usr/bin/python3
#
# easydb.py
#
# Definition for the Database class in EasyDB client
#

# Import Module
import socket
from .packet import *
from .exception import PacketError
from .exception import IntegrityError
from collections import Iterable


# Helper Function
# Function 1: Check Name Legality for columns in a table
def name_legality_col(name):
    if name == "id":
        return False
    if not name[0].isalpha():
        return False
    for char in name:
        if not char.isdigit() and not char.isalpha() and not char == '_':
            return False
    return True


# Function 2: Check General Value Type Legality
def type_legality(val_type):
    legal_types = int, float, str
    if val_type in legal_types:
        return True
    elif type(val_type) == str:
        return True
    else:
        return False


# Function 3: Get Corresponding Numeric Code of the Type
def num_type_map(val_type):
    if val_type == int:
        return 1
    elif val_type == float:
        return 2
    elif val_type == str:
        return 3
    elif type(val_type) == str:
        return 4
    elif val_type is None:
        return 0


# Function 4: Check Name Legality for tables
def name_legality_tb(name):
    if not name[0].isalpha():
        return False
    for char in name:
        if not char.isdigit() and not char.isalpha() and not char == '_':
            return False
    return True


# Database Class
class Database:
    # Data member 1: Dictionary form table "dict_tables"
    #                <dict> -> <str> : <list>, <list> of <column name, column type>

    # Data member 2: Access index with table name "table_index"
    #                <dict> -> <str> : <int>

    # Data member 3: Access a list of column type with table name "num_type"
    #                <dict> -> <str> : <list>, <list> of numeric column types

    # Data member 4: A local copy of tb "tables"
    #                embedded <list>

    # Data member 5: Access the column index with table name and column name "col_index"
    #                <dict> -> <str> : <dict>, <dict> -> <str> : <int>

    # Function 1: Represent
    def __repr__(self):
        return "<EasyDB Database object>"

    # Function 2: Initializer
    def __init__(self, tables):
        self._socket = None
        # Create Data Structure members
        self.dict_tables = dict()
        self.table_index = dict()
        self.num_type = dict()
        self.col_index = dict()
        # Check if the table is iterable
        if not isinstance(tables, Iterable):
            raise TypeError
        table_names = set()
        # Check other TypeError, ValueError, IndexError, and IntegrityError
        for table in tables:
            # Check if the table name is string
            if not isinstance(table[0], str):
                raise TypeError
            # Check length of the 2nd layer
            if len(table) != 2:
                raise IndexError
            # Declare a set to look up column names
            col_names = set()
            for col in table[1]:
                # Check if the column name is string
                if not isinstance(col[0], str):
                    raise TypeError
                # Check length of the 4nd layer
                if len(col) != 2:
                    raise IndexError
                # Check if the column name is legal
                if not name_legality_col(col[0]):
                    raise ValueError
                # Check if the type is legal
                if type_legality(col[1]):
                    # Further check if the reference is legal
                    if type(col[1]) == str:
                        if col[1] not in table_names:
                            raise IntegrityError
                else:
                    raise ValueError
                # Check column name duplication
                if col[0] in col_names:
                    raise ValueError
                else:
                    col_names.add(col[0])
            # Check if the table name is legal
            if not name_legality_tb(table[0]):
                raise ValueError
            # Check table name duplication
            if table[0] in table_names:
                raise ValueError
            else:
                table_names.add(table[0])
        # Schema is correct, parse tb and get desired data structures
        self.tables = tables
        index = 1
        for table in tables:
            self.dict_tables[table[0]] = table[1]
            self.table_index[table[0]] = index
            self.num_type[table[0]] = list()
            self.col_index[table[0]] = dict()
            col_i = 1
            for col in table[1]:
                self.num_type[table[0]].append(num_type_map(col[1]))
                self.col_index[table[0]][col[0]] = col_i
                col_i += 1
            index += 1

    # Function 3: Connector
    def connect(self, host, port):
        assert (self._socket is None)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((host, int(port)))
        code = response(self._socket)
        if code == OK:
            return True
        elif code == SERVER_BUSY:
            self._socket.close()
            self._socket = None
            return False
        else:
            raise PacketError("Unexpected code %d during connect()" % code)

    # Function 4: Close Instance
    def close(self):
        if self._socket is None:
            return
        request(EXIT, 0)
        self._socket.close()
        self._socket = None

    # Function 5: String Output
    def __str__(self):
        str_form = ""
        for table in self.tables:
            str_form += table[0]
            str_form += "{"
            for col in table[1]:
                str_form += col[0]
                str_form += ":"
                if col[1] == int:
                    str_form += "integer"
                elif col[1] == float:
                    str_form += "float"
                elif col[1] == str:
                    str_form += "string"
                elif type(col[1]) == str:
                    str_form += col[1]
                str_form += ";"
            str_form += "}"
        return str_form

    # Function 6: Insert new row
    def insert(self, table_name, values):
        # 6.1 Error Checking: PacketError & InvalidReference
        # 6.1.1 Check If the table name exists
        if table_name not in self.dict_tables:
            raise PacketError("Not found table name during insert()")

        # 6.1.2 Check If the values has the correct length
        if len(values) != len(self.dict_tables[table_name]):
            raise PacketError("Element number mismatch during insert()")

        # Store table column list
        table_columns = self.dict_tables[table_name]

        # traverse both table template and input
        for (column, colInput) in zip(table_columns, values):
            column_name, col_type = column
            if col_type is not type(colInput):
                # 6.1.3 Compare each element's type in values
                if type(col_type) is not str:
                    if type(colInput) is not col_type:
                        raise PacketError("Element types mismatch during insert()")

                # the colType is a foreign ref
                if type(colInput) is not int:
                    raise PacketError("Element types mismatch during insert(): foreign")

        # 6.2 Call Request
        request_insert(self._socket, values, self.table_index[table_name], self.num_type[table_name])

        # 6.3 Wait for Response and Return pk & version
        return response_insert(self._socket)

    # Function 7: Update row
    def update(self, table_name, pk, values, version=None):
        # 7.1 Error Checking: PacketError
        # 7.1.1 pk is not int
        if type(pk) is not int:
            raise PacketError("Not correct id type during update()")

        # version is not int or None
        if type(version) is not int and version is not None:
            raise PacketError("Not correct version type during update()")

        # 7.1.2 table name does not exist
        if table_name not in self.dict_tables:
            raise PacketError("Not found table name during update()")

        # 7.1.3 Check If the values has the correct length
        if len(values) != len(self.dict_tables[table_name]):
            raise PacketError("Element number mismatch during update()")

        # 7.1.4 Check If the types match
        # Store table column list
        table_columns = self.dict_tables[table_name]

        # traverse both table template and input
        for (column, colInput) in zip(table_columns, values):
            column_name, col_type = column
            if col_type is not type(colInput):
                # 6.1.3 Compare each element's type in values
                if type(col_type) is not str:
                    if type(colInput) is not col_type:
                        raise PacketError("Element types mismatch during update()")

                # the colType is a foreign ref
                if type(colInput) is not int:
                    raise PacketError("Element types mismatch during update(): foreign")

        # 7.3 Call Request
        request_update(self._socket, pk, values, version, self.table_index[table_name], self.num_type[table_name])

        # 7.4 Wait for Response and Return new Version
        return response_update(self._socket)

    # Function 8: Drop
    def drop(self, table_name, pk):
        # 8.1 Error Checking: Packet Error
        # 8.1.1 Table Name does not Exist
        if table_name in self.dict_tables:
            # 8.1.2 Parameter is not valid
            if type(pk) is not int:
                raise PacketError("Not correct id type during drop()")
        else:
            raise PacketError("Not found table name during drop()")

        # 8.2 Call Request
        request_drop(self._socket, self.table_index[table_name], pk)

        # 8.3 Wait for Response
        response_drop(self._socket)

    # Function 9: Get
    def get(self, table_name, pk):
        # Error checking
        if type(pk) is not int:
            raise PacketError
        if table_name not in self.dict_tables:
            raise PacketError
        # Error-free, start to interact with server
        request_get(self._socket, self.table_index[table_name], pk)
        return response_get(self._socket)

    # Function 10: Scan
    def scan(self, table_name, op, column_name=None, value=None):
        # Error checking
        legal_tb_name = False
        legal_col_name = False
        legal_rt_op = False
        op_list = [1, 2, 3, 4, 5, 6, 7]

        # Check if the operator is supported
        if op not in op_list:
            raise PacketError("Operator is not supported.")
        # Special case 1: op == AL
        if op == operator.AL:
            legal_col_name = True
            legal_rt_op = True
        # Special case 2: column is preserved "id"
        if column_name == "id":
            legal_col_name = True
            if type(value) == int:
                legal_rt_op = True
            else:
                raise PacketError
        # General case: check table & column name existence
        for table in self.tables:
            if table[0] == table_name:
                legal_tb_name = True
            for col in table[1]:
                if col[0] == column_name:
                    legal_col_name = True
                    if type(value) == col[1]:
                        legal_rt_op = True
        # Special case 3: foreign key
        if legal_tb_name and legal_col_name and not legal_rt_op:
            temp = self.col_index[table_name][column_name]
            if self.num_type[table_name][temp - 1] == FOREIGN and type(value) is int:
                legal_rt_op = True
        # Check the flags to see if there should be an error
        if not legal_tb_name:
            raise PacketError("Illegal table name")
        if not legal_col_name:
            raise PacketError("Illegal column name")
        if not legal_rt_op:
            raise PacketError("Illegal value name")
        # The input is error-free, start to interact with server
        if op == operator.AL:
            request_scan(self._socket, self.table_index[table_name], op, 0, None, 0)
        else:
            if column_name == 'id':
                request_scan(self._socket, self.table_index[table_name], op, 0, value, int)
            else:
                col_idx = self.col_index[table_name][column_name]
                tb_idx = self.table_index[table_name]
                request_scan(self._socket, tb_idx, op, col_idx, value, self.num_type[table_name][col_idx - 1])
        # Receive Response
        return response_scan(self._socket)

