#!/usr/bin/python3
#
# orm.py
#
# Definition for setup and export function
#
import inspect
from .easydb import Database
from .field import *


# Return a database object that is initialized, but not yet connected.
#   database_name: str, database name
#   module: module, the module that contains the schema
def setup(database_name, module):
    # Check if the database name is "easydb".
    if database_name != "easydb":
        raise NotImplementedError("Support for %s has not implemented" % (
            str(database_name)))
    # make up the schema
    schema = []
    for class_name, obj in inspect.getmembers(module):
        if inspect.isclass(obj):
            table_rg = obj._register
            break
    else:
        raise RuntimeError
    # Create the schema
    for cls in table_rg:
        type_list = []
        for name, attr in cls._fields:
            # check if the name is legal
            if "_" in name:
                raise ValueError
            if type(attr) is Foreign:
                type_list.append((name, attr.table.__name__))
            elif type(attr) is String:
                type_list.append((name, str))
            elif type(attr) is Float:
                type_list.append((name, float))
            elif type(attr) is Integer:
                type_list.append((name, int))
            elif type(attr) is DateTime:
                type_list.append((name, float))
            elif type(attr) is Coordinate:
                type_list.append((name + "_lat", float))
                type_list.append((name + "_lon", float))
        schema.append((cls.__name__, tuple(type_list)))
    # return the schema object
    return Database(schema)

# note: the export function is defined in __init__.py
