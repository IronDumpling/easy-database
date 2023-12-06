#!/usr/bin/python3
#
# __init__.py
#
# Contains all the functions and classes that this package wants to export
#

from .easydb import IntegrityError, InvalidReference, ObjectDoesNotExist, \
    TransactionAbort, PacketError
from .table import Table
from .field import Integer, Float, String, Foreign, DateTime, Coordinate
from .orm import setup


def export(database_name, module):
    # this assumes setup() will always return an EasyDB database object
    db = setup(database_name, module)
    return str(db)

