#!/usr/bin/python3
#
# table.py
#
# Definition for an ORM database table and its metaclass
#
from .field import *
from .easydb import *
from collections import OrderedDict
<<<<<<< HEAD
from datetime import datetime, timezone
=======
from datetime import datetime
>>>>>>> 80676bc8aa71c4f986e9d2fa060e44030cb89312

# EasyDB query operators
OP_AL = 1  # all
OP_EQ = 2  # equal
OP_NE = 3  # not equal
OP_LT = 4  # less than
OP_GT = 5  # greater than
OP_LE = 6  # less than or equal
OP_GE = 7  # greater than or equal


# Helper Functions
# Helper 1: process the elements
# Helper function of Save
<<<<<<< HEAD
def flatten(lst):
    # simple flattener, cannot handle cyclical lists
    if len(lst) == 0:
        return []
    if type(lst[0]) in (tuple, list):
        return flatten(lst[0]) + flatten(lst[1:])
=======
def element_processor(input_list):
    if len(input_list) == 0:
        return []
    if type(input_list[0]) in (tuple, list):
        return element_processor(input_list[0]) + element_processor(input_list[1:])
    else:
        return [input_list[0]] + element_processor(input_list[1:])
>>>>>>> 80676bc8aa71c4f986e9d2fa060e44030cb89312



# Helper 2: Get List of id
# Helper function of Filter and Count
def id_list(cls, db, dic, **kwargs):
    # Corner Case 1. Name not exists
    if not type(db) == Database:
        raise TypeError

    # initialize variables
    value = kwargs.items()
    column = kwargs.items()
    column_name = column
    op = OP_AL

    for column, value in kwargs.items():
        if "__" in column:
            column_name, op = column.split("__")
            try:
                op = dic[op]
            except KeyError:
                raise AttributeError
        else:
            op = OP_EQ
<<<<<<< HEAD
=======
            column_name = column
>>>>>>> 80676bc8aa71c4f986e9d2fa060e44030cb89312

    # auto unboxing
    # Case 1. table
    if isinstance(value, Table):
        value = value.pk
    # Case 2. date_time
    if isinstance(value, datetime):
        value = value.timestamp()

    # Case 3. Coordinate
    if isinstance(value, tuple) or isinstance(value, list):
<<<<<<< HEAD
        # Only Coordinate uses tuple/list
        ids_1 = set(db.scan(cls.__name__, op, column_name + '_lat', value[0]))
        ids_2 = set(db.scan(cls.__name__, op, column_name + '_lon', value[1]))
        ids = list(sorted(list(ids_1.intersection(ids_2))))
        return ids

    # Case 4. other cases
    ids = db.scan(cls.__name__, op, column_name, value)
    return ids
=======
        return list(sorted(list(
            set(db.scan(cls.__name__, op, column_name + '_lat', value[0])).intersection(
                set(db.scan(cls.__name__, op, column_name + '_lon', value[1]))))))

    # Case 4. other cases
    return db.scan(cls.__name__, op, column_name, value)
>>>>>>> 80676bc8aa71c4f986e9d2fa060e44030cb89312


# metaclass of table
# used to implement methods only for the class itself?
# Implement me or change me. (e.g. use class decorator instead?)
class MetaTable(type):
<<<<<<< HEAD
    table_register = []
    table_name_register = []

    def __new__(mcs, cls_name, bases, kwargs):
        # initialize class
        cls = super().__new__(mcs, cls_name, bases, kwargs)

        # ignore immediate Table base class
        if cls_name != "Table":
            cls._fields = []

            if cls_name == "pk":
                raise AttributeError

            # generate key and value
            for key, value in kwargs.items():
                if not isinstance(value, Field):
                    continue
                cls._fields.append((key, value))

            if cls_name in MetaTable.table_name_register:
                raise AttributeError

            # append to register
            MetaTable.table_register.append(cls)
            cls._register = MetaTable.table_register
            MetaTable.table_name_register.append(cls_name)

        # define class in global namespace
=======
    cls_obj_list = []
    cls_name_list = []

    def __new__(mcs, cls_name, bases, kwargs):
        cls = super().__new__(mcs, cls_name, bases, kwargs)
        if cls_name != "Table":
            cls._fields = []
            if cls_name == "pk":
                raise AttributeError
            for key, value in kwargs.items():
                if isinstance(value, Field):
                    cls._fields.append((key, value))
            if cls_name in MetaTable.cls_name_list:
                raise AttributeError
            MetaTable.cls_obj_list.append(cls)
            cls._register = MetaTable.cls_obj_list
            MetaTable.cls_name_list.append(cls_name)
>>>>>>> 80676bc8aa71c4f986e9d2fa060e44030cb89312
        globals()[cls_name] = cls
        return cls

    @classmethod
    def __prepare__(cls, name, bases):
        return OrderedDict()

    # get the desired object
    def get(cls, db, pk):
        values, version = db.get(cls.__name__, pk)

        value_index = 0
        kwargs = {}

        for field_name, obj in cls._fields:
            if type(obj) is Coordinate:
                kwargs[field_name] = (values[value_index], values[value_index + 1])
                value_index += 2
            else:
                kwargs[field_name] = values[value_index]
                value_index += 1

        kwargs["pk"] = pk
        kwargs["version"] = version
        return cls(db, **kwargs)

    # filter and return a list of all desired objects
    def filter(cls, db, **kwargs):
        dic = {"ne": OP_NE, "lt": OP_LT, "gt": OP_GT}
        ret = []

        ids = id_list(cls, db, dic, **kwargs)

        for obj_id in ids:
            ret.append(cls.get(db, obj_id))

        return ret

    # Returns the number of matches given the query. If no argument is given,
    # return the number of rows in the table.
    # db: database object, the database to get the object from
    # kwarg: the query argument for comparing
    def count(cls, db, **kwargs):
        dic = {"al": OP_AL, "eq": OP_EQ, "ne": OP_NE, "lt": OP_LT,
               "gt": OP_GT, "le": OP_LE, "ge": OP_GE}

        return len(id_list(cls, db, dic, **kwargs))

    @property
    def fields(self):
        return self._fields


# table class
class Table(object, metaclass=MetaTable):

    def __init__(self, db, **kwargs):
<<<<<<< HEAD
        self.pk = None  # ID
        self.version = None  # version
        if "pk" in kwargs:  # override
            self.pk = kwargs["pk"]
            self.version = kwargs["version"]
        self.db = db  # database object
        self._table_name = self.__class__.__name__

        fields = self.__class__.fields

        bad_fields = set(kwargs) - set(dict(fields)) - {"pk", "version"}
        if len(bad_fields) != 0:
            raise AttributeError
        self._field_names = tuple(zip(*fields))[0]

        # Type conversion occurs
        for k, obj in fields:
            if k in kwargs:
                if type(obj) is Foreign:
                    if type(kwargs[k]) is int:  # parse from int to foreign key
                        kwargs[k] = obj.table.get(self.db, kwargs[k])  # recursive
                if type(obj) is DateTime:
                    if type(kwargs[k]) is float:  # parse from float to datetime
                        kwargs[k] = datetime.fromtimestamp(kwargs[k])
=======
        self.pk = None
        self.version = None
        if "pk" in kwargs:
            self.pk = kwargs["pk"]
            self.version = kwargs["version"]
        self.db = db
        self._table_name = self.__class__.__name__

        fields = self.__class__.fields
        self._field_names = tuple(zip(*fields))[0]

        for k, obj in fields:
            if k in kwargs:
                if type(obj) is Foreign and type(kwargs[k]) is int:
                    kwargs[k] = obj.table.get(self.db, kwargs[k])
                if type(obj) is DateTime and type(kwargs[k]) is float:
                    kwargs[k] = datetime.fromtimestamp(kwargs[k])
>>>>>>> 80676bc8aa71c4f986e9d2fa060e44030cb89312
                setattr(self, k, kwargs[k])
            else:
                setattr(self, k, None)

    def value_processor(self):
        values = list(map(lambda field: getattr(self, field), self._field_names))
        new_values = []
        for i in range(len(values)):
            value = values[i]
<<<<<<< HEAD
            # foreign key parsing
            if isinstance(value, Table):
                new_values.append(value.pk)
            elif type(value) is datetime:
                # convert to POSIX timestamp
                new_values.append(value.timestamp())
            else:
                new_values.append(value)

        return flatten(new_values)

    def _save_subroutine(self, atomic):
        # New entry
        if self.pk is None:
            self.pk, self.version = self.db.insert(self._table_name, self._values())
        else:
            args = [self._table_name, self.pk, self._values()]
            if atomic:
                args.append(self.version)
            self.version = self.db.update(*args)

    # Save the row by calling insert or update commands.
    # atomic: bool, True for atomic update or False for non-atomic update
=======
            if isinstance(value, Table):
                new_values.append(value.pk)
            elif type(value) is datetime:
                new_values.append(value.timestamp())
            else:
                new_values.append(value)
        return element_processor(new_values)

>>>>>>> 80676bc8aa71c4f986e9d2fa060e44030cb89312
    def save(self, atomic=True):
        values = list(map(lambda field: getattr(self, field), self._field_names))
        for i in range(len(values)):
            value = values[i]
<<<<<<< HEAD
            # foreign key
            if isinstance(value, Table):
                # not saved yet
                if value.pk is None:
                    value.save(atomic)
        self._save_subroutine(atomic)

    # Delete the row from the database.
=======
            if isinstance(value, Table):
                if value.pk is None:
                    value.save(atomic)
        if self.pk is None:
            self.pk, self.version = self.db.insert(self._table_name, self.value_processor())
        else:
            args = [self._table_name, self.pk, self.value_processor()]
            if atomic:
                args.append(self.version)
            self.version = self.db.update(*args)

>>>>>>> 80676bc8aa71c4f986e9d2fa060e44030cb89312
    def delete(self):
        table_name = type(self).__name__
        self.db.drop(table_name, self.pk)
        self.version = None
        self.pk = None
