#!/usr/bin/python3
#
# field.py
#
# Definitions for all the field types in ORM layer
#
from datetime import datetime


# Super field of all the other ones
class Field:
    _values = {}

    def __init__(self, blank=True, default=None, choices=()):
        if default is not None:
            if callable(default):
                default = default()
            self.type_error_checking(default)
        for choice in choices:
            self.type_error_checking(choice)
        self.blank = blank
        self.default = default
        self.choices = choices
        Field._values[self] = {}

    def __get__(self, obj, obj_type=None):
        return Field._values[self][obj]

    def __set__(self, obj, value):
        if value is None:
            if obj in Field._values[self]:
                raise AttributeError
            else:
                value = self.default
        value = self.val_cast(value)
        self.type_error_checking(value)
        if self.choices and value not in self.choices:
            raise ValueError
        Field._values[self][obj] = value

    def type_error_checking(self, value):
        pass

    def val_cast(self, value):
        if type(self) is Float:
            return float(value)
        else:
            return value


# INTEGER TYPE
class Integer(Field):
    def __init__(self, blank=False, default=0, choices=()):
        super().__init__(blank, default, choices)

    def type_error_checking(self, value):
        if type(value) is not int:
            raise TypeError


# FLOAT TYPE
class Float(Field):
    def __init__(self, blank=False, default=0., choices=()):
        super().__init__(blank, default, choices)

    def type_error_checking(self, value):
        if type(value) not in (int, float):
            raise TypeError


# STRING TYPE
class String(Field):
    def __init__(self, blank=False, default="", choices=()):
        super().__init__(blank, default, choices)

    def type_error_checking(self, value):
        if type(value) is not str:
            raise TypeError


# FOREIGN KEY TYPE
class Foreign(Field):
    def __init__(self, table, blank=False):
        self.table = table
        super().__init__(blank)

    def type_error_checking(self, value):
        if not (type(value) is self.table or (value is None and self.blank)):
            raise TypeError


# DATETIME TYPE
class DateTime(Field):
    implemented = True

    def __init__(self, blank=False, default=None, choices=()):
        default = datetime.fromtimestamp(0)
        super().__init__(blank, default, choices)

    def type_error_checking(self, value):
        if value is not None and type(value) is not datetime:
            raise TypeError


# COORDINATE TYPE
class Coordinate(Field):
    implemented = True

    def __init__(self, blank=False, default=None, choices=()):
        super().__init__(blank, default, choices)

    def type_error_checking(self, value):
        if value is not None:
            if type(value) not in (list, tuple) or len(value) != 2:
                if value is not None:
                    raise TypeError
            if type(value[0]) not in (int, float) or type(value[1]) not in (int, float):
                raise TypeError
            if not (-90 <= value[0] <= 90) or not (-180 <= value[1] <= 180):
                raise ValueError
