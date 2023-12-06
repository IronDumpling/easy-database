#!/usr/bin/python3
#
# field.py
#
# Definitions for all the field types in ORM layer
#

from datetime import datetime


# Class definition of fields (default, blank. and choices)
# base field type, generic field
class Field:
    implemented = True  # boolean to check whether the field is implemented
    _values = {}

    def __init__(self, blank=True, default=None, choices=()):
        # Error checking
        if default is not None:
            self.type_check(default)
        for choice in choices:
            self.type_check(choice)

        # initialize fields
        self.blank = blank
        self.choices = choices
        self.default = default
        Field._values[self] = {}

    def __get__(self, obj, obj_type=None):
        return Field._values[self][obj]

    def __set__(self, obj, value):
        # initial setup
        if obj not in Field._values[self]:
            if not self.blank and value is None and self.default is None:  # reject blank entries
                raise AttributeError("Field cannot be blank.")
            if value is None:  # set as default value if none provided
                value = self.default
        else:
            if value is None:
                raise AttributeError("Field cannot be blank.")
        self.type_check(value)
        value = self.parser(value)
        if self.choices and value not in self.choices:
            raise ValueError("`{}` is not an allowed value.".format(value))
        Field._values[self][obj] = value

    def __delete__(self, obj):
        if not self.blank:
            raise AttributeError("Field cannot be blank.")
        Field._values[self][obj] = None

    @staticmethod
    def type_check(value):
        pass  # all generic values allowed

    @staticmethod
    def parser(value):
        return value


class Integer(Field):
    def __init__(self, blank=False, default=0, choices=()):
        super().__init__(blank, default, choices)

    @staticmethod
    def type_check(value):
        if type(value) is not int:
            raise TypeError("`{}` is not an integer.".format(value))


class Float(Field):
    def __init__(self, blank=False, default=0., choices=()):
        super().__init__(blank, default, choices)

    @staticmethod
    def type_check(value):
        if type(value) not in (int, float):
            raise TypeError("`{}` is not an integer/float.".format(value))

    @staticmethod
    def parser(value):
        return float(value)


class String(Field):
    def __init__(self, blank=False, default="", choices=()):
        super().__init__(blank, default, choices)

    @staticmethod
    def type_check(value):
        if type(value) is not str:
            raise TypeError("`{}` is not a string.".format(value))


class Foreign(Field):
    def __init__(self, table, blank=False):
        self.table = table
        super().__init__(blank)

    def type_check(self, value):
        if type(value) is not self.table:
            raise TypeError("`{}` is not a valid foreign key reference.".format(value))


class DateTime(Field):
    implemented = True

    def __init__(self, blank=False, default=None, choices=()):
        if default is not None:
            default = default()  # datetime functor
        super().__init__(blank, default, choices)

    @staticmethod
    def type_check(value):
        if type(value) is not datetime:
            raise TypeError("`{}` is not a datetime.".format(value))


class Coordinate(Field):
    implemented = True

    def __init__(self, blank=False, default=None, choices=()):
        super().__init__(blank, default, choices)

    @staticmethod
    def type_check(value):
        if type(value) not in (list, tuple) or len(value) != 2:
            raise TypeError("`{}` is not a valid coordinate.".format(value))
        if type(value[0]) not in (int, float) or type(value[1]) not in (int, float):
            raise TypeError("`{}` are not valid coordinate types.".format(value))
        if not (-90 <= value[0] <= 90) or not (-180 <= value[1] <= 180):
            raise TypeError("`{}` does not contain valid coordinate values.".format(value))
