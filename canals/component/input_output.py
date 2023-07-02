# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
import logging
from enum import Enum
from dataclasses import field, fields, is_dataclass, dataclass, asdict, MISSING

logger = logging.getLogger(__name__)


def _make_fields_optional(class_: type):
    """
    Takes a dataclass definition and modifies its __init__ so that all fields have
    a default value set.
    If a field has a default factory use it to set the default value.
    If a field has neither a default factory or value default to None.
    """
    defaults = []
    for field in fields(class_):
        default = field.default
        if field.default is MISSING and field.default_factory is MISSING:
            default = None
        elif field.default is MISSING and field.default_factory is not MISSING:
            default = field.default_factory()
        defaults.append(default)
    # mypy complains we're accessing __init__ on an instance but it's not in reality.
    # class_ is a class definition and not an instance of it, so we're good.
    # Also only I/O dataclasses are meant to be passed to this function making it a bit safer.
    class_.__init__.__defaults__ = tuple(defaults)  # type: ignore


def _make_comparable(class_: type):
    """
    Overwrites the existing __eq__ method of class_ with a custom one.
    This is meant to be used only in I/O dataclasses, it takes into account
    whether the fields are marked as comparable or not.

    This is necessary since the automatically created __eq__ method in dataclasses
    also verifies the type of the class. That causes it to fail if the I/O dataclass
    is returned by a function.

    In here we don't compare the types of self and other but only their fields.
    """

    def comparator(self, other) -> bool:
        if not is_dataclass(other):
            return False

        fields_ = [f.name for f in fields(self) if f.compare]
        other_fields = [f.name for f in fields(other) if f.compare]
        if not len(fields_) == len(other_fields):
            return False

        self_dict, other_dict = asdict(self), asdict(other)
        for field in fields_:
            if not self_dict[field] == other_dict[field]:
                return False

        return True

    setattr(class_, "__eq__", comparator)


# class MetaIO(type):
#     # def __new__(cls, name, bases, dct):
#     #     x = super().__new__(cls, name, bases, dct)
#     #     x.__setattr__ = 100
#     #     return x

#     def __setattr__(self, name, value):
#         print(f"{self=}, {name=}, {value=},")
#         if not hasattr(self, "self.__dataclass_fields__"):
#             return
#         if name in self.__dataclass_fields__:
#             self.__dataclass_fields__[name].default = value
#         else:
#             self.__dataclass_fields__[name] = field(default=value)
#             self.__dataclass_fields__[name].type = type(value)


# def _make_magic(class_: type):
#     # def getter(self, instance, owner=None):

#     def setter(self, name, value):
#         print(f"{self=}, {name=}, {value=}")
#         if not isinstance(self, type):
#             return super().__set__(self, name, value)

#         if name in self.__dataclass_fields__:
#             self.__dataclass_fields__[name].default = value
#         else:
#             self.__dataclass_fields__[name] = field(default=value)
#             self.__dataclass_fields__[name].type = type(value)

#     class_.__set__ = setter


class Connection(Enum):
    INPUT = 1
    OUTPUT = 2


def _input(class_=None):
    """
    Decorator to mark a method that returns a dataclass defining a Component's input.

    The decorated function becomes a property.
    """

    def decorator(input_class):
        # If the user didn't explicitly declare the returned class
        # as dataclass we do it out of convenience
        if not is_dataclass(input_class):
            input_class = dataclass(input_class)

        _make_comparable(input_class)
        _make_fields_optional(input_class)

        def caller(self, *args, **kwargs):
            instance = self.__class__(*args, **kwargs)
            for f in fields(self):
                # Note: THIS IS WRONG, MUST NOT WORK LIKE THIS
                # I've done it like this just to have a quick
                # way of setting defaults in the new input instance.
                # If a certain field has been explicitly set as None
                # this will overwrite it with the default value if found.
                # TODO: Think about if this is the actual behaviour that we
                # want. I would very much prefer to find a reliable way to
                # set defaults only if they've not been passed to __call__.
                if not getattr(instance, f.name):
                    setattr(instance, f.name, getattr(self, f.name))

            return instance

        input_class.__call__ = caller

        # Magic field to ease some further checks, we set it in the wrapper
        # function so we access it like this <class>.<function>.fget.__canals_connection__
        input_class.__canals_connection__ = Connection.INPUT

        return input_class

    # Check if we're called as @_input or @_input()
    if class_:
        # Called with parens
        return decorator(class_)

    # Called without parens
    return decorator


def _output(class_=None):
    """
    Decorator to mark a method that returns a dataclass defining a Component's output.

    The decorated function becomes a property.
    """

    def decorator(output_class):
        if not is_dataclass(output_class):
            output_class = dataclass(output_class)

        _make_comparable(output_class)
        # _make_magic(output_class)

        output_class.__canals_connection__ = Connection.OUTPUT
        return output_class

    # Check if we're called as @_output or @_output()
    if class_:
        # Called with parens
        return decorator(class_)

    # Called without parens
    return decorator
