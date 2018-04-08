# pysdbd - database abstraction API
# Copyright (C) 2017 Lukas Schwarz
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import datetime
import re

def validate(col, value, fmt, errors):
    """
    Validate value according to format definition.
    For each defined format 'foo', a global validation method '_v_foo()' must
    exist.
    Definition of the the validation method: _v_foo(col, value, errors)

    Parameters
    ----------
    col : str
        Name of field (needed for saving error messages)
    value : mixed
        Value to validate
    fmt : list of str
        Each string defines either a predefined format or is a regular
        expression (marked with starting "r_"). Value is validated based on
        these formats
    errors : dict
        Validation errors are stored in this dict with `col` as key

    Returns
    -------
    mixed
        Validated value
    """
    # Check for None value
    if "not_null" in fmt and value == None:
        errors[col] = "NONE_FIELD"
        return value

    # Skip None value
    if value == None:
        return

    value = str(value)

    # Check for empty field
    if "not_empty" in fmt and value == "":
        errors[col] = "EMPTY_FIELD"
        return value

    if value != "":

        # Loop through all specified formats
        for f in fmt:
            if f in ["not_empty", "not_null", "unique", "text"]:
                continue

            # Validate with user defined regular expression
            if f.startswith("r_"):
                if re.match("^({})$".format(f[2:]), value) == None:
                    errors[col] = "INVALID_REGEX"

            # Validate with predefined format
            else:
                func = "_v_{}".format(f)
                if func not in globals():
                    msg = "Validation format '{}' is not implemented. "
                    msg += "Used in field '{}' with value '{}'"
                    msg = msg.format(f, col, value)
                    raise NotImplementedError(msg)
                value = globals()[func](col, value, errors)
    return value


def _v_datetime(col, value, errors):
    """
    Validate string according to datetime in the format "YYYY-MM-DD HH:MM:SS"

    Parameters
    ----------
    see `validate()` method
    """
    try:
        dt = datetime.datetime.strptime(value,"%Y-%m-%d %H:%M:%S")
    except ValueError:
        errors[col] = "INVALID_DATETIME"
    return value


def _v_date(col, value, errors):
    """
    Validate string according to date in the format "YYYY-MM-DD"

    Parameters
    ----------
    see `validate()` method
    """
    try:
        dt = datetime.datetime.strptime(value,"%Y-%m-%d")
    except ValueError:
        errors[col] = "INVALID_DATE"
    return value


def _v_int(col, value, errors):
    """
    Validate string according to an integer

    Parameters
    ----------
    see `validate()` method
    """
    if re.match("^([+-])?[0-9]+$", value) == None:
        errors[col] = "INVALID_INT"
    return value


def _v_uint(col, value, errors):
    """
    Validate string according to an unsigned integer

    Parameters
    ----------
    see `validate()` method
    """
    if re.match("^[0-9]+$", value) == None:
        errors[col] = "INVALID_UINT"
    return value


def _v_float(col, value, errors):
    """
    Validate string according to an float
    The method does the following modification
    - replaces "," by "."
    - add ending 0 if necessary

    Parameters
    ----------
    see `validate()` method
    """
    value = value.replace(",", ".")
    if value.endswith("."):
        value = value + "0"
    if re.match("^([+-])?[0-9]+(\.[0-9]+)?$", value) == None:
        errors[col] = "INVALID_FLOAT"
    return value


def _v_ufloat(col, value, errors):
    """
    Validate string according to an unsigned float
    The method does the following modification
    - replaces "," by "."
    - add ending 0 if necessary

    Parameters
    ----------
    see `validate()` method
    """
    value = value.replace(",", ".")
    if value.endswith("."):
        value = value + "0"
    if re.match("^[0-9]+(\.[0-9]+)?$", value) == None:
        errors[col] = "INVALID_UFLOAT"
    return value


def _v_text100(col, value, errors):
    """
    Validate string to a maximum length of 100 characters

    Parameters
    ----------
    see `validate()` method
    """
    if len(value) > 100:
        errors[col] = "INVALID_TEXT100"
    return value


def _v_bool(col, value, errors):
    """
    Validate string to a boolean value

    Parameters
    ----------
    see `validate()` method
    """
    if value.lower() == "true" or value == "1":
        return "1"
    if value.lower() == "false" or value == "0":
        return "0"
    errors[col] = "INVALID_BOOL"
    return value
