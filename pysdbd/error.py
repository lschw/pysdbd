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

class Error(Exception):
    """
    Base class for Exceptions
    """
    
    def __init__(self, msg):
        Exception.__init__(self, msg)


class ColumnError(Error):
    """
    Exception for invalid columns
    """
    
    pass


class ValidationError(Error):
    """
    Exception if validation errors occur
    """
    
    def __init__(self, errors):
        Error.__init__(self, "Validation Error")
        self.errors = errors


class QueryError(Error):
    """
    Exception if sql queries are invalid
    """
    
    def __init__(self, msg, code, sql):
        Error.__init__(self, msg)
        self.code = code
        self.sql = sql
