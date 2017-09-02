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

"""database abstraction API"""

from .error import Error, ColumnError, ValidationError, QueryError
from .Table import Table
from .MysqlDriver import MysqlDriver
from .SqliteDriver import SqliteDriver

__version__ = "1.0.0"
__all__ = ["Error", "ColumnError", "ValidationError", "QueryError", "Table",
    "MysqlDriver", "SqliteDriver"]
