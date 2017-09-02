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

import logging
from .error import *
from .condition import *

class Driver:
    """
    Base class for database drivers
    """
    
    # Placeholder character inside sql query
    placeholder = None
    
    # Default timeout for transactions in seconds
    transaction_timeout = 3
    
    def __init__(self):
        """
        Setup connection to database
        """
        self.con = None
        self.log = logging.getLogger("pysdbd")
    
    
    def close(self):
        """
        Close connection to database. The driver should not longer be used
        after this method was called
        """
        pass
    
    
    def quote_name(self, name):
        """
        Quote `name` (e.g. a table name) for usage in sql query
        
        Parameters
        ----------
        name : str
            Name to quote
        
        Returns
        -------
        str
            Quoted name
        """
        return name
    
    
    def table_exists(self, name):
        """
        Check whether table `name` exists
        
        Parameters
        ----------
        name : str
            Name of table
        
        Returns
        -------
        bool
            True if table exists or False if not
        """
        pass
    
    
    def create_table(self, name, columns):
        """
        Create table
        
        Parameters
        ----------
        name : str
            Name of table
        columns : dict { str : list of str }
            Columns of table. Dict key is column name and dict value is list
            of column properties
        """
        pass
    
    
    def delete_table(self, name):
        """
        Delete table
        
        Parameters
        ----------
        name : str
            Name of table to delete
        """
        pass
    
    
    def get_columns(self, table):
        """
        Return all columns of table
        
        Parameters
        ----------
        table : str
            name of table
        
        Returns
        -------
        list of str
            All columns of table
        """
        pass
    
    
    def start_transaction(self, t_state=True, timeout=None):
        """
        Start transaction (only if t_state = True)
        
        Parameters
        ----------
        t_state : bool
            If False, method does nothing
        timeout : int, None
            Timeout in seconds to try starting transaction if a competing
            transaction is in progress
        """
        pass
    
    
    def commit(self, t_state=True):
        """
        Commit transaction (only if t_state = True)
        
        Parameters
        ----------
        t_state : bool
            If False, method does nothing
        """
        pass
    
    
    def rollback(self, t_state=True):
        """
        Rollback transaction (only if t_state = True)
        
        Parameters
        ----------
        t_state : bool
            If False, method does nothing
        """
        pass
    
    
    def execute_multi(self, sql):
        """
        Execute multiple sql queries at once secured by a transaction
        
        Parameters
        ----------
        sql : str
            multiple sql queries separated by ';'
        """
        pass
    
    
    def execute(self, sql, params=None, ret="none"):
        """
        Execute single sql query
        
        Parameters
        ----------
        sql : str
            Sql query as string. Can contain placeholders
        params : None, list of mixed, list of list of mixed
            Values of placholders in sql query. If multiple data sets are given
            all statements are executed with a prepared statement
        ret : {"none", "row", "rows, "col", "cols", "id"}
            What to return
            none - return nothing
            row - return single row
            rows - return multiple rows
            col - return single column
            cols - return multiple columns
            id - return last inserted id
        
        Returns
        -------
        mixed
            Return value depends on `ret` argument
        """
        pass
