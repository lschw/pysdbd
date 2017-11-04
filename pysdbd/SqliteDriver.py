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

import time
import os
import re
import sqlite3
from .Driver import *

class SqliteDriver(Driver):
    """
    Implementation of driver with sqlite backend
    see https://docs.python.org/2.7/library/sqlite3.html
    """
    
    placeholder = "?"
    
    def __init__(self, file_name, create=False):
        """
        Setup connection to database
        
        Parameters
        ----------
        file_name : str
            File name of database file
        create : bool
            Whether to create database file if it does not exist
        """
        Driver.__init__(self)
        self.file_name = file_name
        
        try:
            if not create and not os.path.isfile(file_name):
                raise sqlite3.Error(
                    "File '{}' does not exist".format(file_name)
                )
            self.con = sqlite3.connect(
                file_name,
                isolation_level=None # = autocommit mode
            )
            
            # Convert returned rows to dict
            self.con.row_factory = self._dict_factory
            
            # Handle unicode strings
            self.con.text_factory = str
            
            # Make regexp function available in queries
            self.con.create_function("regexp", 2, self._regexp)
            
            self.log.debug("Database '{}' opened".format(file_name))
        except sqlite3.Error as e:
            raise Error(
                "Opening database '{}' failed: {}".format(file_name, e.args[0])
            )
    
    
    def __del__(self):
        """
        Close connection to database
        """
        self.close()
    
    
    def close(self):
        """
        Close connection to database. The driver should not longer be used
        after this method was called
        """
        try:
            if self.con != None:
                self.con.close()
                self.con = None
                self.log.debug("Database '{}' closed".format(self.file_name))
        except sqlite3.Error as e:
            raise Error(
                "Closing database '{}' failed: {}".format(
                    self.file_name, e.args[0]
            ))
    
    
    @staticmethod
    def db_exists(file_name):
        """
        Check whether database exists
        
        Parameters
        ----------
        file_name : str
            File name of database file
        
        Returns
        -------
        bool
            Whether database exists or not
        """
        return os.path.isfile(file_name)
    
    
    def quote_name(self, name):
        """
        Quote `name` (e.g. a table name) for usage in sql query
        """
        return '"' + name.replace('"', '""') + '"'
    
    
    def table_exists(self, name):
        """
        Check whether table `name` exists
        """
        sql = "SELECT name FROM sqlite_master WHERE name=?"
        return (self.execute(sql, params=[name], ret="row") != None)
    
    
    def create_table(self, name, columns, unique=[]):
        """
        Create table
        """
        col_str = []
        col_str.append("{} integer NOT NULL primary key autoincrement".format(
            self.quote_name("id")
        ))
        for col in columns:
            s = "{} ".format(self.quote_name(col))
            if "text100" in columns[col]:
                s += "varchar(100)"
            elif "float" in columns[col] or "ufloat" in columns[col]:
                s += "float"
            elif "int" in columns[col] or "uint" in columns[col]:
                s += "integer"
            elif "date" in columns[col]:
                s += "date"
            elif "datetime" in columns[col]:
                s += "datetime"
            elif "bool" in columns[col]:
                s += "integer"
            else:
                s += "text"
            if "not_null" in columns[col]:
                s += " NOT NULL"
            col_str.append(s)
        
        if unique:
            col_str.append(
                "UNIQUE({})".format(", ".join(
                    [self.quote_name(col) for col in unique])
                )
            )
        
        sql = "CREATE TABLE {} ({});".format(
            self.quote_name(name),
            ", ".join(col_str)
        )
        self.execute(sql)
    
    
    def delete_table(self, name):
        """
        Delete table
        """
        sql = "DROP TABLE {}".format(self.quote_name(name))
        self.execute(sql)
    
    
    def get_columns(self, table):
        """
        Return all columns of table
        """
        sql = "PRAGMA table_info({});".format(self.quote_name(table))
        rows = self.execute(sql, ret="rows")
        return [x["name"] for x in rows]
    
    
    def start_transaction(self, t_state=True, timeout=None):
        """
        Start transaction (only if t_state = True)
        """
        if not t_state:
            return
        
        timeout = self.transaction_timeout if timeout == None else timeout
        t0 = time.time()
        exc_buf = None
        while time.time() < t0+timeout:
            try:
                c = self.con.cursor()
                c.execute("BEGIN TRANSACTION;")
                c.close()
                self.log.debug("Transaction started")
                return
            except sqlite3.Error as e:
                c.close()
                exc_buf = e
                if str(e) == "cannot start a transaction within a transaction":
                    time.sleep(0.1) # try every 0.1 s to start transaction
                else:
                    break
            self.log.debug("Transaction wait...")
        raise Error(
            "Failed to start transaction (timeout={}s): {}".format(
                timeout, exc_buf.args[0]
            )
        )
    
    
    def commit(self, t_state=True):
        """
        Commit transaction (only if t_state = True)
        """
        if not t_state:
            return
        
        try:
            self.con.commit()
            self.log.debug("Transaction commited")
        except sqlite3.Error as e:
            raise Error("Failed to commit transaction: {}".format(e.args[0]))
    
    
    def rollback(self, t_state=True):
        """
        Rollback transaction (only if t_state = True)
        """
        if not t_state:
            return
        
        try:
            self.con.rollback()
            self.log.debug("Transaction rolled back")
        except sqlite3.Error as e:
            raise Error("Failed to rollback transaction: {}".format(e.args[0]))
    
        
    def execute_multi(self, sql):
        """
        Execute multiple sql queries at once secured by a transaction
        """
        try:
            self.start_transaction()
            c = self.con.cursor()
            c.executescript(sql)
            c.close()
            self.commit()
        except sqlite3.Error as e:
            self.rollback()
            raise QueryError(e.args[0], -1, sql)
    
    
    def execute(self, sql, params=[], ret="none"):
        """
        Execute single sql statement
        """
        
        if params and not isinstance(params[0], list):
            params = [params]
        
        try:
            c = self.con.cursor()
            self.log.debug("Query: {}, Params: {}".format(
                " ".join(sql.replace("\n", " ").split()),
                params
                )
            )
            if not params:
                c.execute(sql, params)
            elif len(params) > 1:
                c.executemany(sql, params)
            else:
                c.execute(sql, params[0])
            
            if ret == "rows":
                ret = c.fetchall()
            elif ret == "row":
                ret = c.fetchone()
            elif ret == "col":
                ret = c.fetchone()
                if ret != None:
                    for k in ret:
                        ret = ret[k]
                        break
            elif ret == "cols":
                ret0 = c.fetchall()
                ret = []
                for row in ret0:
                    ret.append(row[list(row.keys())[0]])
            elif ret == "id":
                ret = c.lastrowid
            else:
                ret = None
            
            c.close()
            return ret
        
        except sqlite3.Error as e:
            c.close()
            raise QueryError(e.args[0], -1, sql)
    
    
    def _regexp(self, expr, item):
        """
        User defined sql function for regular expressions
        
        Parameters
        ----------
        expr : str
            Regular expression
        item : str
            String to match regular expression
        
        Returns
        -------
        bool
            Whether string matches regular expression
        """
        return re.match(expr, item) is not None
    
    
    def _dict_factory(self, cursor, row):
        """
        Method for converting results into dicts
        
        Parameters
        ----------
        cursor : Cursor
            sqlite cursor
        row : 
        
        Returns
        -------
        dict
            Row result as dict
        """
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d
    
