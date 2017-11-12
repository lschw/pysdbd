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
import mysql.connector
from .Driver import *

class MysqlDriver(Driver):
    """
    Implementation of driver with mysql backend
    see https://dev.mysql.com/doc/connector-python/en/
    """

    placeholder = "%s"
    
    def __init__(self, host, db, user, passwd, socket=None):
        """
        Setup connection to database
        
        Parameters
        ----------
        host : str
            Host name of database, e.g. "localhost" or ip address
        db : str
            Name of database
        user : str
            MySQL user used to connect to database
        passwd : str
            Password of MySQL user
        socked : None, str
            Path to socket file, alternative to host
        """
        Driver.__init__(self)
        
        try:
            self.con = mysql.connector.connect(
                host=host,
                database=db,
                user=user,
                password=passwd,
                unix_socket=socket
            )
            self.con.autocommit = False
            self.log.debug("Database connection created")
        except mysql.connector.Error as e:
            msg = "Failed to connect to database: {} (code {})"
            msg = msg.format(e.args[1], e.args[0])
            raise Error(msg)
    
    
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
                self.rollback()
                self.con.close()
                self.con = None
                self.log.debug("Database connection closed")
        except mysql.connector.Error as e:
            msg = "Failed to close database connection: {} (code {})"
            msg = msg.format(e.args[1], e.args[0])
            raise Error(msg)
    
    
    @staticmethod
    def db_exists(host, db, user, passwd, socket=None):
        """
        Check whether database exists
        
        Parameters
        ----------
        see `__init__()` for description
        
        Returns
        -------
        bool
            Whether database exists or not
        """
        try:
            con = mysql.connector.connect(
                host=host,
                database=db,
                user=user,
                password=passwd,
                unix_socket=socket
            )
            con.close()
            return True
        except mysql.connector.Error as e:
            if e.errno == 1049:
                return False
            msg = "Failed to connect to database: {} (code {})"
            msg = msg.format(e.args[1], e.args[0])
            raise Error(msg)
    
    
    def quote_name(self, name):
        """
        Quote `name` (e.g. a table name) for usage in sql query
        """
        return "`" + name + "`"
    
    
    def table_exists(self, name):
        """
        Check whether table `name` exists
        """
        sql = "SHOW TABLES LIKE %s"
        return (self.execute(sql, params=[name], ret="row") != None)
    
    
    def create_table(self, name, columns, unique=[]):
        """
        Create table
        """
        col_str = []
        col_str.append("{} INT NOT NULL AUTO_INCREMENT".format(
            self.quote_name("id")
        ))
        for col in columns:
            s = "{} ".format(self.quote_name(col))
            if "text100" in columns[col]:
                s += "VARCHAR(100)"
            elif "float" in columns[col] or "ufloat" in columns[col]:
                s += "FLOAT"
            elif "int" in columns[col] or "uint" in columns[col]:
                s += "INT"
            elif "date" in columns[col]:
                s += "DATE"
            elif "datetime" in columns[col]:
                s += "DATETIME"
            elif "bool" in columns[col]:
                s += "BOOLEAN"
            else:
                s += "TEXT"
            if "not_null" in columns[col]:
                s += " NOT NULL"
            col_str.append(s)
        
        col_str.append("PRIMARY KEY ({})".format(self.quote_name("id")))
        
        if unique:
            col_str.append(
                "UNIQUE({})".format(", ".join(
                    [self.quote_name(col) for col in unique])
                )
            )
        
        sql = "CREATE TABLE {} ({}) ".format(
            self.quote_name(name),
            ", ".join(col_str)
        )
        sql += "ENGINE = InnoDB CHARSET=utf8 COLLATE utf8_unicode_ci;"
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
        sql = "SHOW COLUMNS FROM {}".format(self.quote_name(table))
        rows = self.execute(sql, ret="rows")
        return [x["Field"] for x in rows]
    
    
    def start_transaction(self, t_state=True, timeout=None):
        """
        Start transaction (only if t_state = True)
        """
        if not t_state:
            return
        
        # Nested transaction
        if self.nested_transactions:
            if self.transaction_cnt == 0:
                self.nested_rollback = False
                try:
                    self.con.start_transaction()
                    self.log.debug("Transaction started")
                except sqlite3.Error as e:
                    raise Error(
                        "Failed to start transaction: {}".format(e.args[0])
                    )
            self.transaction_cnt += 1
        
        # Transaction with timeout
        else:
            timeout = self.transaction_timeout if timeout == None else timeout
            t0 = time.time()
            exc_buf = None
            while time.time() < t0+timeout:
                try:
                    self.con.start_transaction()
                    self.log.debug("Transaction started")
                    return
                except mysql.connector.Error as e:
                    exc_buf = e
                    if e.msg == "Transaction already in progress":
                        time.sleep(0.1) # try every 0.1 s to start transaction
                    else:
                        break
                self.log.debug("Transaction wait...")
            msg = "Failed to start transaction (timeout={}s): {} (code {})"
            msg = msg.format(timeout, exc_buf.args[1], exc_buf.args[0])
            raise Error(msg)
    
    
    def commit(self, t_state=True):
        """
        Commit transaction (only if t_state = True)
        """
        if not t_state:
            return
        
        # Nested transaction
        if self.nested_transactions:
            if self.transaction_cnt == 1:
                if self.nested_rollback:
                    self.rollback()
                    self.transaction_cnt -= 1
                    raise Error(
                        "Transaction was commited despite previous " +
                        "rollback in nested transaction"
                    )
                try:
                    self.con.commit()
                    self.log.debug("Transaction commited")
                except sqlite3.Error as e:
                    raise Error(
                        "Failed to commit transaction: {}".format(e.args[0])
                    )
                
            self.transaction_cnt -= 1
        
        # Transaction with timeout
        else:
            try:
                self.con.commit()
                self.log.debug("Transaction commited")
            except mysql.connector.Error as e:
                msg = "Failed to commit transaction: {} (code {})"
                msg = msg.format(e.args[1], e.args[0])
                raise Error(msg)
        
    
    def rollback(self, t_state=True):
        """
        Rollback transaction (only if t_state = True)
        """
        if not t_state:
            return
        
        # Nested transaction
        if self.nested_transactions:
            self.nested_rollback = True
            if self.transaction_cnt == 1:
                try:
                    self.con.rollback()
                    self.log.debug("Transaction rolled back")
                except sqlite3.Error as e:
                    raise Error(
                        "Failed to rollback transaction: {}".format(e.args[0])
                    )
            self.transaction_cnt -= 1
        
        # Transaction with timeout
        else:
            try:
                self.con.rollback()
                self.log.debug("Transaction rolled back")
            except mysql.connector.Error as e:
                msg = "Failed to rollback transaction: {} (code {})"
                msg = msg.format(e.args[1], e.args[0])
                raise Error(msg)
    
    
    def execute_multi(self, sql):
        """
        Execute multiple sql queries at once secured by a transaction
        """
        try:
            self.start_transaction()
            c = self.con.cursor()
            for result in c.execute(sql, multi=True):
                pass
            c.close()
            self.commit()
        except mysql.connector.Error as e:
            self.rollback()
            raise QueryError(e.args[1], e.args[0], sql)
    
    
    def execute(self, sql, params=[], ret="none"):
        """
        Execute single sql statement
        """
        
        if params and not isinstance(params[0], list):
            params = [params]
        
        if not params:
            # dummy entry to have at least one loop iteration to execute query
            params = [[None]]
        
        try:
            c = None
            prepared = (len(params) > 1)
            
            if ret == "rows" or ret == "row":
                c = self.con.cursor(
                    cursor_class=mysql.connector.cursor.MySQLCursorDict,
                    prepared=prepared
                )
            else:
                c = self.con.cursor(prepared=prepared)
            
            autotrans = False
            if not self.con.in_transaction:
                self.start_transaction()
                autotrans = True
            
            res = []
            for p in params:
                if len(p) == 0 or p == [None]:
                    p = None
                
                self.log.debug("Query: {}, Params: {}".format(
                    " ".join(sql.replace("\n", " ").split()), p
                ))
                c.execute(sql, p)
            
                if ret == "rows":
                    res.append(c.fetchall())
                elif ret == "row":
                    res.append(c.fetchone())
                    
                    # fetch and discard remaining rows to prevent
                    # "Unread result found" error
                    try:
                        c.fetchall()
                    except:
                        pass
                elif ret == "col":
                    row = c.fetchone()
                    res.append(row[0] if row else None)
                    
                    # fetch and discard remaining rows to prevent
                    # "Unread result found" error
                    try:
                        c.fetchall()
                    except:
                        pass
                elif ret == "cols":
                    ret0 = c.fetchall()
                    res_ = []
                    for r in ret0:
                        res_.append(r[0])
                    res.append(res_)
                elif ret == "id":
                    res.append(c.lastrowid)
                else:
                    res.append(None)
            
            c.close()
            
            if autotrans and self.con.in_transaction:
                self.commit()
            
            if ret == "none":
                return None
            if len(params) > 1:
                return res
            else:
                return res[0]
        
        except mysql.connector.Error as e:
            c.close()
            raise QueryError(e.args[1], e.args[0], sql)
