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

import copy
from .error import *
from .condition import *
from .validate import validate

class Table:
    """
    Class representing a single database table
    """
    
    # Name of table (MUST BE SET IN DERIVED CLASS)
    name = None
    
    # Definition of table columns (MUST BE SET IN DERIVED CLASS)
    #
    # It is mandatory that each table has an "id" column, which MUST not be
    # written explicitly in the columns definition.
    # Column name as key, column properties (as list) as value. Column
    # properties can be each format definition which the `validate()` method
    # understands
    #
    # Example:
    # columns = {
    #    "name": ["not_empty", "unique"],
    #    "birthday": ["not_empty", "date"],
    #    "size": ["float"]
    # }
    columns = {}
    
    # List of rows, which must be present (optional in derived class)
    #
    # This may e.g. be useful for tables which contain key-value pairs for
    # settings.
    # Each row is a dict with a column name as key and a regular expression
    # (which the value must match) as value
    #
    # Example:
    # rows = [
    #     {"key" : "version", "value" : "^(([0-9]+)\.([0-9]+)\.([0-9]+))$"},
    #     {"key" : "typ", "value" : "^(XYZ|ABC)$"},
    #     {"key" : "name", "value" : ".+"},
    # ]
    rows = []
    
    # Default values for predifined `rows` (optional in derived class)
    #
    # Each row is a dict with a column name as key and the column's value as
    # value
    #
    # Example:
    # default = [
    #     {"key" : "version", "value" : "1.2.1"},
    #     {"key" : "type", "value" : "ABC"},
    #     {"key" : "name", "value" : "foobar"},
    # ]
    default = []
    
    
    def __init__(self, dbh, create=False):
        """
        Setup table, perform check if table exists
        
        Parameters
        ----------
        dbh : Driver
            Database handle
        create : bool
            Whether to create table if it does not exist
        """
        self.dbh = dbh
        if not self.dbh.table_exists(self.name):
            if create:
                self.dbh.create_table(self.name, self.columns)
                self.create_default_rows()
            else:
                raise Error("Table '{}' does not exist".format(self.name))
    
    
    def count(self, where=Condition()):
        """
        Count rows matching condition
        
        Parameters
        ----------
        where : Condition
            Condition for row to be counted
        
        Returns
        -------
        int
            Number of rows matching condition
        """
        self._validate_where(where)
        sql = "SELECT COUNT(*) FROM {} {}".format(
            self.dbh.quote_name(self.name),
            where.serialize(
                quote=self.dbh.quote_name,
                placeholder=self.dbh.placeholder
            )
        )
        return self.dbh.execute(sql, where.params(), ret="col")
    
    
    def exists(self, where):
        """
        Check whether row which matches condition exists
        
        Parameters
        ----------
        where : Condition
            Condition for row to exists
        
        Returns
        -------
        bool
            Whether row exists or not
        """
        return (self.count(where) > 0)
    
    
    def create(self, data, cb_validate=None):
        """
        Create new row(s)
        
        Parameters
        ----------
        data : list of dict{ str: mixed }
            (Multiple) data row(s). If multiple data rows are given, all rows
            must have the same columns
        cb_validate : None, method(data, errors)
            Additional user-defined validation method, called after all data is
            inserted into database, arguments:
                data : list of dict{ str: mixed }
                    Default validated data as inserted into database
                errors : list of dict { str: str }
                    Error dict for each row. The method must save its errors
                    inside these dicts with column name as key
        
        Returns
        -------
        id, None
            id of new created row or None if multiple rows were created
        """
        validated,cols,values = self._split_col_value(data)
        cols = [self.dbh.quote_name(col) for col in cols]
        sql = "INSERT INTO {} ({}) VALUES ({})".format(
            self.dbh.quote_name(self.name),
            ",".join(cols),
            ",".join([self.dbh.placeholder]*len(cols))
        )
        ids = self.dbh.execute(sql, values, ret="id")
        self._validate2(validated, cb_validate)
        return ids
    
    
    def update(self, data, where, cb_validate=None):
        """
        Update row(s)
        
        Parameters
        ----------
        data : list of dict{ str: mixed }
            (Multiple) data row(s). If multiple data rows are given, all rows
            must have the same columns
        where : Condition
            Condition for rows to update
        cb_validate : None, method(data, errors)
            see `create()` for description
        """
        validated,cols,values = self._split_col_value(data)
        values = self._join_where_params(values, where)
        cols_str = []
        for col in cols:
            cols_str.append(
                "{} = {}".format(self.dbh.quote_name(col),
                self.dbh.placeholder)
            )
        sql = "UPDATE {} SET {} {}".format(
            self.dbh.quote_name(self.name),
            ",".join(cols_str),
            where.serialize(
                quote=self.dbh.quote_name,
                placeholder=self.dbh.placeholder
            )
        )
        self.dbh.execute(sql, values)
        self._validate2(validated, cb_validate)
        
    
    def delete(self, where=Condition()):
        """
        Delete row
        
        Parameters
        ----------
        where : Condition
            Condition for rows to delete
        """
        self._validate_where(where)
        sql = "DELETE FROM {} {}".format(
            self.dbh.quote_name(self.name),
            where.serialize(
                quote=self.dbh.quote_name,
                placeholder=self.dbh.placeholder
            )
        )
        self.dbh.execute(sql, where.params())
    
    
    def get(self, where=Condition(), ret="rows", cols="*", order="id",
            distinct=False, limit=None, offset=None):
        """
        Fetch data
        
        Parameters
        ----------
        where : Condition
            Condition for rows to get
        ret : {"none", "row", "rows, "col", "cols", "id"}
            What to return
            none - return nothing
            row - return single row
            rows - return multiple rows
            col - return single column
            cols - return multiple columns
            id - return last insert id
        cols : "*", str, list of str
            Which columns to fetch
            * - fetch all columns
            str - fetch single column
            list of str - fetch theses columns
        order : str, dict
            Row ordering
            str - order rows by this column ascending
            dict - order rows by theses columns, format
                  {col1: ORDER, col2: order, ...}, ORDER can be ASC or DESC
        distinct : bool
            Whether to return only distinct rows (True) or not (False)
        limit : None, int
            Limit count of returned rows by this value
        offset : None, int
            In combination with `limit`, sets offset of limited data set
        
        Returns
        -------
        mixed
            Return value depends on `ret` argument
        """
        self._validate_where(where)
        
        # Create query string for parameters
        if cols != "*":
            if not isinstance(cols, list):
                cols = [cols]
            for i in range(len(cols)):
                if cols[i] != "id" and cols[i] not in self.columns:
                    msg = "Invalid column '{}'".format(cols[i])
                    raise ColumnError(msg)
                cols[i] = self.dbh.quote_name(cols[i])
        
        # Create query string for order
        if isinstance(order, str):
            order = {order: "ASC"}
        order_str = []
        for col in order:
            if col != "id" and col not in self.columns:
                msg = "Invalid column '{}'".format(col)
                raise ColumnError(msg)
            if order[col].upper() not in ["ASC", "DESC"]:
                msg = "Invalid ordering direction '{}'".format(order[col])
                raise Error(msg)
            order_str.append("{} {}".format(
                    self.dbh.quote_name(col),
                    order[col]
                )
            )
        order = ", ".join(order_str)
        
        # Create query string for limit, offset and distinct
        limit = " LIMIT {}".format(limit) if limit != None else ""
        offset = " OFFSET {}".format(offset) if offset != None else ""
        distinct = " DISTINCT" if distinct else ""
        
        # Create total query string
        sql = " SELECT {} {} FROM {} {} ORDER BY {} {} {}".format(
            distinct,
            ",".join(cols),
            self.dbh.quote_name(self.name),
            where.serialize(
                quote=self.dbh.quote_name,
                placeholder=self.dbh.placeholder
            ),
            order,
            limit,
            offset,
        )
        return self.dbh.execute(sql, where.params(), ret=ret)
    
    
    def check_cols(self):
        """
        Check if defined columns exist
        Raise exception if not all defined columns exist
        """
        cols = self.dbh.get_columns(self.name)
        cols_defined = list(self.columns.keys()) + ["id"]
        missing = []
        for col in cols_defined:
            if col not in cols:
                missing.append("'{}'".format(col))
        
        if missing:
            raise Error(
                "Table '{}' is invalid. ".format(self.name) + 
                "The following columns do not exist: {}".format(
                    ", ".join(missing)
                )
            )
    
    
    def check_predefined_rows(self):
        """
        Check if predefined rows exist and contain valid data
        Raise exception if rows are missing or do not contain valid data
        """
        if not self.rows:
            return
        invalid = []
        for row in self.rows:
            where = And()
            for col in row:
                where.add(Re(col, row[col]))
            if self.count(where=where) == 0:
                invalid.append("{}".format(repr(row)))
        
        if invalid:
            raise Error(
                "Table '{}' is invalid. ".format(self.name) +
                "The following predefined rows are missing or invalid: " +
                "{}".format(", ".join(invalid))
            )
    
    
    def create_default_rows(self):
        """
        Create default rows defined for this table
        """
        for row in self.default:
            self.create([row])
    
    
    def _validate_data(self, data, errors):
        """
        Validate data
        
        This function selects all columns which are defined for the table
        from `data` and validates the values according to the definition. The
        validation process may alter the data. If values are invalid, the error
        information is stored in the `error` dict.
        The validated data is returned.
        
        Parameters
        ----------
        data : dict { str: mixed }
            Single data row to validate
        errors : dict { str: mixed }
            Error information for invalid columns
        
        Returns
        -------
        dict { str: mixed }
            Validated data
        """
        validated = {}
        for col in data:
            if col not in self.columns:
                continue
            
            validated[col] = validate(
                col, data[col], self.columns[col], errors
            )
        return validated
    
    
    def _split_col_value(self, data):
        """
        Extract columns and values from data
        
        Parameters
        ----------
        data : list of dict{ str: mixed }
            see `create()` for description
        
        Returns
        -------
        list of dict
            Validated data
        list of str
            Extracted columns
        list of list of mixed
            Extracted values
        """
        validated = []
        cols = []
        values = []
        errors = []
        
        for i in range(len(data)):
            error = {}
            v = self._validate_data(data[i], error)
            errors.append(copy.copy(error))
            if error:
                continue
            
            if not cols:
                # Assume columns in first data set to be valid for all
                # other data sets
                cols = [col for col in v]
            else:
                # Check whether columns from first data set are valid for
                # this data set
                for col in cols:
                    if col not in v:
                        raise Error("Data sets have not the same columns")
            values.append([v[col] for col in cols])
            validated.append(v)
        
        if not all(not e for e in errors):
            raise ValidationError(errors)
        
        if not cols:
            raise Error("No valid columns available")
        
        return validated,cols,values
    
    
    def _validate2(self, data, cb_validate=None):
        """
        Validate data after inserting into database
        
        Parameters
        ----------
        data : list of dict{ str: mixed }
            Validated data, see `create()` for description
        cb_validate : None, method
            see `create()` for description
        """
        errors = []
        for i,d in enumerate(data):
            errors.append({})
            for col in self.columns:
                if "unique" in self.columns[col] and col in d:
                    if self.count(Eq(col, d[col])) > 1:
                        errors[i][col] = "NOT_UNIQUE"
        
        if not all(not e for e in errors):
            print(errors)
            raise ValidationError(errors)
        
        if cb_validate != None:
            cb_validate(data, errors)
            if not all(not e for e in errors):
                raise ValidationError(errors)
    
    
    
    def _join_where_params(self, values, where):
        """
        Join parameter values of conditions with `values`
        
        Parameters
        ----------
        values : list of list of mixed
            data values
        where : Condition, list of Conditions
            Condition to join parameter values
        
        Returns
        -------
        list of list of mixed
            Joined values
        """
        self._validate_where(where)
        values_where = where.params()
        N_sets = max(len(values),len(values_where))
        values_joined = []
        for i in range(N_sets):
            v = []
            if i < len(values):
                v += values[i]
            else:
                v += values[-1]
            if i < len(values_where):
                v += values_where[i]
            else:
                v += values_where[-1]
            values_joined.append(v)
        return values_joined
        
        
    def _validate_where(self, where):
        """
        Check if columns in condition are valid
        
        Parameters
        ----------
        where : Condition
            Condition to check
        """
        for col in where.cols():
            if col == "id" or col in self.columns:
                continue
            msg = "Invalid column '{}' in condition '{}'".format(
                col,
                where.serialize(
                    quote=self.dbh.quote_name,
                    placeholder=self.dbh.placeholder
                )
            )
            raise ColumnError(msg)
    
