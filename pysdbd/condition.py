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

class Condition:
    """
    Base class for condition
    """

    def serialize(self, nested=False, quote=None, placeholder="?"):
        """
        Serialize condition, i.e. return string for usage in sql query

        Parameters
        ----------
        nested : bool
            If True, condition is considered to be nested or subsequent. No
            "WHERE" statement is added
        quote : None, method
            Method which is used to quote names
        placeholder : str
            Placeholder character

        Returns
        -------
        str
            Serialized condition
        """
        return ""


    def params(self):
        """
        Return all parameters for condition

        Returns
        -------
        list of list of mixed
            All parameters, which can be multiple sets of parameters, i.e.
            [ [p1,p2,...] (, [p1,p2,...], ...) ]
        """
        return []


    def cols(self):
        """
        Return all columns occuring in condition

        Returns
        -------
        list of str
            All occuring columns
        """
        return []


class And(Condition):
    """
    Container for conditions, whereby each condition is joined with "AND"
    """

    operator = "AND"

    def __init__(self, *conditions):
        """
        Create list of condition

        Parameters
        ----------
        *conditions : Condition
            Multiple conditions
        """
        self.conditions = list(conditions)


    def serialize(self, nested=False, quote=None, placeholder="?"):
        if not nested and len(self.conditions) == 0:
            return ""
        ret = " WHERE " if not nested else ""
        ret += "("
        for i in range(len(self.conditions)):
            ret += self.conditions[i].serialize(True, quote, placeholder)
            if i+1 < len(self.conditions):
                ret += " {} ".format(self.operator)
        ret += ")"
        return ret;


    def params(self):
        # Get maximum number of parameter sets
        N_sets = 1
        for cond in self.conditions:
            if len(cond.params()) > N_sets:
                N_sets = len(cond.params())

        ps = [] # New list of parameter sets
        for i in range(N_sets):
            p = [] # List of parameters
            for cond in self.conditions:
                if i < len(cond.params()):
                    p += cond.params()[i]
                else:
                    # Repeat parameter value from last set
                    p += cond.params()[-1]
            ps.append(p)
        return ps


    def cols(self):
        c = []
        for cond in self.conditions:
            c += cond.cols()
        return c


    def add(self, condition):
        """
        Add additional condition

        Parameters
        ----------
        condition : Condition
            New condition to add
        """
        self.conditions.append(condition)


class Or(And):
    """
    Container for conditions, whereby each condition is joined with "OR"
    """

    operator = "OR"


class Null(Condition):
    """
    Condition that value of column is NULL
    """

    operator = "IS"

    def __init__(self, col):
        """
        Setup condition

        Parameters
        ----------
        col : str
            Column which shall be NULL
        """
        self.col = col

    def serialize(self, nested=False, quote=None, placeholder="?"):
        ret = " WHERE " if not nested else ""
        col = quote(self.col) if quote else self.col
        ret += "{} {} NULL".format(col, self.operator)
        return ret


    def params(self):
        return [[]]


    def cols(self):
        return [self.col]


class NotNull(Null):
    """
    Condition that value of column is not NULL
    """
    operator = "IS NOT"


class Eq(Condition):
    """
    Condition that value of column is equal to some value
    """

    operator = "="

    def __init__(self, col, value):
        """
        Setup condition

        Parameters
        ----------
        col : str
            Column, whose value shall be checked
        value : mixed
            Value with which column's value shall be compared
        """
        self.col = col
        if isinstance(value, list):
            self.value = [[v] for v in value]
        else:
            self.value = [[value]]


    def serialize(self, nested=False, quote=None, placeholder="?"):
        ret = " WHERE " if not nested else ""
        col = quote(self.col) if quote else self.col
        ret += "{} {} {}".format(col, self.operator, placeholder)
        return ret


    def params(self):
        return self.value


    def cols(self):
        return [self.col]


class NotEq(Eq):
    """
    Condition that value of column is not equal to some value
    """

    operator = "!="


class Le(Eq):
    """
    Condition that value of column is less than some value
    """

    operator = "<"


class Gt(Eq):
    """
    Condition that value of column is greater than some value
    """

    operator = ">"


class Leq(Eq):
    """
    Condition that value of column is less or equal than some value
    """

    operator = "<="


class Geq(Eq):
    """
    Condition that value of column is greater or equal than some value
    """

    operator = ">="


class Re(Eq):
    """
    Condition that value of column matches regular expression
    """

    operator = "REGEXP"


class Like(Eq):
    """
    Condition that value of column is 'like' some value
    """

    operator = "LIKE"
