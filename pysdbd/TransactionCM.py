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

class TransactionCM:
    """
    Context Manager for transactions
    """

    def __init__(self, dbh):
        self.dbh = dbh


    def __enter__(self):
        self.dbh.start_transaction()
        return self


    def __exit__(self, type, value, traceback):
        if traceback:
            self.dbh.rollback()
        else:
            self.dbh.commit()
        return False
