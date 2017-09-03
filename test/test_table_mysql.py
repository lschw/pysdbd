import sys
import traceback
import unittest
import logging
import datetime

from test_table_sqlite import *

class TableTestMysql(TableTestSqlite):
    
    ret1 = [{"id": 1, "name": "Peter", "birthday": datetime.date(2010,1,1), "size": 14.3},
            {"id": 2, "name": "Mayer", "birthday": datetime.date(2010,1,1), "size": 16.3},
            {"id": 3, "name": "Hans", "birthday": datetime.date(2010,1,1), "size": 16.3}]
    
    ret2 = [{"id": 3, "name": "Hans", "birthday": datetime.date(2010,1,1), "size": 16.3}]
    
    ret3 = [{"id": 3, "name": "Hans", "birthday": datetime.date(2010,1,1), "size": 16.3},
            {"id": 2, "name": "Mayer", "birthday": datetime.date(2010,1,1), "size": 16.3},
            {"id": 1, "name": "Peter", "birthday": datetime.date(2010,1,1), "size": 14.3},
            ]
    
    ret4 = [{"id": 3, "name": "Hans", "birthday": datetime.date(2010,1,1), "size": 16.3},
            {"id": 2, "name": "Mayer", "birthday": datetime.date(2010,1,1), "size": 16.3},
            ]
    
    ret5 = [{"id": 1, "name": "Peter", "birthday": datetime.date(2010,1,1), "size":14.3},
            ]
    
    ret6 = {"id": 1, "name": "Peter", "birthday": datetime.date(2010,1,1), "size": 14.3}
    
    ret7 = {"id": 1, "name": "Peter", "birthday": datetime.date(2010,1,1), "size": 18.3}
    
    def setUp(self):
        self.dbh = db.MysqlDriver(
            host="localhost",
            db="testdb",
            user="root",
            passwd="test",
            socket="/opt/lampp/var/mysql/mysql.sock",
        )
    
    
    def tearDown(self):
        if self.dbh.table_exists("persons"):
            self.dbh.delete_table("persons")
        if self.dbh.table_exists("settings"):
            self.dbh.delete_table("settings")


if __name__ == '__main__':
    unittest.main(verbosity=2)
