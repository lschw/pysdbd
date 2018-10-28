import sys
import os
import unittest
import logging
import datetime

sys.path.append("../")
import pysdbd as db

from test_sqlite import *

host = "localhost"
name = "testdb"
user = "root"
passwd = "test"
socket = "/opt/lampp/var/mysql/mysql.sock"


class MysqlTest(SqliteTest):

    exc_table_exists = "1050 (42S01): Table '{}' already exists"
    exc_table_missing = "1051 (42S02): Unknown table 'testdb.{}'"
    exc_not_null = "1048 (23000): Column 'col8' cannot be null"
    exc_transaction = "Failed to start transaction (timeout=1s): " + \
        "Transaction already in progress (code -1)"
    exc_invalid_syntax = "1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MariaDB server version for the right syntax to use near 'Invalid syntax' at line 1"

    ret_value1 = [{u'col8': u'some other text ...', u'col6': 0,
            u'col7': u'some text ...', u'col4': datetime.date(2017, 2, 3),
            u'col5': datetime.datetime(2017, 3, 1, 13, 23, 55), u'col2': 4.22,
            u'col3': 11, u'col1': u'abcdefg', u'id': 1}, {
            u'col8': u'some other text ...', u'col6': 0,
            u'col7': u'some text ...', u'col4': datetime.date(2017, 2, 3),
            u'col5': datetime.datetime(2017, 3, 1, 13, 23, 55), u'col2': 4.22,
            u'col3': 11, u'col1': u'abcdefg', u'id': 2}, {
            u'col8': u'more some other text ...', u'col6': 1,
            u'col7': u'more some text ...', u'col4': datetime.date(2017, 2, 6),
            u'col5': datetime.datetime(2017, 6, 1, 13, 23, 55), u'col2': 1.5,
            u'col3': 77, u'col1': u'hijklmn', u'id': 3}]

    ret_value2 = {'col8': 'more some other text ...', 'col6': 1,
            'col7': 'more some text ...', 'col4': datetime.date(2017,2,6),
            'col5': datetime.datetime(2017,6,1,13,23,55), 'col2': 1.5, 'col3': 77,
            'col1': 'hijklmn', 'id': 3}

    ret_value3 = datetime.datetime(2017,3,1,13,23,55)

    sql_multi = """
        CREATE TABLE `xyz` (
            `id` INT NOT NULL AUTO_INCREMENT,
            `key` TEXT NOT NULL,
            `value` INT,
            PRIMARY KEY (`id`)
        ) ENGINE = InnoDB CHARSET=utf8 COLLATE utf8_unicode_ci;
        CREATE TABLE `foobar` (
            `id` INT NOT NULL AUTO_INCREMENT,
            `blub` TEXT NOT NULL,
            `blab` DATE,
            PRIMARY KEY (`id`)
        ) ENGINE = InnoDB CHARSET=utf8 COLLATE utf8_unicode_ci;
        INSERT INTO `xyz` (`key`, `value`) VALUES ("abc", 99);
        INSERT INTO `foobar` (`blub`, `blab`) VALUES ("def", "2017-01-02");
        """


    def open_db(self, retobj=False):
        if retobj:
            return db.MysqlDriver(
                host,
                name,
                user,
                passwd,
                socket
            )
        else:
            self.dbh = db.MysqlDriver(
                host,
                name,
                user,
                passwd,
                socket
            )


    def close_db(self, remove=True):
        if remove and self.dbh and self.dbh.table_exists(tn):
            self.dbh.delete_table(tn)

        if remove and self.dbh and self.dbh.table_exists("xyz"):
            self.dbh.delete_table("xyz")

        if remove and self.dbh and self.dbh.table_exists("foobar"):
            self.dbh.delete_table("foobar")

        if self.dbh:
            self.dbh.close()
            self.dbh = None


    def test_A_connect(self):
        self.open_db()
        self.close_db()


    def test_B_db_exists(self):
        pass

    def test_C_quote_name(self):
        self.open_db()
        self.assertEqual(self.dbh.quote_name("foobar"), "`foobar`")
        self.close_db()


if __name__ == '__main__':
    unittest.main(verbosity=2)
