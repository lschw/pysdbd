import sys
import os
import unittest
import logging

sys.path.append("../")
import pysdbd as db


# setup console handler for logger
log = logging.getLogger("pysdbd")
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)s: %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)

# test db file name
fn = "/tmp/test.db"

# table name
tn = "table1"

# table columns
cols = {
    "col1" : ["text100"],
    "col2" : ["float"],
    "col3" : ["int"],
    "col4" : ["date"],
    "col5" : ["datetime"],
    "col6" : ["bool"],
    "col7" : ["text"],
    "col8" : ["text", "not_null"],
}


class SqliteTest(unittest.TestCase):

    exc_table_exists = "table \"{}\" already exists"
    exc_table_missing = "no such table: {}"
    exc_not_null = "NOT NULL constraint failed: table1.col8"
    exc_transaction = "Failed to start transaction (timeout=1s): " + \
        "cannot start a transaction within a transaction"
    exc_ntrans_commit = "Transaction was commited despite previous rollback in nested transaction"
    exc_invalid_syntax = "near \"Invalid\": syntax error"

    ret_value1 = [{'col8': 'some other text ...', 'col6': 0,
            'col7': 'some text ...', 'col4': '2017-02-03',
            'col5': '2017-03-01 13:23:55', 'col2': 4.22, 'col3': 11,
            'col1': 'abcdefg', 'id': 1}, {'col8': 'some other text ...',
            'col6': 0, 'col7': 'some text ...', 'col4': '2017-02-03',
            'col5': '2017-03-01 13:23:55', 'col2': 4.22, 'col3': 11,
            'col1': 'abcdefg', 'id': 2}, {'col8': 'more some other text ...',
            'col6': 1, 'col7': 'more some text ...', 'col4': '2017-02-06',
            'col5': '2017-06-01 13:23:55', 'col2': 1.5, 'col3': 77,
            'col1': 'hijklmn', 'id': 3}]

    ret_value2 = {'col8': 'more some other text ...', 'col6': 1,
            'col7': 'more some text ...', 'col4': '2017-02-06',
            'col5': '2017-06-01 13:23:55', 'col2': 1.5, 'col3': 77,
            'col1': 'hijklmn', 'id': 3}

    ret_value3 = "2017-03-01 13:23:55"

    sql_multi = """
        CREATE TABLE xyz (
            id integer NOT NULL primary key autoincrement,
            key text NOT NULL,
            value integer
        );
        CREATE TABLE "foobar" (
            id integer NOT NULL primary key autoincrement,
            blub text NOT NULL,
            blab date
        );
        INSERT INTO "xyz" ("key", "value") VALUES ("abc", 99);
        INSERT INTO "foobar" ("blub", "blab") VALUES ("def", "2017-01-02");
        """

    def setUp(self):
        self.dbh = None
        self.close_db()

    def tearDown(self):
        self.close_db()

    def open_db(self, retobj=False):
        if retobj:
            return db.SqliteDriver(fn, True)
        else:
            self.dbh = db.SqliteDriver(fn, True)

    def close_db(self, remove=True):
        if self.dbh:
            del(self.dbh)
        self.dbh = None
        if remove and os.path.isfile(fn):
            os.remove(fn)

    def test_A_connect(self):

        # check connection to non-existing db
        with self.assertRaises(db.Error) as cm:
            dbh = db.SqliteDriver(fn, False)

        self.assertEqual(
            cm.exception.__str__(),
            "Opening database '"+fn+"' failed: File '"+fn+"' does not exist"
        )

        # check connection to non-existing db -> create db file
        dbh = db.SqliteDriver(fn, True)
        del(dbh)
        self.assertEqual(os.path.isfile(fn), True)

        # check connection to existing db
        dbh = db.SqliteDriver(fn, True)
        del(dbh)
        os.remove(fn)


    def test_B_db_exists(self):

        # db does not exist
        self.assertEqual(db.SqliteDriver.db_exists(fn), False)

        # db exists
        dbh = db.SqliteDriver(fn, True)
        del(dbh)
        self.assertEqual(db.SqliteDriver.db_exists(fn), True)
        os.remove(fn)


    def test_C_quote_name(self):
        self.open_db()
        self.assertEqual(self.dbh.quote_name("foobar"), "\"foobar\"")
        self.assertEqual(self.dbh.quote_name("foo\"bar"), "\"foo\"\"bar\"")
        self.close_db()


    def test_D_table(self):
        self.open_db()

        # table does not exist
        self.assertEqual(self.dbh.table_exists(tn), False)

        # create table
        self.dbh.create_table(tn, cols)

        # create table -> error table already exists
        with self.assertRaises(db.QueryError) as cm:
            self.dbh.create_table(tn, cols)
        self.assertEqual(
            cm.exception.__str__(),
            self.exc_table_exists.format(tn)
        )

        # table exists
        self.assertEqual(self.dbh.table_exists(tn), True)

        # check columns
        self.assertEqual(
            sorted(self.dbh.get_columns(tn)),
            sorted(["id"] + list(cols.keys()))
        )

        # delete table
        self.dbh.delete_table(tn)

        # delete table -> error table does not exist
        with self.assertRaises(db.QueryError) as cm:
            self.dbh.delete_table(tn)
        self.assertEqual(
            cm.exception.__str__(),
            self.exc_table_missing.format(tn)
        )

        self.close_db()


    def test_E_execute(self):
        self.open_db()

        self.dbh.create_table(tn, cols)

        sql1 = "INSERT INTO {} ({}) VALUES ({})".format(
            self.dbh.quote_name(tn),
            ",".join([self.dbh.quote_name("col{}".format(i)) for i in range(1,9)]),
            ",".join([self.dbh.placeholder]*8)
        )

        params = [
            ["abcdefg", 4.22, 11, "2017-02-03", "2017-03-01 13:23:55", False,
            "some text ...", "some other text ..."],
            ["hijklmn", "1.5", 77, "2017-02-06", "2017-06-01 13:23:55", True,
            "more some text ...", "more some other text ..."],
            ["opqrstu", "this is invalid", 77, "2017-02-06",
            "2017-06-01 13:23:55", True, "more some text ...",
            None],
        ]

        # insert single row -> return row id
        self.assertEqual(self.dbh.execute(sql1, params[0], "id"), 1)

        # insert multiple rows -> (not possible to retrieve all ids)
        self.dbh.execute(sql1, params[:2])

        # insert row with invalid data
        with self.assertRaises(db.QueryError) as cm:
            self.dbh.execute(sql1, params[2])
        self.assertEqual(
            cm.exception.__str__(),
            self.exc_not_null
        )

        # fetch all rows
        sql2 = "SELECT * from {}".format(self.dbh.quote_name(tn))
        print(self.dbh.execute(sql2, ret="rows"))
        self.assertEqual(
            self.dbh.execute(sql2, ret="rows"),
            self.ret_value1
        )

        # fetch all rows with specific columns
        sql3 = "SELECT id,col2 from {}".format(self.dbh.quote_name(tn))
        self.assertEqual(
            self.dbh.execute(sql3, ret="rows"),
            [{'col2': 4.22, 'id': 1}, {'col2': 4.22, 'id': 2},
            {'col2': 1.5, 'id': 3}]
        )

        # fetch single row
        sql4 = "SELECT * from {} WHERE col2 = {}".format(
            self.dbh.quote_name(tn),
            self.dbh.placeholder
        )
        self.assertEqual(
            self.dbh.execute(sql4, [1.5], ret="row"),
            self.ret_value2
        )

        # fetch value of one columns for multiple rows
        sql5 = "SELECT col8 from {} WHERE id > 1".format(self.dbh.quote_name(tn))
        self.assertEqual(
            self.dbh.execute(sql5, ret="cols"),
            ['some other text ...', 'more some other text ...']
        )

        # fetch single column of single row
        sql6 = "SELECT col5 from {} WHERE id = 1".format(self.dbh.quote_name(tn))
        self.assertEqual(
            self.dbh.execute(sql6, ret="col"),
            self.ret_value3
        )

        # reconnect and check if data was saved
        self.close_db(remove=False)
        self.open_db()

        # fetch all rows
        sql2 = "SELECT * from {}".format(self.dbh.quote_name(tn))
        self.assertEqual(
            self.dbh.execute(sql2, ret="rows"),
            self.ret_value1
        )

        self.close_db()


    def test_F_regex(self):
        self.open_db()
        self.dbh.create_table(tn, cols)

        sql1 = "INSERT INTO {} ({}) VALUES ({})".format(
            self.dbh.quote_name(tn),
            ",".join([self.dbh.quote_name("col{}".format(i)) for i in range(1,9)]),
            ",".join([self.dbh.placeholder]*8)
        )

        params = [
            ["abcdefg", 4.22, 11, "2017-02-03", "2017-03-01 13:23:55", False,
            "some text ...", "some other text ..."],
            ["hijklmn", "1.5", 77, "2017-02-06", "2017-06-01 13:23:55", True,
            "more some text ...", "more <443> other text"]
        ]

        # insert data
        self.dbh.execute(sql1, params)

        # select
        sql = "SELECT id from {} WHERE col8 regexp \"^[a-z ]+<443>\"".format(
            self.dbh.quote_name(tn))
        self.assertEqual(
            self.dbh.execute(sql, ret="cols"),
            [2]
        )

        self.close_db()


    def test_G_transaction(self):
        self.open_db()
        self.dbh.create_table(tn, cols)

        sql1 = "INSERT INTO {} ({}) VALUES ({})".format(
            self.dbh.quote_name(tn),
            ",".join([self.dbh.quote_name("col{}".format(i)) for i in range(1,9)]),
            ",".join([self.dbh.placeholder]*8)
        )

        params = [
            ["abcdefg", 4.22, 11, "2017-02-03", "2017-03-01 13:23:55", False,
            "some text ...", "some other text ..."],
            ["hijklmn", "1.5", 77, "2017-02-06", "2017-06-01 13:23:55", True,
            "more some text ...", "more |443| other text"]
        ]

        sql2 = "SELECT COUNT(*) from {}".format(self.dbh.quote_name(tn))

        # insert data inside transaction with rollback
        self.dbh.start_transaction()
        self.dbh.execute(sql1, params)

        # rows should be available
        self.assertEqual(
            self.dbh.execute(sql2, ret="col"),
            2
        )
        self.dbh.rollback()

        # rows shouldn't be available
        self.assertEqual(
            self.dbh.execute(sql2, ret="col"),
            0
        )

        # insert data inside transaction with commit
        self.dbh.start_transaction()
        self.dbh.execute(sql1, params)

        # rows should be available
        self.assertEqual(
            self.dbh.execute(sql2, ret="col"),
            2
        )
        self.dbh.commit()

        # rows should be available
        self.assertEqual(
            self.dbh.execute(sql2, ret="col"),
            2
        )

        # check failure of multiple transactions after timeout
        self.dbh.start_transaction()

        with self.assertRaises(db.Error) as cm:
            self.dbh.start_transaction(timeout=1)
        self.assertEqual(
            cm.exception.__str__(),
            self.exc_transaction
        )
        self.dbh.rollback()

        self.close_db()


    def test_H_multithread(self):

        import threading
        import time

        self.open_db()
        self.dbh.create_table(tn, cols)

        def thread_func(i):
            print("thread {}".format(i))

            dbh = self.open_db(retobj=True)

            sql1 = "INSERT INTO {} ({}) VALUES ({})".format(
                self.dbh.quote_name(tn),
                ",".join([self.dbh.quote_name("col{}".format(i)) for i in range(1,9)]),
                ",".join([self.dbh.placeholder]*8)
            )

            params = [
                ["abcdefg", 4.22, 11, "2017-02-03", "2017-03-01 13:23:55",
                False, "some text ...", "some other text ..."],
                ["hijklmn", "1.5", 77, "2017-02-06", "2017-06-01 13:23:55",
                True, "more some text ...", "more |443| other text"]
            ]

            # insert data
            dbh.start_transaction()
            dbh.execute(sql1, params)
            time.sleep(1)
            dbh.commit()

        # start two threads which insert 2 rows and waits 1 second
        for i in range(2):
            t = threading.Thread(target=thread_func, args=(i,))
            t.start()


        sql2 = "SELECT COUNT(*) from {}".format(self.dbh.quote_name(tn))

        # wait until threads have ended
        # in the mean time insert additional rows in a transaction which is
        # rollbacked and show count of already inserted rows
        while threading.active_count() > 1:
            self.dbh.start_transaction()
            self.dbh.execute(
                "INSERT INTO {} (col8) VALUES (\"testval\")".format(
                self.dbh.quote_name(tn))
            )
            print("no of rows: {}".format(self.dbh.execute(sql2, ret="col")))
            self.dbh.rollback()
            time.sleep(0.1)

        # finally there should be for rows available, two from each thread
        self.assertEqual(
            self.dbh.execute(sql2, ret="col"),
            4
        )

        self.close_db()


    def test_I_execute_multi(self):
        self.open_db()

        # create tables and insert some values
        self.dbh.execute_multi(self.sql_multi)

        # check if table exists
        self.assertEqual(self.dbh.table_exists("xyz"), True)
        self.assertEqual(self.dbh.table_exists("foobar"), True)

        # count rows
        self.assertEqual(
            self.dbh.execute("SELECT COUNT(*) from xyz", ret="col"), 1
        )
        self.assertEqual(
            self.dbh.execute("SELECT COUNT(*) from foobar", ret="col"), 1
        )

        # delete tables
        self.dbh.delete_table("xyz")
        self.dbh.delete_table("foobar")

        self.close_db()


    def test_J_nested_transaction(self):
        self.open_db()
        self.dbh.create_table(tn, cols)
        self.dbh.nested_transactions = True

        self.dbh.start_transaction()
        self.dbh.start_transaction()
        self.dbh.rollback()
        self.dbh.rollback()

        self.dbh.start_transaction()
        self.dbh.start_transaction()
        self.dbh.commit()
        self.dbh.rollback()

        self.dbh.start_transaction()
        self.dbh.start_transaction()
        self.dbh.rollback()

        with self.assertRaises(db.Error) as cm:
            self.dbh.commit()
        self.assertEqual(
            cm.exception.__str__(),
            self.exc_ntrans_commit
        )

        self.dbh.start_transaction()
        self.dbh.start_transaction()
        self.dbh.commit()
        self.dbh.commit()

        self.close_db()


    def test_K_nested_transaction_multithread(self):

        import threading
        import time

        self.open_db()
        self.dbh.create_table(tn, cols)

        def thread_func(i):
            print("thread {}".format(i))

            dbh = self.open_db(retobj=True)
            dbh.nested_transactions = True

            sql1 = "INSERT INTO {} ({}) VALUES ({})".format(
                self.dbh.quote_name(tn),
                ",".join([self.dbh.quote_name("col{}".format(i)) for i in range(1,9)]),
                ",".join([self.dbh.placeholder]*8)
            )

            params = [
                ["abcdefg", 4.22, 11, "2017-02-03", "2017-03-01 13:23:55",
                False, "some text ...", "some other text ..."],
                ["hijklmn", "1.5", 77, "2017-02-06", "2017-06-01 13:23:55",
                True, "more some text ...", "more |443| other text"]
            ]

            # insert data
            dbh.start_transaction()
            dbh.start_transaction()
            dbh.execute(sql1, params)
            time.sleep(1)
            dbh.commit()
            dbh.commit()

        # start two threads which insert 2 rows and waits 1 second
        for i in range(2):
            t = threading.Thread(target=thread_func, args=(i,))
            t.start()


        sql2 = "SELECT COUNT(*) from {}".format(self.dbh.quote_name(tn))

        # wait until threads have ended
        # in the mean time insert additional rows in a transaction which is
        # rollbacked and show count of already inserted rows
        while threading.active_count() > 1:
            self.dbh.start_transaction()
            self.dbh.execute(
                "INSERT INTO {} (col8) VALUES (\"testval\")".format(
                self.dbh.quote_name(tn))
            )
            print("no of rows: {}".format(self.dbh.execute(sql2, ret="col")))
            self.dbh.rollback()
            time.sleep(0.1)

        # finally there should be for rows available, two from each thread
        self.assertEqual(
            self.dbh.execute(sql2, ret="col"),
            4
        )

        self.close_db()


    def test_L_transactionCM(self):
        self.open_db()
        self.dbh.create_table(tn, cols)

        sql1 = "INSERT INTO {} ({}) VALUES ({})".format(
            self.dbh.quote_name(tn),
            ",".join([self.dbh.quote_name("col{}".format(i)) for i in range(1,9)]),
            ",".join([self.dbh.placeholder]*8)
        )

        params = [
            ["abcdefg", 4.22, 11, "2017-02-03", "2017-03-01 13:23:55", False,
            "some text ...", "some other text ..."],
            ["hijklmn", "1.5", 77, "2017-02-06", "2017-06-01 13:23:55", True,
            "more some text ...", "more |443| other text"]
        ]

        sql2 = "SELECT COUNT(*) from {}".format(self.dbh.quote_name(tn))

        # insert data inside transaction with rollback
        with self.dbh.transaction():
            self.dbh.execute(sql1, params)

        # rows should be available
        self.assertEqual(
            self.dbh.execute(sql2, ret="col"),
            2
        )

        with self.dbh.transaction():
            with self.assertRaises(db.Error) as cm:
                self.dbh.start_transaction(timeout=1)
            self.assertEqual(
                cm.exception.__str__(),
                self.exc_transaction
                )

        with self.assertRaises(db.Error) as cm:
            with self.dbh.transaction():
                self.dbh.execute(sql1, params)
                self.dbh.execute("Invalid syntax")
        self.assertEqual(
            cm.exception.__str__(),
            self.exc_invalid_syntax
        )


        self.close_db()


if __name__ == '__main__':
    unittest.main(verbosity=2)
