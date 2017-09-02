import sys
import os
import unittest
import logging
import datetime

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


class TablePersons(db.Table):
    
    name = "persons"
    columns = {
        "name": ["not_empty", "unique"],
        "birthday": ["not_empty", "date"],
        "size": ["float"],
    }


class TableSettings(db.Table):
    
    name = "settings"
    columns = {
        "key" : ["not_empty", "unique"],
        "value": []
    }
    rows = [
        {"key" : "version", "value" : r"^(([0-9]+)\.([0-9]+)\.([0-9]+))$"},
        {"key" : "typ", "value" : r"^(XYZ|ABC)$"},
        {"key" : "name", "value" : r".+"},
    ]
    default = [
        {"key" : "version", "value" : r"1.2.1"},
        {"key" : "type", "value" : r"ABC"},
        {"key" : "name", "value" : r"foobar"},
    ]


class TableTestSqlite(unittest.TestCase):
    
    ret1 = [{"id": 1, "name": "Peter", "birthday": "2010-01-01", "size": 14.3},
            {"id": 2, "name": "Mayer", "birthday": "2010-01-01", "size": 16.3},
            {"id": 3, "name": "Hans", "birthday": "2010-01-01", "size": 16.3}]
    
    ret2 = [{"id": 3, "name": "Hans", "birthday": "2010-01-01", "size": 16.3}]
    
    ret3 = [{"id": 3, "name": "Hans", "birthday": "2010-01-01", "size": 16.3},
            {"id": 2, "name": "Mayer", "birthday": "2010-01-01", "size": 16.3},
            {"id": 1, "name": "Peter", "birthday": "2010-01-01", "size": 14.3},
            ]
    
    ret4 = [{"id": 3, "name": "Hans", "birthday": "2010-01-01", "size": 16.3},
            {"id": 2, "name": "Mayer", "birthday": "2010-01-01", "size": 16.3},
            ]
    
    ret5 = [{"id": 1, "name": "Peter", "birthday": "2010-01-01", "size":14.3},
            ]
    
    ret6 = {"id": 1, "name": "Peter", "birthday": "2010-01-01", "size": 14.3}
    
    ret7 = {"id": 1, "name": "Peter", "birthday": "2010-01-01", "size": 18.3}
    
    def setUp(self):
        if os.path.isfile("test.db"):
            os.remove("test.db")
        self.dbh = db.SqliteDriver("test.db", True)
    
    
    def tearDown(self):
        del(self.dbh)
        if os.path.isfile("test.db"):
            os.remove("test.db")
    
    
    def create_tables(self):
        self.persons = TablePersons(self.dbh, True)
        self.settings = TableSettings(self.dbh, True)
    
    
    def remove_tables(self):
        if self.dbh.table_exists("persons"):
            self.dbh.delete_table("persons")
        if self.dbh.table_exists("settings"):
            self.dbh.delete_table("settings")
    
    
    def person_validation(self, data, errors):
        for i,d in enumerate(data):
            if "birthday" in d:
                d1 = datetime.datetime.strptime(d["birthday"], "%Y-%m-%d")
                d2 = datetime.datetime.strptime("2011-01-01", "%Y-%m-%d")
                if d1 < d2:
                    errors[i]["birthday"] = "INVALID_AGE"
    
    
    def test_A_create(self):
        
        # non-existing table
        with self.assertRaises(db.Error) as cm:
            persons = TablePersons(self.dbh, False)
        self.assertEqual(
            cm.exception.__str__(),
            "Table 'persons' does not exist"
        )
        
        self.create_tables()
        self.remove_tables()
    
    
    def test_B_validation(self):
        
        self.create_tables()
        
        # invalid date
        self.dbh.start_transaction()
        data = [
            {"name": "Peter", "size": 14.3, "birthday": "201a-01-01"},
            {"name": "Mayer", "size": 16.3, "birthday": "2010-01-01"},
            {"name": "Hans", "size": 16.3, "birthday": "2010-01-01"}
        ]
        
        with self.assertRaises(db.ValidationError) as cm:
            self.persons.create(data)
        
        self.assertEqual(
            cm.exception.errors,
            [{"birthday":"INVALID_DATE"},{},{}]
        )
        self.dbh.rollback()
        
        
        # invalid age
        self.dbh.start_transaction()
        data = [
            {"name": "Mayer", "size": 16.3, "birthday": "2012-01-01"},
            {"name": "Hans", "size": 16.3, "birthday": "2010-01-01"}
        ]
        
        with self.assertRaises(db.ValidationError) as cm:
            self.persons.create(data, self.person_validation)
        
        self.assertEqual(
            cm.exception.errors,
            [{},{"birthday":"INVALID_AGE"}]
        )
        self.dbh.rollback()
        
        self.remove_tables()
        
    
    def test_C_count(self):
        
        self.create_tables()
        
        self.assertEqual(
            self.persons.count(db.condition.Eq("birthday", "2010-01-01")),
            0
        )
        
        data = [
            {"name": "Peter", "size": 14.3, "birthday": "2010-01-01"},
            {"name": "Mayer", "size": 16.3, "birthday": "2010-01-01"},
            {"name": "Hans", "size": 16.3, "birthday": "2010-01-04"}
        ]
        self.persons.create(data)
        
        self.assertEqual(
            self.persons.count(db.condition.Eq("birthday", "2010-01-01")),
            2
        )
        
        self.remove_tables()
    
    
    def test_D_exists(self):
        
        self.create_tables()
        
        self.assertEqual(
            self.persons.exists(db.condition.Eq("birthday", "2010-01-01")),
            False
        )
        
        data = [
            {"name": "Peter", "size": 14.3, "birthday": "2010-01-01"},
            {"name": "Mayer", "size": 16.3, "birthday": "2010-01-01"},
            {"name": "Hans", "size": 16.3, "birthday": "2010-01-01"}
        ]
        self.persons.create(data)
        
        self.assertEqual(
            self.persons.exists(db.condition.Eq("birthday", "2010-01-01")),
            True
        )
        
        self.remove_tables()
    
    
    def test_E_create(self):
        
        self.create_tables()
        
        data = [
            {"name": "Peter", "size": 14.3, "birthday": "2010-01-01"},
            {"name": "Mayer", "size": 16.3, "birthday": "2010-01-01"},
            {"name": "Hans", "size": 16.3, "birthday": "2010-01-01"}
        ]
        self.persons.create(data)
        
        self.assertEqual(
            self.persons.count(),
            3
        )
        
        self.remove_tables()
    
    
    def test_F_delete(self):
        self.create_tables()
        
        data = [
            {"name": "Peter", "size": 14.3, "birthday": "2010-01-01"},
            {"name": "Mayer", "size": 16.3, "birthday": "2010-01-03"},
            {"name": "Hans", "size": 16.3, "birthday": "2010-01-01"}
        ]
        self.persons.create(data)
        
        self.assertEqual(
            self.persons.count(),
            3
        )
        
        self.persons.delete(where=db.condition.And(
            db.condition.Eq("birthday", "2010-01-03"),
            db.condition.Gt("size", 15),
        ))
        
        self.assertEqual(
            self.persons.count(),
            2
        )
        
        self.remove_tables()
    
    
    def test_G_get(self):
        self.create_tables()
        
        data = [
            {"name": "Peter", "size": 14.3, "birthday": "2010-01-01"},
            {"name": "Mayer", "size": 16.3, "birthday": "2010-01-01"},
            {"name": "Hans", "size": 16.3, "birthday": "2010-01-01"}
        ]
        self.dbh.start_transaction()
        ids = self.persons.create(data)
        self.dbh.commit()
        
        # all rows
        self.assertEqual(
            self.persons.get(ret="rows"),
            self.ret1
        )
        
        # where cond with regex
        self.assertEqual(
            self.persons.get(ret="rows",
                where=db.condition.And(
                    db.condition.Re("name", ".*an"),
                )
            ),
            self.ret2
        )
        
        # order by name
        self.assertEqual(
            self.persons.get(ret="rows",
                order={"name":"asc"}
            ),
            self.ret3
        )
        
        # limit
        self.assertEqual(
            self.persons.get(ret="rows",
                order={"name":"asc"},
                limit=2
            ),
            self.ret4
        )
        
        # limit offset
        self.assertEqual(
            self.persons.get(ret="rows",
                order={"name":"asc"},
                limit=2,
                offset=2
            ),
            self.ret5
        )
        
        
        # single row
        self.assertEqual(
            self.persons.get(ret="row"),
            self.ret6
        )
        
        # single row, selected cols
        self.assertEqual(
            self.persons.get(ret="row",
            cols=["size", "id"]
            ),
            {"id": 1, "size": 14.3}
        )
        
        # single column
        self.assertEqual(
            self.persons.get(ret="col",
            cols=["id"]
            ),
            1
        )
        
        # multiple columns
        self.assertEqual(
            self.persons.get(ret="cols",
            cols=["id"]
            ),
            [1,2,3]
        )
        
        self.remove_tables()
        
    
    def test_H_update(self):
        self.create_tables()
        
        data = [
            {"name": "Peter", "size": 14.3, "birthday": "2010-01-01"},
            {"name": "Mayer", "size": 16.3, "birthday": "2010-01-01"},
            {"name": "Hans", "size": 16.3, "birthday": "2010-01-01"}
        ]
        self.persons.create(data)
        
        self.assertEqual(
            self.persons.get(where=db.condition.Eq("name", "Peter"),ret="row"),
            self.ret6
        )
        
        data = [
            {"size": 18.3},
        ]
        self.persons.update(data, where=db.condition.Eq("name", "Peter"))
        
        self.assertEqual(
            self.persons.get(where=db.condition.Eq("name", "Peter"),ret="row"),
            self.ret7
        )
        
        self.remove_tables()
    
    
    def test_I_check_cols(self):
        
        # valid table
        self.create_tables()
        self.persons.check_cols()
        self.settings.check_cols()
        self.remove_tables()
        
        # create invalid table
        columns = {
            "name": ["not_empty", "unique"],
            "birthday": ["not_empty", "date"],
        }
        self.dbh.create_table("persons", columns)
        self.persons = TablePersons(self.dbh)
        
        with self.assertRaises(db.Error) as cm:
            self.persons.check_cols()
        self.assertEqual(
            cm.exception.__str__(),
            "Table 'persons' is invalid. The following columns do not exist: 'size'"
        )
        
        self.remove_tables()
    
    def test_J_predefined_rows(self):
        
        self.create_tables()
        
        # -> ok
        self.persons.check_predefined_rows()
        
        # -> should raise error
        with self.assertRaises(db.Error) as cm:
            self.settings.check_predefined_rows()
        
        self.assertEqual(
            cm.exception.__str__(),
            "Table 'settings' is invalid. The following predefined rows are missing or invalid: {'value': '^(([0-9]+)\\\\.([0-9]+)\\\\.([0-9]+))$', 'key': 'version'}, {'value': '^(XYZ|ABC)$', 'key': 'typ'}, {'value': '.+', 'key': 'name'}"
        )
        
        self.settings.create_default_rows()
        self.settings.check_predefined_rows()
        
        self.remove_tables()
    

if __name__ == '__main__':
    unittest.main(verbosity=2)
