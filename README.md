# pysdbd
database abstraction API for python with sqlite and mysql backend

## Installation
Download the latest release from [/releases/latest](https://github.com/lschw/pysdbd/releases/latest).

Either copy the *pysdbd/* directory to your desired location or install via

    cd /path/to/extracted/files
    pip install .


## Usage
The general usage is as following. Derive a class from pysdbd.Table and define the name and columns of the table. Create a database driver object (either pysdbd.SqliteDriver or pysdbd.MysqlDriver) and instantiate an object of the table class with the chosen driver. Operations on the table's data can be performed with the Table.get(), Table.create(), Table.update(), ... methods.

See the following code for a small example.


### Example

    import pysdbd as db

    class TablePersons(db.Table):

        name = "persons"
        columns = {
            "name": ["not_empty"],
            "birthday": ["date", "not_empty"],
            "size": ["float"],
        }

    dbh = db.SqliteDriver("test.db", True)
    persons = TablePersons(dbh, True)

    dbh.start_transaction()
    persons.create(
        [
            {"name": "Peter", "size": 10.1, "birthday": "1966-07-02"},
            {"name": "John", "size": 16.3, "birthday": "1999-03-22"},
            {"name": "David", "size": 13.2, "birthday": "2010-02-14"}
        ]
    )
    dbh.commit()

    dbh.start_transaction()
    persons.update(
        [
            {"size": 8.2},
            {"size": 10.5},
        ],
        where=db.condition.Eq("id", [1,2])
    )
    dbh.commit()

    print(persons.get())

    print(
        persons.get(
            where=db.condition.And(
                db.condition.Gt("size", 11),
                db.condition.Re("name", ".*vid")
            ),
            cols=["name", "size"],
            ret="rows"
        )
    )

    persons.delete(
        where=db.condition.Eq("name", "Peter")
    )
