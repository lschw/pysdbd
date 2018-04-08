import sys
import unittest

sys.path.append("../")
import pysdbd as db

def quote(name):
    return '"' + name.replace('"', '""') + '"'


class ConditionTest(unittest.TestCase):

    def test_nested_conditions(self):

        cond1 = db.condition.Or(
            db.condition.And(
                db.condition.Eq("name", "value"),
                db.condition.Null("name2"),
            ),
            db.condition.And(
                db.condition.Eq("foo", "value"),
                db.condition.Eq("bar", "value2"),
            ),
            db.condition.Re("baz", "(.*)[abc|def]"),
            db.condition.Like("blub", ["blabla", "bloblo"]),
            db.condition.NotEq("haha", None),
            db.condition.Gt("number1", 44),
            db.condition.Le("number2", -3),
            db.condition.Leq("keyXY", "jaja"),
            db.condition.Geq("number3", 33.2),
            db.condition.Geq("abc", [14,3,11]),
        )

        res_serialize = ' WHERE (("name" = ? AND "name2" IS NULL) OR ("foo" = ? AND "bar" = ?) OR "baz" REGEXP ? OR "blub" LIKE ? OR "haha" != ? OR "number1" > ? OR "number2" < ? OR "keyXY" <= ? OR "number3" >= ? OR "abc" >= ?)'
        res_cols = ['name', 'name2', 'foo', 'bar', 'baz', 'blub', 'haha',
            'number1', 'number2', 'keyXY', 'number3', 'abc']
        res_params = [
            ['value', 'value', 'value2', '(.*)[abc|def]', 'blabla', None, 44, -3, 'jaja', 33.2, 14],
            ['value', 'value', 'value2', '(.*)[abc|def]', 'bloblo', None, 44, -3, 'jaja', 33.2, 3],
            ['value', 'value', 'value2', '(.*)[abc|def]', 'bloblo', None, 44, -3, 'jaja', 33.2, 11]
        ]

        self.assertEqual(cond1.serialize(quote=quote), res_serialize)
        self.assertEqual(cond1.cols(), res_cols)
        self.assertEqual(cond1.params(), res_params)


if __name__ == '__main__':
    unittest.main(verbosity=2)
