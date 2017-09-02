import sys
import unittest
import datetime

sys.path.append("../")
import pysdbd as db

def _v_foobar(col, value, errors):
    if value != "foobar":
        errors[col] = "INVALID_FOOBAR"
    return value

# register user defined validation method
db.validate._v_foobar = _v_foobar


class ValidateTest(unittest.TestCase):
    
    def test_notimplemented(self):
        col = "col"
        value = "abc"
        fmt = ["invalid_format"]
        errors = {}
        
        with self.assertRaises(NotImplementedError):
            db.validate.validate(col, "abc", fmt, errors)
        pass
    
    def test_formats(self):
        col = "col"
        
        test_cases = [
            [["int"], 33, "33", {}], 
            [["int"], -200, "-200", {}], 
            [["int"], "22", "22", {}], 
            [["int"], "-881", "-881", {}], 
            [["int"], 4.1, "4.1", {col: "INVALID_INT"}],
            [["int"], "foobar", "foobar", {col: "INVALID_INT"}],
            
            [["uint"], 33, "33", {}], 
            [["uint"], -200, "-200", {col: "INVALID_UINT"}], 
            [["uint"], "22", "22", {}], 
            [["uint"], "-881", "-881", {col: "INVALID_UINT"}], 
            [["uint"], 4.1, "4.1", {col: "INVALID_UINT"}],
            [["uint"], "foobar", "foobar", {col: "INVALID_UINT"}],
            
            [["float"], 33, "33", {}], 
            [["float"], .23, "0.23", {}], 
            [["float"], -.11, "-0.11", {}], 
            [["float"], -0.11, "-0.11", {}], 
            [["float"], -0., "-0.0", {}], 
            [["float"], "4,88", "4.88", {}], 
            [["float"], "4..88", "4..88", {col: "INVALID_FLOAT"}], 
            [["float"], "foobar", "foobar", {col: "INVALID_FLOAT"}],
            
            [["ufloat"], 33, "33", {}], 
            [["ufloat"], .23, "0.23", {}], 
            [["ufloat"], -.11, "-0.11", {col: "INVALID_UFLOAT"}], 
            [["ufloat"], -0.11, "-0.11", {col: "INVALID_UFLOAT"}], 
            [["ufloat"], -0., "-0.0", {col: "INVALID_UFLOAT"}], 
            [["ufloat"], "4,88", "4.88", {}], 
            [["ufloat"], "4..88", "4..88", {col: "INVALID_UFLOAT"}], 
            [["ufloat"], "foobar", "foobar", {col: "INVALID_UFLOAT"}],
            
            [["date"], "1999-01-01", "1999-01-01", {}], 
            [["date"], "1999", "1999", {col:"INVALID_DATE"}], 
            
            [["text100"], "abcdefg", "abcdefg", {}], 
            [["text100"], "a"*100, "a"*100, {}], 
            [["text100"], "a"*101, "a"*101, {col:"INVALID_TEXT100"}], 
            
            [["bool"], True, "1", {}], 
            [["bool"], False, "0", {}], 
            [["bool"], "trUE", "1", {}], 
            [["bool"], "false", "0", {}], 
            [["bool"], 0, "0", {}], 
            [["bool"], 1, "1", {}], 
            [["bool"], 22, "22", {col:"INVALID_BOOL"}], 
            [["bool"], "abc", "abc", {col:"INVALID_BOOL"}], 
            
            [["r_[xyz]+55"], "xy55", "xy55", {}], 
            [["r_[xyz]+55"], "a55", "a55", {col:"INVALID_REGEX"}], 
            
            [["datetime"], "1999-01-01 12:33:05", "1999-01-01 12:33:05", {}], 
            [
                ["datetime"],
                datetime.datetime.strptime(
                    "1999-01-01 12:33:05",
                    "%Y-%m-%d %H:%M:%S"
                ),
                "1999-01-01 12:33:05",
                {}
            ],
            [
                ["datetime"],
                "1999-14-01 12:33:05",
                "1999-14-01 12:33:05",
                {col:"INVALID_DATETIME"}
            ],
            
            [[""], None, None, {}], 
            [[""], "", "", {}], 
            
            [["not_null"], None, None, {col: "NONE_FIELD"}], 
            
            [["not_empty"], "", "", {col: "EMPTY_FIELD"}], 
            
            [["foobar"], "foobar", "foobar", {}], 
            [["foobar"], "barfoo", "barfoo", {col: "INVALID_FOOBAR"}], 
        ]
        
        for test_case in test_cases:
            errors = {}
            result = db.validate.validate(
                col, test_case[1], test_case[0], errors
            )
            self.assertEqual(result, test_case[2])
            self.assertEqual(errors, test_case[3])

    
if __name__ == '__main__':
    unittest.main(verbosity=2)
