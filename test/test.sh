#!/bin/bash

PYTHON=python2

$PYTHON test_condition.py
if [ $? != 0 ]; then exit 1; fi

$PYTHON test_validate.py
if [ $? != 0 ]; then exit 1; fi

$PYTHON test_sqlite.py
if [ $? != 0 ]; then exit 1; fi

$PYTHON test_table_sqlite.py
if [ $? != 0 ]; then exit 1; fi

$PYTHON test_mysql.py MysqlTest
if [ $? != 0 ]; then exit 1; fi

$PYTHON test_table_mysql.py TableTestMysql
if [ $? != 0 ]; then exit 1; fi
