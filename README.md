# sql-database-engine
Built from the ground up in Python, this engine is designed to replicate SQLite library output.

![image](https://github.com/ant-cap/sql-database-engine/assets/61441638/77421487-50d8-4388-97ed-8e6cf9c50873)


# Functionality
- Essential query support (create, insert, select, order by, where, delete, default, update, etc.)
- Joins
- Persistence
- Transactions (isolation, rollback, etc.)
- Views
- Parameters
- Functions
- Aggregates

# How to use
The tests folder contains sql queries that you can execute by running cli.py with the test filename.

You may also pass in --sqlite to produce the SQLite output for comparison purposes.

Examples:

> py cli.py test.where.02.sql

> py cli.py test.where.02.sql --sqlite


project.py must be in the same directory as cli.py.

The cli.py files are specific to each test query pack, so do not mix them up.
