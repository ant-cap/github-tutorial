As this was developed for a class, please do not share this project.
This SQL database engine is designed to replicate the SQLite output. It is not a full replication, but it is able to support all basic and some advanced functionality.
The tests folder contains sql queries that you can executing running cli.py with the test filename.
You may also pass in --sqlite to produce the SQLite output, for comparison purposes.
example: 'py cli.py test.where.02.sql' or 'py cli.py test.where.02.sql --sqlite'
move project.py to the same directory as the cli.py
The cli.py files are specific to each test query pack, do not mix them up.
