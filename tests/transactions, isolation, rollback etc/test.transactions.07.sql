1: CREATE TABLE students (name TEXT);
1: INSERT INTO students VALUES ('James');
1: BEGIN TRANSACTION;
1: INSERT INTO students VALUES ('Yaxin');
2: BEGIN TRANSACTION;
2: INSERT INTO students VALUES ('Yaxin');
