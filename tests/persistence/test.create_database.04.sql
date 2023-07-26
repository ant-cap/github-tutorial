FILENAME: test4.db
1: CREATE TABLE student (name TEXT, grade REAL, piazza INTEGER);
1: INSERT INTO student VALUES ('James', 4.0, 1), ('Yaxin', 4.0, 2);
1: INSERT INTO student VALUES ('Li', 3.2, 2);
1: SELECT * FROM student ORDER BY piazza, grade;
1: CLOSE