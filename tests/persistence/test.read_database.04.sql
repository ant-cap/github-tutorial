OPEN: test4.db
1: CREATE TABLE student2 (name TEXT, grade REAL, piazza INTEGER);
1: INSERT INTO student2 VALUES ('James2', 4.0, 1), ('Yaxin2', 4.0, 2);
1: INSERT INTO student2 VALUES ('Li2', 3.2, 2);
1: SELECT * FROM student2 ORDER BY piazza, grade;
1: INSERT INTO student VALUES ('Li2', 3.2, 4);
1: SELECT * FROM student ORDER BY piazza, grade;
1: ENDTEST