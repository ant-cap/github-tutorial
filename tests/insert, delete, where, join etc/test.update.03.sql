CREATE TABLE student (name TEXT, grade REAL, piazza INTEGER);
INSERT INTO student VALUES ('James', 4.0, 1);
INSERT INTO student VALUES ('Yaxin', 4.0, 2);
INSERT INTO student VALUES ('Li', 3.2, 2);
UPDATE student SET grade = 3.0, piazza = 3 WHERE name = 'Yaxin';
SELECT * FROM student ORDER BY name;
