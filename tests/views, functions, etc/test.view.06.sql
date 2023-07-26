1: CREATE TABLE grades (id TEXT, grade REAL);
1: CREATE TABLE students (name TEXT, student_id TEXT);
1: INSERT INTO grades VALUES ('speaker', 2.4), ('thinker', 3.5), ('reader', 3.7), ('listener', 4.0);
1: INSERT INTO students VALUES ('James', 'speaker'), ('Yaxin', 'reader'), ('Li', 'thinker'), ('Charles', 'listener');
1: CREATE VIEW stu_view AS SELECT * FROM students LEFT OUTER JOIN grades ON students.student_id = grades.id ORDER BY students.name;
1: SELECT * FROM stu_view ORDER BY name;
1: INSERT INTO students VALUES ('Anna', 'walker'), ('Jake', 'jumper');
1: INSERT INTO grades VALUES ('walker', 4.5), ('jumper', 2.0);
1: SELECT * FROM stu_view ORDER BY grade;
