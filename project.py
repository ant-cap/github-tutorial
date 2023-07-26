# Written by Antonio Capozzoli

_ALL_DATABASES = {}
_LOCKS = {}
# locks map:
# key   : filename
# value : {0:0, 1:0, 2:0, 3:0}

import string, copy
from operator import itemgetter
import xml.etree.ElementTree as et

class Connection(object):
    def __init__(self, filename):
        self.__filename = filename
        if filename in _ALL_DATABASES:
            self.__db = _ALL_DATABASES[filename]
        else:
            self.__db = Database()
        
        self.__transmode = 0
        self.__lock = 0
        self.__copy = None

        self.open(filename)  # attempts to open the filename

    def db(self) -> 'Database':
        return self.__db
    
    def copy(self) -> 'Database':
        return self.__copy
    
    def filename(self):
        return self.__filename
    
    def lock(self):
        return self.__lock
    
    def save(self):
        if self.__filename in _ALL_DATABASES:
            if _ALL_DATABASES[self.__filename] == self.__db:
                return
        _ALL_DATABASES[self.__filename] = self.__db

    def load(self):
        if self.__db != _ALL_DATABASES[self.__filename]:
            self.__db = _ALL_DATABASES[self.__filename]

    def get_tmode(self):
        return self.__transmode

    def begin_transaction(self, tmode):
        # tmode map:
        #   0 : no transaction
        #   1 : deferred
        #   2 : immediate
        #   3 : exclusive

        self.__transmode = tmode
        self.__copy = self.__db.copy()

        if self.__filename not in _LOCKS:
            _LOCKS[self.__filename] = {0:0, 1:0, 2:0, 3:0}

        if tmode == 2:
            self.set_lock(2)
        elif tmode == 3:
            self.set_lock(3)
        
        _LOCKS[self.__filename][0] += 1

    def commit_transaction(self):
        if self.__lock == 3:
            self.__db = self.__copy.copy()
        self.__transmode = 0
        _LOCKS[self.__filename][self.__lock] -= 1
        self.__lock = 0

    def rollback_transaction(self):
        self.__transmode = 0
        _LOCKS[self.__filename][self.__lock] -= 1
        self.__lock = 0

    def set_lock(self, lock):
        #print(f"currlock: {self.__lock}\nsetlock:  {lock}\n_LOCKS: {_LOCKS}")
        if self.__lock >= lock: # setting lock to 0 is done in commit_transaction
            return
        _LOCKS[self.__filename][self.__lock] -= 1
        if lock == 1:  # shared
            if _LOCKS[self.__filename][3]:
                raise Exception("unable to obtain a shared lock")
            _LOCKS[self.__filename][1] += 1
            self.__lock = 1
        elif lock == 2:  # reserved
            if _LOCKS[self.__filename][2] or _LOCKS[self.__filename][3]:
                raise Exception("unable to obtain a reserved lock")
            _LOCKS[self.__filename][2] += 1
            self.__lock = 2
        elif lock == 3:  # exclusive
            if self.__lock != 1:
                if _LOCKS[self.__filename][1] or _LOCKS[self.__filename][2] or _LOCKS[self.__filename][3]:
                    raise Exception("unable to obtain an exclusive lock")
                _LOCKS[self.__filename][3] += 1
                self.__lock = 3
            else:
                _LOCKS[self.__filename][self.__lock] += 1



    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """

        def cond_met(cond, entry):
            """
            'WHERE' helper function.

            given a condition, returns true if value meets condition.
            returns false if condition is not met.

            structure of cond:
                [column_index, operator, test_value]
            """
            i, op, test_val = cond

            if not entry[i] and test_val:
                return False

            if op == '=':
                return True if entry[i] == test_val else False
            if op == '!=':
                return True if entry[i] != test_val else False
            if op == '>':
                return True if entry[i] >  test_val else False
            if op == '<':
                return True if entry[i] <  test_val else False
            return False

        def where(data, cond):
            """
            'WHERE' helper function. Takes in data and returns a list of indexes
            where each index is a row that matches the condition.

            data    : the rows to test          [row1, row2, ...]
            cond    : the condition to test     [column_index, operator, test_value]
            return  : list of indexes
            """
            winds = []
            i = 0
            for row in data:
                if cond_met(cond, row):
                    winds.append(i)
                i += 1
            return winds
        
        dbname = self.filename()
        if dbname in _ALL_DATABASES:
            self.load()  # grabs updated database if another transaction updated it

        if self.get_tmode() != 0:
            db = self.copy()
            t = True
            if not db:
                raise Exception('uhhhh')
        else:
            db = self.db()
            t = False
        tokens = tokenize(statement)  # will auto-qualify columns to tables
        lock = self.lock()

        ##################################################
        ##########            BEGIN             ##########
        ##################################################
        if tokens[0] == 'BEGIN':
            if t:
                raise Exception("trying to begin a new transaction with a currently open transaction")
            if tokens[1] == 'TRANSACTION' or tokens[1] == 'DEFERRED':
                self.begin_transaction(1)
            elif tokens[1] == 'IMMEDIATE':
                self.begin_transaction(2)
            elif tokens[1] == 'EXCLUSIVE':
                self.begin_transaction(3)
            data = []
        
        ##################################################
        ##########            COMMIT            ##########
        ##################################################
        elif tokens[0] == 'COMMIT':
            if not t:
                raise Exception("trying to commit a transaction with no currently open transaction")
            self.set_lock(3)
            self.commit_transaction()
            data = []

        ##################################################
        ##########           ROLLBACK           ##########
        ##################################################
        elif tokens[0] == 'ROLLBACK':
            if not t:
                raise Exception("trying to rollback a transaction with no currently open transaction")
            self.rollback_transaction()
            data = []

        ##################################################
        ##########         CREATE VIEW          ##########
        ##################################################
        elif tokens[0] == 'CREATE' and tokens[1] == 'VIEW':
            i = statement.index('AS') + 3
            viewstatement = statement[i:]
            viewname = tokens[2]
            vtokens = tokenize(viewstatement)

            cols = []
            i = 1
            while True:
                t,c = vtokens[i].split('.')
                if c == '*':
                    cols += db.grab_table(t).grab_col_names()
                    if 'LEFT' in vtokens:
                        othername = vtokens[vtokens.index('JOIN')+1]
                        cols += db.grab_table(othername).grab_col_names()
                else:
                    cols.append(c)
                if vtokens[i+1] == ',':
                    i += 2
                    continue
                break


            db.create_view(viewname, viewstatement, cols)
                

            data = []

        ##################################################
        ##########         CREATE TABLE         ##########
        ##################################################
        elif tokens[0] == 'CREATE' and tokens[1] == 'TABLE':
            name = tokens[2 + ((int('EXISTS' in tokens)) * 3)]
            if 'EXISTS' in tokens:
                if db.grab_table(name):
                    return []

            cols = []
            types = []
            defaults = []
            i = tokens.index('(') + 1
            while True:
                default = False
                cols.append(tokens[i])
                types.append(tokens[i+1])
                if tokens[i+2] == 'DEFAULT':
                    default = True
                    defaults.append(tokens[i+3])
                else:
                    defaults.append(None)
                if tokens[(i+2) + (2*default)] == ',':
                    i += 3 + (2*default)
                    continue
                break
        
            table = Table(cols, types, defaults)
            db.create_table(name, table)
            data = []

        
        ##################################################
        ##########             DROP             ##########
        ##################################################
        elif tokens[0] == 'DROP':
            name = tokens[2 + (int('EXISTS' in tokens)*2)]
            if 'EXISTS' in tokens:
                if db.grab_table(name) == None:
                    return []
            db.remove_table(name)
            data = []

        
        ##################################################
        ##########            INSERT            ##########
        ##################################################
        elif tokens[0] == 'INSERT':
            if t:
                self.set_lock(2)

            name = tokens[2]
            table_cols = db.grab_table(name).grab_col_names()

            # INSERT can now take incomplete rows and out-of-order rows as queries
            # STEP 1: define a default row with all default values
            default_row = db.grab_table(name).grab_defaults()
            # STEP 2: if complex, cols holds a list of integers representing the data's final position in a row
            #         if not, cols holds the range of all columns (0 to end order)
            #         if inserting default values, add default row

            if tokens[3] == 'DEFAULT':
                # just add the default row
                db.grab_table(name).add_row(default_row)
            else:
                if tokens[3] == '(':
                    # complexity check
                    i = 4
                    cols = []
                    while True:
                        cols.append(table_cols.index(tokens[i]))
                        if tokens[i+1] == ',':
                            i += 2
                            continue
                        break
                else:
                    cols = [i for i in range(len(table_cols))]

                # STEP 3: When iterating through a 'row' from tokens, load in a default_row
                #         iterate with each column index in cols, set to the value in tokens
                #         final row will have everything in the correct order, and any columns not defined are default values

                i = tokens.index('VALUES') + 2
                while True:
                    row = default_row
                    for j in cols:
                        row[j] = tokens[i]
                        i += 2
                    db.grab_table(name).add_row(row)

                    # check if we have another row to add
                    if tokens[i] == ',':
                        i += 2
                        continue
                    break
            data = []

        
        ##################################################
        ##########            SELECT            ##########
        ##################################################
        elif tokens[0] == 'SELECT':
            if t:
                self.set_lock(1)
            name = tokens[tokens.index('FROM') + 1]
            view = False
            if name in db.views():
                view = True
            
            distinct = True if 'DISTINCT' in tokens else False
            maxagg = True if 'MAX' in tokens else False
            minagg = True if 'MIN' in tokens else False
            orderdir = True if 'DESC' in tokens else False
            name = tokens[tokens.index('FROM') + 1]

            # STEP 1: Get variables for our data and our columns.
            #         if JOIN, we need to implement our join to our variables

            if 'JOIN' in tokens:
                #here we go
                name2 = tokens[tokens.index('JOIN') + 1]
                t1cols = db.grab_table(name).grab_qcol_names(name)
                t1offset = len(t1cols)
                t2cols = db.grab_table(name2).grab_qcol_names(name2)
                table_cols = t1cols + t2cols
                i = tokens.index('ON') + 1
                t1c = table_cols.index(tokens[i])
                t2c = table_cols.index(tokens[i+2])

                # ok lets actually LEFT OUTER JOIN now
                # could be more efficient but dont care

                data1 = db.grab_table(name).grab_rows()
                data2 = db.grab_table(name2).grab_rows()
                initdata = []
                for ri in data1:
                    finalrow = list(ri)
                    match = False
                    for rj in data2:
                        if finalrow[t1c] == rj[t2c - t1offset]:
                            match = True
                            finalrow += list(rj)
                            break
                    if not match:
                        finalrow += [None for c in data2[0]]
                    initdata.append(tuple(finalrow))
                
            else:
                if view:
                    vstatement = db.grab_table(name).statement()
                    initdata = self.execute(vstatement)
                else:
                    initdata = db.grab_table(name).grab_rows()
                table_cols = db.grab_table(name).grab_qcol_names(name)

            # grab return columns from query
            cols = []
            i = 1 + distinct + (2 * ((maxagg) + (minagg)))
            while True:
                t,c = tokens[i].split('.')
                if c == '*':
                    cols += db.grab_table(t).grab_qcol_names(t)
                    if 'JOIN' in tokens:
                        othername = tokens[tokens.index('JOIN')+1]
                        cols += db.grab_table(othername).grab_qcol_names(othername) 
                else:
                    cols.append(tokens[i])
                if tokens[i+1] == ',':
                    i += 2
                    continue
                break

            order = []
            i = tokens.index('BY') + 1
            while True:
                order.append(tokens[i])
                if tokens[i+1] == ',':
                    i += 2
                    continue
                break

            # Return all data (where clause first), but you only show the selected columns.
            # You can order by a column you have not selected.
            # The idea: First grab all data, and sort the rows by order by.
            # If aggregate, return one row.
            # Place the sorted data in column dictionary. Pull only the columns selected.
                
            if 'WHERE' in tokens:
                i = tokens.index('WHERE')
                col = table_cols.index(tokens[tokens.index('WHERE') + 1])
                op = tokens[i+2]
                tval = tokens[i+3]
                winds = where(initdata, [col, op, tval])
                data = []
                for i in range(len(initdata)):
                    if i in winds:
                        data.append(initdata[i])
            else:
                data = initdata

            # order in reverse so the first order column is the 'primary' order
            for i in range(len(order)-1, -1, -1):
                data.sort(key=itemgetter(table_cols.index(order[i])), reverse=orderdir)

            # make a dictionary of the sorted items so we can grab only the selected columns

            cd = {}
            for i in range(len(table_cols)):
                coldata = []
                for j in range(len(data)):
                    coldata.append(data[j][i])
                cd[table_cols[i]] = coldata

            # grab only selected columns

            data2 = []
            for i in range(len(data)):
                entry = []
                for j in range(len(cols)):
                    entry.append(cd[cols[j]][i])
                data2.append(tuple(entry))

            # if aggregate, set data to aggregate
            if maxagg:
                data = [tuple(max(data2))]
            elif minagg:
                data = [tuple(min(data2))]
            else:
                # remove duplicates if DISTINCT and return
                
                if distinct:
                    data = []
                    for row in data2:
                        if row not in data:
                            data.append(row)
                else:
                    data = data2
            return data


        ##################################################
        ##########            UPDATE            ##########
        ##################################################
        elif tokens[0] == 'UPDATE':
            if t:
                self.set_lock(2)

            name = tokens[1]
            cols = db.grab_table(name).grab_col_names()

            if 'WHERE' not in tokens:
                winds = [i for i in range(len(cols))]
            else:
                i = tokens.index('WHERE')
                qcol = tokens[tokens.index('WHERE') + 1]
                qcol = qcol[qcol.index('.')+1:]
                col = cols.index(qcol)
                op = tokens[i+2]
                tval = tokens[i+3]
                data = db.grab_table(name).grab_rows()
                winds = where(data, [col, op, tval])
            
            i = tokens.index('SET') + 1
            sets = []
            while True:
                col = cols.index(tokens[i])
                val = tokens[i+2]
                sets.append([col, val])
                if tokens[i+3] == ',':
                    i += 4
                    continue
                break

            db.grab_table(name).update(sets, winds)
            data = []


        ##################################################
        ##########            DELETE            ##########
        ##################################################
        if tokens[0] == 'DELETE':
            if t:
                self.set_lock(2)
            
            name = tokens[tokens.index('FROM') + 1]
            cols = db.grab_table(name).grab_col_names()
            if 'WHERE' not in tokens:
                db.grab_table(name).clear()
            else:
                i = tokens.index('WHERE')
                qcol = tokens[tokens.index('WHERE') + 1]
                qcol = qcol[qcol.index('.')+1:]
                col = cols.index(qcol)
                op = tokens[i+2]
                tval = tokens[i+3]
                data = db.grab_table(name).grab_rows()
                winds = where(data, [col, op, tval])
                db.grab_table(name).delete(winds)
            data = []

        self.save()
        return data
    
    def executemany(self, statement, wildcards):
        '''
        Execute an SQL statement with parameterized queries
        statement:   Sql statement with wildcard placeholders
        wildcards:   list of tuples to be inserted as wildcards
        '''
        initstatement = statement
        for row in wildcards:
            statement = initstatement
            for i in range(len(row)):
                value = row[i]
                if isinstance(value, str):
                    value = "'" + value + "'"
                else:
                    value = str(value)
                indx = statement.index('?')

                statement = statement[:indx] + value + statement[indx+1:]

            self.execute(statement)

    def close(self):
        """
        Closes the database and writes it to an XML-style .db file
        """
        filename = self.filename()

        '''
        Structure of the XML file
        <filename>
            <table1>
                <columnquery>(name TEXT, ...)</columnquery>
                <rowquery>('Ant', ...)</rowquery>
            </table1>
            <table2>
                ...
            ...
        </filename>
        '''
        database = et.Element(filename.strip())
        
        db = self.db()
        for table_name in db.tables():
            table = et.SubElement(database, table_name)
            
            # construct query to create table with columns
            coldata = db.grab_table(table_name).grab_cols()
            colquery = "("
            for i in range(len(coldata)): # 0: colname  1: types  2: defaults
                colquery += coldata[i][0] + " " + coldata[i][1] + ", "
            colquery = colquery[:-2] + ")"  # remove ,
            columns = et.SubElement(table, 'columnquery')
            columns.text = colquery

            # construct query to insert data
            rowdata = db.grab_table(table_name).grab_rows()
            rowquery = "("
            for i in range(len(rowdata)):
                for j in range(len(coldata)):
                    if not rowdata[i][j]:
                        rowquery += "NULL, "
                    elif coldata[j][1] == 'TEXT':
                        rowquery += "'" + rowdata[i][j] + "', "
                    else:
                        rowquery += str(rowdata[i][j]) + ", "
                rowquery = rowquery[:-2] + "), ("
            rowquery = rowquery[:-3]
            rows = et.SubElement(table, 'rowquery')
            rows.text = rowquery
        
        # write the database to file w/ given filename
        fp = open(filename, 'wb')
        fp.write(et.tostring(database))
        fp.close()

    def open(self, filename):
        """
        Opens a database file and runs the queries needed to replicate it
        """
        try:
            database = et.parse(filename)
        except:
            return
        
        database = database.getroot()
        for table in database:
            colquery = table[0].text
            rowquery = table[1].text

            tablequery = f"CREATE TABLE {table.tag} {colquery};"
            self.execute(tablequery)
            if rowquery:
                dataquery = f"INSERT INTO {table.tag} VALUES {rowquery};"
                self.execute(dataquery)





##################################################
##################################################
##########                              ##########
##########       DATABASE CLASSES       ##########
##########                              ##########
##################################################
##################################################

class Database(object):
    # can have multiple tables
    # will have views in the future
    # tables will have unique names
    
    def __init__(self):
        self.__tables = {}  # key = table name, value = table class
        self.__views = {}   # key = view name, value = view class

    def tables(self):
        return self.__tables

    def create_table(self, name: str, table: 'Table'):
        if name in self.__tables:
            raise Exception(f"tried to create table {name} but it already exists")
            return
        self.__tables[name] = table

    def remove_table(self, name: str):
        if name not in self.__tables:
            raise Exception(f"tried to delete table {name} but it does not exist")
            return
        self.__tables.pop(name)

    def grab_table(self, name: str) -> 'Table':
        if name in self.__tables:
            return self.__tables[name]
        if name in self.__views:
            return self.__views[name]
        return None
    
    def copy(self):
        # returns a database object w the same data
        dbcopy = Database()
        for table in self.__tables:
            ccols = []
            ctypes = []
            cdefs = []
            for col in self.__tables[table].grab_col_all():
                ccols.append(col[0])
                ctypes.append(col[1])
                cdefs.append(col[2])
            tablecopy = Table(ccols, ctypes, cdefs)
            dbcopy.create_table(table, tablecopy)
            for row in self.__tables[table].grab_rows():
                dbcopy.grab_table(table).add_row(list(row))
        return dbcopy
    
    def views(self):
        return self.__views.keys()
    
    def create_view(self, name, statement, cols):
        self.__views[name] = View(statement, cols)

class View(object):
    def __init__(self, statement, cols):
        self.__statement = statement
        self.__cols = cols
    
    def statement(self):
        return self.__statement
    
    def grab_qcol_names(self, name):
        # here to mimic Table Class (maybe pointless lol)
        cols = []
        for col in self.__cols:
            cols.append(name + '.' + col)
        return cols
    



class Table(object):
    # columns, with specified type and default values
    # rows
    def __init__(self, cols: list, types: list, defaults: list):
        assert len(cols) == len(types) == len(defaults)

        self.__columns = []
        for i in range(len(cols)):
            column = tuple([cols[i], types[i], defaults[i]])
            self.__columns.append(column)

        self.__rows = []  # row objects
    
    def grab_cols(self):
        return self.__columns
    
    def grab_rows(self):
        data = []
        rows = self.__rows
        for row in rows:
            data.append(row.grab_data())
        return data
    
    def grab_col_all(self):
        cols = []
        for c in self.__columns:
            cols.append(copy.deepcopy(c))
        return cols
    
    def grab_col_names(self):
        cols = []
        for c in self.__columns:
            cols.append(c[0])
        return cols
    
    def grab_defaults(self):
        defaults = []
        for c in self.__columns:
            defaults.append(c[2])
        return defaults
    
    def grab_qcol_names(self, name):
        """
        essentially the same as grab_col_names, just adds the table qualification to it.
        name : the table name (not an object property)
        """
        cols = []
        for c in self.__columns:
            cols.append(name + '.' + c[0])
        return cols
    
    def add_row(self, data: list):
        cols = self.grab_cols()
        assert len(cols) == len(data)
        for i in range(len(cols)):
            if self.verify_type(data[i], cols[i][1]):
                continue
            else:
                #print("data does not meet type specifications:\n", data)
                return
        #print("add success")
        self.__rows.append(Row(data))

    def verify_type(self, value, type):
        if value == None:
            return True
        elif type == "INTEGER":
            return isinstance(value, int)
        elif type == "REAL":
            return isinstance(value, float)
        elif type == "TEXT":
            return isinstance(value, str)
        else:
            #print("what the dog doin")
            return False
        
    def clear(self):
        self.__rows.clear()

    def update(self, sets, inds):  # inds from where()
        for i in range(len(self.__rows)):
            if i in inds:
                data = list(self.__rows[i].grab_data())
                for s in sets:
                    data[s[0]] = s[1]
                self.__rows[i].update_data(data)

    def delete(self, inds):  # inds from where()
        newrows = []
        for i in range(len(self.__rows)):
            data = self.__rows[i].grab_data()
            if i not in inds:
                newrows.append(Row(data))
        self.__rows = newrows

class Row(object):
    def __init__(self, data):
        self.__data = tuple(data)

    def grab_data(self):
        return copy.deepcopy(self.__data)
    
    def update_data(self, data):
        self.__data = tuple(data)

##################################################
##################################################
##########                              ##########
##########           TOKENIZE           ##########
##########                              ##########
##################################################
##################################################

def collect_characters(query, allowed_characters):
    letters = []
    for letter in query:
        if letter not in allowed_characters:
            break
        letters.append(letter)
    return "".join(letters)


def remove_leading_whitespace(query, tokens):
    whitespace = collect_characters(query, string.whitespace)
    return query[len(whitespace):]


def remove_word(query, tokens):
    word = collect_characters(query,
                              string.ascii_letters + "_" + "." + "*" + string.digits)
    if word == "NULL":
        tokens.append(None)
    else:
        tokens.append(word)
    return query[len(word):]


def remove_text(query, tokens):
    assert query[0] == "'"
    query = query[1:]
    i = 0
    text = ''
    while True:  # ex query: Li''s dog''s stuffed animal',
        if query[i] == "'":
            if query[i+1] == "'":
                text += "'"
                i += 2
                continue
            # end of text
            i += 1
            break
        text += query[i]
        i += 1
    tokens.append(text)
    return query[i:]


def remove_number(query, tokens):
    assert query[0] in string.digits or query[0] == '-'
    text = ""
    while True:
        if query[0] in string.digits or query[0] in '-.':
            text += query[0]
        else:
            break
        query = query[1:]
    if '.' in text:
        tokens.append(float(text))
    else:
        tokens.append(int(text))
    return query


def tokenize(query):
    tokens = []
    while query:
        #print("Query:{}".format(query))
        #print("Tokens: ", tokens)
        old_query = query

        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query, tokens)
            continue

        if query[0] in (string.ascii_letters + "_"):
            
            # convert some keywords for simplicity
            if query[0:6] == 'IS NOT':
                tokens.append('!=')
                query = query[6:]
                continue
            if query[0:2] == 'IS':
                tokens.append('=')
                query = query[2:]
                continue
            if query[0:4] == 'NULL':
                tokens.append(None)
                query = query[4:]
                continue

            query = remove_word(query, tokens)
            continue

        if query[0] in "(),;*><=":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] in '!' and query[1] in '=':
            tokens.append(query[:2])
            query = query[2:]
            continue

        if query[0] == "'":
            query = remove_text(query, tokens)
            continue

        if (query[0] in string.digits) or (query[0] == '-' and query[1] in string.digits):
            query = remove_number(query, tokens)



        if len(query) == len(old_query):
            print("ERRTOK:", tokens)
            raise AssertionError(f"Query didn't get shorter.")

    # add qualifications (helps lower complexity on execute)
    if 'FROM' in tokens or 'UPDATE' in tokens:
        if 'UPDATE' in tokens:
            master = tokens[1]
        else:
            master = tokens[tokens.index('FROM') + 1]
        if 'SELECT' == tokens[0]:
            i = 1 + ('DISTINCT' in tokens) + (2 * (('MAX' in tokens) + ('MIN' in tokens)))
            while True:
                v = tokens[i]
                if '.' not in v:
                    tokens[i] = master + '.' + v
                if tokens[i+1] == ',':
                    i += 2
                    continue
                break
            i = tokens.index('BY') + 1
            while True:
                v = tokens[i]
                if '.' not in v:
                    tokens[i] = master + '.' + v
                if tokens[i+1] == ',':
                    i += 2
                    continue
                break
        if 'WHERE' in tokens:
            v = tokens[tokens.index('WHERE') + 1]
            if '.' not in v:
                tokens[tokens.index('WHERE') + 1] = master + '.' + v
    #print("Tokens:", tokens)
    return tokens

def connect(filename, timeout = 0.1, isolation_level = None):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)