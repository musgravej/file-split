import re 
import sqlite3
import os
import csv
import time
import sys

"""
This version doesn't show field names, only asks for field number 
"""


class GlobalVar(object):

    def __init__(self):
        self.del_type = str()
        self.split_field_n = str()
        self.saveDir = os.curdir + os.path.join(os.curdir, "\\Processed")
        self.searchType = str()
        self.action = str()
        self.query_head = str()
        self.only_reports = str()
        self.original_header = list()

    def query_header(self):
        # If the first line is a header record, written files will have the same header
        self.query_head = int(input("First line as header record? (1/yes, 0/no): "))

    def query_action(self):
        # Set action ('action'), 0 to create 1 file, with 1 sample from each group
        # 1 outputs a new file for each group
        action = int(input("Sample File or File Break? (0 for sample, 1 for break): "))
        if action not in [0, 1]:
            print("Error: must be 0 or 1")
            time.sleep(6)
            sys.exit()
        else:
            self.action = action

    def query_del_type(self):
        # Set delimiter type ('del_type'), tab or comma are the only options
        del_type = int(input("Enter delimiter type (0 for tab, 1 for comma): "))
        if del_type not in [0, 1]:
            print("Error: must be 0 or 1")
            time.sleep(6)
            return
        else:
            self.del_type = (',' if del_type == 1 else '\t')

    def query_only_reports(self):
        ans = int(input("Only output reports (no files)? (0/no, 1/yes): "))
        if ans not in [0, 1]:
            print("Error: must be 0 or 1")
            time.sleep(3)
            sys.exit()
        else:
            self.only_reports = ans

    def query_split_field(self):
        # Field position that the group selections will be based on
        self.split_field_n = int(input("Enter field number to split by: "))

    def query_search_type(self):
        # Type of file to search, written files will  be the same format
        self.searchType = input("Enter file type (ex: txt, csv, tab): ")

    def set_header(self, head):
        self.original_header = head

    def get_query_fields(self, header):
        # print(self.original_header)
        query_string = ""
        for n, fld in enumerate(header, 1):
            query_string += "{:<10}({}): {}\n".format("", n, fld)

        print("Enter field number to split by (ex: 3): ")
        print(query_string)
        self.split_field_n = int(input(""))

    def ask_questions(self):
        self.query_header()
        self.query_action()
        if self.action:
            self.query_only_reports()
        self.query_del_type()
        self.query_split_field()
        self.query_search_type()
        # Create a new directory for resulting files
        if not os.path.exists(self.saveDir):
            os.makedirs(self.saveDir)


def new_dbfields(fields):
    """
    create sql statement string for new database fields
    """

    sql_stmt = ' varchar (100), '.join(fields[:-1])
    sql_stmt = (sql_stmt +
                str(' varchar (100), {} varchar (100)'.format(fields[-1])))
    return sql_stmt


def import_records(file_to_import, headers):
    """
    Import records from [import_file]
    Creates new db file, split.db
    Use list [headers] to create new database
    """
    db = sqlite3.connect('split.db')
    db.execute('DROP TABLE IF EXISTS records;')
    db.execute('CREATE TABLE records ({0});'.format(new_dbfields(headers)))

    print('Importing {}'.format(file_to_import))

    with open(file_to_import, 'r') as f:
        read = csv.reader(f, delimiter=g.del_type)
        for n, row in enumerate(read, 1):

            inserts = '","'.join(["%s"] * len(row))
            fields = tuple(row)

            query = ('INSERT INTO records VALUES ("' + inserts + '");') % (fields)

            db.execute(query)

    if g.query_head:
        db.execute("DELETE FROM records WHERE rowid = 1;")

    db.commit()
    db.close()


def clean_header(field):
    """
    Removes sql offending characters from the header record
    Turns empty fields into 'EMPTY'
    """
    empty_cnt = 1
    expr = re.compile('[^A-Za-z0-9]')
    lead_num = re.compile('[0-9]')

    g.set_header(field)

    # Remove any characters that aren't US alphabet letters, numbers
    # (removes spaces)
    field = [re.sub(expr, '', elem) for elem in field]
    # Field names can't start with a number, removes that too
    field = [elem[1:] if lead_num.match(elem) else elem for elem in field]

    clean_field = []
    # Field names can't be empty either, creates a new field name 'EMPTY'
    # each empty field is numbered to avoid duplicates
    for elem in field:
        if elem == '':
            clean_field.append("EMPTY{}".format(empty_cnt))
            empty_cnt += 1
        else:
            clean_field.append(elem)

    return clean_field


def clean_filename(filename):
    """
    Removes filename offending characters from the 
    filename string being written
    """
    expr = re.compile('[^A-Za-z0-9-_\s]')
    filename = re.sub(expr, '', filename)
    return filename


def get_header_csv(imported_file):
    """
    Creates a string to be used for database field names
    If errors, or header contains duplicates, returns False
    """
    try:
        with open(imported_file, 'r') as o:
            csvr = csv.reader(o, delimiter=g.del_type)
            fields = next(csvr)

        if g.query_head:
            fields = clean_header(fields)
        else:
            return ['Fld' + str(x) for x in range(1, len(fields) + 1)]

        if len(fields) == len(set(fields)):
            return fields
        else:
            print("Header file:")
            for field in fields:
                print(field)
            print("Error: Header file contains duplicate field names.")
            time.sleep(6)
            return False

    except Exception as e:
        print(e)
        print("File Not Found")
        time.sleep(3)
        return False


def export_file(proc_file):
    """
    Exports records according to action type:
    1: Break into separate files, one for each group
    0: Creates one file with one record per group
    """
    if g.only_reports:
        export_report(proc_file)
    else:
        if g.action == 1:
            export_break(proc_file)
            export_report(proc_file)
        if g.action == 0:
            export_samples(proc_file)
            export_report(proc_file)


def export_report(proc_file):
    """
    writes a convenient report with counts for each group
    """
    field_select, header_array = header_info()

    db = sqlite3.connect('split.db')

    # Get the longest field name, save as max_len
    max_len = db.execute(('SELECT MAX(LENGTH([{field}])) '
                          'FROM records;').format(field=field_select))
    max_len = int(list(max_len)[0][0])

    rpt = db.execute(('SELECT {field}, count() AS Total '
                      'FROM records '
                      'GROUP BY {field} ORDER BY {field};').format(field=field_select))

    write_rep = os.path.join(os.curdir, g.saveDir,
                             proc_file[:-(len(g.searchType) + 1)] +
                             '_CountReport.txt')

    with open(write_rep, 'w+') as r:
        r.write("{g:<{m}}{s}{t}\n".format(m=max_len, g="Group", s=(" " * 5), t="Total"))
        r.write("{g:<{m}}{s}{t}\n".format(m=max_len, 
                                          g=(("-" * max_len) + "-" * 4), 
                                          s=" ", t=("-" * 10)))
        for line in rpt:
            r.write("{g:<{m}}{s}{t:,}\n".format(m=max_len, g=line[0], s=(" " * 5), t=line[1]))

    db.close()


def export_break(proc_file):
    """
    Uses a loop to write one file for each group
    """
    field_select, header_array = header_info()

    db = sqlite3.connect('split.db')
    qry_groups = db.execute(('SELECT {field} '
                             'FROM records '
                             'GROUP BY {field};').format(field=field_select))

    for n, group in enumerate(qry_groups):

        write_rec = os.path.join(os.curdir, g.saveDir,
                                 proc_file[:-(len(g.searchType) + 1)] +
                                 ('_' + clean_filename(group[0])) +
                                 ('.' + g.searchType))

        group_rec = db.execute(("SELECT * FROM records "
                                "WHERE {field} = '{group}' "
                                "ORDER BY ROWID;").format(field=field_select, 
                                                          group=str(group[0]).replace("'", "''")))
        
        print('Writing {}'.format(proc_file[:-(len(g.searchType) + 1)] +
                                  ('_' + group[0]) +
                                  ('.' + g.searchType)))

        with open(write_rec, 'w+', newline='') as s:
            csvw = csv.writer(s, delimiter=g.del_type, quoting=csv.QUOTE_ALL)
            if g.query_head:
                csvw.writerow(g.original_header)
            for rec in group_rec:
                csvw.writerow(rec)

    db.close()


def export_samples(proc_file):
    """
    Exports one sample file, one record per group
    """
    field_select, header_array = header_info()

    db = sqlite3.connect('split.db')

    write_rec = os.path.join(os.curdir, g.saveDir,
                             proc_file[:-(len(g.searchType) + 1)] +
                             ('_BREAK.' + g.searchType))

    group_rec = db.execute(("SELECT * FROM records "
                            "GROUP BY {field} "
                            "HAVING MIN(ROWID);").format(field=field_select))
    
    with open(write_rec, 'w+', newline='') as s:
            csvw = csv.writer(s, delimiter=g.del_type, quoting=csv.QUOTE_ALL)
            if g.query_head:
                csvw.writerow(g.original_header)
            for rec in group_rec:
                csvw.writerow(rec)

    db.close()


def header_info():
    """
    returns a list with the header info
    [0]: The name of the field to break by (as defined in split_field_n)
    [1]: The schema of the split.db database.  Can be used to 
         get field header names
    """
    db = sqlite3.connect('split.db')
    qry = db.execute("PRAGMA TABLE_INFO (records);")

    split_field_name = {}

    for n, elem in enumerate([a for a in qry], 1):
        split_field_name[n] = elem

    db.close()

    try:
        return [split_field_name[g.split_field_n][1], split_field_name.values()]
    except KeyError:
        print("Ooops, something went wrong, check field break index")
        time.sleep(6)
        db.close()
        sys.exit()


def import_file(file_import):
    """
    Imports records from [file_import] after getting the 
    headers.  If get_header function fails, does not import
    If import attempt is made, returns True
    """
    headers = get_header_csv(file_import)

    if headers:
        import_records(file_import, headers)
        return True


def test_main():
    """
    Function for testing purposes
    """
    # test_global_var()
    global g
    g = GlobalVarTest()
    g.ask_questions()

    dir_list = os.listdir(os.curdir)
    for proc_file in dir_list:
        if proc_file[-(len(g.searchType)):] == g.searchType:
            print("Processing: " + proc_file)
            if import_file(proc_file):
                export_file(proc_file)
            if os.path.isfile(os.path.join(os.curdir, 'split.db')):
                os.remove(os.path.join(os.curdir, 'split.db'))


def main():
    """
    Asks questions in set_global_var() function
    Runs processing on all files in current directory of specified file type.
    Imports records and exports results
    Deletes split.db database when done
    """
    global g
    g = GlobalVar()
    g.ask_questions()

    dir_list = os.listdir(os.curdir)

    for proc_file in dir_list:
        if proc_file[-(len(g.searchType)):] == g.searchType:
            print("Processing: " + proc_file)

            if import_file(proc_file):
                export_file(proc_file)

    if os.path.isfile(os.path.join(os.curdir, 'split.db')):
        os.remove(os.path.join(os.curdir, 'split.db'))


if __name__ == '__main__':
    main()
    sys.exit()
