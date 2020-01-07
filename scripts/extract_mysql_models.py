'''
Create the web2py code needed to access your mysql legacy db.

To make this work all the legacy tables you want to access need to have an "id" field.

This plugin needs:
mysql
mysqldump
installed and globally available.

Under Windows you will probably need to add the mysql executable directory to the PATH variable,
you will also need to modify mysql to mysql.exe and mysqldump to mysqldump.exe below.
Just guessing here :)

Access your tables with:
legacy_db(legacy_db.mytable.id>0).select()

If the script crashes this is might be due to that fact that the data_type_map dictionary below is incomplete.
Please complete it, improve it and continue.

Created by Falko Krause, minor modifications by Massimo Di Pierro, Ron McOuat and Marvi Benedet
'''
from __future__ import print_function
import subprocess
import re
import sys
from collections import OrderedDict

COLUMN_REGEX = re.compile(r'(?P<name>\S+)\s+(?P<type>\S+?)'
                          r'(\((?P<options>[^)]+)\))?(,| )( .*)?')

DATA_TYPE_MAP = dict(
    enum='string',
    varchar='string',
    int='integer',
    integer='integer',
    tinyint='integer',
    smallint='integer',
    mediumint='integer',
    bigint='integer',
    float='double',
    double='double',
    char='string',
    decimal='integer',
    date='date',
    #year = 'date',
    time='time',
    timestamp='datetime',
    datetime='datetime',
    binary='blob',
    blob='blob',
    tinyblob='blob',
    mediumblob='blob',
    longblob='blob',
    text='text',
    tinytext='text',
    mediumtext='text',
    longtext='text',
)

FKEY_REGEX = re.compile(r'FOREIGN KEY \(`(?P<name>[^`]+)`\) REFERENCES '
                        r'(`(?P<schema>[^`]+)`\.)?(`(?P<table>[^`]+)`) '
                        r'\(`(?P<column>[^`]+)`\)')


def mysql(database_name, username, password, host, port):
    p = subprocess.Popen(['/usr/bin/mysql',
                          '-u' + username,
                          '-p' + password,
                          '-h' + host,
                          '-P' + port,
                          database_name,
                          '-e' + 'show tables;'],
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    sql_showtables, stderr = p.communicate()
    tables = [re.sub(
        r'\|\s+([^\|*])\s+.*', '\1', x) for x in sql_showtables.split()[1:]]
    connection_string = "legacy_db = DAL('mysql://%s:%s@%s:%s/%s')" % (
        username, password, host, port, database_name)
    legacy_db_table_web2py_code = []
    for table_name in tables:
        # get the sql create statement
        p = subprocess.Popen(['mysqldump',
                              '-u' + username,
                              '-p' + password,
                              '-h' + host,
                              '-P' + port,
                              '--skip-triggers',
                              '--skip-add-drop-table',
                              '--no-data', database_name,
                              table_name], stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        sql_create_stmnt, stderr = p.communicate()
        if 'CREATE' in sql_create_stmnt:  # check if the table exists
            # remove garbage lines from sql statement
            sql_lines = sql_create_stmnt.split('\n')
            sql_lines = filter(
                lambda x: not(x in ('','\r') or x[:2] in ('--', '/*')),
                sql_lines)
            #generate the web2py code from the create statement
            web2py_table_code = ''
            try:
                table_name = re.search(
                    'CREATE TABLE .(\S+). \(', sql_lines[0]).group(1)
            except AttributeError:
                continue

            fields = OrderedDict()
            for line in sql_lines[1:-1]:
                line = line.strip()
                if (' ID' in line) or line.startswith(')'):
                    continue
                elif 'PRIMARY' in line:
                    continue
                elif 'UNIQUE' in line:
                    continue
                elif 'FOREIGN KEY' in line:
                    fk_match = re.search(FKEY_REGEX, line)
                    affected_field = fields[fk_match.group('name')]
                    affected_field['type'] = 'references %s' % fk_match.group('table')

                    try:
                        del affected_field['requires'], affected_field['length']
                    except KeyError:
                        pass
                elif line.startswith('KEY'):
                    continue
                else:

                    hit = re.search(COLUMN_REGEX, line)

                    if hit:
                        name = re.sub('`', '', hit.group('name'))
                        options = hit.group('options')
                        current_field = OrderedDict()
                        is_enum = False
                        current_field['type'] = hit.group('type')

                        if current_field['type'] == 'enum':
                            is_enum = True
                            current_field['requires'] =  ', requires=IS_IN_SET((%s))' % options
                        elif 'NOT NULL' in line:
                            current_field['requires'] = ', requires=IS_NOT_EMPTY()'

                        current_field['type'] = DATA_TYPE_MAP[current_field['type']]

                        if (current_field['type'] == 'string') and not is_enum:
                            try:
                                current_field['length'] = ', length=%d' % int(options)
                            except ValueError:
                                pass

                        fields[name] = current_field

            for field_name, field_def in fields.items():
                    web2py_table_code += "\n    Field('%s', '%s'%s)," % (
                        field_name, field_def.popitem(last=False)[1], ''.join(field_def.values()))

            web2py_table_code = "legacy_db.define_table('%s',%s\n    migrate=False)" % (table_name, web2py_table_code)
            legacy_db_table_web2py_code.append(web2py_table_code)
    #----------------------------------------
    #write the legacy db to file
    legacy_db_web2py_code = connection_string + "\n\n"
    legacy_db_web2py_code += "\n\n#--------\n".join(
        legacy_db_table_web2py_code)
    return legacy_db_web2py_code

regex = re.compile(r'(?P<username>.*):(?P<password>.*)@(?P<host>.*)?'
                   r'(?P<port>:\d+)/(?P<db_name>.*)')

if len(sys.argv) < 2 or not regex.match(sys.argv[1]):
    print('USAGE:\n\n    extract_mysql_models.py username:password@[host]'
          '[:port]/data_basename\n\n')
else:
    m = regex.match(sys.argv[1])
    username = m.group('username')
    password = m.group('password')
    host     = m.group('host') or 'localhost'
    port     = m.group('port')[1:] or '3309'
    db_name  = m.group('db_name')
    print(mysql(database_name=db_name, username=username, password=password,
                host=host, port=port))
