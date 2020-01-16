#!/usr/bin/env /usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "Verizhnikov Konstantin"
__email__ = "kverizhnikov@beget.ru"

"""
This "program" generates mysql dump from source binary data
which represented as set of .frm and .ibd files.

It takes two positional arguments and one optional one:

   * path is the path to source directory with files
   * db is the name of database which will be dumped
   * --charset is the charset of that database and dump

Whole data is stored in tmp dir defined in TMP_DIR

ATTENTION
The module requires MySQLdb module and mysql-utilities
"""

import os
import re
import MySQLdb
import subprocess
import pwd
import grp

from argparse import ArgumentParser
from shutil import copy

TMP_DIR = "/tmp/"
MYSQL_DIR = "/var/lib/mysql/"
MYSQL_DEFAULTS_FILE = "/home/kverizhnikov/.my.cnf"
REGEXP = re.compile(r'^#(.*)')


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("path", type=str, help="Path to binary mysql files")
    parser.add_argument("db", type=str, help="Name of database")
    parser.add_argument("--charset", type=str, help="Charset for database. Default is UTF8", default="utf8")
    return parser.parse_args()


def prepare_dump(dump, charset):
    db_name = str(os.path.basename(dump).split(".")[0])

    if os.path.exists(dump) and os.path.isfile(dump):
        os.remove(dump)

    with open(dump, 'w') as d:
        d.write("DROP DATABASE IF EXISTS {};\n".format(db_name))
        d.write("CREATE DATABASE {} CHARACTER SET {};\n\n".format(db_name, charset))


def is_not_blank(string):
    return bool(string and string.strip())


def collect_files(path, extension):
    files = []

    for (_, _, filenames) in os.walk(path):
        for f in filenames:
            if f.endswith(extension):
                files.append(f)

    return files


def collect_dump(frm_files, dump, path, charset):
    prepare_dump(dump, charset)

    for frm in frm_files:
        p = subprocess.check_output(["mysqlfrm", "--diagnostic", path + frm])
        lines = p.splitlines()
        for line in lines:
            match = re.match(REGEXP, line)
            if not match and is_not_blank(line):
                with open(dump, 'a') as d:
                    d.write(line + "\n")

    cmd = "/usr/bin/mysql --defaults-file={} < {}".format(MYSQL_DEFAULTS_FILE, dump)
    print(cmd)
    subprocess.call(cmd, shell=True)


def connect():
    db = MySQLdb.connect("localhost", read_default_file=MYSQL_DEFAULTS_FILE)
    cursor = db.cursor()

    return cursor


def get_full_dump(ibd_files, database, source_path, charset):
    c = connect()
    c.execute("USE " + database)

    for ibd in ibd_files:
        table = str(os.path.basename(ibd).split(".")[0])
        print("Discarding tablespace for table: {}").format(table)
        c.execute("ALTER TABLE `{}` DISCARD TABLESPACE".format(table))
        destination = os.path.join(MYSQL_DIR, database) + "/"
        print("Copying tablespace for table: {}").format(table)
        copy(source_path + ibd, destination)
        print("Set owner and group")
        uid = pwd.getpwnam("mysql").pw_uid
        gid = grp.getgrnam("mysql").gr_gid
        os.chown(destination + ibd, uid, gid)
        print("Importing tablespace for table: {}").format(table)
        c.execute("ALTER TABLE `{}` IMPORT TABLESPACE".format(table))

    full_dump = TMP_DIR + database + "_complete_dump.sql"

    if os.path.isfile(full_dump):
        os.remove(full_dump)

    cmd = "/usr/bin/mysqldump --defaults-file={} --default-character-set={} --databases {} --skip-comments --result-file={}".\
        format(MYSQL_DEFAULTS_FILE, charset, database, full_dump)
    subprocess.call(cmd, shell=True)

    return full_dump


def main():
    args = parse_args()

    path = args.path
    db = args.db
    charset = args.charset
    dump = TMP_DIR + db + ".sql"

    frm_files = collect_files(path, ".frm")
    ibd_files = collect_files(path, ".ibd")
    collect_dump(frm_files, dump, path, charset)
    full_dump = get_full_dump(ibd_files, db, path, charset)
    print("\n\nThe dump has been saved as {}").format(full_dump)


if __name__ == "__main__":
    main()
