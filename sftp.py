from os.path import join
import sqlite3 as sqlite
import pysftp
import os


class Sftp:
    def __init__(self, host, user, passwd, remote_directory, local_directory, fh, message):
        self.local_root = os.path.dirname(__file__)
        self.host = host
        self.user = user
        self.passwd = passwd
        self.remote_directory = remote_directory
        self.local_directory = local_directory
        self.db_path = join(self.local_root, "data\\info.db")
        self.file = None
        self.cnopts = pysftp.CnOpts()
        self.cnopts.hostkeys = None
        self.sqlite_conn = sqlite.connect(self.db_path)
        self.fh = fh
        self.message = message

    def check(self):
        with pysftp.Connection(host=self.host, username=self.user, password=self.passwd, port=22,
                               cnopts=self.cnopts) as cxn:
            if self.fh:
                if cxn.isdir(self.remote_directory):
                    with cxn.cd(self.remote_directory):
                        lst = cxn.listdir(self.remote_directory)
                        lst = [x for x in lst if ".csv" not in x]
                        # lst = lst[-100:]
                        cursor = self.sqlite_conn.cursor()
                        cursor.execute('SELECT file_name FROM fh_files ORDER BY file_name')
                        output_tup = cursor.fetchall()
                        cursor.close()
                        output_lst = [item for t in output_tup for item in t]
                        for lst_item in lst:
                            if lst_item not in output_lst:
                                return lst_item

                        return None
            else:
                if cxn.isdir(self.remote_directory):
                    with cxn.cd(self.remote_directory):
                        lst = cxn.listdir(self.remote_directory)
                        lst = [x for x in lst if ".txt" not in x]
                        # lst = lst[-100:]
                        cursor = self.sqlite_conn.cursor()
                        cursor.execute('SELECT file_name FROM np_files ORDER BY file_name')
                        output_tup = cursor.fetchall()
                        cursor.close()
                        output_lst = [item for t in output_tup for item in t]
                        for lst_item in lst:
                            if lst_item not in output_lst:
                                return lst_item

                        return None

    def update(self, file):
        self.file = file
        if self.file:
            if self.fh:
                cursor = self.sqlite_conn.cursor()
                cursor.execute(f"INSERT INTO fh_files (file_name) VALUES ('{self.file}')")
                self.sqlite_conn.commit()
                self.sqlite_conn.close()
            else:
                cursor = self.sqlite_conn.cursor()
                cursor.execute(f"INSERT INTO np_files (file_name) VALUES ('{self.file}')")
                self.sqlite_conn.commit()
                self.sqlite_conn.close()

    def download(self):
        obit_type = "Funeral Home:" if self.fh else "Newspaper:"
        progressDict = {}
        progressEveryPercent = 10

        for i in range(0, 101):
            if i % progressEveryPercent == 0:
                progressDict[str(i)] = ""

        def print_progress_decimal(x, y):
            if int(100 * (int(x) / int(y))) % progressEveryPercent == 0 and progressDict[
                str(int(100 * (int(x) / int(y))))] == "":
                self.message("{}% ({} MB / {} MB Downloaded)".format(str("%.1f" % (100 * (int(x) / int(y)))),
                                                                     (x / 1000000),
                                                                     (y / 1000000)))
                progressDict[str(int(100 * (int(x) / int(y))))] = "1"

        with pysftp.Connection(host=self.host, username=self.user, password=self.passwd, port=22,
                               cnopts=self.cnopts) as cxn:

            if cxn.isdir(self.remote_directory):
                print(f"{obit_type} Remote directory /daily/ found!")
                with cxn.cd(self.remote_directory):
                    if cxn.isfile(self.file):
                        self.message(f"{self.file} found!")
                        cxn.get(self.file, callback=lambda x, y: print_progress_decimal(x, y),
                                localpath=str(join(self.local_directory, self.file)))
                    else:
                        raise FileNotFoundError("ftp file not found")
            else:
                raise FileNotFoundError("Remote Directory Not Found")
