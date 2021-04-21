import sqlite3 as sqlite
from sys import exit
from os.path import join, dirname, isfile
import datetime as dt
from datetime import datetime as dt2
import sys
import time


def main():
    t = dt2.now()
    td_day = dt.timedelta(days=1)
    t_td = t - td_day
    # Format (20201007) It will always be one day before
    file_type = input("Are you re-processing a Funeral Home or Newspaper file (np/fh)?")
    date_to_reprocess = input(
        "Please enter the date to re-process in the following format: 10/21/2020\n")

    if file_type.strip(' ').lower() == 'np' or file_type.strip(' ').lower() == 'fh':
        if file_type.lower() == 'np':
            file_is_fh = False
        else:
            file_is_fh = True
    else:
        print("Please enter a valid response")
        time.sleep(10)
        sys.exit(1)

    date_arr = date_to_reprocess.split('/')
    date = str(date_arr[2]) + str(date_arr[0]) + str(date_arr[1])
    fh_filename = f"avsllc_daily_{str(date).strip()}.txt"
    np_filename = f"avsllc_daily_{str(date).strip()}.csv"
    root = dirname(__file__)
    path_to_db = join(root, "data\\info.db")
    if isfile(path_to_db):
        sqlite_conn = sqlite.connect(path_to_db)
        c = sqlite_conn.cursor()
        date_to_rm = input(
            f"Are you sure you would like to reprocess {str(date_to_reprocess)}? (y/N): ")
        if date_to_rm.lower() == "y" or date_to_rm.lower() == "yes":
            # fh_filename = '%%'
            try:
                if file_is_fh:
                    c.execute('DELETE FROM fh_files WHERE file_name = ?', [fh_filename])
                elif not file_is_fh:
                    c.execute('DELETE FROM np_files WHERE file_name = ?', [np_filename])
                else:
                    sys.exit("Failed to convert user input")
            except sqlite.OperationalError as e:
                print(str(e))
            else:
                print("Re-Processed successfully")
                sqlite_conn.commit()
                sqlite_conn.close()
        else:
            exit()
    else:
        print("Failed to locate database")


if __name__ == '__main__':
    main()
