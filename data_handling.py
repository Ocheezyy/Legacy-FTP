from os.path import isfile, isdir, join, dirname
from sqlite3.dbapi2 import DatabaseError
from datetime import datetime as dt2
import sqlite3 as sqlite
import pandas as pd
import numpy as np
import openpyxl
import shutil
import time
import pyodbc
import logging
import os
import csv
import sys


# The "fh" referenced in this class is for file handling not funeral home
class Logger:
    def __init__(self, log_path, file_type):
        self.log_path = log_path
        self.logger = logging.getLogger('debug_log')
        self.logger.setLevel(logging.DEBUG)

        self.fh = logging.FileHandler(self.log_path)
        self.fh.setLevel(logging.DEBUG)

        self.ch = logging.StreamHandler()
        self.ch.setLevel(logging.ERROR)

        self.formatter = logging.Formatter(
            f'{file_type}: %(levelname)s: %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S')

        self.ch.setFormatter(self.formatter)
        self.fh.setFormatter(self.formatter)

        self.logger.addHandler(self.ch)
        self.logger.addHandler(self.fh)

    def log_debug(self, msg):
        self.logger.debug(msg)

    def log_info(self, msg):
        self.logger.info(msg)

    def log_warning(self, msg):
        self.logger.warning(msg)

    def log_error(self, msg):
        self.logger.error(msg)

    def log_critical(self, msg):
        self.logger.critical(msg)


def fh_check(filename):
    if filename.find(".txt") != -1:
        return True
    elif filename.find(".csv") != -1:
        return False
    else:
        print("Unrecognized filename")


# The following class is not used due to the database missing being a fatal error
class Database:
    def __init__(self, message, db_path):
        self.message = message
        self.root = dirname(__file__)
        self.db_path = db_path

    def create(self):
        conn = sqlite.connect(self.db_path)
        if isfile(self.db_path):
            try:
                c = conn.cursor()
                c.execute("""CREATE TABLE fh_files(
                                id integer primary key AUTOINCREMENT,
                                file_name TEXT NOT NULL,
                                Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                            )""")
                conn.commit()
            except Exception as e:
                self.message("Failed to find or create database" +
                             str(e), exception=True)


class Converter:
    def __init__(self, filename, message):
        # This class is not used by the NP CSV files as there is no need for conversion
        self.root = os.path.dirname(__file__)
        self.message = message
        if not isdir(join(self.root, "data")):
            message("Data directory doesn't exist", exception=True)
        self.fh_path = join(self.root, f"data\\to_process\\{filename}")
        if not isfile(self.fh_path):
            self.message(
                f"Failed to find FH file at {self.fh_path}", exception=True)
            raise FileNotFoundError
        self.path_to_csv = join(
            self.root, f"data\\to_process\\{filename.rstrip('.txt.gz')}.csv")
        try:
            csv.field_size_limit(sys.maxsize)
        except OverflowError:
            csv.field_size_limit(int(2147483647))

    def remove_ending(self):
        with open(self.fh_path, "rb+") as file:

            self.message("Finding EOF")
            file.seek(0, os.SEEK_END)
            self.message("EOF found")
            pos = file.tell() - 1

            while pos > 0 and file.read(1) != "\n":
                pos -= 1
                file.seek(pos, os.SEEK_SET)

            if pos > 0:
                file.seek(pos, os.SEEK_SET)
                file.truncate()
                self.message("EOF removed")

    def convert_fh(self):
        if not isfile(self.path_to_csv):
            try:
                import gzip
                txt_path = self.fh_path.rstrip('.gz')
                with gzip.open(self.fh_path, 'rb') as f, open(txt_path, 'wb') as f_out:
                    f_out.write(f.read())
                self.fh_path = txt_path
                with open(self.fh_path, "rt", encoding="utf-8") as file_pipe:
                    with open(self.path_to_csv, 'wt', encoding="utf-8", newline="\n") as file_comma:
                        csv.writer(file_comma, delimiter=',').writerows(
                            csv.reader(file_pipe, delimiter='|'))
            except Exception as e:
                self.message("Failed to convert to CSV\n" +
                             str(e), exception=True)
            else:
                self.message("Converted to CSV successfully")
                return str(self.fh_path).split('\\')[-1]
        else:
            self.message("CSV already exists for FH", warning=True)
            raise IOError("CSV already exists")


class Comparison:
    def __init__(self, message, filename, is_fh):
        self.root = os.path.dirname(__file__)
        self.message = message
        self.filename = filename
        self.is_fh = is_fh
        if self.is_fh:
            self.path_to_csv = join(
                self.root, f"data\\to_process\\{filename.rstrip('.txt')}.csv")
            self.datatypes = {'id': str, 'obit_start_date': str, 'row_created': str, 'row_updated': str,
                              'row_deleted': str, 'salutation': str,
                              'first_name': str, 'middle_name': str, 'last_name': str, 'maiden_name': str,
                              'nick_name': str, 'date_of_birth': str,
                              'date_of_death': str, 'age': str, 'gender': str, 'current_city': str,
                              'current_state': str, 'FuneralServiceInCity': str,
                              'FuneralServiceInState': str, 'AssociatedFuneralHome': str, 'education': str,
                              'military': str, 'donation': str,
                              'funeral_services': str, 'obit_text': str}
        else:
            self.path_to_csv = join(
                self.root, f"data\\to_process\\{filename}")
            self.datatypes = {'Obituary ID': str, 'Obit Date(publish)': str, 'Updated At': str, 'First Name': str,
                              'Middle Name': str, 'Last Name': str,
                              'Maiden Name': str, 'Nick Name': str, 'DoB': str, 'DoD': str, 'Age': str,
                              'Funeral Service in City': str, 'Funeral Service in State': str,
                              'Service Location Zip Code': str, 'Funeral Service Info': str, 'Obituary Link': str,
                              'Newspaper Source': str, 'Newspaper City': str,
                              'Newspaper Zip Code': str}
        if isfile(self.path_to_csv):
            self.csv_file = pd.read_csv(
                filepath_or_buffer=self.path_to_csv, dtype=self.datatypes)
        else:
            raise FileNotFoundError("Failed to find CSV")
        self.fname_prefixes = ['Mr.', 'Mr', 'Ms.', 'Ms', 'Mrs.', 'Mrs', 'Miss', 'Mister', 'Father', 'Mother',
                               'Reverend', 'General', 'Rep.',
                               'Representative', 'Prince', 'Princess', 'King', 'Queen', 'Madam', 'Mayor', 'Governor',
                               'President',
                               'Airman', 'Seaman', 'Corporal', 'Sergeant', 'Officer', 'Lieutenant', 'Captain',
                               'Colonel', 'Admiral', ]
        self.lname_suffixes = ['jr.', 'sr.',
                               'jr', 'sr', 'iii', 'iiii', 'iiiii']

    def intake(self, conn):
        def fname_format(fname, row_index=None):
            if fname.find(' ') != -1:
                arr = fname.split(' ')
                if arr[0] in self.fname_prefixes:
                    return str(arr[1])
                elif arr[1].find('?') != -1 and arr[0] not in self.fname_prefixes:
                    nick = str(arr[1]).strip('?')
                    self.csv_file.at[row_index, 'Nick Name'] = nick
                    return str(arr[0])
                elif arr[1].find('"') != -1 and arr[0] not in self.fname_prefixes:
                    nick = str(arr[1]).strip('"')
                    self.csv_file.at[row_index, 'Nick Name'] = nick
                    return str(arr[0])
                elif arr[1].find('(') != -1 and arr[0] not in self.fname_prefixes:
                    nick = str(arr[1]).rstrip(')').lstrip('(').capitalize()
                    self.csv_file.at[row_index, 'Nick Name'] = nick
                    return str(arr[0])
                else:
                    return fname
            else:
                return fname

        def mname_format(mname, row_index=None):
            try:
                if mname.find(' ') != -1:
                    arr = mname.split(' ')
                    if arr[1].find('"') != -1:
                        nick = str(arr[1]).strip('"')
                        self.csv_file.at[row_index, 'Nick Name'] = nick
                    elif arr[1].find('?') != -1:
                        nick = str(arr[1]).strip('?')
                        self.csv_file.at[row_index, 'Nick Name'] = nick
                    elif arr[1].find('(') != -1:
                        maiden = str(arr[1]).lstrip('(').rstrip(')')
                        self.csv_file.at[row_index, 'Maiden Name'] = maiden
                    else:
                        pass
                else:
                    if mname.find('"') != -1:
                        self.csv_file.at[row_index,
                                         'Nick Name'] = mname.strip('"')
                        return ""
                    elif mname.find('?') != -1:
                        self.csv_file.at[row_index,
                                         'Nick Name'] = mname.strip('?')
                        return ""
                    else:
                        return mname
            except:
                return mname

        def lname_format(lname, row_index=None):
            if lname.find('-') != -1:
                arr = lname.split('-')
                maiden = str(arr[0])
                self.csv_file.at[row_index, 'Suffix'] = maiden
                return str(arr[1])
            if lname.find(' ') != -1:
                arr = lname.split(' ')
                if str(arr[1]).lower() in self.lname_suffixes:
                    suff = str(arr[1]).capitalize()
                    l = str(arr[0]).rstrip(',')
                    self.csv_file.at[row_index, 'Suffix'] = suff
                    return l
                elif len(str(arr[0])) > 3:
                    l = str(arr[0]).rstrip(',')
                    return l
            else:
                return lname

        if self.is_fh:

            self.csv_file.drop(self.csv_file.tail(1).index, inplace=True)
            self.csv_file = self.csv_file.applymap(lambda x: x.replace(
                "\ufeff", "") if isinstance(x, str) else x)
            self.csv_file['id'] = self.csv_file['id'].apply(pd.to_numeric)

            self.csv_file = self.csv_file.drop(
                columns=['row_created', 'row_updated', 'row_deleted'], axis=1)
            self.csv_file = self.csv_file.drop_duplicates(
                subset=['first_name', 'last_name', 'date_of_birth'])

            self.csv_file["hasMatch"] = 0
            self.csv_file["SSN"] = ""
            self.csv_file["isDead"] = 0
            self.csv_file["src"] = ""
            self.csv_file["ignore"] = 0

            # parse_date(self.csv_file)

            df_length = len(self.csv_file.index)

            def has_values(df, col):
                if df_length != len(df[df[col].isna()]):
                    return True
                return False

            import re

            def check(string):
                string = str(string)
                alpha_regex = '[a-zA-Z]'
                if re.search(alpha_regex, string):
                    return string[:10]
                elif len(string) == 8 and string.find("-") == -1 and string.find("/") == -1:
                    x = '{0:0>8}'.format(string)
                    new_date_final = '-'.join([x[:2],
                                               x[2:4], x[4:]])
                    return new_date_final
                else:
                    return string

            if has_values(self.csv_file, 'date_of_birth'):
                self.csv_file['date_of_birth'] = self.csv_file['date_of_birth'].apply(
                    lambda x: check(x))
            if has_values(self.csv_file, 'date_of_death'):
                self.csv_file['date_of_death'] = self.csv_file['date_of_death'].apply(
                    lambda x: check(x))

            self.csv_file['first_name'] = self.csv_file['first_name'].str.replace(
                "'", " ")
            if has_values(self.csv_file, 'middle_name'):
                self.csv_file['middle_name'] = self.csv_file['middle_name'].str.replace(
                    "'", " ")

            self.csv_file['last_name'] = self.csv_file['last_name'].str.replace(
                "'", " ")

            if has_values(self.csv_file, 'maiden_name'):
                self.csv_file['maiden_name'] = self.csv_file['maiden_name'].str.replace(
                    "'", " ")

            if has_values(self.csv_file, 'nick_name'):
                self.csv_file['nick_name'] = self.csv_file['nick_name'].replace(
                    "'", " ")

            if has_values(self.csv_file, 'AssociatedFuneralHome'):
                self.csv_file['AssociatedFuneralHome'] = self.csv_file['AssociatedFuneralHome'].str.replace(
                    "'", " ")

            if has_values(self.csv_file, 'education'):
                self.csv_file['education'] = self.csv_file['education'].str.replace(
                    "'", " ")

            if has_values(self.csv_file, 'FuneralServiceInCity'):
                self.csv_file['FuneralServiceInCity'] = self.csv_file['FuneralServiceInCity'].str.replace(
                    "'", " ")

            if has_values(self.csv_file, 'current_city'):
                self.csv_file['current_city'] = self.csv_file['current_city'].str.replace(
                    "'", " ")

            if has_values(self.csv_file, 'current_state'):
                self.csv_file['current_state'] = self.csv_file['current_state'].apply(
                    lambda x: x.lower().capitalize() if len(x) > 3 else x)

            if has_values(self.csv_file, 'military'):
                self.csv_file['military'] = self.csv_file['military'].str.replace(
                    "'", " ")

            if has_values(self.csv_file, 'military'):
                self.csv_file['donation'] = self.csv_file['military'].str.replace(
                    "'", " ")

            if has_values(self.csv_file, 'funeral_services'):
                self.csv_file['funeral_services'] = self.csv_file['funeral_services'].str.replace(
                    "'", " ")

            if has_values(self.csv_file, 'obit_text'):
                self.csv_file['obit_text'] = self.csv_file['obit_text'].str.replace(
                    "'", " ")

        else:
            if isfile(self.path_to_csv):
                self.csv_file = pd.read_csv(
                    filepath_or_buffer=self.path_to_csv, dtype=self.datatypes)
                self.csv_file['Obituary ID'] = self.csv_file['Obituary ID'].apply(
                    pd.to_numeric)
            else:
                raise FileNotFoundError("Failed to find CSV")

            self.csv_file = self.csv_file.drop(
                columns=['Updated At', 'Service Location Zip Code', 'Funeral Service Info'], axis=1)
            self.csv_file = self.csv_file.sort_values('Obituary ID')
            self.csv_file = self.csv_file.drop_duplicates(
                subset=['First Name', 'Last Name', 'DoB'])

            self.csv_file = self.csv_file[self.csv_file['First Name'].notna()]
            self.csv_file = self.csv_file[self.csv_file['Last Name'].notna()]
            self.csv_file['Suffix'] = ""

            df_length = len(self.csv_file.index)

            def has_values(df, col):
                if df_length != len(df[df[col].isna()]):
                    return True
                return False

            # Lambda formatting for nickname/maidenname
            self.csv_file['First Name'] = self.csv_file.apply(
                lambda x: fname_format(x['First Name'], x.name), axis=1)
            if has_values(self.csv_file, 'Middle Name'):
                self.csv_file['Middle Name'] = self.csv_file.apply(
                    lambda x: mname_format(x['Middle Name'], x.name), axis=1)
            self.csv_file['Last Name'] = self.csv_file.apply(
                lambda x: lname_format(x['Last Name'], x.name), axis=1)

            # ObitDate formatting
            self.csv_file['Obit Date(publish)'] = self.csv_file['Obit Date(publish)'].astype(str).apply(
                lambda x: x.split(" ")).apply(lambda x: str(x[0]) + str(
                    ' 00:00:00'))  # (" ", "/").split("/").apply(lambda x: '/'.join(x[0], x[1], x[2])).apply(lambda x: str(x) + "00:00:00")

            # FName and LName capitalization
            self.csv_file['First Name'] = self.csv_file['First Name'].astype(str).apply(
                lambda x: x.lower().capitalize() if "str" in str(type(x)) else x.str.lower().capitalize())
            self.csv_file['Last Name'] = self.csv_file['Last Name'].astype(str).apply(
                lambda x: x.lower().capitalize() if "str" in str(type(x)) else x.str.lower().capitalize())

            # DOB and DOD Null checks
            self.csv_file = self.csv_file[self.csv_file['DoB'].notna()]
            self.csv_file = self.csv_file[self.csv_file['DoB'] != 0.0]
            self.csv_file = self.csv_file[self.csv_file['DoB'] != "00000000"]
            self.csv_file = self.csv_file[self.csv_file['DoD'].notna()]
            self.csv_file = self.csv_file[self.csv_file['DoD'] != 0.0]
            self.csv_file = self.csv_file[self.csv_file['DoD'] != "00000000"]

            # parse_date(self.csv_file)

            # DOB and DOD parsing
            self.csv_file['DoB'] = self.csv_file['DoB'].astype(np.int64)
            self.csv_file['DoB'] = self.csv_file['DoB'].astype(str).apply(lambda x: '{0:0>8}'.format(x)).apply(
                lambda x: '-'.join([x[:2], x[2:4], x[4:]])).str.replace('-00-', '-01-')
            self.csv_file['DoB'] = self.csv_file['DoB'].apply(
                lambda x: x.replace('00-', '01-'))
            self.csv_file['DoD'] = self.csv_file['DoD'].astype(np.int64)
            self.csv_file['DoD'] = self.csv_file['DoD'].astype(str).apply(lambda x: '{0:0>8}'.format(x)).apply(
                lambda x: '-'.join([x[:2], x[2:4], x[4:]]))

            # "SQL Injection" formatting
            self.csv_file['First Name'] = self.csv_file['First Name'].str.replace(
                "'", "").replace('"', '')

            if has_values(self.csv_file, 'Middle Name'):
                self.csv_file['Middle Name'] = self.csv_file['Middle Name'].str.replace(
                    "'", "").replace('"', '')
            self.csv_file['Last Name'] = self.csv_file['Last Name'].str.replace(
                "'", "").replace('"', '')

            if has_values(self.csv_file, 'Maiden Name'):
                self.csv_file['Maiden Name'] = self.csv_file['Maiden Name'].str.replace(
                    "'", "").replace('"', '')

            if has_values(self.csv_file, 'Nick Name'):
                self.csv_file['Nick Name'] = self.csv_file['Nick Name'].str.replace(
                    "'", "").replace('"', '')
            self.csv_file['Obituary Link'] = self.csv_file['Obituary Link'].str.replace(
                "'", "").replace('"', '')

            if has_values(self.csv_file, 'Funeral Service in City'):
                self.csv_file['Funeral Service in City'] = self.csv_file['Funeral Service in City'].str.replace(
                    "'", "").replace('"', '')

            if has_values(self.csv_file, 'Newspaper City'):
                self.csv_file['Newspaper City'] = self.csv_file['Newspaper City'].str.replace(
                    "'", "").replace('"', '')

            if has_values(self.csv_file, 'Newspaper Zip Code'):
                self.csv_file['Newspaper Zip Code'] = self.csv_file['Newspaper Zip Code'].astype(str).apply(
                    lambda x: x.split('-')[0])

            if has_values(self.csv_file, 'Middle Name'):
                self.csv_file['Middle Name'] = self.csv_file['Middle Name'].apply(
                    lambda x: str(x) + '.' if (not isinstance(x, float) or x is not None) and len(str(x)) == 1 else x)

        try:
            new_conn = pyodbc.connect(conn)
            cursor = new_conn.cursor()
            for row in self.csv_file.itertuples():
                if self.is_fh:
                    query_sp = f"""exec usp_LegacyDeathInsert
                                        @fh = 1,
                                        @id = {row[1]}
                                        ,@Obit_Date = '{row[2]}'
                                        {f",@Salutation = '{row[3]}'" if pd.notnull(
                        row[3]) else ""}
                                        {f",@FName = '{row[4]}'" if pd.notnull(
                        row[4]) else ""}
                                        {f",@MName = '{row[5]}'" if pd.notnull(
                        row[5]) else ""}
                                        {f",@LName = '{row[6]}'" if pd.notnull(
                        row[6]) else ""}
                                        {f",@MaidenName = '{row[7]}'" if pd.notnull(
                        row[7]) else ""}
                                        {f",@NickName = '{row[8]}'" if pd.notnull(
                        row[8]) else ""}
                                        {f",@DOB = '{row[9]}'" if pd.notnull(
                        row[9]) else ""}
                                        {f",@DOD = '{row[10]}'" if pd.notnull(
                        row[10]) else ""}
                                        {f",@Age = {str(row[11]).rstrip('.0')}" if pd.notnull(
                        row[11]) else ""}
                                        {f",@Gender = '{row[12]}'" if pd.notnull(
                        row[12]) else ""}
                                        {f",@CurrentCity = '{row[13]}'" if pd.notnull(
                        row[13]) else ""}
                                        {f",@CurrentState = '{row[14]}'" if pd.notnull(
                        row[14]) else ""}
                                        {f",@FuneralServiceInCity = '{row[15]}'" if pd.notnull(
                        row[15]) else ""}
                                        {f",@FuneralServiceInState = '{row[16]}'" if pd.notnull(
                        row[16]) else ""}
                                        {f",@AssociatedFuneralHome = '{row[17]}'" if pd.notnull(
                        row[17]) else ""}
                                        {f",@Education = '{row[18]}'" if pd.notnull(
                        row[18]) else ""}
                                        {f",@military = '{row[19]}'" if pd.notnull(
                        row[19]) else ""}
                                        {f",@donation = '{row[20]}'" if pd.notnull(
                        row[20]) else ""}
                                        {f",@FuneralServices = '{row[21]}'" if pd.notnull(
                        row[21]) else ""}
                                """
                else:
                    query_sp = f"""exec usp_LegacyDeathInsert
                                        @fh = 0
                                        ,@id = {row[1]}
                                        ,@Obit_Date = '{row[2]}'
                                        {f",@FName = '{row[3]}'" if pd.notnull(
                        row[3]) else ""}
                                        {f",@MName = '{row[4]}'" if pd.notnull(
                        row[4]) else ""}
                                        {f",@LName = '{row[5]}'" if pd.notnull(
                        row[5]) else ""}
                                        {f",@MaidenName = '{row[6]}'" if pd.notnull(
                        row[6]) else ""}
                                        {f",@NickName = '{row[7]}'" if pd.notnull(
                        row[7]) else ""}
                                        {f",@DOB = '{row[8]} 00:00:00'" if pd.notnull(
                        row[8]) else ""}
                                        {f",@DOD = '{row[9]} 00:00:00'" if pd.notnull(
                        row[9]) else ""}
                                        {f",@FuneralServiceInCity = '{row[11]}'" if pd.notnull(
                        row[11]) else ""}
                                        {f",@FuneralServiceInState = '{row[12]}'" if pd.notnull(
                        row[12]) else ""}
                                        {f",@ObituaryLink = '{row[13]}'" if pd.notnull(
                        row[13]) else ""}
                                        {f",@NewspaperSource = '{row[14]}'" if pd.notnull(
                        row[14]) else ""}
                                        {f",@NewspaperCity = '{row[15]}'" if pd.notnull(
                        row[15]) else ""}
                                        {"" if "nan" in row[16]
                    else f",@NewspaperZip = '{row[16]}'"}
                                 """
                try:
                    cursor.execute(query_sp)
                except Exception as e:
                    self.message(f"{query_sp} {str(e)}")
        except Exception as e:
            self.message(
                f"Failed to export to table: {str(e)}", exception=True)
        else:
            new_conn.commit()
            try:
                time.sleep(1)
                if self.is_fh:
                    obit_text_query = None
                    for row in self.csv_file.itertuples():
                        try:
                            if row[22] is not None and row[22] != "Pending" and row[22] != " " and row[22] != "nan":
                                obit_text_query = f"INSERT INTO tLegacyObitText VALUES ({row[1]}, '{row[22]}')"
                                cursor.execute(obit_text_query)
                        except Exception as e:
                            pass
                            # self.message(f"{str(e)}")

            except:
                self.message("Failed to commit changes to table",
                             exception=True)
            else:
                new_conn.commit()
                time.sleep(10)
                new_conn.close()
                self.message("Successfully exported to table")

        return len(self.csv_file.index)

    def fetch_deaths(self, conn, dflength):
        if self.is_fh:
            query = f"""
                    WITH CTE AS (SELECT
						TOP {dflength}
                        p.SSN,
                        p.PatFname AS FName,
                        l.FName AS FName_L,
                        p.PatMName AS Middle,
                        l.MName AS Middle_L,
                        p.PatLName AS LName,
                        l.LName AS LName_L,
                        p.DOB AS DOB,
                        l.DOB AS DOB_L,
                        l.DOD AS DOD_L,
                        u.state_name AS"State",
                        u.state_id AS "StateAbbrev",
                        l.CurrentState AS "State_L",
                        l.id AS l_id
                    FROM
                        (SELECT FName, MName, LName, DOB, DOD, CurrentState, id, hasMatch, DateEntered, Ignore, isFH FROM tLegacyDeaths) l
                    INNER JOIN
                            (SELECT DISTINCT SSN, PatFName, PatMName, DOB, PatLName, Died, ZipCode FROM PersonalData) p ON p.PatLName = l.LName AND p.DOB = l.DOB
                    INNER JOIN
                            (SELECT state_id, state_name, zip FROM USZips) u on p.ZipCode = u.zip
                    WHERE
						(l.isFH = 1)
                            AND
                        (l.ignore = 0 OR l.ignore IS NULL)
                            AND
                        (p.Died IS NULL)
							AND
                        (p.SSN NOT IN (SELECT DISTINCT SSN FROM tLegacyDeaths WHERE hasMatch = 1 AND ignore = 0 or ignore is null))
                    ORDER BY DateEntered DESC
                    )
                    SELECT
                        SSN,
                        FName,
                        FName_L,
                        Middle,
                        Middle_L,
                        LName,
                        LName_L,
                        DOB,
                        DOB_L,
                        DOD_L,
                        State,
                        StateAbbrev,
                        State_L,
                        l_id
                    FROM
                        CTE
                    """
        else:
            query = f"""
                    WITH CTE AS (SELECT
						TOP {dflength}
                        p.SSN,
                        p.PatFname AS FName,
                        l.FName AS FName_L,
                        p.PatMName AS Middle,
                        l.MName AS Middle_L,
                        p.PatLName AS LName,
                        l.LName AS LName_L,
                        p.DOB AS DOB,
                        l.DOB AS DOB_L,
                        l.DOD AS DOD_L,
                        u.state_name AS"State",
                        u.state_id AS "StateAbbrev",
                        l.CurrentState AS "State_L",
                        l.id AS l_id
                    FROM
                        (SELECT FName, MName, LName, DOB, DOD, CurrentState, id, hasMatch, DateEntered, Ignore, isFH FROM tLegacyDeaths) l
                    INNER JOIN
                            (SELECT DISTINCT SSN, PatFName, PatMName, DOB, PatLName, Died, ZipCode FROM PersonalData) p ON p.PatLName = l.LName AND p.DOB = l.DOB
                    INNER JOIN
                            (SELECT state_id, state_name, zip FROM USZips) u on p.ZipCode = u.zip
                    WHERE
						(l.isFH = 0)
                            AND
                        (l.ignore = 0 OR l.ignore IS NULL)
                            AND
                        (p.Died IS NULL)
							AND
                        (p.SSN NOT IN (SELECT DISTINCT SSN FROM tLegacyDeaths WHERE hasMatch = 1 AND ignore = 0 or ignore is null))
                    ORDER BY DateEntered DESC
                    )
                    SELECT
                        SSN,
                        FName,
                        FName_L,
                        Middle,
                        Middle_L,
                        LName,
                        LName_L,
                        DOB,
                        DOB_L,
                        DOD_L,
                        State,
                        StateAbbrev,
                        State_L,
                        l_id
                    FROM
                        CTE
                    """
        try:
            df = pd.read_sql(query, conn)
        except pyodbc.Error as e:
            raise DatabaseError(str(e))
        else:
            self.message("Fetched deaths")
            return df


class df_manipulation:
    def __init__(self, df, conn, is_fh):
        self.conn = conn
        self.df = df
        self.FName_index = 2
        self.FName_L_index = 3
        self.State_index = 11
        self.State_abbrev_index = 12
        self.State_L_index = 13
        self.Middle_index = 4
        self.Middle_L_index = 5
        self.first_name_lst = []
        self.middle_name_lst = []
        self.state_lst = []
        self.lst_100 = []
        self.is_fh = is_fh

    def checks(self):
        if self.is_fh:
            for row in self.df.itertuples():
                if str(row[self.FName_index]).lower() != str(row[self.FName_L_index]).lower():
                    self.first_name_lst.append(row)
                elif str(row[self.State_index]).lower() != str(row[self.State_L_index]).lower():
                    if str(row[self.State_abbrev_index]) != str(row[self.State_L_index]):
                        self.state_lst.append(row)
                    else:
                        self.lst_100.append(row)
                elif str(row[self.Middle_L_index]) != "NULL" or row[self.Middle_index] is not None:
                    if len(str(row[self.Middle_index])) > 3 and len(str(row[self.Middle_L_index])) > 3:
                        if str(row[self.Middle_index]) != str(row[self.Middle_L_index]):
                            self.middle_name_lst.append(row)
                        else:
                            self.lst_100.append(row)
                    elif len(str(row[self.Middle_index])) <= 3 and len(str(row[self.Middle_L_index])) <= 3:
                        if str(row[self.Middle_index]).strip(".") != str(row[self.Middle_L_index]).strip("."):
                            self.middle_name_lst.append(row)
                        else:
                            self.lst_100.append(row)
                    elif (len(str(row[self.Middle_index])) <= 3) and (len(str(row[self.Middle_L_index])) > 3):
                        if not str(row[self.Middle_L_index]).startswith(str(row[self.Middle_index]).strip(".")):
                            self.middle_name_lst.append(row)
                        else:
                            self.lst_100.append(row)
                    elif (len(str(row[self.Middle_index])) > 3) and (len(str(row[self.Middle_L_index])) <= 3):
                        if not str(row[self.Middle_index]).startswith(str(row[self.Middle_L_index]).strip(".")):
                            self.middle_name_lst.append(row)
                        else:
                            self.lst_100.append(row)
                else:
                    self.lst_100.append(row)

        else:
            for row in self.df.itertuples():
                if str(row[self.FName_index]).lower() != str(row[self.FName_L_index]).lower():
                    self.first_name_lst.append(row)
                elif str(row[self.State_index]).lower() != str(row[self.State_L_index]).lower():
                    if str(row[self.State_abbrev_index]) != str(row[self.State_L_index]):
                        self.state_lst.append(row)
                    else:
                        self.lst_100.append(row)
                elif str(row[self.Middle_L_index]) != "NULL" or row[self.Middle_index] is not None:
                    if len(str(row[self.Middle_index])) > 3 and len(str(row[self.Middle_L_index])) > 3:
                        if str(row[self.Middle_index]) != str(row[self.Middle_L_index]):
                            self.middle_name_lst.append(row)
                        else:
                            self.lst_100.append(row)
                    elif len(str(row[self.Middle_index])) <= 3 and len(str(row[self.Middle_L_index])) <= 3:
                        if str(row[self.Middle_index]).strip(".") != str(row[self.Middle_L_index]).strip("."):
                            self.middle_name_lst.append(row)
                        else:
                            self.lst_100.append(row)
                    elif (len(str(row[self.Middle_index])) <= 3) and (len(str(row[self.Middle_L_index])) > 3):
                        if not str(row[self.Middle_L_index]).startswith(str(row[self.Middle_index]).strip(".")):
                            self.middle_name_lst.append(row)
                        else:
                            self.lst_100.append(row)
                    elif (len(str(row[self.Middle_index])) > 3) and (len(str(row[self.Middle_L_index])) <= 3):
                        if not str(row[self.Middle_index]).startswith(str(row[self.Middle_L_index]).strip(".")):
                            self.middle_name_lst.append(row)
                        else:
                            self.lst_100.append(row)
                else:
                    self.lst_100.append(row)

        df_100 = pd.DataFrame(self.lst_100)
        df_first_name = pd.DataFrame(self.first_name_lst)
        df_middle_name = pd.DataFrame(self.middle_name_lst)
        df_state = pd.DataFrame(self.state_lst)

        df_100["hasMatch"] = 1
        df_100["isDead"] = 0
        df_100["src"] = "df_100"

        df_first_name["src"] = "df_first_name"
        df_first_name["isDead"] = 0
        df_first_name["hasMatch"] = 0

        df_middle_name["src"] = "df_middle_name"
        df_middle_name["isDead"] = 0
        df_middle_name["hasMatch"] = 0

        df_state["src"] = "df_state"
        df_state["isDead"] = 0
        df_state["hasMatch"] = 0

        frames = [df_100, df_first_name, df_middle_name, df_state]
        result_df = pd.concat(frames)

        return result_df

    def update_table(self, df):
        conn = pyodbc.connect(self.conn)
        cursor = conn.cursor()
        df['l_id'] = df['l_id'].astype(str).apply(
            lambda x: x.rstrip('.0') if x.find('.0') != -1 else x)
        df.fillna(0)
        if self.is_fh:
            for row in df.itertuples():
                if pd.notnull(row[1]):
                    query = f"""
                                    UPDATE tLegacyDeaths SET 
                                        hasMatch = 1,
                                        isDead = CAST('{str(row[3])[0]}' AS bit),
                                        SSN = '{row[2]}',
                                        src = '{row[4]}'
                                    WHERE id = {row[1]}
                                    """
                    cursor.execute(query)
        else:
            for row in df.itertuples():
                if pd.notnull(row[1]):
                    query = f"""
                                    UPDATE tLegacyDeaths SET 
                                        hasMatch = 1,
                                        isDead = CAST('{str(row[3])[0]}' AS bit),
                                        SSN = '{row[2]}',
                                        src = '{row[4]}'
                                    WHERE id = {row[1]}
                                    """
                    cursor.execute(query)
        conn.commit()
        conn.close()


class Cleanup:
    def __init__(self, root, filename, conn_func, message, is_fh):
        self.root = root
        self.is_fh = is_fh
        self.filename = filename
        self.conn_func = conn_func
        self.path_to_txt = join(
            self.root, f"data\\to_process\\{self.filename}")
        self.path_to_csv = join(
            self.root, f"data\\to_process\\{self.filename.rstrip('.txt')}.csv")
        self.path_to_db = join(self.root, "data\\info.db")
        self.message = message
        self.path_to_gz = join(
            self.root, f"data\\to_process\\{self.filename}.gz")

    def clean_files(self):
        self.message("Removing temporary files...")
        try:
            if isfile(self.path_to_txt):
                os.remove(self.path_to_txt)
            if isfile(self.path_to_csv):
                os.remove(self.path_to_csv)
            if isfile(self.path_to_gz):
                os.remove(self.path_to_gz)
        except:
            self.message("Failed to remove temporary files")
        else:
            self.message("Successfully removed temporary files")
        try:
            if isfile(self.path_to_db):
                shutil.copyfile(src=self.path_to_db, dst=join(
                    self.root, "data\\backup\\info.db"))
        except:
            self.message("Failed to save backup of local DB", warning=True)

    def clean_db_table(self):
        conn = pyodbc.connect(self.conn_func, autocommit=True)
        cursor = conn.cursor()
        if self.is_fh:
            useless_recs_query = """
                                    DELETE FROM tLegacyDeaths WHERE 
                                        (FName LIKE '%.florist%' OR FName LIKE '%-Cups%' OR FName LIKE '%?INTEGRITY%')
                                            OR
                                        (MName LIKE 'football' OR MName LIKE 'href=%' OR MName LIKE '%DIGNITY?%')
                                            OR
                                        (LName LIKE 'link' OR LName LIKE 'dollars' OR LName LIKE 'title=');
                                """
            empty_obitText_query = "DELETE FROM tLegacyObitText WHERE ObitText = 'nan'"
            cursor.execute(empty_obitText_query)
        else:
            useless_recs_query = """
                                    DELETE FROM tLegacyDeaths WHERE 
                                        (FName LIKE '%.florist%' OR FName LIKE '%-Cups%' OR FName LIKE '%?INTEGRITY%')
                                            OR
                                        (MName LIKE 'football' OR MName LIKE 'href=%' OR MName LIKE '%DIGNITY?%')
                                            OR
                                        (LName LIKE 'link' OR LName LIKE 'dollars' OR LName LIKE 'title=');
                                """
        self.message("Deleting useless data")
        cursor.execute(useless_recs_query)

        conn.commit()
        conn.close()


class DeathEntry:
    def __init__(self, conn_func, message, is_fh):
        self.conn_func = conn_func
        self.is_fh = is_fh
        self.message = message

    def Enter(self):
        def df_difference_right(df1, df2):
            # Find rows which are different between two DataFrames.
            comparison_df = df1.merge(
                df2, indicator=True, how='outer', on='SSN')
            right_only = comparison_df[(comparison_df._merge == 'right_only')]
            return right_only

        conn = pyodbc.connect(self.conn_func)
        if self.is_fh:
            death_fetch_query = "SELECT id, SSN, DOD as Died FROM tLegacyDeaths WHERE hasMatch = 1 AND isDead = 1 AND ignore = 0"
        else:
            death_fetch_query = "SELECT id, SSN, DOD as Died FROM tLegacyDeaths WHERE hasMatch = 1 AND isDead = 1 AND ignore = 0"

        try:
            df_deaths_to_enter = pd.read_sql(sql=death_fetch_query, con=conn)
        except Exception as e:
            self.message(str(e))
        else:
            if df_deaths_to_enter.empty:
                raise Exception("No Deaths to enter")

            ssn_in_statement = ', '.join(
                [f"'{i}'" for i in df_deaths_to_enter['SSN']])
            check_query = f"SELECT DISTINCT SSN, Died as date_of_death FROM PersonalData WHERE DiedEnteredDate IS NOT NULL AND SSN IN ({ssn_in_statement})"

            df_deaths_already_entered = pd.read_sql(sql=check_query, con=conn)

            df_to_enter_final = df_difference_right(
                df_deaths_already_entered, df_deaths_to_enter)

            self.message("Entering Deaths")

            cursor = conn.cursor()
            if self.is_fh:
                try:
                    for row in df_to_enter_final.itertuples():
                        if str(row[4]) == 'NaT' or str(row[4]) == pd.NaT:
                            self.message(
                                f"{str(row[3])} does not have a date of death, this will need to be manually checked.",
                                exception=True)
                            continue
                        sp_viator_died = f"""SET NOCOUNT ON; exec sp_ViatorDied 
                                                @SSN = '{str(row[3])}', 
                                                @Died = '{row[4]}', 
                                                @SSADeathMaster = 0, 
                                                @SSDI = 0, 
                                                @Website = 0, 
                                                @WebsiteComment = ' ', 
                                                @Emails = 0, 
                                                @EmailsComment = ' ', 
                                                @Other = 1, 
                                                @OtherComment = 'LDS: Auto', 
                                                @UsrName = 'LDS: Auto', 
                                                @DMFMatching = 0, 
                                                @DODPriorDateCompleted = 0, 
                                                @SSADeathMasterComment = ' ', 
                                                @Comment = ' ', 
                                                @ComServ = 0;"""
                        cursor.execute(sp_viator_died)
                except Exception as e:
                    self.message(str(e), exception=True)
                else:
                    self.message("Successfully Entered Deaths")
                    try:
                        query = "UPDATE tLegacyDeaths SET ignore = 1 WHERE src='df_100' AND isDead = 1"

                        cursor.execute(query)
                    except Exception as e:
                        self.message(
                            "Failed to set deaths in tLegacyDeaths table\nto ignore")
                        # self.message(df_to_enter_final)
                        self.message(str(e), exception=True)
            else:
                try:
                    for row in df_to_enter_final.itertuples():
                        # Check
                        sp_viator_died = f"""SET NOCOUNT ON; exec sp_ViatorDied 
                                                @SSN = '{str(row[3])}', 
                                                @Died = '{row[4]}', 
                                                @SSADeathMaster = 0, 
                                                @SSDI = 0, 
                                                @Website = 0, 
                                                @WebsiteComment = ' ', 
                                                @Emails = 0, 
                                                @EmailsComment = ' ', 
                                                @Other = 1, 
                                                @OtherComment = 'LDS: Auto', 
                                                @UsrName = 'LDS: Auto', 
                                                @DMFMatching = 0, 
                                                @DODPriorDateCompleted = 0, 
                                                @SSADeathMasterComment = ' ', 
                                                @Comment = ' ', 
                                                @ComServ = 0;"""
                        cursor.execute(sp_viator_died)
                except Exception as e:
                    self.message(str(e), exception=True)
                else:
                    self.message("Successfully Entered Deaths")
                    try:
                        query = "UPDATE tLegacyDeaths SET ignore = 1 WHERE src='df_100' AND isDead = 1"

                        cursor.execute(query)
                    except Exception as e:
                        self.message(
                            "Failed to set deaths in tLegacyDeaths table\nto ignore")
                        # self.message(df_to_enter_final)
                        self.message(str(e), exception=True)
            conn.commit()
            conn.close()
