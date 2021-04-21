from email.mime.multipart import MIMEMultipart
from cryptography.fernet import Fernet
from datetime import datetime as dt2
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from data_handling import Logger
from os.path import join, isfile
from email import encoders
import pandas as pd
import traceback
import smtplib
import pyodbc
import time
import os
import ssl
import sys

root = os.path.dirname(__file__)
spreadsheet_path = join(
    root, "data\\ae_output\\{}.xlsx".format(dt2.now().strftime('%b-%d')))
ss_filename = dt2.now().strftime('%b-%d' + '.xlsx')

log_name = f"{str(dt2.now().strftime('%b-%d'))}.log"
log_path = os.path.join(root, f"logs\\{log_name}")
db_path = join(root, "data\\info.db")

np_logger = Logger(log_path, 'NP')


class MainNP:
    def np_main(self):
        db_error_count = 0
        if not isfile(db_path):
            db_error_count = db_error_count + 1
            if db_error_count > 0:
                sys.exit(10)
            np_message("Failed to find sqlite DB", critical=True)

        from sftp import Sftp
        np_ftp = Sftp(host="sftp.placeholder.com", user="user", passwd="pass", remote_directory="/daily/",
                      local_directory=join(root, "data\\to_process"), fh=False, message=np_message)

        np_filename = None

        try:
            np_filename = np_ftp.check()
        except Exception as e:
            pass
        else:
            if np_filename is None or np_filename == "":
                print("Newspaper: No new files found")
                now = dt2.now()
                if now.hour > 9:
                    time.sleep(57900)
                else:
                    time.sleep(1800)
                return
            np_ftp.update(np_filename)

        try:
            np_message("Starting SFTP Download...")
            np_ftp.download()
        except FileNotFoundError as e:
            np_message("Failed to fetch newspaper file", exception=True)
        except Exception as e:
            np_message(msg=traceback.format_exc(), exception=True)
        else:
            np_message("Files successfully fetched")

        from data_handling import Comparison
        np_compare = Comparison(
            message=np_message, filename=np_filename, is_fh=False)
        try:
            dflength = np_compare.intake(conn=get_conn())
        except FileNotFoundError as e:
            if str(e).find("CSV") != -1:
                np_message("Unable to find CSV", exception=True)
        except Exception as e:
            np_message(msg=traceback.format_exc(), exception=True)
        else:
            np_message("Formatted dataframes")
            try:
                df2 = np_compare.fetch_deaths(
                    conn=pyodbc.connect(get_conn()), dflength=dflength)
                if df2.empty:
                    np_message("No possible matches", warning=True)
                    from data_handling import Cleanup
                    np_cleanup = Cleanup(root=root, filename=np_filename, conn_func=get_conn(), message=np_message,
                                         is_fh=False)
                    np_cleanup.clean_files()
                    np_cleanup.clean_db_table()
            except Exception as e:
                np_message(msg=str(e), exception=True)
            else:
                from data_handling import df_manipulation
                np_manip = df_manipulation(df2, conn=get_conn(), is_fh=False)
                try:
                    df3 = np_manip.checks()
                except Exception as e:
                    np_message("Failed on condition checking\n" +
                               traceback.format_exc(), exception=True)
                else:
                    try:
                        np_message("Finished matching condition")
                        if df3.empty:
                            np_message("No rows to update")
                        # else:
                        # columns = ['hasMatch', 'isDead', 'src', 'Index', 'SSN', 'sex', 'FName', 'FName_L', 'Middle', 'Middle_L',
                        #         'LName', 'LName_L', 'DOB', 'DOB_L', 'DOD_L', 'DateRecd', 'State',
                        #         'State_Abbrev', 'State_L', 'l_id']
                        # df3 = df3.reindex(columns=columns)
                        else:
                            np_manip.update_table(
                                df3[['l_id', 'SSN', 'isDead', 'src']])
                    except Exception as e:
                        np_message("Failed to update table\n" +
                                   str(traceback.format_exc()), exception=True)

                    from data_handling import DeathEntry
                    np_death_entry = DeathEntry(
                        conn_func=get_conn(), message=np_message, is_fh=False)
                    try:
                        np_death_entry.Enter()
                    except Exception as e:
                        if str(e).find("No Deaths to enter") != -1:
                            np_message("No deaths to enter")
                        else:
                            np_message(str(e), exception=True)

                    from data_handling import Cleanup
                    np_cleanup = Cleanup(root=root, filename=np_filename, conn_func=get_conn(), message=np_message,
                                         is_fh=False)

                    np_cleanup.clean_files()
                    np_cleanup.clean_db_table()

                    try:
                        total = generate_spreadsheet(get_conn())
                    except Exception as e:
                        np_message(
                            "Failed to generate spreadsheet: \n" + str(e))
                    else:
                        sendmail(path=spreadsheet_path, filename=ss_filename,
                                 totals=total)
                        time.sleep(2)
        time.sleep(2)


def np_message(msg, warning=False, exception=False, critical=False):
    print("Newspaper: " + msg)
    if warning:
        np_logger.log_warning(msg)
    elif exception:
        np_logger.log_error(msg)
        np_logger.log_error(msg)
        if msg.find("E-Mail") == -1:
            sendmail(path=log_path, error=True, filename=log_name)
    elif critical:
        np_logger.log_critical(msg)
        sendmail(path=log_path, error=True, filename=log_name)
    else:
        np_logger.log_info(msg)


def get_conn():
    encrypted_str = open(join(root, 'conn\\connectionlive.key'), 'rb')
    encrypted = encrypted_str.read()

    kr = open(join(root, 'data\\tmp\\key.key'), 'rb')
    k = kr.read()
    f = Fernet(k)
    decrypted = f.decrypt(encrypted)
    return decrypted.decode("utf-8")


def generate_spreadsheet(conn_func):
    conn = pyodbc.connect(conn_func)

    # total_query = "SELECT ID, FName, MName, LName, DOB, DOD, DateEntered FROM tLegacyDeaths WHERE isFH = 0 AND DateEntered > DATEADD(dd, -1, GETDATE())"
    match_query = "SELECT ID, src, SSN, FName, MName, LName, DOB, DOD, DateEntered FROM tLegacyDeaths WHERE isFH = 0 AND DateEntered > DATEADD(dd, -1, GETDATE()) AND hasMatch = 1"

    # total_df = pd.read_sql(total_query, conn)
    match_df = pd.read_sql(match_query, conn)

    with pd.ExcelWriter(spreadsheet_path) as writer:
        # total_df.to_excel(writer, sheet_name='Total Entered', index=False)
        match_df.to_excel(writer, sheet_name='Matches', index=False)

    return len(match_df.index)


def sendmail(path, filename, error=False, totals=0):
    EMAIL = "#####"
    PASS = "######"
    MAILSERVER = "######"
    PORTTLS = 25
    FROM = "######"
    SUBJECT = "Newspaper Intake " + str(dt2.now().strftime('%m-%d-%Y'))
    EMAIL_TO = ["#######"]

    msg = MIMEMultipart()
    msg["From"] = EMAIL
    msg["To"] = ', '.join(EMAIL_TO)
    msg["Subject"] = SUBJECT

    if error:
        body = "Error encountered, please check logs for more details."
    else:
        body = f"Matches: {str(totals)}\nThe others have been successfully imported."
    msg.attach(MIMEText(body, 'plain'))
    attachment = MIMEBase('application', "octet-stream")
    attachment.set_payload(open(path, "rb").read())
    encoders.encode_base64(attachment)
    attachment.add_header('Content-Disposition',
                          'attachment', filename=filename)
    msg.attach(attachment)

    context = ssl.create_default_context()
    try:
        server = smtplib.SMTP(MAILSERVER, PORTTLS)
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(EMAIL, PASS)
        server.sendmail(FROM, EMAIL_TO, msg.as_string())
    except smtplib.SMTPAuthenticationError:
        np_message('E-Mail authentication error', exception=True)
    except Exception as e:
        np_message('Failed to send E-mail\n' +
                   str(traceback.format_exc()), exception=True)
    else:
        np_message('Email sent')
