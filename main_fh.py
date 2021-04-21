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

t = dt2.now()
root = os.path.dirname(__file__)
spreadsheet_path = join(
    root, "data\\ae_output\\{}.xlsx".format(dt2.now().strftime('%b-%d')))
ss_filename = dt2.now().strftime('%b-%d' + '.xlsx')

log_name = f"{str(dt2.now().strftime('%b-%d'))}.log"
log_path = os.path.join(root, f"logs\\{log_name}")
db_path = join(root, "data\\info.db")

fh_logger = Logger(log_path, "FH")


class MainFh:
    def fh_main(self):
        if not isfile(db_path):
            fh_message("Failed to find sqlite DB", critical=True)
            return

        from sftp import Sftp
        fh_ftp = Sftp(host="sftp.placeholder.com", user="user", passwd="pass", remote_directory="/daily/",
                      local_directory=join(root, "data\\to_process"), fh=True, message=fh_message)

        fh_filename = None

        try:
            fh_filename = fh_ftp.check()
        except Exception as e:
            pass
        else:
            if fh_filename is None or fh_filename == "":
                print("Funeral Home: No new files found")
                if dt2.now().hour > 9:
                    time.sleep(57900)
                else:
                    time.sleep(1800)
                return
            fh_ftp.update(fh_filename)

        try:
            fh_message("Starting SFTP Download...")
            fh_ftp.download()
        except FileNotFoundError as e:
            if str(e).find("fh") != -1:
                fh_message("Failed to fetch funeral home file", exception=True)
            else:
                fh_message(str(e), warning=True)
        except Exception as e:
            fh_message(msg=traceback.format_exc(), exception=True)
        else:
            fh_message("Files successfully fetched")

        from data_handling import Converter
        converter = Converter(filename=fh_filename, message=fh_message)
        csv_error_check = False

        try:
            fh_filename = converter.convert_fh()
        except IOError as e:
            if str(e).find("already") != -1:
                fh_message("Re-processing CSV for deaths", warning=True)
        except Exception as e:
            csv_error_check = True
            fh_message(msg=traceback.format_exc(), error=True)

        if not csv_error_check:
            from data_handling import Comparison
            fh_compare = Comparison(
                message=fh_message, filename=fh_filename, is_fh=True)
            try:
                dflength = fh_compare.intake(conn=get_conn())
            except FileNotFoundError as e:
                if str(e).find("CSV") != -1:
                    fh_message("Unable to find CSV", exception=True)
            except Exception as e:
                fh_message(msg=traceback.format_exc(), exception=True)
            else:
                fh_message("Formatted dataframes")
                try:
                    df2 = fh_compare.fetch_deaths(
                        conn=pyodbc.connect(get_conn()), dflength=dflength)
                    if df2.empty:
                        fh_message("No possible matches", warning=True)
                        from data_handling import Cleanup
                        fh_cleanup = Cleanup(
                            root=root, filename=fh_filename, conn_func=get_conn(), message=fh_message, is_fh=True)
                        fh_cleanup.clean_files()
                        fh_cleanup.clean_db_table()
                        return
                except Exception as e:
                    fh_message(msg=str(e), exception=True)
                else:
                    from data_handling import df_manipulation
                    fh_manip = df_manipulation(
                        df2, conn=get_conn(), is_fh=True)
                    try:
                        df3 = fh_manip.checks()
                    except Exception as e:
                        fh_message("Failed on condition checking\n" +
                                   str(traceback.format_exc()), exception=True)
                    else:
                        try:
                            fh_message("Finished matching condition")
                            if df3.empty:
                                fh_message("No rows to update")
                            else:
                                columns = ['hasMatch', 'isDead', 'src', 'Index', 'SSN', 'sex', 'FName', 'FName_L',
                                           'Middle', 'Middle_L',
                                           'LName', 'LName_L', 'DOB', 'DOB_L', 'DOD_L', 'DateRecd', 'State',
                                           'State_Abbrev', 'State_L', 'l_id']
                                df3 = df3.reindex(columns=columns)
                                fh_manip.update_table(
                                    df3[['l_id', 'SSN', 'isDead', 'src']])
                        except Exception as e:
                            fh_message("Failed to update table\n" +
                                       str(traceback.format_exc()), exception=True)

                        from data_handling import DeathEntry
                        fh_death_entry = DeathEntry(
                            conn_func=get_conn(), message=fh_message, is_fh=True)
                        try:
                            fh_death_entry.Enter()
                        except Exception as e:
                            if str(e).find("No Deaths to enter") != -1:
                                fh_message("No deaths to enter")
                            else:
                                fh_message(str(e), exception=True)

                        from data_handling import Cleanup
                        fh_cleanup = Cleanup(
                            root=root, filename=fh_filename, conn_func=get_conn(), message=fh_message, is_fh=True)

                        fh_cleanup.clean_files()

                        fh_cleanup.clean_db_table()

                        try:
                            total = generate_spreadsheet(get_conn())
                        except Exception as e:
                            fh_message(
                                "Failed to generate spreadsheet:\n" + str(e))
                        else:
                            sendmail(path=spreadsheet_path, filename=ss_filename,
                                     totals=total)
                            time.sleep(2)
        time.sleep(5)


def fh_message(msg, warning=False, exception=False, critical=False):
    print("Funeral Home: " + msg)
    if warning:
        fh_logger.log_warning(msg)
    elif exception:
        fh_logger.log_error(msg)
        if msg.find("E-Mail") == -1:
            sendmail(path=log_path, error=True, filename=log_name)
    elif critical:
        fh_logger.log_critical(msg)
        sendmail(path=log_path, error=True, filename=log_name)
    else:
        fh_logger.log_info(msg)


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

    # total_query = "SELECT ID, FName, MName, LName, DOB, DOD, DateEntered FROM ##### WHERE isFH = 1 AND DateEntered > DATEADD(dd, -1, GETDATE())"
    match_query = "SELECT ID, src, SSN, FName, MName, LName, DOB, DOD, DateEntered FROM ##### WHERE isFH = 1 AND DateEntered > DATEADD(dd, -1, GETDATE()) AND hasMatch = 1"

    # total_df = pd.read_sql(total_query, conn)
    match_df = pd.read_sql(match_query, conn)

    with pd.ExcelWriter(spreadsheet_path) as writer:
        # total_df.to_excel(writer, sheet_name='Total Entered', index=False)
        match_df.to_excel(writer, sheet_name='Matches', index=False)

    return len(match_df.index)


def sendmail(path, filename, error=False, totals=0):
    EMAIL = "#####"
    PASS = "####"
    MAILSERVER = "#####"
    PORTTLS = 25
    FROM = "#####"
    SUBJECT = "Funeral Home Intake " + \
        str(dt2.now().strftime('%m-%d-%Y'))
    EMAIL_TO = ["####"]

    msg = MIMEMultipart()
    msg["From"] = EMAIL
    msg["To"] = ', '.join(EMAIL_TO)
    msg["Subject"] = SUBJECT

    if error:
        body = "Error encountered, please check logs for more details."
    else:
        body = f"Matches found: {str(totals)}\nThe matches have been successfully exported to Death Manager."
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
        fh_message('E-Mail authentication error', exception=True)
    except Exception as e:
        fh_message('Failed to send E-mail\n' +
                   str(traceback.format_exc()), exception=True)
    else:
        fh_message('Email sent')
