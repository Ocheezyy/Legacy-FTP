# Legacy-FTP

## Note

This project is form a production environment and all sensitive data
has been redacted. If you would like to utilize this, search for "#"
and replace it with whatever variables are needed.

## Table of Contents

- [Summary](#Summary)
- [Setup](#Setup)
- [Packages](#Packages)
- [Files and Directories](#Files-and-Directories)

### Summary

This is a utility that will run 24/7 that fetches files from an SFTP server.
It then converts the files into dataframes and compares them against data
from our database to check for matches. It will automatically enter anything
it finds is an exact match based on first name, middle name, last name, DOB,
and Zip. The rest will be seen left for manual comparison inside DM.

### Setup

- Install [SQL Server ODBC Driver 13](https://www.microsoft.com/en-us/download/details.aspx?id=50420)
- Install [Python 3.8+](https://www.python.org/downloads/)
- Create venv with requirements.txt
- Start application

### Packages

The packages can also be found in _requirements.txt_

- [pandas~=1.1.0](https://pypi.org/project/pandas/)
- [pysftp~=0.2.9](https://pypi.org/project/pysftp/)
- [openpyxl~=3.0.4](https://pypi.org/project/openpyxl/)
- [numpy~=1.19.1](https://pypi.org/project/numpy/)
- [cryptography~=3.0](https://pypi.org/project/cryptography/)
- [pyodbc~=4.0.30](https://pypi.org/project/pyodbc/)

### Files and Directories

- main.py
  - This is where multiple threads are created and sub processes of main_fh
    and main_np are instantiated
- main_fh.py
  - All subclasses are instantiated and executed for the funeral
    home datasets
- main_np.py
  - All subclasses are instantiated and executed for the newspaper
    datasets
- sftp.py
  - Contains the class to connect and download the files from the
    SFTP server
- Re-Process.py
  - This is used to re-process files upon any errors
- data(Directory)
  - Where output spreadsheets are stored, and local db keeping track of files processed
- logs(Directory)
  - Holds all log files
- conn(Directory)
  - Contains encrypted connections strings
