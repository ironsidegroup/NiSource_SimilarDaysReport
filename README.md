The purpose of this package is to take daily dropped files from Dropbox, perform matching based on specified criteria, and add to excel file

Stack:
- Python
    - CSV (stdlib)
    - Dropbox 
    - Pandas?
    - OpenPyXL

Dropbox folder structure:
    - /archive
        - 20200104
            - company1_20200104.csv
            - company2_20200104.csv
            - company3_20200104.csv
            - company4_20200104.csv
            - company5_20200104.csv
            - company6_20200104.csv
    - company1.csv
    - company2.csv
    - company3.csv
    - company4.csv
    - company5.csv
    - company6.csv

Flags
- --token, default=TOKEN
- --archive, default=yes
- --report_file, default=LOAD VIA FUNCTION

Flow
- (NS) Drop daily csv file in dropbox folder
- (AWS) Kick off
    - Pick current report
    - Generate report
    - Save
    - Archive daily csv to /archive/{date}/