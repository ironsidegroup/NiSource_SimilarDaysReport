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
- --history_folder, default=HISTORICAL

Flow
- (NS) Drop daily csv file in dropbox folder
- (AWS) Kick off
    - Pick current report
    - Generate report
    - Save
    - Archive daily csv to /archive/{date}/

Unused code:
```

# df_work = df_work[df_work['DAY_SHORTNAME'] == df_day.iloc[0]['DAY_TYPE']]
# df_work['DAYOFWEEK_DELTA'] = abs(df_work['GAS_DATE'].dt.dayofweek - df_day.iloc[0]['GAS_DATE'].dayofweek)

# --- #

# day = {}
# day['COMPANY'] = self.companies[sheet.title]
# day['GAS_DATE'] = sheet[f'{col}4'].value
# day['DTH'] = sheet[f'{col}6'].value
# day['GAS_DAY_AVG_TMP'] = sheet[f'{col}8'].value
# day['PRIOR_TEMP'] = sheet[f'{col}9'].value
# day['GAS_DAY_WIND_SPEED'] = sheet[f'{col}10'].value
# day['DAY_TYPE'] = sheet[f'{col}7'].value

# df_day = pd.DataFrame([day], columns=list(df_hist))

# --- #

print('WRITING TO CSV')
pd.concat([df_day, df_work.head(15)]).to_csv(f'dropbox-local/matches/{list(self.companies.keys())[list(self.companies.values()).index(df_day.iloc[0]["COMPANY"])]}_matches.csv')
print('WROTE TO CSV')

# --- #

```