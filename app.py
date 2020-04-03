import csv
import os
import argparse
import sys
import unicodedata
import glob
import math
import calendar
import pathlib

from dotenv import load_dotenv

import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils.cell import get_column_letter
import dropbox
import boto3

load_dotenv(verbose=True)

TOKEN=os.getenv("DROPBOX_ACCESS_TOKEN")
APP_KEY=os.getenv("DROPBOX_APP_KEY")   

def main():
    """Connects to Dropbox, pulls all current day .csv files, 
    parses based on matching criteria,
    creates excel report and places back in Dropbox.
    """
    
    report = SimilarDayReport()
    report.generate(save=False)

class SimilarDayReport:

    companies = {'CKY': 32, 'COH': 34, 'CMD': 35, 'CPA': 37, 'CGV': 38, 'CMA': 80}

    def __init__(self):
        self.daily_file = self.get_daily_file()
        self.report_file = self.get_current_report()

        self.daily_folder = '.'
        self.hist_folder = '/historical'
        self.backup_folder = '/bak'
        self.wb = load_workbook(self.report_file, data_only=True)
    
    def generate(self, save=True):
        if save():
            self.create_backup()

        df_daily = self.load_daily(self.working_file, self.daily_folder)
        for _, df_row in df_daily.iterrows():
            for sheet in self.wb.worksheets:
                df_hist = self.load_historical(sheet.title, self.hist_folder)
                df_day = df_row[df_daily['COMPANY'] == self.companies[sheet.title]]
                for cell in sheet.iter_cols(min_col=2, min_row=6, max_row=6):
                    col = get_column_letter(cell[0].column)
                    if not (sheet[f'{col}16'].value is None and sheet[f'{col}23'].value is None and sheet[f'{col}30'].value is None): # only continue if Similar Days are empty
                        continue
                    
                    print(f'Found blank day in {sheet.title}: ', sheet[f'{col}4'].value)

                    # df_day = df_hist[df_hist['GAS_DATE'] == pd.to_datetime(sheet[f'{col}4'].value)].reset_index() # modify this line to adjust for incoming data format
                    df_matches = self.find_similar(df_day, df_hist, 3)

                    avg_similar_day = (df_matches.iloc[0]['DTH'] + df_matches.iloc[1]['DTH'] + df_matches.iloc[2]['DTH']) / 3
                    pct_diff = ((df_day.iloc[0]['DTH'] - avg_similar_day)/avg_similar_day)

                    sheet[f'{col}12'] = pct_diff
                    sheet[f'{col}14'] = avg_similar_day

                    self.pprint(df_day, pct_diff, avg_similar_day, df_matches)

                    for i in range(0, len(df_matches)):
                        sheet[f'{col}{16+(7*i)}'] = df_matches.iloc[i]['DTH']
                        sheet[f'{col}{17+(7*i)}'] = df_matches.iloc[i]['GAS_DATE']
                        sheet[f'{col}{18+(7*i)}'] = df_matches.iloc[i]['DAY_SHORTNAME']
                        sheet[f'{col}{19+(7*i)}'] = df_matches.iloc[i]['GAS_DAY_AVG_TMP']
                        sheet[f'{col}{20+(7*i)}'] = df_matches.iloc[i]['PRIOR_TEMP']
                        sheet[f'{col}{21+(7*i)}'] = df_matches.iloc[i]['GAS_DAY_WIND_SPEED']
            
        if save:
            self.save()
    
    def get_daily_file(self):
        newest = min(glob.iglob('* data.csv'), key=os.path.getctime)
        print(f'Using daily file {newest}')
        return newest

    def get_current_report(self):
        newest = min(glob.iglob('Similar Days*.xlsx'), key=os.path.getctime)
        print(f'Using daily file {newest}')
        return newest

    def load_daily(self, date, folder_path):
        filename = os.path.join(folder_path, f'{date} data.csv') # NEED TO UPDATE FOR S3
        df = pd.read_csv(filename)
        df.rename(columns = {'DayType':'DAY_TYPE'}, inplace=True) # rename for column name consistency
        df['DTH'] = df['DTH'].apply(lambda x: x/1000) # convert to dekatherm
        df['GAS_DATE'] = pd.to_datetime(df['GAS_DATE']) # convert to datetime
        df['DAY_SHORTNAME'] = df['GAS_DATE'].dt.dayofweek.apply(to_dayname) # add shortname column (Mon, Tues)
        df.sort_values(by=['GAS_DATE']).reset_index()
        return df

    def load_historical(self, company_name, folder_path):
        filename = os.path.join(folder_path, f'{company_name} data.csv') # NEED TO UPDATE FOR S3
        df = pd.read_csv(filename)
        df.rename(columns = {'DayType':'DAY_TYPE'}, inplace=True) # rename for column name consistency
        df['DTH'] = df['DTH'].apply(lambda x: x/1000) # convert to dekatherm
        df['GAS_DATE'] = pd.to_datetime(df['GAS_DATE']) # convert to datetime
        df['DAY_SHORTNAME'] = df['GAS_DATE'].dt.dayofweek.apply(to_dayname) # add shortname column (Mon, Tues)
        df.reset_index()
        return df

    def find_similar(self, df_day, df_hist, num_matches):
        """Criteria:
        +/- 2 degrees
        Start on minus year, same day"""
        factor_year = 1.0
        factor_month = 10.0
        factor_day = 2.0
        factor_time = 2.0
        factor_wind = 1.25
        factor_dayofweek = 2.0

        df_work = df_hist.copy(deep=True)
        df_work = df_work[df_work['GAS_DATE'] > pd.to_datetime('20190101')]
        df_work = df_work[df_work['GAS_DATE'] < pd.to_datetime('20200301')]
        df_work = df_work[df_work['GAS_DAY_AVG_TMP'] > df_day.iloc[0]['GAS_DAY_AVG_TMP'] - 2]
        df_work = df_work[df_work['GAS_DAY_AVG_TMP'] < df_day.iloc[0]['GAS_DAY_AVG_TMP'] + 2]

        df_work['YEAR_DELTA'] = (abs(df_work['GAS_DATE'].dt.year - df_day.iloc[0]['GAS_DATE'].year)+1) * factor_year 
        df_work['MONTH_DELTA'] = (abs(df_work['GAS_DATE'].dt.month - df_day.iloc[0]['GAS_DATE'].month)+1) * factor_month
        df_work['DAY_DELTA'] = (abs(df_work['GAS_DATE'].dt.day - df_day.iloc[0]['GAS_DATE'].day)+1) * factor_day
        df_work['TIME_DELTA'] = df_work['YEAR_DELTA'] + df_work['MONTH_DELTA'] + df_work['DAY_DELTA'] * factor_time

        df_work['WIND_DELTA'] = abs(df_work['GAS_DAY_WIND_SPEED'] - df_day.iloc[0]['GAS_DAY_WIND_SPEED']) * factor_wind
        df_work['DAYOFWEEK_MULTIPLE'] = (abs((df_work['DAY_TYPE'] == to_daytype(df_day.iloc[0]['DAY_TYPE'])).astype(int) - 1) + 1) * factor_dayofweek

        df_work['TMP_DELTA'] = abs(df_work['GAS_DAY_AVG_TMP'] - df_day.iloc[0]['GAS_DAY_AVG_TMP']) # display only, not used in final weight
        df_work['DTH_DELTA'] = abs(df_work['DTH'] - df_day.iloc[0]['DTH']) # display only, not used in final weight

        df_work['DELTA_WEIGHTED'] = abs(df_work['TIME_DELTA'] - df_work['WIND_DELTA']) * df_work['DAYOFWEEK_MULTIPLE']

        df_work.sort_values(by=['DELTA_WEIGHTED'], inplace=True)

        return df_work.head(num_matches).reset_index()

    def save(self):
        # Save old workbook as Report Month_yestdate.xlsx (archive)
        # Save new workbook as Report Month.xlsx
        self.wb.save(self.output_file)

        # Append daily data to each company file
        # Archive daily data 

        pathlib.Path(backup_folder)

    def create_backup(self):
        """Creates temporary backups for all files in case rollback is needed.
        These backups are deleted upon successful save.
        """
        import shutil

        pathlib.Path(self.backup_folder).mkdir(exist_ok=True) # create backup directory, overwriting if necessary
        pathlib.Path(self.backup_folder + self.hist_folder).mkdir(exist_ok=True)
        daily_filepath = pathlib.Path(self.daily_file)
        report_filepath = pathlib.Path(self.report_file)
        history_folderpath = pathlib.Path(self.hist_folder)

        # Create backups
        daily_bak = shutil.copy(daily_filepath, daily_filepath.with_suffix('.bak'))
        print(f'{daily_bak} created')
        report_bak = shutil.copy(report_filepath, report_filepath.with_suffix('.bak'))
        print(f'{report_bak} created')
        for f in history_folderpath.iterdir():
            hist_bak = shutil.copy(f, f.with_suffix('.bak'))
            print(f'{hist_bak} created')

        # validation check
        daily_ok = daily_filepath.with_suffix('.bak').is_file()
        report_ok = report_filepath.with_suffix('.bak').is_file()
        hist_ok = all(f.is_file() for f in history_folderpath.iterdir())

        if not (daily_ok and report_ok and hist_ok):
            print('Backup creation failed. Exiting...')
            sys.exit(1)

    def delete_backup(self):
        backup_dir = pathlib.Path(self.backup_folder)
        for f in backup_dir.rglob('*'):
            f.unlink()
        backup_dir.rmdir()
        print('Successfully deleted backups')
        return True

    def pprint(self, df_day, pct_diff, avg_similar_day, df_matches):
        print(f'{df_day}')
        print(f'{df_matches}\n')
        print(f'Percent difference: {pct_diff*100:.2f}%')
        print(f'Avg similar days: {avg_similar_day}\n')
        print(f'{"-"*80}\n')

    def log(self, *args):
        for arg in args:
            if type(arg) == list:
                for a in arg:
                    print(f'{a}')
                print()
            else:
                print(f'{arg}\n')
    
def to_dayname(day):
        return ['Mon', 'Tues', 'Wed', 'Thurs', 'Fri', 'Sat', 'Sun'][day]

def to_daytype(day):
    return 'Weekend' if day.upper() in ['SAT', 'SUN'] else 'Weekday'


if __name__ == '__main__':
    main()
