import csv
import os
import argparse
import sys
import unicodedata
import glob
import math
import calendar

from dotenv import load_dotenv

import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils.cell import get_column_letter
import dropbox
import boto3

load_dotenv(verbose=True)

TOKEN=os.getenv("DROPBOX_ACCESS_TOKEN")
APP_KEY=os.getenv("DROPBOX_APP_KEY")


parser = argparse.ArgumentParser(description='Generate Similar Days excel report')
parser.add_argument('--token', default=TOKEN,
                    help='Access token '
                    '(see https://www.dropbox.com/developers/apps)')
parser.add_argument('--history-file', default='history.csv',
                    help='3-year history reference file')
    

def main():
    """Connects to Dropbox, pulls all current day .csv files, 
    parses based on matching criteria,
    creates excel report and places back in Dropbox.
    """

    args = parser.parse_args()
    if not args.token:
        print('--please specify a token in .env or via the --token flag')
        sys.exit(2)
    
    report = SimilarDayReport('dropbox-local/Similar Days March.xlsx', 'dropbox-local/Similar Days March_20200402.xlsx', 'dropbox-local/historical')
    report.generate()
    report.save()

class SimilarDayReport:

    companies = {'CKY': 32, 'COH': 34, 'CMD': 35, 'CPA': 37, 'CGV': 38, 'CMA': 80}

    def __init__(self, report_file, output_file, hist_folder):
        self.report_filename = report_file
        self.output_file = output_file
        self.histor_folder = hist_folder
        self.wb = load_workbook(self.report_filename, data_only=True)
    
    def generate(self):
        for sheet in self.wb.worksheets:
            # if sheet.title != 'CKY':
            #     continue
            for cell in sheet.iter_cols(min_col=2, min_row=6, max_row=6):
                col = get_column_letter(cell[0].column)
                # if not (sheet[f'{col}16'].value is None and sheet[f'{col}23'].value is None and sheet[f'{col}30'].value is None):
                if sheet[f'{col}6'].value == "" or sheet[f'{col}6'].value is None:
                    break

                df_hist = self.load_historical(sheet.title, self.hist_folder)
                
                day = {}
                day['COMPANY'] = self.companies[sheet.title]
                day['GAS_DATE'] = sheet[f'{col}4'].value
                day['DTH'] = sheet[f'{col}6'].value
                day['GAS_DAY_AVG_TMP'] = sheet[f'{col}8'].value
                day['PRIOR_TEMP'] = sheet[f'{col}9'].value
                day['GAS_DAY_WIND_SPEED'] = sheet[f'{col}10'].value
                day['DAY_TYPE'] = sheet[f'{col}7'].value

                df_day = pd.DataFrame([day], columns=list(df_hist))
                df_matches = self.find_similar(df_day, df_hist, 3)

                avg_similar_day = (df_matches.iloc[0]['DTH'] + df_matches.iloc[1]['DTH'] + df_matches.iloc[2]['DTH']) / 3
                percent_diff = ((df_day.iloc[0]['DTH'] - avg_similar_day)/avg_similar_day)

                print(df_day)
                print()
                print(percent_diff)
                print(avg_similar_day)
                print()
                print(df_matches)

                sheet[f'{col}12'] = percent_diff
                sheet[f'{col}14'] = avg_similar_day

                for i in range(0, len(df_matches)):
                    sheet[f'{col}{16+(7*i)}'] = df_matches.iloc[i]['DTH']
                    sheet[f'{col}{17+(7*i)}'] = df_matches.iloc[i]['GAS_DATE']
                    sheet[f'{col}{18+(7*i)}'] = df_matches.iloc[i]['DAY_SHORTNAME']
                    sheet[f'{col}{19+(7*i)}'] = df_matches.iloc[i]['GAS_DAY_AVG_TMP']
                    sheet[f'{col}{20+(7*i)}'] = df_matches.iloc[i]['PRIOR_TEMP']
                    sheet[f'{col}{21+(7*i)}'] = df_matches.iloc[i]['GAS_DAY_WIND_SPEED']
    
    def load_historical(self, company_name, folder_path):
        filename = os.path.join(folder_path, f'{company_name} data.csv') # NEED TO UPDATE FOR S3
        df = pd.read_csv(filename)
        df.rename(columns = {'DayType':'DAY_TYPE'}, inplace=True) # rename for column name consistency
        df['DTH'] = df['DTH'].apply(lambda x: x/1000) # convert to dekatherm
        df['GAS_DATE'] = pd.to_datetime(df['GAS_DATE']) # convert to datetime
        df.reset_index()
        return df

    def find_similar(self, df_day, df_hist, num_matches):
        """Criteria
        +/- 2 degrees
        Start on minus year, same day"""
        df_work = df_hist.copy(deep=True)
        df_work = df_work[df_work['GAS_DATE'] > pd.to_datetime('20190101')]
        df_work = df_work[df_work['GAS_DATE'] < pd.to_datetime('20200301')]
        
        df_work = df_work[df_work['GAS_DAY_AVG_TMP'] > df_day.iloc[0]['GAS_DAY_AVG_TMP'] - 2]
        df_work = df_work[df_work['GAS_DAY_AVG_TMP'] < df_day.iloc[0]['GAS_DAY_AVG_TMP'] + 2]

        df_work['DAY_SHORTNAME'] = df_work['GAS_DATE'].dt.dayofweek.apply(to_dayname)
        # df_work = df_work[df_work['DAY_SHORTNAME'] == df_day.iloc[0]['DAY_TYPE']]
        # df_work['DAYOFWEEK_DELTA'] = abs(df_work['GAS_DATE'].dt.dayofweek - df_day.iloc[0]['GAS_DATE'].dayofweek)
        df_work['SAME_DAYOFWEEK_MULTIPLE'] = abs((df_work['DAY_TYPE'] == to_daytype(df_day.iloc[0]['DAY_TYPE'])).astype(int) - 1) + 1 # same day type => 1, opposing = 2 (divided by at the end)
        df_work['TMP_DELTA'] = abs(df_work['GAS_DAY_AVG_TMP'] - df_day.iloc[0]['GAS_DAY_AVG_TMP'])
        df_work['TIME_DELTA'] = abs(df_work['GAS_DATE'].dt.year*1 - df_day.iloc[0]['GAS_DATE'].year*1) + \
            abs(df_work['GAS_DATE'].dt.month*100 - df_day.iloc[0]['GAS_DATE'].month*100) + \
            abs(df_work['GAS_DATE'].dt.day*10 - df_day.iloc[0]['GAS_DATE'].day*10) # in order of weightedness => (month, day, year)
        df_work['WIND_DELTA'] = abs(df_work['GAS_DAY_WIND_SPEED'] - df_day.iloc[0]['GAS_DAY_WIND_SPEED'])
        df_work['DTH_DELTA'] = abs(df_work['DTH'] - df_day.iloc[0]['DTH'])

        df_work['DELTA_WEIGHTED'] = abs(df_work['TIME_DELTA'] - df_work['WIND_DELTA']/2 / (df_work['SAME_DAYOFWEEK_MULTIPLE']/0.5))

        df_work.sort_values(by=['DELTA_WEIGHTED'], inplace = True)

        return df_work.head(num_matches).reset_index()

    def save(self):
        self.wb.save(self.output_file)    
    
def to_dayname(day):
        return ['Mon', 'Tues', 'Wed', 'Thurs', 'Fri', 'Sat', 'Sun'][day]

def to_daytype(day):
    return 'Weekend' if day.upper() in ['SAT', 'SUN'] else 'Weekday'

if __name__ == '__main__':
    main()
