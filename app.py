import csv
import os
import argparse
import sys
import unicodedata

from dotenv import load_dotenv

import dropbox

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
    """Connects to Dropbox, pulls all current day .csv files, parses based on matching criteria,
    created excel report and places back in Dropbox.
    """

    args = parser.parse_args()
    if not args.token:
        print('--please specify a token in .env or via the --token flag')
        sys.exit(2)

    folder = '.' # args.folder
    rootdir = os.path.expanduser('.')

    # Connect to Dropbox folder
    dbx = dropbox.Dropbox(args.token)

    # Pull history file
    hist = pull_file(arg.history-file)

    # Pull company files

    # Load/prep excel report file

    for files in os.walk('.'):
        for name in files:
            fullname = os.path.join(rootdir, name)
    
            # Ensure filename is usable
            if not isinstance(name, str):
                name = name.decode('utf-8')
            name = unicodedata.normalize('NFC', name)
            
            # Generic ignores
            if name.startswith('.'):
                print('Ignoring dot (hidden) file:', name)
            elif name.startswith('@') or name.endswith('~'):
                print('Skipping temporary file:', name)
            elif name.endswith('.pyc') or name.endswith('.pyo'):
                print('Skipping generated file:', name)
            

    


if __name__ == '__main__':
    main()