import csv
import os
import argparse

import dropbox

TOKEN = os.environ.get('DROPBOX_ACCESS_TOKEN')

parser = argparse.ArgumentParser(description='Generate Similar Days excel report')
parser.add_argument('--token', default=TOKEN,
                    help='Access token '
                    '(see https://www.dropbox.com/developers/apps)')
    

def main():
    # Connect to Dropbox folder
    dbx = dropbox.Dropbox()

    # Confirm correct user is signed in
    if dbx.users_get_current_account() != environ.get('CURRENT_ACCOUNTNAME') :
        print('Not connected to correct account. Please fix in ./.env')
        quit()

    


if __name__ == '__main__':
    main()