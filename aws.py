import os
import sys

import boto3
import dropbox
from dropbox.exceptions import AuthError

from dotenv import load_dotenv

load_dotenv()

TOKEN = os.environ['DROPBOX_ACCESS_TOKEN']

def connect_to_dropbox():
    if (len(TOKEN) == 0):
        sys.exit("Please ensure the Dropbox Access Token has been added to the environment")

    # Create an instance of a Dropbox class, which can make requests to the API.
    print("Creating a Dropbox object...")
    dbx = dropbox.Dropbox(TOKEN)
    
    try:
        print('Attempting to get current account')
        dbx.users_get_current_account()
        print('Connected.')
    except AuthError:
        sys.exit("ERROR: Invalid access token; try re-generating an "
            "access token from the app console on the web.")

    for entry in dbx.files_list_folder('/NiSource').entries:
        print(entry.name)


def from_dropbox():
    pass

def to_dropbox():
    pass