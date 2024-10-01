#!/usr/bin/env python
# Refer to tropomi_data_download.ipynb for additional details

import requests
import json
from datetime import datetime, time
import pandas as pd
import sys 
import os
import zipfile
import io
import argparse
import configparser

### Functions ##########
def valid_date(date_str):
    try:
        datetime.strptime(date_str, "%Y%m%d")
        return date_str
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: '{date_str}'. Expected YYYYMMDD.")

def get_credentials(path):
    config = configparser.ConfigParser()
    config.read(path)
    user = config.get('credentials','username')
    passwd = config.get('credentials','password')
    return user, passwd

# Parse input arguments
parser = argparse.ArgumentParser(description='User-specified parameters')
parser.add_argument('parameter', metavar= 'X', type=str,
                    choices=['CH4', 'CO'], help='Atmospheric parameter (CH4 or CO)')
parser.add_argument('date', metavar='YYYYMMDD', type=valid_date, help='Date')
args = parser.parse_args()
parameter, date_str = args.parameter, args.date
date = datetime.strptime(date_str, "%Y%m%d")

# Define querying variables
if parameter=='CH4':
    product = 'L2__CH4___' # SENTINEL-5P CH4 nomenclature
elif parameter=='CO':
    product = 'L2__CO____' # SENTINEL-5P CO
start_date = date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] +'Z'
end_date = datetime.combine(date,time.max).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] +'Z'

# Define authentication variables
client_id = 'cdse-public'
token_url = 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token'
grant_type = 'password'

# Locate authentication credentials
home_dir = os.path.expanduser("~")
auth_path = home_dir + '/.copernicus_auth.ini'

# Obtain Authentication Token
while True:
    username, password = get_credentials(auth_path)
    auth_data = {'client_id': client_id, 'username': username, 'password': password, 
               'grant_type': grant_type}
    
    # Make a POST request
    response = requests.post(token_url, data=auth_data)
    
    # Confirm request successful
    if response.status_code == 200:
         # Parse the JSON response and extract token
         token_data = response.json()
         access_token = token_data.get('access_token')
         # Check access token retrieved + print if so
         if access_token:
             print(f'Authentication Token Retrieved')
             break
         else:
             print('Token not found in the response')
    else:
        text_dict = json.loads(response.text)
        print(f'Failed to obtain Authentication Token. Error: {text_dict['error_description']}')

# Perform OpenSearch Query:
opensearch_url = 'https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel5P/search.json'

## Define the query parameters for Sentinel-5P CH4 and CO data
query_params = { 
    'startDate': start_date, 
    'completionDate': end_date, # Date range for the current day
    'productType': product,  
}
## Make a GET request to the OpenSearch Catalog
response = requests.get(opensearch_url, 
                            params=query_params).json()

## Import features into dataframe and extract information
response_df = pd.DataFrame.from_dict(response['features'])
# download_urls = response_df['properties'].apply(lambda x: x.get('services').get('download').get('url')).values.tolist()
titles = response_df['properties'].apply(lambda x: x.get('title')).values.tolist()
ids = response_df['id'].values.tolist() #ids needed to download data

# Use OData to download files
## Create a list of urls from ids
urls = []
for id in ids:
    urls.append('https://download.dataspace.copernicus.eu/odata/v1/Products(' + id + ')/$value')

## Create a session and update headers
headers = {"Authorization": f"Bearer {access_token}"}
session = requests.Session()
session.headers.update(headers)

## Define download folder name
download_directory = 'tropomi_download_' +  product + '_' + date_str
check_set = set() #set for checking successful response

## Loop over queries to extract
for url, title in zip(urls, titles):
    filename = title[:-3] + '.zip'
    response = session.get(url, stream=True) # Make GET request for url
    
    if response.status_code == 200: # Check if the request was successful
        zip_buffer = io.BytesIO(response.content) # create file for ZIP  memory
        with zipfile.ZipFile(zip_buffer, 'r') as zip_file: 
            zip_file.extractall(download_directory) # Extract files to directory
            print(f"Extracting {filename} to {download_directory}")
        
    else: 
        print(f"Failed to download: {filename} from {url} (Status code: {response.status_code})") 
        print(response.text)

if check_set == {200}:
    print('Download complete')

