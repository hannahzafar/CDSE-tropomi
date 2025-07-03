#!/usr/bin/env python

import requests
import json
from datetime import datetime, time
import pandas as pd
import os
import argparse
from netrc import netrc

### Functions ##########
def valid_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: '{date_str}'. Expected YYYY-MM-DD.")

def yes_no_input(prompt):
    while True:
        answer = input(prompt).lower()
        if answer in {"y", "n"}:
            return answer
        else:
            print("Invalid input. Enter y/n")

# Parse input arguments
parser = argparse.ArgumentParser(description='User-specified parameters')
varlist = ['ch4', 'co', 'hcho', 'so2', 'no2', 'o3']
parser.add_argument('var', metavar= 'var', type=str,
                    choices=varlist, help=f'Atmospheric gas ({', '.join(varlist)})')
parser.add_argument('date', metavar='YYYY-MM-DD', type=valid_date, help='Date')
args = parser.parse_args()
var, date = args.var, args.date

# Define querying variables
product = f"L2__{var.upper()}{'_' * (6 - len(var))}"

# Get isoformat date start and end
start_date = date.isoformat(timespec='milliseconds') +'Z'
end_date = datetime.combine(date,time.max).isoformat(timespec='milliseconds') +'Z'
# Define authentication variables
host = 'identity.dataspace.copernicus.eu'
client_id = 'cdse-public'
token_url = 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token'
grant_type = 'password'

# Obtain Authentication Token
while True:
    username, _, password = netrc().authenticators(host)
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
             print('Authentication Token Retrieved')
             break
         else:
             print('Token not found in the response')
    else:
        text_dict = json.loads(response.text)
        print(f'Failed to obtain Authentication Token. Error: {text_dict["error_description"]}')

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

## Create download directory
download_dir = 'tropomi_download_' +  product + '_' + date.strftime('%Y-%m-%d')

# Test if download directory exists
try:
    os.makedirs(download_dir)
except FileExistsError:
    choice = yes_no_input(f"{download_dir} already exists. Overwrite directory? (y/n) ")
    if choice == "y":
        print("Directory overwritten.")
        os.makedirs(download_dir, exist_ok = True)
    else:
        download_dir = input("Enter alternate directory name: ")
        os.makedirs(download_dir)

## Loop over queries to extract
for url, title in zip(urls, titles):
    # Get request
    response = session.get(url, stream=True)
    response.raise_for_status()
    # print(from_buffer(response.content)) #NOTE: This is no longer a zip file! Now its hdf5

    # Path to save file
    path = os.path.join(download_dir, title)

    # Write to file
    with open(path, "wb") as f:
        f.write(response.content)
        # Option to chunk
        # for chunk in response.iter_content(chunk_size=8192):
        #     if chunk:
        #         f.write(chunk)

print('Download complete')
