#!/usr/bin/env python

# Original code by Hannah Zafar, edits by Brad Weir

import requests
import json
import pandas as pd
import xarray as xr
import sys
import os
import argparse
import re
from datetime import datetime, timedelta
from netrc import netrc
from time import sleep
from typing import Tuple

VARLIST = ['ch4', 'co', 'hcho', 'so2', 'no2', 'o3']
MODELIST = ['RPRO', 'OFFL', 'NRTI']
DEFVER = None
DEFOUT = '.'
MAXTRIES = 10

token_url = 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token'
odata_url = 'https://catalogue.dataspace.copernicus.eu/odata/v1/Products'

def get_date(ss):
    try:
        return datetime.strptime(ss, '%Y-%m-%d')
    except ValueError:
        raise argparse.ArgumentTypeError(f'Invalid date format: "{date}". Expected YYYY-MM-DD.')

def get_tokens(username: str, password: str) -> Tuple[str, str]:
    auth_data = {'client_id':'cdse-public', 'username':username,
        'password':password, 'grant_type':'password'}

    try:
        response = requests.post(token_url, data=auth_data)
        response.raise_for_status()
    except Exception as e:
        raise Exception(f'Keycloak token creation failed. ' +
            'Reponse from the server was: {response.json()}')
    print('Authentication token retrieved')

    return response.json()['access_token'], response.json()['refresh_token']

def refresh_access_token(refresh_token: str) -> str:
    auth_data = {'client_id':'cdse-public', 'refresh_token':refresh_token,
        'grant_type': 'refresh_token'}

    try:
        response = requests.post(token_url, data=auth_data)
        response.raise_for_status()
    except Exception as e:
        raise Exception(f'Access token refresh failed. ' +
            'Reponse from the server was: {response.json()}')
    print('Authentication token refreshed')

    return response.json()['access_token']

def get_orbits(var, date, mode=None, ver=DEFVER):
    product = 'L2__' + var.upper() + '_'*(6 - len(var))

    # Start with the broadest possible search
    date0 = date - timedelta(days=1)
    dateF = date + timedelta(days=2)



    # Fix for OpenSearch deprecation:
    # Build filter from parameters
    print(product)
    collection = 'SENTINEL-5P'
    filter_parts = [
        f"Collection/Name eq '{collection}'",
        f"ContentDate/Start gt {date0.isoformat(timespec='milliseconds') + 'Z'}",
        f"ContentDate/Start lt {dateF.isoformat(timespec='milliseconds') + 'Z'}",
        f"Attributes/OData.CSC.StringAttribute/any("
        f"att:att/Name eq 'productType' and "
        f"att/OData.CSC.StringAttribute/Value eq '{product}')"
    ]
    
    query_params = {
        "$filter": " and ".join(filter_parts)
    }

    # Make a get request to the OpenSearch catalog
    # response = requests.get(opensearch_url, params=query_params).json()
    response = requests.get(odata_url, params=query_params).json()
    # print(response)
    # sys.exit()

    df = pd.DataFrame.from_dict(response['value'])
    print(len(df))
    print(df['Name'][0])
    sys.exit()
    # columns_to_print = ['Id', 'Name','S3Path','GeoFootprint']
    columns_to_print = ['Name','S3Path']
    print(df[columns_to_print])
    sys.exit()

    # Check for errors
    if response.get('features') is None:
        print(response)
        sys.exit()

    # Import features into dataframe and extract information
    response_df = pd.DataFrame.from_dict(response['features'])
    if response_df.empty:
        return [], []
    titles0 = response_df['properties'].apply(lambda x: x.get('title')).values.tolist()
    ids0 = response_df['id'].values.tolist()

    # Narrow result to include only orbits for today
    pattern1 = re.compile(date.strftime('%Y%m%d') + 'T[0-9]{6}_')
    # Narrow results by mode and/or version
    if mode is not None:
        pattern2 = re.compile('_' + mode.upper() + '_')
    else:
        pattern2 = re.compile('')

    if ver is not None:
        # Could improve the version parsing, but it's non-trivial
        pattern3 = re.compile(product + '_[0-9]{8}T[0-9]{6}_' +
            '[0-9]{8}T[0-9]{6}_[0-9]{5}_[0-9]{2}_' + f'{int(ver):02}')
    else:
        pattern3 = re.compile('')

    # Probably a more elegant way to apply regexs, but whatevs
    titles = []
    urls = []
    for nn in range(len(titles0)):
        if (pattern1.search(titles0[nn]) is not None and
            pattern2.search(titles0[nn]) is not None and
            pattern3.search(titles0[nn]) is not None):
            titles.append(titles0[nn])
            urls.append('https://download.dataspace.copernicus.eu/odata/v1/' +
                'Products(' + ids0[nn] + ')/$value')

    return titles, urls

def download(var, date, mode=None, ver=DEFVER, dirout=DEFOUT):
    # Obtain token
    xx = netrc()
    username, _, password = xx.authenticators('identity.dataspace.copernicus.eu')
    access_token, refresh_token = get_tokens(username, password)

    # Perform OpenSearch query
    titles, urls = get_orbits(var, date, mode, ver)

    # Use OData to download files
    ## Create a session and update headers
    session = requests.Session()
    headers = {'Authorization': f'Bearer {access_token}'}
    session.headers.update(headers)

    ## Create download directory
    os.makedirs(dirout, exist_ok=True)

    ## Loop over queries to extract
    for url, title in zip(urls, titles):
        # Get request
        for nn in range(MAXTRIES):
            try:
                response = session.get(url, stream=True)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                access_token = refresh_access_token(refresh_token)
                headers = {'Authorization': f'Bearer {access_token}'}
                session.headers.update(headers)

        # Path to save file
        ff = os.path.join(dirout, title)

        # Write to file
        # This can throw a connection reset by peer error
        # Need to wrap it somehow, maybe as above
        with open(ff, 'wb') as fid:
            fid.write(response.content)

    print(f'Downloaded {len(titles)} files to {dirout}')

    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='TROPOMI downloader',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('var', metavar='var', type=str, choices=VARLIST,
        help='gas name: ' + ', '.join(VARLIST))
    parser.add_argument('date', metavar='yyyy-mm-dd', type=get_date,
        help='date')
    parser.add_argument('-m', '--mode', metavar='MODE', type=str,
        choices=MODELIST, help='data mode: ' + ', '.join(MODELIST))
    parser.add_argument('-v', '--ver', type=float, default=DEFVER,
        help='data version')
    parser.add_argument('-o', '--output', metavar='DIR', default=DEFOUT,
        help='output directory')

    # Read args and translate to input vars for download
    args = vars(parser.parse_args())
    args['dirout'] = args.pop('output')

    download(**args)
