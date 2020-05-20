# -*- coding: utf-8 -*-
"""
Created on Mon Nov  4 08:49:17 2019

@author: buriona
"""

import sys
import json
import asyncio
import pandas as pd
import numpy as np
from datetime import date, datetime
from os import path, makedirs, listdir, remove
from concurrent.futures import ThreadPoolExecutor
import logging
from logging.handlers import TimedRotatingFileHandler
from requests import post as req_post
from urllib.request import HTTPError
from hdb_api.hdb_api import Hdb, HdbTables, HdbTimeSeries
from hdb_api.hdb_utils import get_eng_config, create_hdb_engine

# slots for the following sites need to be created 'NVRN5LOC','GJNC2LOC','GJLOC'

def get_frcst_url(office='uc'):
    frcst_url_dict ={
        'uc': 'https://www.cbrfc.noaa.gov/outgoing/ucbor',
        'alb': 'https://www.cbrfc.noaa.gov/outgoing/abqbor'
    }
    return frcst_url_dict[office]

def get_nws_region(office='uc'):
    nws_region_dict ={
        'uc': 'CB',
        'alb': 'WG'
    }
    return nws_region_dict[office]

def get_api_url():
    return 'http://ibr3lcrsrv02.bor.doi.net/series/m-write'


def get_write_hdr(hdb_config): 
    return {
        'api_hdb': hdb_config['database'], 
        'api_user': hdb_config['username'], 
        'api_pass': hdb_config['psswrd']
    }

def create_log(log_path='get_esp.log'):
    logger = logging.getLogger('get_esp rotating log')
    logger.setLevel(logging.INFO)

    handler = TimedRotatingFileHandler(
        log_path,
        when="W6",
        backupCount=1
    )

    logger.addHandler(handler)

    return logger

def print_and_log(log_str, logger=None):
    print(log_str)
    if logger:
        logger.info(log_str)
    
def get_mrid_dict():
    with open('mrid_map.json', 'r') as j:
        mrid_dict =json.load(j)
    return mrid_dict

def get_mrid(trace, frcst_type, mrid_dict):
    adj_raw = {
        'espdly.5yr': 'ADJ',
        'espdly.1yr': 'RAW',
        'espmvol.5yr.raw': 'RAW',
        'espmvol.5yr.adj': 'ADJ'
    }
    model_name = f'ESP {adj_raw[frcst_type]} {trace}'
    return mrid_dict.get(model_name, None)

def get_interval(frcst_type):
    interval_dict = {
        'espdly.5yr': 'day',
        'espdly.1yr': 'day',
        'espmvol.5yr.raw': 'month',
        'espmvol.5yr.adj': 'month'
    }
    return interval_dict.get(frcst_type, None)

def get_frcst_type(interval='daily', period=5, adj=False):
    adj_str = 'raw'
    if adj:
        adj_str = 'adj'
    if interval == 'daily':
        return f'espdly.{period}yr'
    return f'espmvol.{period}yr.{adj_str}'

def parse_m_write(col, sdi, frcst_type, mrid_dict):
    m_write_list = []
    mrid = get_mrid(col.name, frcst_type, mrid_dict)
    interval = get_interval(frcst_type)
    if mrid and interval:
        for row in col.items():
            val = float(row[1])
            if not np.isnan(val):
                m_write_dict = dict(
                    model_run_id = mrid,
                    site_datatype_id = int(sdi),
                    start_date_time = row[0].strftime('%Y-%m-%dT00:00:00Z'),
                    value = float(row[1]),
                    interval = interval,
                    do_update_y_or_n = False
                )
                m_write_list.append(m_write_dict)
    if m_write_list:
        m_write_list = [i for i in m_write_list if i]
        return json.dumps(m_write_list)

def get_frcst_obj(rfc_id, frcst_type, mrid_dict, office='uc', write_json=False):
    filename = f'{rfc_id}.{frcst_type}'
    url = f'{get_frcst_url(office)}/{filename}.csv'
    try:
        if 'espdly' in frcst_type.lower():
            df = pd.read_csv(
                url, 
                comment='$', 
                parse_dates=['DATE'],
                index_col = 'DATE'
            )
            # df = df * 1.983471
        else:
            df_vol = pd.read_csv(
                url, 
                skiprows=2, 
                parse_dates=['DATE'],
                index_col = 'DATE'
            )
            # df = df_vol * 1000.0
            df = (df_vol * 1000.0) / 1.983471
            
        df.dropna(how='all', inplace=True)
        df_stats = df.transpose()
        df_stats = df_stats.describe(percentiles=[0.10, 0.50, 0.90])
        df_stats = df_stats.transpose()
        df['MOST'] = df_stats['50%']
        df['MAX'] = df_stats['90%']
        df['MIN'] = df_stats['10%']
        
        df_m_write = df.apply(
            lambda col: parse_m_write(col, sdi, frcst_type, mrid_dict)
        )

        df_m_write = pd.DataFrame(
            {'m_write': df_m_write}
        )
        if write_json:
            json_path = path.join('m_write_jsons', f'{filename}.json')
            df_m_write.to_json(
                json_path,
                date_format='iso',
                date_unit='s'
            )
        return df_m_write
    
    except HTTPError:
        print(
            f"    Skipping {rfc_id}.{frcst_type}, file not found - "
            f"{url}"
        )

def make_eng(db='uc'):
    db_config = get_eng_config(db=db)
    eng = create_hdb_engine(**db_config)
    return eng

def make_mrid(conn, model_id, run_name):
    curr_date = date.today().strftime('%d-%b-%y').upper()
    date_str = f"TO_DATE('{curr_date}', 'DD-MON-RR')"
    sql_str = (
        f'INSERT INTO uchdba.ref_model_run '
        f'(MODEL_RUN_NAME, MODEL_ID, DATE_TIME_LOADED, MODELTYPE, EXTRA_KEYS_Y_N, RUN_DATE) '
        f"VALUES ('{run_name}', {model_id}, {date_str}, 'F', 'N', {date_str})"
    )
    print(sql_str)
    try:
        result = conn.execute(sql_str)
        print(f'{run_name} mrid created')
        return result
    except Exception as e:
        return f'Failed! - {e}'

def create_esp_mrids():
    results = []
    model_ids = {53: 'RAW', 52: 'ADJ'}
    eng = make_eng(db='uc')
    with eng.connect() as conn:
        for model_id, data_type in model_ids.items():
            esp_traces = [str(i) for i in range(1981, 2016)]
            esp_traces.extend(['MAX', 'MIN', 'MOST'])
            mrid_names = [f'ESP {data_type} {i}' for i in esp_traces]
            for i, esp_trace in enumerate(esp_traces):
                run_name = mrid_names[i]
                results.append(make_mrid(conn, model_id, run_name))

def chunk_requests(req_list, n=500):
    for i in range(0, len(req_list), n):
        yield req_list[i:i + n]

def post_chunked_traces(df_m_write, hdb_site_name, frcst_type, logger):
    
    for idx, row in df_m_write.iterrows():
        m_write = json.loads(row['m_write'])
        
        print_and_log(
                f'  Writing {hdb_site_name} {idx}-{frcst_type} to HDB.',
                logger
            )
        m_write_chunks = chunk_requests(m_write)
        chunk_codes = []
        failed_posts = []
        for chunk in m_write_chunks:
            result = req_post(
                get_api_url(),
                json=chunk,
                headers=get_write_hdr(config)
            )
            response_code = result.status_code
            chunk_codes.append(response_code)
            if not response_code == 200:
                print_and_log(
                    f"   Chunk failed - {result.json()['message']}",
                    logger
                )
                failed_posts.append(chunk)
        if sum(set(chunk_codes)) == 200:
            print_and_log('    Success!', logger)
        else:
            fail_codes = [i for i in chunk_codes if not i == 200]
            percent_fail = 100 * (len(fail_codes) / len(chunk_codes))
            print_and_log(f'    {percent_fail:0.0f}% of data failed', logger)
        return failed_posts

async def async_post_traces(df_m_write, logger, workers=10):
    
    def post_m_year(m_year_dict):
        m_year = m_year_dict['year']
        m_data = m_year_dict['data']
        print_and_log(
            f"    Writing {m_year}...",
            logger
        )
        result = req_post(
            get_api_url(), 
            json=m_data, 
            headers=get_write_hdr(config)
        )
        if not result.status_code == 200:
            print_and_log(
                f" {m_year} failed - {result.json()['message']}",
                logger)
            return m_year_dict['data']
    
    m_write_list = []
    for idx, row in df_m_write.iterrows():
        m_write_list.append({'year': idx, 'data': json.loads(row['m_write'])})
        
    with ThreadPoolExecutor(max_workers=workers) as executor:
        loop = asyncio.get_event_loop()
        futures = [
            loop.run_in_executor(
                executor, 
                post_m_year,
                m_year
            )
            for m_year in m_write_list
        ]
        
        result = await asyncio.gather(*futures)
        return [i for i in result if i]

def post_traces(df_m_write, hdb_site_name, frcst_type):
    failed_posts = []
    for idx, row in df_m_write.iterrows():
        m_write = json.loads(row['m_write'])
        print_and_log(
            f'  Writing {hdb_site_name} {idx}-{frcst_type} to HDB.',
            logger
        )
        m_post = req_post(
            get_api_url(),
            json=m_write,
            headers=get_write_hdr(config)
        )
        response_code = m_post.status_code
        if not response_code == 200:
            failed_posts.append(m_write)
            print_and_log(
                f'    Write failed - {response_code} - '
                f'{m_post.json()["message"]}', 
                logger
            )
        else:
            print_and_log('    Success!', logger)
    return failed_posts

def clean_up(logger, failed_file_dir='failed_posts'):
    failed_post_files = listdir(failed_file_dir)
    failed_post_files[:] = [path.join(failed_file_dir, i) for i in failed_post_files]
    for failed_post_file in failed_post_files:
        print_and_log(
            f'Attemping to write failed data from - {failed_post_file}.', 
            logger
        )
        with open(failed_post_file, 'r') as j:
            failed_posts = json.load(j)
        failed_again = []
        for failed_post in failed_posts:
            result = req_post(
                get_api_url(),
                json=failed_post,
                headers=get_write_hdr(config)
            )
            if not result.status_code == 200:
                failed_again.append(failed_post)
        if not failed_again:
            print_and_log(
                f'  Success! - {failed_post_file} written to HDB.', 
                logger
            )
            remove(failed_post_file)
        else:
            print_and_log(
                f'  Error! - {failed_post_file} failed to write HDB - '
                f"{result.json()['message']}",
                logger
            )
            remove(failed_post_file)

def get_frcst_types(args): #dear god this is ugly clean up with dicts
    frcst_types = []
    if args.raw:
        if args.daily:
            frcst_types = [daily_raw]
        elif args.monthly:
            frcst_types = [mnthly_raw]
        else:
            frcst_types = [daily_raw, mnthly_raw]
    if args.adj:
        if args.daily:
            frcst_types = [daily_adj]
        elif args.monthly:
            frcst_types = [mnthly_adj]
        else:
            frcst_types = [daily_adj, mnthly_adj]
    if not args.raw and not args.adj:
        if args.daily:
            frcst_types = [daily_adj, daily_raw]
        elif args.monthly:
            frcst_types = [mnthly_adj, mnthly_raw]
        else:
            frcst_types = [daily_adj, mnthly_adj, daily_raw, mnthly_raw]
        
    return frcst_types
if __name__ == '__main__':
    
    import argparse
    cli_desc = '''
    Downloads RFC ESP Traces and pushes data to UCHDB, 
    defaults to both raw and adjusted datasets.
    '''
    parser = argparse.ArgumentParser(description=cli_desc)
    parser.add_argument(
        "-V", "--version", help="show program version", action="store_true"
    )
    parser.add_argument(
        "-r", "--raw", help="only write raw ESP data", action="store_true"
    )
    parser.add_argument(
        "-a", "--adj", help="only write adjusted ESP data", action="store_true"
    )
    parser.add_argument(
        "-d", "--daily", help="only write daily ESP data", action="store_true"
    )
    parser.add_argument(
        "-m", "--monthly", help="only write daily ESP data", action="store_true"
    )
    parser.add_argument(
        "-p", "--print", help="print mrid mapping", action="store_true"
    )
    parser.add_argument(
        "-f", "--file", help="export mrid mapping to provided filepath"
    )
    parser.add_argument(
        "-o", "--office", help="pick between 'uc' or 'alb' office", default='uc'
    )
    parser.add_argument(
        "-w", "--workers", help="number of threads to use in async write"
    )
    parser.add_argument(
        "-s", 
        "--single", 
        help=""""run as single threaded non async process, 
        important for connection limited users""",
        action="store_true"
    )
    args = parser.parse_args()
    
    if args.version:
        print('rfc_to_hdb.py v1.0')
    if args.print:
        print(json.dumps(get_mrid_dict(), sort_keys=True, indent=4))
        sys.exit(0)
    if args.file:
        if path.isdir(args.file):
            with open(path.join(args.file, 'mrid_esp_map.json'), 'w') as f:
                json.dump(get_mrid_dict(), f, sort_keys=True, indent=4)
                print(f'mrid_esp_map.json exported to {args.file}')
        else:
            print(f'{args.file} does not exist.')
        sys.exit(0)
    workers = 10
    if args.workers:
        if str(args.workers).isnumeric():
            workers = int(args.workers)
    async_run = True
    if args.single:
        async_run = False
    if args.office:
        office = args.office
    nws_region = get_nws_region(office)   
    
    s_time = datetime.now()
    this_dir = path.dirname(path.realpath(__file__)) 
    log_dir = path.join(this_dir, 'logs')
    makedirs(log_dir, exist_ok=True)
    logger = create_log(path.join(this_dir, 'logs', f'{office}_get_esp.log'))
    rfc_dir = path.join(this_dir, 'm_write_jsons')
    makedirs(rfc_dir, exist_ok=True)
    failed_post_dir = path.join(this_dir, 'failed_posts')
    makedirs(failed_post_dir, exist_ok=True)

    print_and_log(
        f'{office.upper()} ESP fetch starting at {s_time.strftime("%x %X")}...', 
        logger
    )
    config = get_eng_config(db='uc')
    hdb = Hdb(config)
    tbls = HdbTables
    ts = HdbTimeSeries
    
    site_datatypes = tbls.sitedatatypes(
        hdb,
        # did_list=[20, 30, 32, 34, 124] # volumes
        did_list=[19, 29, 31, 33, 123] # flows
        # sdi_list=[23408, 1849, 1848, 1847, 1786] #aspinal sdis
    )
    site_datatypes.sort_values(
        by=['datatype_id'], ascending=False, inplace=True
    )
    site_datatypes.drop_duplicates(
        subset=['site_id'], keep='first', inplace=True
    )

    mrid_dict = get_mrid_dict()
    frcst_url = get_frcst_url(office)
    site_map_url = f'{frcst_url}/idmaplist.csv'
    df_site_map = pd.read_csv(
        site_map_url,
        dtype={f'{nws_region}RFCID': str, 'USGSID': str, 'DESCRIPTION': str})
    
    frcst_dict = {}
    daily_adj = get_frcst_type(period=5)
    daily_raw = get_frcst_type(period=1)
    mnthly_adj = get_frcst_type(interval='monthly', adj=True)
    mnthly_raw = get_frcst_type(interval='monthly')
    frcst_types = get_frcst_types(args)
    
    for idx, row in df_site_map.iterrows():
        for frcst_type in frcst_types:
            frcst_dict[frcst_type] = {}
        site_name = row['DESCRIPTION']
        rfc_id = row[f'{nws_region}RFCID']
        usgs_id = str(row['USGSID'])
        sdi = None
        meta_row = None
        if usgs_id:
            
            meta_row = site_datatypes[site_datatypes['site_metadata.nws_code'] == rfc_id]
            if not meta_row.empty:
                datatype_name = meta_row['datatype_metadata.datatype_name'].iloc[0].upper()
                sdi = meta_row['site_datatype_id'].iloc[0]
                hdb_site_name = meta_row['site_metadata.site_name'].iloc[0].upper()
                print(site_name, datatype_name)
            else:
                print_and_log(
                    f'Could not match {rfc_id} - {site_name} to an HDB site',
                    logger
                )
                continue
        
        print_and_log(f"Getting ESP data for {hdb_site_name}", logger)
        
        for frcst_type in frcst_types:
            
            print_and_log(
                f'  Downloading and processing {frcst_type}', 
                logger
            )
            
            frcst_obj = get_frcst_obj(
                rfc_id, frcst_type, mrid_dict, office=office
            )
            frcst_dict[frcst_type][rfc_id] = frcst_obj
            
        all_failed_posts = []

        for frcst_type in frcst_dict.keys():
            for site_frcst in frcst_dict[frcst_type].keys():
                failed_posts = []
                df_m_write = frcst_dict[frcst_type][site_frcst]
                esp_id = f'{site_frcst}.{frcst_type}'
                if async_run and df_m_write is not None:
                ###########################################
                # async multi-threaded application
                ###########################################
                    print_and_log(
                        f'  Writing {hdb_site_name} {datatype_name} {frcst_type} to HDB.', 
                        logger
                    )
                    loop = asyncio.get_event_loop()
                    failed_posts.extend(
                        loop.run_until_complete(
                            async_post_traces(
                                df_m_write, logger, workers=workers
                            )
                        )
                    )

                elif df_m_write is not None:
                ##############################################
                # single threaded syncronous application
                ##############################################
                    
                    failed_posts.extend(
                        post_chunked_traces(
                            df_m_write, hdb_site_name, frcst_type, logger
                        )
                    )
                
            if not failed_posts:
                print_and_log('    Success!', logger)
            else:
                all_failed_posts.extend(failed_posts)
                percent_fail = 100 * (len(failed_posts) / df_m_write.size)
                print_and_log(
                    f'    {percent_fail:0.0f}% of data failed.', 
                    logger
                )
                
        if all_failed_posts:
            failed_filename = f'{rfc_id}_failed_posts.json'
            failed_path = path.join('failed_posts', failed_filename)
            with open(failed_path, 'w') as j:
                json.dump(all_failed_posts, j)
            
    e_time = datetime.now()
    elapsed_sec = (e_time - s_time).seconds
    print_and_log(
        f'ESP fetch finished at {e_time.strftime("%x %X")}...'
        f'Total elapsed time {elapsed_sec / 3600:0.2f} hours. ',
        logger)
    clean_up(logger, failed_post_dir)
                