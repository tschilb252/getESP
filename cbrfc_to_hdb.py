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
from os import path, makedirs
from concurrent.futures import ThreadPoolExecutor
import logging
from logging.handlers import TimedRotatingFileHandler
from requests import post as req_post
from urllib.request import HTTPError
from hdb_api.hdb_api import Hdb, HdbTables, HdbTimeSeries
from hdb_api.hdb_utils import get_eng_config, create_hdb_engine

# slots for the following sites need to be created 'NVRN5LOC','GJNC2LOC','GJLOC'

get_frcst_url():
    return 'https://www.cbrfc.noaa.gov/outgoing/ucbor'

SITE_MAPPING_URL = f'{DATA_URL}/idmaplist.csv'

def get_api_url():
    return 'http://ibr3lcrsrv02.bor.doi.net/series/m-write'

def get_m_write_hdr(): 
    # return {
    #     'api_hdb': 'uchdb2', 
    #     'api_user': 'buriona', 
    #     'api_pass': 'Moki8080'
    # }
    return {
        'api_hdb': 'uchdb2', 
        'api_user': 'app_remote', 
        'api_pass': 'UChdb22..'
    }

def create_log(path='get_esp.log'):
    logger = logging.getLogger('get_esp rotating log')
    logger.setLevel(logging.INFO)

    handler = TimedRotatingFileHandler(
        path,
        when="W6",
        backupCount=1
    )

    logger.addHandler(handler)

    return logger

def print_and_log(log_str, logger):
    print(log_str)
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
                    do_update_y_or_n = True
                )
                m_write_list.append(m_write_dict)
    if m_write_list:
        m_write_list = [i for i in m_write_list if i]
        return json.dumps(m_write_list)

def get_frcst_obj(cbrfc_id, frcst_type, mrid_dict, write_json=False):
    filename = f'{cbrfc_id}.{frcst_type}'
    url = f'{get_frcst_url()}/{filename}.csv'
    try:
        if 'espdly' in frcst_type.lower():
            df = pd.read_csv(
                url, 
                comment='$', 
                parse_dates=['DATE'],
                index_col = 'DATE'
            )
            df_vol = df * 1.98347
        else:
            df_vol = pd.read_csv(
                url, 
                skiprows=2, 
                parse_dates=['DATE'],
                index_col = 'DATE'
            )
        df_vol.dropna(how='all', inplace=True)
        df_vol_stats = df_vol.transpose()
        df_vol_stats = df_vol_stats.describe(percentiles=[0.10, 0.50, 0.90])
        df_vol_stats = df_vol_stats.transpose()
        df_vol['MOST'] = df_vol_stats['50%']
        df_vol['MAX'] = df_vol_stats['90%']
        df_vol['MIN'] = df_vol_stats['10%']
        
        df_m_write = df_vol.apply(
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
            f"    Skipping {row['CBRFCID']}.{frcst_type}, file not found - "
            f"{url}"
        )

def make_eng(db='buriona_uc'):
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
        print(f'Failed! - {e}')
        return f'Failed! - {e}'

def create_esp_mrids():
    results = []
    model_ids = {53: 'RAW', 52: 'ADJ'}
    eng = make_eng(db='buriona_uc')
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

def post_chunked_traces(df_m_write, hdb_site_name, frcst_type):
    
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
                headers=get_m_write_hdr()
            )
            response_code = result.status_code
            chunk_codes.append(response_code)
            if not response_code == 200:
                print(f"   Chunk failed - {result.json()['message']}")
                failed_posts.append(chunk)
        if sum(set(chunk_codes)) == 200:
            print('    Success!')
        else:
            fail_codes = [i for i in chunk_codes if not i == 200]
            percent_fail = 100 * (len(fail_codes) / len(chunk_codes))
            print(f'    {percent_fail:0.0f}% of data failed')
        return failed_posts

async def async_post_traces(df_m_write, workers=10):
    
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
            headers=get_m_write_hdr()
        )
        if not result.status_code == 200:
            print(f" {m_year} failed - {result.json()['message']}")
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
            headers=get_m_write_hdr()
        )
        response_code = m_post.status_code
        if not response_code == 200:
            failed_posts.append(m_write)
            print_and_log(
                f'    Write failed - {response_code}', 
                logger
            )
        else:
            print_and_log('    Success!', logger)
    return failed_posts
    
if __name__ == '__main__':
    
    async_run = True
    s_time = datetime.now()
    this_dir = path.dirname(path.realpath(__file__))
    logger = create_log(path.join(this_dir, 'get_esp.log'))
    cbrfc_dir = path.join(this_dir, 'm_write_jsons')
    makedirs(cbrfc_dir, exist_ok=True)
    failed_post_dir = path.join(this_dir, 'failed_posts')
    makedirs(failed_post_dir, exist_ok=True)
    
    print_and_log(f'ESP fetch starting at {s_time.strftime("%x %X")}...', logger)
    config = get_eng_config(db='uc')
    hdb = Hdb(config)
    tbls = HdbTables
    ts = HdbTimeSeries
    
    site_datatypes = tbls.sitedatatypes(
        hdb,
        did_list=[20, 30]
    ) 
    
    mrid_dict = get_mrid_dict()
    
    df_site_map = pd.read_csv(
        SITE_MAPPING_URL,
        dtype={'CBRFCID': str, 'USGSID': str, 'DESCRIPTION': str})
    
    frcst_dict = {}
    daily_5yr = get_frcst_type(period=5)
    daily_1yr = get_frcst_type(period=1)
    mnthly_raw = get_frcst_type(interval='monthly')
    mnthly_adj = get_frcst_type(interval='monthly', adj=True)
    frcst_types = [daily_5yr, daily_1yr, mnthly_raw, mnthly_adj]
        
    for idx, row in df_site_map.iterrows():
        for frcst_type in frcst_types:
            frcst_dict[frcst_type] = {}
        site_name = row['DESCRIPTION']
        cbrfc_id = row['CBRFCID']
        usgs_id = str(row['USGSID'])
        sdi = None
        meta_row = None
        if usgs_id:
            meta_row = site_datatypes[site_datatypes['site_metadata.nws_code'] == cbrfc_id]
            if not meta_row.empty:
                sdi = meta_row['site_datatype_id'].iloc[0]
                hdb_site_name = meta_row['site_metadata.site_name'].iloc[0].upper()
                datatype_name = meta_row['datatype_metadata.datatype_name'].iloc[0].upper()
            else:
                continue

        print_and_log(f"Getting ESP data for {hdb_site_name}", logger)
        
        for frcst_type in frcst_types:
            
            print_and_log(f'  Downloading and processing {frcst_type}', logger)
            
            frcst_obj = get_frcst_obj(cbrfc_id, frcst_type, mrid_dict)
            continue
            frcst_dict[frcst_type][cbrfc_id] = frcst_obj
        all_failed_posts = []
        for frcst_type in frcst_dict.keys():
            for site_frcst in frcst_dict[frcst_type].keys():
                failed_posts = []
                df_m_write = frcst_dict[frcst_type][site_frcst]
                esp_id = f'{site_frcst}.{frcst_type}'
                if async_run and df_m_write is not None:
                ###########################################
                # testing async multi-threaded application
                ###########################################
                    print_and_log(
                        f'  Writing {hdb_site_name} {datatype_name} {frcst_type} to HDB.', 
                        logger
                    )
                    loop = asyncio.get_event_loop()
                    failed_posts.append(
                        loop.run_until_complete(
                            async_post_traces(df_m_write)
                        )
                    )

                elif df_m_write is not None:
                ##############################################
                # single threaded syncronous application
                ##############################################
                    
                    failed_posts.append(
                        post_chunked_traces(df_m_write, hdb_site_name, frcst_type)
                    )
                        
                    # failed_posts.append(
                    #     post_traces(df_m_write, hdb_site_name, frcst_type)
                    # )
                
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
            failed_filename = f'{cbrfc_id}_failed_posts.json'
            failed_path = path.join('failed_posts', failed_filename)
            with open(failed_path, 'w') as j:
                json.dump(all_failed_posts, j)
            
    e_time = datetime.now()
    elapsed_sec = (e_time - s_time).seconds
    print_and_log(
        f'ESP fetch finished at {e_time.strftime("%x %X")}...'
        f'Total elapsed time {elapsed_sec / 3600:0.2f} hours. ',
        logger)
                