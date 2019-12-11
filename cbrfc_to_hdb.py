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
from datetime import date
from os import path, makedirs
import concurrent.futures
import logging
from logging.handlers import TimedRotatingFileHandler
from requests import post as req_post
from urllib.request import HTTPError
from hdb_api.hdb_api import Hdb, HdbTables, HdbTimeSeries
from hdb_api.hdb_utils import get_eng_config, create_hdb_engine

# slots for the following sites need to be created 'NVRN5LOC','GJNC2LOC','GJLOC'

DATA_URL = 'https://www.cbrfc.noaa.gov/outgoing/ucbor'
HDB_API_URL = 'http://ibr3lcrsrv02.bor.doi.net/series/m-write'

this_dir = path.dirname(path.realpath(__file__))
cbrfc_dir = path.join(this_dir, 'cbrfc_to_hdb')
makedirs(cbrfc_dir, exist_ok=True)

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
    if 'espdly.' in frcst_type:
        return 'month'
    return 'day'

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
    if mrid:
        for row in col.items():
            val = float(row[1])
            if not np.isnan(val):
                m_write_dict = dict(
                    model_run_id = mrid,
                    site_datatype_id = int(sdi),
                    start_date_time = str(row[0]),
                    value = float(row[1]),
                    interval = interval,
                    do_update_y_or_n = True
                )
                m_write_list.append(m_write_dict)
    return json.dumps(m_write_list)

def get_frcst_obj(cbrfc_id, frcst_type, mrid_dict, frcst_url=DATA_URL, write_json=False):
    filename = f'{cbrfc_id}.{frcst_type}'
    url = f'{frcst_url}/{filename}.csv'
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
            json_path = path.join('cbrfc_to_hdb', f'{filename}.json')
            df_m_write.to_json(
                json_path,
                date_format='iso',
                date_unit='s'
            )
        return df_m_write
    
    except HTTPError:
        print(
            f"  Skipping {row['CBRFCID']}.{frcst_type}, file not found - "
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

def chunk_requests(req_list, n=100):
    for i in range(0, len(req_list), n):
        yield req_list[i:i + n]

def post_m_write(m_write):
    m_write_chunks = chunk_requests(m_write)
    chunk_codes = []
    failed_posts = []
    for chunk in m_write_chunks:
        m_post = req_post(
            HDB_API_URL,
            json=chunk,
            headers=m_write_hdr
        )
        response_code = m_post.status_code
        chunk_codes.append(response_code)
        if not response_code == 200:
            print(f'    {response_code}')
            failed_posts.append(
                {f'{idx} {site_frcst}.{frcst_type}': chunk}
            )
    if sum(set(chunk_codes)) == 200:
        print('    Success!')
    else:
        fail_codes = [i for i in chunk_codes if not i == 200]
        percent_fail = 100 * (len(fail_codes) / len(chunk_codes))
        print(f'    {percent_fail:0.0f}% of data failed')
    return failed_posts

async def post_traces(df_m_write, m_write_hdr, api_url=HDB_API_URL, workers=8):
    
    def post_chunk(json_chunk):
        return req_post(api_url, json=json_chunk, headers=m_write_hdr)
    
    m_write_list = []
    for idx, row in df_m_write.iterrows():
        m_write_list.append(json.loads(row['m_write']))
        
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        loop = asyncio.get_event_loop()
        futures = [
            loop.run_in_executor(
                executor, 
                post_chunk,
                json_chunk
            )
            for json_chunk in m_write_list
        ]
        
        result = await asyncio.gather(*futures)
        return result

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
    
if __name__ == '__main__':
    
    this_dir = path.dirname(path.realpath(__file__))
    logger = create_log(path.join(this_dir, 'get_esp.log'))
    config = get_eng_config(db='uc')
    hdb = Hdb(config)
    tbls = HdbTables
    ts = HdbTimeSeries
    
    site_datatypes = tbls.sitedatatypes(
        hdb,
        did_list=[20, 30]
    ) 
    m_write_hdr = {
        'api_hdb': 'uchdb2', 
        'api_user': 'buriona', 
        'api_pass': 'Moki8080'
    }
    mrid_dict = get_mrid_dict()
    SITE_MAPPING_URL = f'{DATA_URL}/idmaplist.csv'
    df_site_map = pd.read_csv(
        SITE_MAPPING_URL,
        dtype={'CBRFCID': str, 'USGSID': str, 'DESCRIPTION': str})
    
    frcst_dict = {}
    daily_5yr = get_frcst_type(period=5)
    daily_1yr = get_frcst_type(period=1)
    mnthly_raw = get_frcst_type(interval='monthly')
    mnthly_adj = get_frcst_type(interval='monthly', adj=True)
    frcst_types = [daily_5yr, daily_1yr, mnthly_raw, mnthly_adj]
    for frcst_type in frcst_types:
        frcst_dict[frcst_type] = {}
    for idx, row in df_site_map.iterrows():
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

        print_and_log(f"Gettting ESP data for {hdb_site_name}", logger)
        
        for frcst_type in frcst_types:
            
            print_and_log(f'  Downloading and processing {frcst_type}', logger)
            
            frcst_obj = get_frcst_obj(cbrfc_id, frcst_type, mrid_dict)
            frcst_dict[frcst_type][cbrfc_id] = frcst_obj
        failed_posts = []
        for frcst_type in frcst_dict.keys():
            for site_frcst in frcst_dict[frcst_type].keys():
                df_m_write = frcst_dict[frcst_type][site_frcst]
                
                ##############################################
                # single threaded syncronous application
                ##############################################
                for idx, row in df_m_write.iterrows():
                    m_write = json.loads(row['m_write'])
                    print_and_log(
                        f'  Writting {hdb_site_name} {idx}-{frcst_type} to HDB.',
                )
                    m_post = req_post(
                        HDB_API_URL,
                        json=m_write,
                        headers=m_write_hdr
                    )
                    response_code = m_post.status_code
                    if not response_code == 200:
                        failed_posts.append(
                            {f'{idx} {site_frcst}.{frcst_type}': m_write}
                        )
                        print(f'    Write failed - {response_code}')
                    else:
                        print('    Success!')
                
                
                ###########################################
                # testing async multi-threaded application
                ###########################################
                # print_and_log(
                #     f'  Writting {hdb_site_name} {datatype_name} {frcst_type} to HDB.', 
                #     logger
                # )
                
                # m_write_list = []
                # for idx, row in df_m_write.iterrows():
                #     m_write_list.append(json.loads(row['m_write']))
                # loop = asyncio.get_event_loop()
                # responses = loop.run_until_complete(
                #     post_traces(df_m_write, m_write_hdr)
                # )
                # status_codes = [i.status_code for i in responses]
                # if sum(set(status_codes)) == 200:
                #     print_and_log('    Success!', logger)
                # else:
                #     fail_codes = [i for i in status_codes if not i == 200]
                #     percent_fail = 100 * (len(fail_codes) / len(status_codes))
                #     print_and_log(
                #         f'    {percent_fail:0.0f}% of data failed - {fail_codes}', 
                #         logger
                #     )