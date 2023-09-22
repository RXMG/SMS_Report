#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 27 09:33:41 2023

@author: liliguo
"""

import pandas as pd 
import numpy as np 
import smartsheet
import os
import filepath
import pygsheets
from datetime import date
# if you didn't install smartsheet, please uncomment the following code: pip install smartsheet 

def get_smartsheet(sheet):
    options=['offers_sms','test_sms','emit','offers_email']
    if sheet not in options:
        raise ValueError("Avaliable Smartsheets are: %s" % options)
    offer_id='4192377687566212'
    test_id = '3137750414190468'
    sheet_id='4063260640077700'
    emit_id='6899759718918020'
    
    download_path= filepath.smartsheet_folder
    smartsheet_client_lili = smartsheet.Smartsheet('2jw8gxGHmbJsw0b7aNLsgkz0cdlwqumLOy3Sj') #  at token is fully used, we need extra token to load content test csv 
    offer_sheet=smartsheet_client_lili.Sheets.get_sheet_as_csv(offer_id,download_path,'SMS Offer Sheet.csv')
    content_submission_sheet=smartsheet_client_lili.Sheets.get_sheet_as_csv(test_id,download_path,'SMS Testing Pipeline.csv')
    email_offer_csv=smartsheet_client_lili.Sheets.get_sheet_as_csv(sheet_id,download_path,'Email Offer Data Workbook.csv')
    emit_sheet=smartsheet_client_lili.Sheets.get_sheet_as_csv(emit_id,download_path)

    
    if sheet=='offers_sms':
        return pd.read_csv(os.path.join(download_path,'SMS Offer Sheet.csv'))
    elif sheet=='test_sms':
        return pd.read_csv(os.path.join(download_path,'SMS Testing Pipeline.csv'))
    elif sheet=='offers_email':
        return pd.read_csv(os.path.join(download_path,'Email Offer Data Workbook.csv'))
    elif sheet=='emit':
        return pd.read_csv(os.path.join(download_path,"Email Identifier Mapping Tracker.csv"))

    else:
        print('Sheet name not recongized')
        
def get_mamba():
    """ 
    s = schedule.columns.to_series()
    s.iloc[0] = 'viper'
    s.iloc[1] = 'mongoose'
    schedule.columns = s
    """
    gc = pygsheets.authorize(service_account_file=filepath.service_account_location)
    mamba = gc.open_by_url('https://docs.google.com/spreadsheets/d/12vqSDueybprNphtsw7gXR5vmgcPG6_5ZNcnWzNpiasY/edit#gid=534096291') 
    new_mamba  = mamba.worksheet('title','New Mamba')
    schedule = new_mamba.get_as_df()

    schedule2=schedule.transpose()
    schedule2['Date']=pd.to_datetime(schedule2[0],errors='coerce')
    schedule2.index=schedule2['Date']
         
    drops=['Drop 1','Drop 2','Drop 3','Drop 4','Drop 5','Drop 6']
    frames={}
         
    topdexes = schedule[schedule['Mailers']=="Drop 1"].index.to_list()
    indexes = schedule[schedule['Mailers'].isin(drops)].index.to_list()
    endexes = schedule[schedule['Dataset']=="Job Name"].index.to_list()
         
              
    for i in range(len(indexes)):
        if indexes[i] in topdexes:
            pub = schedule2.iloc[1,(indexes[i]-1)]
        drop = schedule2.iloc[0,(indexes[i])]
        mini_frame = schedule2.iloc[1:,indexes[i]:(endexes[i]+1)]
        mini_frame.columns = mini_frame.iloc[0]
        mini_frame['dataset'] = pub
        mini_frame['drop'] = drop
        if drop is not np.nan:
            mini_frame = mini_frame.set_index([mini_frame.index,'dataset','drop'])
            frames[(pub, drop)] = mini_frame
     

    columns=list(frames[(pub, drop)].columns)
    snakes=pd.DataFrame(columns=columns)

         
    for i in frames.values():
        snakes=pd.concat([snakes,i])
    snakes=snakes.sort_index(level=0)
         
    index=pd.MultiIndex.from_tuples(snakes.index,names=['Date','Dataset','Drop'])
    snakes.index=index
    snakes=snakes[~snakes.index.duplicated()]
         
    snakes['Hitpath Offer ID']=pd.to_numeric(snakes['Offer'].str.split('-').str[0],errors='coerce')
    snakes=snakes.reset_index()
    snakes['Shortcode'] = snakes['Dataset'].str.split('_',expand = True)[0]
    #snakes['Shortcode_DP.sV']= snakes['Segment '].str.extract(r'(\w+\.\w+)_')
    snakes = snakes.loc[(snakes['Offer']!='' ) & (snakes['Date'].isna() ==False) ,]
    snakes['shortcode_DP.SV'] = snakes['Dataset'].str[:-7]

    return snakes 


def transform_sms_df(df):   
    # Rename similar columns in the 'sms' dataframe
    df.rename(columns={
        'Hitpath_Offer_ID': 'Hitpath Offer ID',
        'Affiliate_Id': 'Affiliate ID',
        'Message': 'Message',
        'Delivered': 'Delivered',
        'Clicks': 'Clicks',
        'Revenue': 'Revenue',
        'Date': 'Date',
        'Time': 'Time',
        'Gross Margin':  'Margin',
        'Ecpm': 'Revenue CPM (eCPM)',
        'Creativename': 'Creative Type'
        }, inplace=True)

    df['Date'] = pd.to_datetime(df['Date'])
    df = df[(df['Revenue Source']!= 'Email') & (df['Revenue Source']!= 'Push') & (df['Revenue Source']!=  'Short Code - Opt In')]
    df = df.loc[~df['Send Strategy'].isin(['Null','Opt In',np.nan,'AR'])]
    df = df[df['Date']>= '2022-11-01']
    mamba = get_mamba()
    mamba['Affiliate ID'] = mamba['Dataset'].str.split('_',expand = True)[2].astype(int)
    current_active_pubid= mamba.loc[mamba['Date'] == pd.to_datetime(date.today()),'Affiliate ID'].unique().tolist()
    df = df[df['Affiliate ID'].isin(current_active_pubid)]
    # import publisher information 
    gc = pygsheets.authorize(service_account_file=filepath.service_account_location)
    rxmgref = gc.open_by_url('https://docs.google.com/spreadsheets/d/1Tzda6Djr3zQmOhWu7Ief3GVR9Cjaml8238CeX7chj_U/edit#gid=1620368362') 
    publisher  = rxmgref.worksheet('title','Publisher Configurations').get_as_df()
    df = df.merge(publisher[['PUBID','NEW DP.DS or DP.sV','Sub Vertical']], how = 'left', left_on ='Affiliate ID', right_on = 'PUBID')
    df.rename(columns={
        'NEW DP.DS or DP.sV': 'DP.sV',
        'Sub Vertical': 'Data Vertical'
        }, inplace=True) 
    df['PUBID1'] = df['PUBID'].astype(str).str.split('.',expand = True)[0]
    df['DP&Pub'] = df['DP.sV']+'_'+ df['PUBID1']
    df['SC_DP&Pub'] = ''
    df.loc[(df['DP&Pub'].isnull() == False) & (df['Shortcode Name'].isnull() == False), 'SC_DP&Pub'] = df['Shortcode Name'] + '_' + df['DP&Pub']
    # filter out if there's no SC_DP&Pub
    df = df[df['SC_DP&Pub'] != '']
    df['eCPM'] = df['Revenue']*1000 / df['Delivered']
    df['CTR'] = df['Clicks']*1000 / df['Delivered']
    df['eCPM Ratio'] = (df['Revenue'])/((df['Revenue'])-df['Opportunity Cost'])
    df['CTR Normalized']=(df['CTR']-df['CTR'].min())/(df['CTR'].max()-df['CTR'].min())
    df['eCPM Normalized']=(df['eCPM']-df['eCPM'].min())/(df['eCPM'].max()-df['eCPM'].min())
    df['CTR50'] = df['CTR Normalized'] + df['eCPM Normalized']
    df['Profit'] = df['Revenue'].fillna(0) - df['Cost'].fillna(0)
    
    
    return df

def get_lanina():
    # La nina 
    gc = pygsheets.authorize(service_account_file=filepath.service_account_location)
    lanina = gc.open_by_url('https://docs.google.com/spreadsheets/d/1obszkCQoE0ELOR1O0CrLVETUEmEIWlGuyAmK3FgWSJg/edit#gid=1060654066')
    lanina_sheet =  lanina.worksheet('title','La Nina (Current)')
    lanina_df = lanina_sheet.get_as_df()
    return lanina_df


def get_publisher():
    # import publisher information 
    gc = pygsheets.authorize(service_account_file=filepath.service_account_location)
    rxmgref = gc.open_by_url('https://docs.google.com/spreadsheets/d/1Tzda6Djr3zQmOhWu7Ief3GVR9Cjaml8238CeX7chj_U/edit#gid=1620368362') 
    publisher  = rxmgref.worksheet('title','Publisher Configurations').get_as_df()
    return publisher

def get_mamba_directory():
    gc = pygsheets.authorize(service_account_file=filepath.service_account_location)
    mamba_dic = gc.open_by_url('https://docs.google.com/spreadsheets/d/12vqSDueybprNphtsw7gXR5vmgcPG6_5ZNcnWzNpiasY/edit#gid=1726482538')
    mamba_dic_df  = mamba_dic.worksheet('title','Directory')
    mamba_dic_df = mamba_dic_df.get_as_df()
    return mamba_dic_df