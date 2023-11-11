#!/usr/bin/env python3# -*- coding: utf-8 -*-
"""
Created on Thu Apr 22 19:00:23 2021

@author: qianhuig
"""

import filepaths
import pygsheets
import pandas as pd
import numpy as np 
import smartsheet
import os
import infrastructure
import re

# use creds to create a client to interact with the Google Drive API
gc = pygsheets.authorize(service_account_file = filepaths.service_account_location)
cobra = gc.open_by_url('https://docs.google.com/spreadsheets/d/1xxbG66GYmKU8Rmtq1vUvvYGDgfMSI0v16I8lwuEw7m4/edit#gid=1186670009') 
content_test = gc.open_by_url("https://docs.google.com/spreadsheets/d/127z4-QF8uuAsfzCd0yI9RbJnzkeb7H9VwXIO-N2Isng/edit#gid=0&fvid=876764931")
content_test_wks = content_test.worksheet('title','CT Schedule')
ct_df = content_test_wks.get_as_df()
# open worksheet - cobra
schedule_wks  =  cobra.worksheet('title','New Mamba')
schedule = schedule_wks.get_as_df()

ct_df['Limit'] = ct_df['Limit'].fillna("")
ct_df['Offset'] = ct_df['Offset'].fillna("")
ct_df['Date'] = pd.to_datetime(ct_df['Date'])

# set date to columns name 
header = schedule.iloc[0,:]
schedule.columns = header
schedule1 = schedule.iloc[1:,:]

# find position for each pubID
position = schedule1.loc[schedule1.iloc[:,1].str.contains("[A-Z.]+_[A-Z.]+_[0-9]+")].reset_index().iloc[:,0:3]
position.columns = ['index','mailer','account']
position['Affliate ID'] = position['account'].str[-6:]
position['Shortcode_Dp.sv'] = position['account'].str[:-7]
number = 0 
problem_drop = [] 


def insert_drop(ct_row_num,time,date,pubid,dropNumber,offerName,segment,sendStrategy,limit,offeset,ccID, jobname): 
    global number
    index = position[position['account']==pubid]['index'].values[0]
    account = position[position['account']==pubid].account.values[0]
    #segment = account+"_"+segment
    start = 2  # the first 2 rows standing for date and day 
    
    # row_num is the row stand for which drop and time
    row_num = start + index + 1 + (dropNumber-1)*9 # format 
    col = schedule1.columns.get_loc(date) + 1 
    
    while (schedule_wks.get_value((int(row_num)+3, int(col))) !="") & (schedule_wks.get_value((int(row_num)+3, int(col))) !=np.nan) : # make sure if the drop is filled, then go to the next drop number 
        offer_id_in_cobra = re.findall(r'\b(\d{4,5})\b',schedule_wks.get_value((int(row_num)+3, int(col))))[0]
        if offer_id_in_cobra == re.findall(r'\b(\d{4,5})\b',offerName)[0]:
            break
        else: 
            row_num += 9
    
    print(row_num)
    row_segment = row_num + 1
    row_sendstrategy = row_num + 2
    row_offername = row_num + 3 
    row_limit = row_num + 4 
    row_offeset = row_num + 5
    row_creative = row_num+6
    row_jobname = row_num + 7 
   
    
    schedule_wks.update_value((row_num,col), time)
    schedule_wks.update_value((row_segment,col), segment)
    #schedule_wks.update_value((row_segment,col), segment) 
    schedule_wks.update_value((row_offername,col), offerName) 
    schedule_wks.update_value((row_sendstrategy,col), sendStrategy)
    schedule_wks.update_value((row_limit,col), limit)
    schedule_wks.update_value((row_creative,col), ccID)
    schedule_wks.update_value((row_offeset,col), offeset)
    schedule_wks.update_value((row_jobname,col),jobname )
    content_test_wks.update_value((ct_row_num,14),'Scheduled')
    #schedule_wks.update_value((row_mmid,col), mmid)4

    
    number += 1 
    print([offerName,row_num,col,account])

# add one offer
def add_cobra_by_pub(row): 
    global problem_drop
    ct_row_num =int(row['index'])+2

    date = row['Date'].strftime("%-m/%-d/%Y")
    time = row['Time']
    pubid = row['Dataset']
    dropNumber = int(row['Drop Number'])
    offerName = row['Offer']
    segment = row['Segment']
    sendStrategy = row['Send Strategy']
    limit = row['Limit']
    offeset = row['Offset']
    ccID = row['Creative']
    #body_content = row.at[0,'Body Content']
    #mmid = row.at[0,'MMID']
    jobname = row['Job Name']
    #insert_drop(ct_row_num,time,date,pubid,dropNumber,offerName,segment,sendStrategy,note,creativeType,ccID, akshad_note)

    try: 
        print(row)
        insert_drop(ct_row_num,time,date,pubid,dropNumber,offerName,segment,sendStrategy,limit,offeset,ccID, jobname)
   
    except: 
        print("Something went wrong with the test for "+pubid)
        problem_drop += [str(offerName) + " in " +pubid ]
        pass 
    

def add_cobra_by_non_scheduled(): 
    row = ct_df.loc[(ct_df['Result']=="Special Setup"),].reset_index()
    row.apply( add_cobra_by_pub,axis = 1)
    print("Completed!")
    
#### write in cobra  
add_cobra_by_non_scheduled()   




print(number,' drops has been added')
print('These drop were not scheduled successfully:', problem_drop)


