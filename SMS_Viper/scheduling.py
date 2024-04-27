import numpy as np
import pandas as pd
import re
import math
import datetime
from datetime import timedelta
from datetime import date
import inspect
import pygsheets
import time
from pygsheets.datarange import DataRange
#import threading
#from multiprocessing import Pool
import os
os.environ['PYGAME_HIDE_SUPPORT_PROMPT'] = "hide"
import pygame
import random
import infrastructure
import filepath
import sys
import viper_main
del sys.modules["viper_main"]
import viper_main

import offer_selection
del sys.modules["offer_selection"]
import offer_selection

import find_viper_settings
del sys.modules["find_viper_settings"]
import find_viper_settings

import content_selection
del sys.modules["content_selection"]
import content_selection



class inputs:
    def __init__(self, fvs=None, cs=None, offs=None, cobra=None, cobra_clean=None, cobra_sheet=None, cobra_sheet_df=None, 
                 emit=None, offers=None, lexi=None, lexi_payouts = None, viper_settings_sheet = None,
                 sc_dppub_affiliate = None, days_ahead=None, today=date.today(), load_log=False, all_aff_log=None):
        self.today = today
#         self.lock = threading.Lock()
#         print('creating sched')
        self.fvs = fvs
        self.cs = cs
        self.offs = offs
        self.cobra = cobra
        self.cobra_clean = cobra_clean
        self.cobra_sheet = cobra_sheet
        self.cobra_sheet_df = cobra_sheet_df
        self.emit = emit
        self.offers = offers
        self.lexi = lexi
        self.lexi_clean = lexi.copy()
        self.affiliate_log = viper_main.get_affiliate_log_template()
        self.affiliate_log_clean = self.affiliate_log.copy()
        
        #self.lexi_conversions = lexi.groupby(['Affiliate ID','Send Strategy','Hitpath Offer ID']).agg({'Conversions':'mean'})

        self.lexi_segment_sizes = lexi[(lexi['Date']>=lexi['Date'].max() + timedelta(days=-30)) & 
                                      (lexi['Send Strategy']=='P')].groupby(['Affiliate ID']).agg({'Delivered':'mean'})
        
        
        self.lexi_payouts = lexi_payouts
        # special for SMS 
        self.sc_dppub_affiliate = sc_dppub_affiliate
        self.sc = self.sc_dppub_affiliate.split('_')[0]
        self.dppub = self.sc_dppub_affiliate.split('_')[1]
        self.sc_dppub = self.sc + "_" +self.dppub
        self.affiliate = self.sc_dppub_affiliate.split('_')[2]
        affiliate = self.affiliate
        self.days_ahead = days_ahead
        
        self.empties = self.find_initial_empties(self.days_ahead)
        
        self.production_empty_indices = self.empties[self.empties['Drop Number']>=1].index
        
        affiliate_ot_drop_days = fvs.get_affiliate_ot_drop_days()        
        ot_days_to_schedule = affiliate_ot_drop_days[sc_dppub_affiliate]
        ot_days = fvs.find_days_available(ot_days_to_schedule)
        
        affiliate_e_drop_days = fvs.get_affiliate_e_drop_days()
        e_days_to_schedule = affiliate_e_drop_days[sc_dppub_affiliate]
        e_days = fvs.find_days_available(e_days_to_schedule)
        
        
        self.ot_empty_indices = self.empties[(self.empties['Drop Number']==1) & (self.empties['Date'].dt.date>today) &
                                (self.empties['Day of Week'].apply(lambda x: x in ot_days))].index
        
        self.e_empty_indices = self.empties[(self.empties['Drop Number']==1) & (self.empties['Date'].dt.date>today) &
                                (self.empties['Day of Week'].apply(lambda x: x in e_days))].index

            
        self.past_cobras = self.find_past_cobras()
        self.upcoming_cobras = self.find_upcoming_cobras()
        self.gc = infrastructure.get_gc()
        self.ss_offer = infrastructure.get_ss_offer()


        self.viper_settings_sheet = fvs.viper_settings_sheet
        self.aff_settings_frame = self.viper_settings_sheet.worksheet_by_title('Affiliate Settings').get_as_df()

#         if load_log==True:
#             try:
#                 self.log = fvs.load_log(affiliates=[sc_dppub_affiliate],download=False)
#             except:
#                 all_aff_log = fvs.load_log(affiliates=[sc_dppub_affiliate],download=True)
#                 self.log = all_aff_log[all_aff_log['Affiliate ID']==sc_dppub_affiliate]

#             log=self.log

        if all_aff_log is not None:
            self.log = all_aff_log[all_aff_log['SC_DP&Pub']==sc_dppub_affiliate]
            self.tier_three_drop_indices = self.log[(self.log['Selection Confidence']=='Tier Three')
                                             & (self.log['Date'].dt.date>today)].sort_values('Date').index
     
        
        self.switch_notes = []
        
    def find_past_cobras(self): 
        sc_dppub = self.sc_dppub 
        sc_dppub_affiliate = self.sc_dppub_affiliate
        cobra = self.cobra
        today = self.today
        
        #return cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
        #                         ((cobra['Send Strategy'].isin(['P','E','CT','OT','MA'])) |
        #                         (cobra['Segment'].str.contains('A120',na=False))) &
        #                         (cobra['Date'].dt.date<today)]
        return cobra[(cobra['shortcode_DP.SV']==sc_dppub) & 
                                 (cobra['Send Strategy'].isin(['P','PT','CT','OT','MI']))  &
                                 (cobra['Date'].dt.date<today)]        
    def find_upcoming_cobras(self):   
        sc_dppub = self.sc_dppub
        sc_dppub_affiliate = self.sc_dppub_affiliate
        cobra = self.cobra
        today = self.today
     
        #return cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
        #                         ((cobra['Send Strategy'].isin(['P','E','CT','OT','MA'])) |
        #                         (cobra['Segment'].str.contains('A120',na=False))) &
        #                         (cobra['Date'].dt.date>=today)]
        return cobra[(cobra['shortcode_DP.SV']==sc_dppub) & 
                                 (cobra['Send Strategy'].isin(['P','PT','CT','OT','MI']))  &
                                 (cobra['Date'].dt.date>=today)]        
        
    def find_initial_empties(self, days_ahead):
        fvs = self.fvs
        sc_dppub = self.sc_dppub
        sc_dppub_affiliate = self.sc_dppub_affiliate
        cobra = self.cobra
        today = self.today

        
        affiliate_p_drops = fvs.affiliate_p_drops
        affiliate_p_drops_weekend = affiliate_p_drops.copy()

        number_of_drops = affiliate_p_drops[sc_dppub_affiliate]
        number_of_drops_weekend = affiliate_p_drops_weekend[sc_dppub_affiliate]
        
        affiliate_extra_drop_days = fvs.get_affiliate_extra_drop_days()
        extra_days = fvs.find_days_available(affiliate_extra_drop_days[sc_dppub_affiliate])
        
        latest_date_to_schedule = today + timedelta(days=days_ahead)

        #initial_empties = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
        #                   (cobra['Date'].dt.date>=today) &
        #                   (cobra['Date'].dt.date<=latest_date_to_schedule) &
        #                   (cobra['Drop Number']>=1) &
        #                   ( ( (cobra['Drop Number']<=(number_of_drops)) & (cobra['Day of Week'].apply(lambda x: x in range(0,5))) ) |
        #                     ( (cobra['Drop Number']<=(number_of_drops_weekend)) & (cobra['Day of Week'].apply(lambda x: x in range(5,7))) )  |
        #                     ( (cobra['Drop Number']<=(number_of_drops+1)) & (cobra['Day of Week'].apply(lambda x: x in extra_days)))) & 
        #                   ((cobra['Offer'].isnull())|( cobra['Offer']==''))
        #                       ]
        initial_empties = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
                           (cobra['Date'].dt.date>=today) &
                           (cobra['Date'].dt.date<=latest_date_to_schedule) &
                           (cobra['Drop Number']>=1) &
                           ( ( (cobra['Drop Number']<=(number_of_drops)) & (cobra['Day of Week'].apply(lambda x: x in range(0,5))) ) |
                             ( (cobra['Drop Number']<=(number_of_drops_weekend)) & (cobra['Day of Week'].apply(lambda x: x in range(5,7))) )  |
                             ( (cobra['Drop Number']<=(number_of_drops+1)) & (cobra['Day of Week'].apply(lambda x: x in extra_days)))) & 
                           ((cobra['Offer'].isnull())|( cobra['Offer']==''))
                               ]
        return initial_empties
 
    def format_existing_drops(self,sc_dppub_affiliate,days_ahead,manual_offers,exclude_offers,update_cobra):

        cobra = self.cobra
        cobra_clean = self.cobra_clean
        cobra_sheet = self.cobra_sheet
        cobra_sheet_df = self.cobra_sheet_df
        emit = self.emit
        functions_dict = self.fvs.find_affiliate_functions(sc_dppub_affiliate)
        cs = self.cs
        today = self.today


        print('\n**********\nNow Formatting Existing Drops on Cobra:\n**********')


        def cpm_content(creative_col, cc_id_col, body_content_col, hitpath_col):

            if creative_col == 'CC':
                if (cc_id_col is np.nan) & (body_content_col is np.nan):
                    if 'content_id_determination' in functions_dict.keys():
                        chosen_content_selection_function = getattr(cs,functions_dict['content_id_determination'])
                    else:
                        chosen_content_selection_function = getattr(cs,'choose_ccid')
                    ccid = chosen_content_selection_function(int(hitpath_col),sc_dppub_affiliate)
                    return ccid
                else:
                    return body_content_col
            else:
                return body_content_col

        cpm_missing_drops = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
                                   (cobra['Date'].dt.date>=today + timedelta(days=2)) &
                                   (cobra['Date'].dt.date<=today + timedelta(days=days_ahead)) &
                                   ~((cobra['Offer'].isnull())|( cobra['Offer']=='')) &
                                    (cobra['Drop Number']>=1) &
                                    (cobra['Drop Number']<4) & 
                                    (cobra['Creative'].isna()) 
                                  ]

        cpm_missing_creative_drops = cpm_missing_drops[(cpm_missing_drops['Creative Type'].isnull()) &
                           ~(cpm_missing_drops['Offer'].str.contains('1001-',na=False))
                           &
                           ~(cpm_missing_drops['Offer'].str.contains('1002-',na=False))]
        
        def cpm_creative(hit):
            selected_creative_function = getattr(cs, functions_dict['creative_type_determination'])
            creative_type,ccid = selected_creative_function(sc_dppub_affiliate,hit,print_bool=True)
            return creative_type,ccid
        
        if len(cpm_missing_creative_drops)>0:
            cpm_missing_creative_drops[['Creative Type','Creative']] = cpm_missing_creative_drops['Hitpath Offer ID'].apply(lambda x: pd.Series(cpm_creative(x)))
        
        
        
        cpm_missing_content_drops = cpm_missing_drops[cpm_missing_drops['Creative Type']=='CC']
        

        cpm_missing_content_drops['Creative'] = cpm_missing_content_drops.apply(lambda x: cpm_content(x['Creative Type'], x['Creative'], x['Body Content'], x['Hitpath Offer ID']), axis=1)
        
        cpm_content_to_fill = pd.concat([cpm_missing_content_drops,cpm_missing_creative_drops])

        for index, drop in cpm_content_to_fill.iterrows():
            cpm_drop = drop.to_frame().transpose()
            drop_number = cpm_drop['Drop Number'].values[0]
            date_string = cpm_drop['Date'].dt.date.values[0].strftime('%-m/%-d/%Y')

            pub_row = cobra_sheet_df[cobra_sheet_df['B'].str.contains(sc_dppub_affiliate)].index.values[0]
            drop_row = int(pub_row + (drop_number-1)*11 + 1)
            date_column = cobra_sheet_df.columns.get_loc(date_string)+1

            ccid_row = drop_row + 6

            ccid = cpm_drop['Creative'].values[0]

            cpm_campaign = cpm_drop['Offer'].values[0]

            print('\nGiving CC ID {} on {} for {}.'.format(ccid,date_string,cpm_campaign))
            if update_cobra:
                if ccid is not np.nan:
                    time = cpm_drop['Time'].values[0] if cpm_drop['Time'].values[0] is not np.nan else ''
                    cobra_segment = cpm_drop['Segment'].values[0] if cpm_drop['Segment'].values[0] is not np.nan else ''
                    send_strategy = cpm_drop['Send Strategy'].values[0] if cpm_drop['Send Strategy'].values[0] is not np.nan else ''
                    creative_type = cpm_drop['Creative Type'].values[0] if cpm_drop['Creative Type'].values[0] is not np.nan else ''
                    akshad_notes = cpm_drop['Akshad Notes:'].values[0] if cpm_drop['Akshad Notes:'].values[0] is not np.nan else ''
                    mailer_notes = cpm_drop['Mailer Notes:'].values[0] if cpm_drop['Mailer Notes:'].values[0] is not np.nan else ''

                    self.add_to_log(sc_dppub_affiliate,date_string,drop_number,time,cobra_segment,send_strategy,
                             cpm_campaign,creative_type,ccid,akshad_notes,mailer_notes,
                             days_ahead,manual_offers,exclude_offers,functions_dict)

                    cpm_drop['Creative'] = ccid
                    cobra.loc[cpm_drop.index, :] = cpm_drop[:]

    #                 cobra_sheet.update_value((ccid_row,date_column), ccid)

        ## Fill in Time Slots            
        cpm_missing_time_drops = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
                                   (cobra['Date'].dt.date>=today + timedelta(days=2)) &
                                   (cobra['Date'].dt.date<=today + timedelta(days=days_ahead)) &
                                   ~((cobra['Offer'].isnull())|( cobra['Offer']=='')) &
                                    (cobra['Drop Number']>=1) &
                                    (cobra['Drop Number']<7) & 
                                    (cobra['Time'].isna())
                                  ]           
        cpm_missing_time_drops['Time'] = cpm_missing_time_drops['Drop Number'].apply(lambda x: self.find_time_of_day(sc_dppub_affiliate,date)) 

        for index, drop in cpm_missing_time_drops.iterrows():
            cpm_drop = drop.to_frame().transpose()
            drop_number = cpm_drop['Drop Number'].values[0]
            date_string = cpm_drop['Date'].dt.date.values[0].strftime('%-m/%-d/%Y')

            pub_row = cobra_sheet_df[cobra_sheet_df['B'].str.contains(sc_dppub_affiliate)].index.values[0]
            drop_row = int(pub_row + (drop_number-1)*11 + 1)
            date_column = cobra_sheet_df.columns.get_loc(date_string)+1

            time_row = drop_row + 0
            cpm_drop_time = cpm_drop['Time'].values[0]

            print('\nUpdating time to {} on {} in drop {}.'.format(cpm_drop_time,date_string,drop_number))
            if update_cobra:
                if cpm_drop_time is not np.nan:
                    time = cpm_drop['Time'].values[0] if cpm_drop['Time'].values[0] is not np.nan else ''
                    cobra_segment = cpm_drop['Segment'].values[0] if cpm_drop['Segment'].values[0] is not np.nan else ''
                    cpm_campaign = cpm_drop['Offer'].values[0] if cpm_drop['Offer'].values[0] is not np.nan else ''
                    send_strategy = cpm_drop['Send Strategy'].values[0] if cpm_drop['Send Strategy'].values[0] is not np.nan else ''
                    creative_type = cpm_drop['Creative Type'].values[0] if cpm_drop['Creative Type'].values[0] is not np.nan else ''
                    ccid = cpm_drop['Creative'].values[0] if cpm_drop['Creative'].values[0] is not np.nan else ''
                    akshad_notes = cpm_drop['Akshad Notes:'].values[0] if cpm_drop['Akshad Notes:'].values[0] is not np.nan else ''
                    mailer_notes = cpm_drop['Mailer Notes:'].values[0] if cpm_drop['Mailer Notes:'].values[0] is not np.nan else ''

                    self.add_to_log(sc_dppub_affiliate,date_string,drop_number,time,cobra_segment,send_strategy,
                             cpm_campaign,creative_type,ccid,akshad_notes,mailer_notes,
                             days_ahead,manual_offers,exclude_offers,functions_dict)

                    cpm_drop['Time'] = cpm_drop_time
                    cobra.loc[cpm_drop.index, :] = cpm_drop[:]

    #                 cobra_sheet.update_value((time_row,date_column), cpm_drop_time)

        ## Fill in Akshad Notes   
        cpm_missing_akshad_notes_drops = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
                                   (cobra['Date'].dt.date>=today + timedelta(days=2)) &
                                   (cobra['Date'].dt.date<=today + timedelta(days=days_ahead)) &
                                   ~((cobra['Offer'].isnull())|( cobra['Offer']=='')) &
                                    (cobra['Drop Number']>=1) &
                                    (cobra['Drop Number']<7) &
                                    ( cobra['Akshad Notes:'].isna() | (cobra['Akshad Notes:']=='' ) )
                                  ]            
        cpm_missing_akshad_notes_drops['Akshad Notes:'] = cpm_missing_akshad_notes_drops['Drop Number'].apply(lambda x: self.find_akshad_notes(sc_dppub_affiliate,x))   

        for index, drop in cpm_missing_akshad_notes_drops.iterrows():
            cpm_drop = drop.to_frame().transpose()
            drop_number = cpm_drop['Drop Number'].values[0]
            date_string = cpm_drop['Date'].dt.date.values[0].strftime('%-m/%-d/%Y')

            pub_row = cobra_sheet_df[cobra_sheet_df['B'].str.contains(sc_dppub_affiliate)].index.values[0]
            drop_row = int(pub_row + (drop_number-1)*11 + 1)
            date_column = cobra_sheet_df.columns.get_loc(date_string)+1

            akshad_row = drop_row + 8
            cpm_akshad_notes = cpm_drop['Akshad Notes:'].values[0]      

            if cpm_akshad_notes!='':
                print('\nUpdating Akshad Notes to {} on {} in drop {}.'.format(cpm_akshad_notes,date_string,drop_number))
                if update_cobra:
                    if cpm_akshad_notes is not np.nan:
                        time = cpm_drop['Time'].values[0] if cpm_drop['Time'].values[0] is not np.nan else ''
                        cobra_segment = cpm_drop['Segment'].values[0] if cpm_drop['Segment'].values[0] is not np.nan else ''
                        cpm_campaign = cpm_drop['Offer'].values[0] if cpm_drop['Offer'].values[0] is not np.nan else ''
                        send_strategy = cpm_drop['Send Strategy'].values[0] if cpm_drop['Send Strategy'].values[0] is not np.nan else ''
                        creative_type = cpm_drop['Creative Type'].values[0] if cpm_drop['Creative Type'].values[0] is not np.nan else ''
                        ccid = cpm_drop['Creative'].values[0] if cpm_drop['Creative'].values[0] is not np.nan else ''
                        akshad_notes = cpm_drop['Akshad Notes:'].values[0] if cpm_drop['Akshad Notes:'].values[0] is not np.nan else ''
                        mailer_notes = cpm_drop['Mailer Notes:'].values[0] if cpm_drop['Mailer Notes:'].values[0] is not np.nan else ''
                        self.add_to_log(sc_dppub_affiliate,date_string,drop_number,time,cobra_segment,send_strategy,
                                 cpm_campaign,creative_type,ccid,akshad_notes,mailer_notes,
                                 days_ahead,manual_offers,exclude_offers,functions_dict)

                        cpm_drop['Akshad Notes:'] = cpm_akshad_notes
                        cobra.loc[cpm_drop.index, :] = cpm_drop[:]
    #                     cobra_sheet.update_value((akshad_row,date_column), cpm_akshad_notes)
    #                     cobra_sheet.cell((akshad_row,date_column)).wrap_strategy = 'CLIP'

        #Correct Segments
        cpm_wrong_segment_drops = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
                                   (cobra['Date'].dt.date>=today + timedelta(days=2)) &
                                   (cobra['Date'].dt.date<=today + timedelta(days=days_ahead)) &
                                   ~((cobra['Offer'].isnull())|( cobra['Offer']=='')) &
                                    (cobra['Drop Number']>=1) &
                                    (cobra['Drop Number']<7) &
                                    (cobra['Send Strategy']=='P') &
                                    ( (cobra['Segment'].isnull()) | (cobra['Segment'].str[0].str.isdigit()) )
                                  ]         

        dp_pub = emit[emit['Revenue Pub ID']==int(sc_dppub_affiliate)]['DP&Pub'].mode().values[0] #dont need to create every time
        cpm_cobra_segment = dp_pub + "_" + 'A120'

        cpm_wrong_segment_drops['Segment'] = cpm_cobra_segment

        for index, drop in cpm_wrong_segment_drops.iterrows():
            cpm_drop = drop.to_frame().transpose()
            drop_number = cpm_drop['Drop Number'].values[0]
            date_string = cpm_drop['Date'].dt.date.values[0].strftime('%-m/%-d/%Y')

            pub_row = cobra_sheet_df[cobra_sheet_df['B'].str.contains(sc_dppub_affiliate)].index.values[0]
            drop_row = int(pub_row + (drop_number-1)*11 + 1)
            date_column = cobra_sheet_df.columns.get_loc(date_string)+1

            segment_row = drop_row + 1
            cpm_segment = cpm_drop['Segment'].values[0]      


            print('\nUpdating segment to {} on {} in drop {}.'.format(cpm_segment,date_string,drop_number))
            if update_cobra:
                if cpm_segment is not np.nan:
                    time = cpm_drop['Time'].values[0] if cpm_drop['Time'].values[0] is not np.nan else ''
                    cobra_segment = cpm_drop['Segment'].values[0] if cpm_drop['Segment'].values[0] is not np.nan else ''
                    cpm_campaign = cpm_drop['Offer'].values[0] if cpm_drop['Offer'].values[0] is not np.nan else ''
                    send_strategy = cpm_drop['Send Strategy'].values[0] if cpm_drop['Send Strategy'].values[0] is not np.nan else ''
                    creative_type = cpm_drop['Creative Type'].values[0] if cpm_drop['Creative Type'].values[0] is not np.nan else ''
                    ccid = cpm_drop['Creative'].values[0] if cpm_drop['Creative'].values[0] is not np.nan else ''
                    akshad_notes = cpm_drop['Akshad Notes:'].values[0] if cpm_drop['Akshad Notes:'].values[0] is not np.nan else ''
                    mailer_notes = cpm_drop['Mailer Notes:'].values[0] if cpm_drop['Mailer Notes:'].values[0] is not np.nan else ''

                    self.add_to_log(sc_dppub_affiliate,date_string,drop_number,time,cobra_segment,send_strategy,
                             cpm_campaign,creative_type,ccid,akshad_notes,mailer_notes,
                             days_ahead,manual_offers,exclude_offers,functions_dict)

                    cpm_drop['Segment'] = cpm_segment
                    cobra.loc[cpm_drop.index, :] = cpm_drop[:]
    #                 cobra_sheet.update_value((segment_row,date_column), cpm_segment)    

    
    def find_time_of_day(self, pub, date):

        cobra_sheet_df = self.cobra_sheet_df
        
        pub_row = cobra_sheet_df[cobra_sheet_df['B'].str.contains(pub)].index.values[0]
        # by default, drop_number =1
        drop_number =1
        drop_row = int(pub_row + (drop_number-1)*11 + 1)

        s = cobra_sheet_df.replace('', np.nan).loc[drop_row]

        new_s = s.reset_index()
        new_s.columns = ['Date','Time']
        new_s = new_s.iloc[2:]
        new_s['Date'] = pd.to_datetime(new_s['Date'])
        new_s['Weekday'] = new_s['Date'].dt.weekday
        new_s = new_s.loc[(new_s['Weekday']==date.weekday()) & (new_s['Time'].isna() == False),]
        # get the last one from the dataframe
        time_string = new_s['Time'].iloc[-1]
        #time_string = s.iloc[s.index.get_loc(s.last_valid_index())].lower()
        """ 
        #if 'pst' in time_string:
            #time_string = time_string.replace('pst','')
        time_string = time_string.strip()
        time_string = time_string.replace('am',' am')
        time_string = time_string.replace('pm',' pm')
        
        try:
            time_converted = datetime.datetime.strptime(time_string, '%I:%M %p')
        except:
            try:
                time_converted = datetime.datetime.strptime(time_string, '%I:%M:%S %p')
            except:
                random_hours = random.randint(7, 18)
                random_minutes = random.choice([0, 30])
                time_converted = datetime.datetime(year=2024, month=1, day=1, hour=random_hours, minute=random_minutes)
#                 time_converted = datetime.datetime.strptime("12:00 PM", '%I:%M %p')
             

        time_converted_string = time_converted.strftime('%I:%M %p')
        """ 
        return time_string
    
    def find_akshad_notes(self, pub, drop_number, date_string=date.today().strftime('%-m/%-d/%Y'), send_strategy='P'):
    
        previous_notes = []
        cobra_sheet_df = self.cobra_sheet_df

        pub_row = cobra_sheet_df[cobra_sheet_df['B'].str.contains(pub)].index.values[0]
        akshad_row = int(pub_row + (drop_number-1)*11 + 1) + 6
        date_column = cobra_sheet_df.columns.get_loc(date_string)+1

        for i in range(1,8):
            previous_notes.append(cobra_sheet_df.iloc[akshad_row, (date_column-i)])

        notes = max(previous_notes, key=previous_notes.count)

#         if send_strategy == 'OT':
#             notes = notes.replace('A120','A7')
#             notes = notes.replace('C120','C1')

        if send_strategy == 'MI':
            if notes=='':
                notes = "Intelliseed_March_2023"
            elif "Intelliseed" not in notes:
                notes = "Intelliseed_March_2023\n" + notes

        return notes
    
    def find_latest_scheduled_date(self,sc_dppub_affiliate,hitpath,drops_back=0):
        sc_dppub = sc_dppub_affiliate.strip()[:-7]
        cobra = self.cobra
        today = self.today
        #hitpath = int(hitpath)

        #latest_past_date = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
        #                           (cobra['Date'].dt.date<today) &
        #                           (cobra['Offer'].str.contains(str(hitpath)))
        #                          ]['Date'].max()
        latest_past_date = cobra[(cobra['shortcode_DP.SV']==sc_dppub) & 
                                   (cobra['Date'].dt.date<today) &
                                   (cobra['Offer'].str.contains(str(hitpath)))
                                  ]['Date'].max()
        #upcoming_dates = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
        #                       (cobra['Date'].dt.date>=today) &
        #                       (cobra['Offer'].str.contains(str(hitpath)))
        #                      ].sort_values('Date',ascending=False)['Date'][drops_back:]
        upcoming_dates = cobra[(cobra['shortcode_DP.SV']==sc_dppub) & 
                               (cobra['Date'].dt.date>=today) &
                               (cobra['Offer'].str.contains(str(hitpath)))
                              ].sort_values('Date',ascending=False)['Date'][drops_back:]
        if len(upcoming_dates)>=1:
            latest_scheduled_date = upcoming_dates.max()
        elif pd.isnull(latest_past_date): 
            latest_scheduled_date = pd.Timestamp('2020-01-01')
        else:
            latest_scheduled_date = latest_past_date
            
        return latest_scheduled_date
    
    def find_earliest_next_date(self,sc_dppub_affiliate,hitpath,drops_back=0):
        offs = self.offs
        functions_dict = self.fvs.find_affiliate_functions(sc_dppub_affiliate)
        sc_dppub = sc_dppub_affiliate.strip()[:-7]
        today = self.today
        if pd.isna(hitpath):
            return today
        
        offer_gap_function = getattr(offs, functions_dict['offer_gap_determination'])
        gap = offer_gap_function(sc_dppub,hitpath)
        
        latest_scheduled_date = self.find_latest_scheduled_date(sc_dppub,hitpath,drops_back=drops_back)

        earliest_next_date = max(latest_scheduled_date + timedelta(days=(gap)), today)
        
        return earliest_next_date
        
    def fill_placeholders(self,sc_dppub_affiliate,days_ahead,
                          exclude_offers=[],write_to_cobra_bool=False,print_bool=True,payout_tool=False):
        sc_dppub = sc_dppub_affiliate.strip()[:-7]
        cobra = self.cobra
#         fvs = find_viper_settings.inputs(lexi=self.lexi, cobra=self.cobra, offers=self.offers, affiliate_log=self.affiliate_log)
        fvs = self.fvs
        offs = self.offs
        functions_dict = self.fvs.find_affiliate_functions(sc_dppub_affiliate)
        offers = self.offers
        emit = self.emit
        cs = self.cs
        ss_offer = self.ss_offer
        today = self.today
        sc = sc_dppub_affiliate.split('_')[0]
        past_cobras = self.past_cobras
        
        affiliate_p_drops = fvs.affiliate_p_drops
        affiliate_p_drops_weekend = affiliate_p_drops.copy()
        
#         affiliate_p_drops = find_viper_settings.get_affiliate_p_drops()
#         affiliate_p_drops_weekend = find_viper_settings.get_affiliate_p_drops().copy()

        number_of_drops = affiliate_p_drops[sc_dppub_affiliate]
        number_of_drops_weekend = affiliate_p_drops_weekend[sc_dppub_affiliate]

        #find placeholder slots in upcoming cobra
        places = cobra[(cobra['shortcode_DP.SV']==sc_dppub) & 
                       (cobra['Date'].dt.date>=today) &
                       (cobra['Date'].dt.date<=(today + timedelta(days=days_ahead))) &
                       ( (cobra['Drop Number']>=1) | (cobra['Drop Number']<=4) ) &
                       ( 
                           (cobra['Offer'].str.contains('1001-'))
                           |
                           (cobra['Offer'].str.contains('1002-'))
                       )
                      ]

        for index,row in places.iterrows():

            campaign_id = row['Offer']
    #         advertiser = re.search('place(.*)offer here', campaign_id, re.IGNORECASE).group(1).strip()
            advertiser = campaign_id.split('-')[2]

            placeholder_function = getattr(offs, functions_dict['placeholder_offer_determination'])
            advertiser_offers = placeholder_function(sc_dppub_affiliate,advertiser)

            schedule_new_offer = False

            for hit in advertiser_offers:

                if not schedule_new_offer:
    #                 dates = affiliate_recent_production[affiliate_recent_production['Hitpath Offer ID']==hit]['Date']
#                     offer_gap_function = getattr(offs, functions_dict['offer_gap_determination'])
#                     gap = offer_gap_function(sc_dppub_affiliate,hit)

#                     latest_past_date = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
#                                    (cobra['Date'].dt.date<today) &
#                                    (cobra['Offer'].str.contains(str(hit)))
#                                   ]['Date'].max()

#                     upcoming_dates = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
#                                            (cobra['Date'].dt.date>=today) &
#                                            (cobra['Offer'].str.contains(str(hit)))
#                                           ]['Date']
#                     if len(upcoming_dates)>=1:
#                         latest_scheduled_date = upcoming_dates.max()
#                     elif pd.isnull(latest_past_date): 
#                         latest_scheduled_date = pd.Timestamp('2020-01-01')
#                     else:
#                         latest_scheduled_date = latest_past_date
                    
#                     earliest_next_date = max(latest_scheduled_date + timedelta(days=(gap)), today)
                    earliest_next_date = self.find_earliest_next_date(sc_dppub_affiliate,hit)
    
                    if row['Date'] >= earliest_next_date:
                        schedule_new_offer = True

                        next_drop = row.to_frame().transpose()

                        next_drop_date = next_drop['Date'].dt.date.values[0].strftime('%-m/%-d/%Y')

                        next_drop_number = next_drop['Drop Number'].values[0]

#                         campaign = offers[offers['Hitpath Offer ID']==hit]['Scheduling Name'].values[0]
                        #campaign = offers.loc[hit]['Scheduling Name']
                        
                        try: 
                            campaign = ss_offer.loc[(ss_offer['SS Offers (updated)'].str.contains(hit, na = False)) & (ss_offer['SS Offers (updated)'].str[-4:].str.contains(sc, na = False)), 'SS Offers (updated)' ].values[0]
                        except: 
                            print("The offer is not added in the SS.")
                        next_drop['Offer'] = campaign

                        if not payout_tool:
                            send_strategy = 'P'
                            next_drop['Send Strategy'] = send_strategy

                            #segment = 'A120'
                            #dp_pub = emit[emit['Revenue Pub ID']==int(sc_dppub_affiliate)]['DP&Pub'].mode().values[0]
                            #cobra_segment = dp_pub + "_" + segment
                            cobra_segment = past_cobras[(past_cobras['Dataset'] == sc_dppub_affiliate) & (past_cobras['Segment']!='') & (past_cobras['Drop Number'] == 1)].sort_values(by = 'Date', ascending = False)['Segment'].values[0]
                            date = next_drop['Date'].dt.date.values[0]
                            time = self.find_time_of_day(sc_dppub_affiliate,date)
                            next_drop['Time'] = time

                            selected_creative_function = getattr(cs, functions_dict['creative_type_determination'])
                            ccid = selected_creative_function(sc_dppub_affiliate,hit,print_bool=print_bool) 
        #                     creative_type = decide_creative_type(sc_dppub_affiliate,hit)

                            ##next_drop['Creative Type'] = creative_type
                            next_drop['Creative'] = ccid


                            #akshad_notes = self.find_akshad_notes(sc_dppub_affiliate,next_drop_number,send_strategy=send_strategy)

                            mailer_notes = 'Viper Scheduled'
                            ##next_drop['Mailer Notes:'] = mailer_notes 
                            ##next_drop['Akshad Notes:'] = akshad_notes 
                            next_drop['Segment'] = cobra_segment
                            next_drop['Limit'] = ''
                            next_drop['Offset'] = ''
                            next_drop['Creative'] = ccid
                            date1 = next_drop['Date'].dt.date.values[0].strftime("%d%b%y")  
                            next_drop['Job Name'] = "SS_"+cobra_segment[:3] + "_"+cobra_segment[4:].replace(".",'-').replace("_",'-')+"_"+ hit +"_"+ send_strategy + "_" +   date1
                            cobra.loc[next_drop.index, :] = next_drop[:]

                            self.past_cobras = cobra.loc[self.past_cobras.index.union(next_drop.index)]
                            self.upcoming_cobras = cobra.loc[self.upcoming_cobras.index.union(next_drop.index)]

                            if print_bool:
                                print("\nScheduling placeholder",advertiser,hit,"on",next_drop_date,"and ccid",ccid,
                          "in drop number",next_drop_number,"at",time)
                                

                            if write_to_cobra_bool:
                                manual_offers=[]

                                self.add_to_log(sc_dppub_affiliate,next_drop_date,next_drop_number,time,cobra_segment,send_strategy,
                                 campaign,ccid,
                                 days_ahead,manual_offers,exclude_offers,functions_dict,selection_confidence='Placeholder')

    def schedule_mining_drops(self,sc_dppub_affiliate, days_ahead, exclude_offers=[], update_cobra=False):
        cobra = self.cobra
        functions_dict = self.fvs.find_affiliate_functions(sc_dppub_affiliate)
        offs = self.offs
        fvs = self.fvs
        today = self.today


        print('\n**********\nNow Scheduling Mining Drops:\n**********')
        days_to_schedule = fvs.find_mi_drop_days(sc_dppub_affiliate)
        mining_days = fvs.find_days_available(days_to_schedule)
#         ((cobra['Date'].dt.dayofyear%2==0) == ote_bool) &
        
        def lambda_schedule_mining_offers(mining_segment,mining_drops):
            
            select_mining_offers = getattr(offs, functions_dict['mining_offer_determination'])
            mining_offers = select_mining_offers(sc_dppub_affiliate,mining_segment)
            mining_offers = [x for x in mining_offers if x not in exclude_offers]
            mining_offers = offs.return_unpaused_offers(sc_dppub_affiliate, mining_offers)
            
            empty_mine_slots = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
                               (cobra['Date'].dt.date>=today) &
                               (cobra['Date'].dt.date<=(today + timedelta(days=days_ahead))) &
                               (cobra['Drop Number'].isin(mining_drops)) &
                               ((cobra['Offer'].isnull())|( cobra['Offer']=='')) &
                               (cobra['Day of Week'].apply(lambda x: x in mining_days))
                              ]
            mining_drops_to_fill = len(empty_mine_slots)
            
            for hitpath in mining_offers:

                if mining_drops_to_fill > 0:
                    scheduled_bool = self.schedule_offer(sc_dppub_affiliate,hitpath,days_ahead,send_strategy='MI',
                                                         mining_drops=mining_drops,
                                                         write_to_cobra_bool=update_cobra)

                    if scheduled_bool:
                        mining_drops_to_fill-=1
                            
        
        if 'x2' in days_to_schedule.lower():
            
            if sc_dppub_affiliate in ['160398','460632']: #only relevant now for these two to have different mining segment offers         
                def gmail_or_nongmail(seg):
                    if 'non' in seg.lower():
                        return 'MINGM'
                    else:
                        return 'MI'

                eight_segment = gmail_or_nongmail(self.find_segment(sc_dppub_affiliate,8))
                mining_drops = [8]
#                 select_mining_offers = getattr(offs, functions_dict['mining_offer_determination'])
#                 mining_offers_eight = select_mining_offers(sc_dppub_affiliate,eight_segment)
#                 mining_offers_eight = [x for x in mining_offers_eight if x not in exclude_offers]
#                 mining_offers_eight = offs.return_unpaused_offers(sc_dppub_affiliate, mining_offers_eight)
                print('Begin scheding')
                lambda_schedule_mining_offers(eight_segment,mining_drops)
                print('EIGHT DONE')
        
                nine_segment = gmail_or_nongmail(self.find_segment(sc_dppub_affiliate,9))
                mining_drops = [9]
#                 select_mining_offers = getattr(offs, functions_dict['mining_offer_determination'])
#                 mining_offers_nine = select_mining_offers(sc_dppub_affiliate,nine_segment)
#                 mining_offers_nine = [x for x in mining_offers_nine if x not in exclude_offers]
#                 mining_offers_nine = offs.return_unpaused_offers(sc_dppub_affiliate, mining_offers_nine)
                
                lambda_schedule_mining_offers(nine_segment,mining_drops)
                
            else:
                mining_segment = 'default'
                mining_drops = [8,9]
                lambda_schedule_mining_offers(mining_segment,mining_drops)
                
            
            
        else:
            mining_drops = [8]
            
#             mining_offers = select_mining_offers(sc_dppub_affiliate)
#             mining_offers = [x for x in mining_offers if x not in exclude_offers]
#             mining_offers = offs.return_unpaused_offers(sc_dppub_affiliate, mining_offers)
            mining_segment = 'default'
            
            lambda_schedule_mining_offers(mining_segment,mining_drops)
            
            

        empty_mine_slots = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
                   (cobra['Date'].dt.date>=today) &
                   (cobra['Date'].dt.date<=(today + timedelta(days=days_ahead))) &
                   (cobra['Drop Number'].isin(mining_drops)) &
                   ((cobra['Offer'].isnull())|( cobra['Offer']=='')) &
                   (cobra['Day of Week'].apply(lambda x: x in mining_days))
                  ]

        if len(empty_mine_slots)==0:
            print('\nCompleted Scheduling all Mining Drops through',(today + timedelta(days=days_ahead)).strftime('%-m/%-d/%Y'))
        else:
            print('There are still {} mining drops to schedule between now and {}'.format(len(empty_mine_slots),(today + timedelta(days=days_ahead)).strftime('%-m/%-d/%Y')))
            
            
            
            
            
    def schedule_mining_drops_old(self,sc_dppub_affiliate, days_ahead, exclude_offers=[], update_cobra=False):
        cobra = self.cobra
        functions_dict = self.fvs.find_affiliate_functions(sc_dppub_affiliate)
        offs = self.offs
        fvs = self.fvs
        today = self.today

    #     mining_offers = offer_selection.find_top_mi_offers(sc_dppub_affiliate,lexi)

        select_mining_offers = getattr(offs, functions_dict['mining_offer_determination'])
        mining_offers = select_mining_offers(sc_dppub_affiliate)

        mining_offers = [x for x in mining_offers if x not in exclude_offers]

        mining_offers = offs.return_unpaused_offers(sc_dppub_affiliate, mining_offers)

        print('\n**********\nNow Scheduling Mining Drops:\n**********')
        days_to_schedule = fvs.find_mi_drop_days(sc_dppub_affiliate)
        mining_days = fvs.find_days_available(days_to_schedule)
        if 'x2' in days_to_schedule.lower():
            mining_drops = [8,9]
        else:
            mining_drops = [8]

        empty_mine_slots = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
                           (cobra['Date'].dt.date>=today) &
                           (cobra['Date'].dt.date<=(today + timedelta(days=days_ahead))) &
                           (cobra['Drop Number'].isin(mining_drops)) &
                           ((cobra['Offer'].isnull())|( cobra['Offer']=='')) &
                           (cobra['Day of Week'].apply(lambda x: x in mining_days))
                          ]
        mining_drops_to_fill = len(empty_mine_slots)

        for hitpath in mining_offers:

            if mining_drops_to_fill > 0:
                scheduled_bool = self.schedule_offer(sc_dppub_affiliate,hitpath,days_ahead,send_strategy='MI',
                                                     write_to_cobra_bool=update_cobra)
                if scheduled_bool:
                    mining_drops_to_fill-=1

        empty_mine_slots = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
                   (cobra['Date'].dt.date>=today) &
                   (cobra['Date'].dt.date<=(today + timedelta(days=days_ahead))) &
                   (cobra['Drop Number'].isin(mining_drops)) &
                   ((cobra['Offer'].isnull())|( cobra['Offer']=='')) &
                   (cobra['Day of Week'].apply(lambda x: x in mining_days))
                  ]

        if len(empty_mine_slots)==0:
            print('\nCompleted Scheduling all Mining Drops through',(today + timedelta(days=days_ahead)).strftime('%-m/%-d/%Y'))
        else:
            print('There are still {} mining drops to schedule between now and {}'.format(len(empty_mine_slots),(today + timedelta(days=days_ahead)).strftime('%-m/%-d/%Y')))



    def schedule_production_drops(self,sc_dppub_affiliate,days_ahead,manual_offers,exclude_offers,
                                  update_cobra=False,print_bool=True,metric='eCPM'):
        if print_bool:
            print('\n**********\nNow Scheduling Production Drops:\n**********')

        cobra = self.cobra
        functions_dict = self.fvs.find_affiliate_functions(sc_dppub_affiliate)
        if 'tier_one_metric' in functions_dict.keys():
            metric = functions_dict['tier_one_metric']
        else:
            metric = 'eCPM'
        print(f'*** Scheduling Tier One with {metric} ***')
        offs=self.offs
        fvs = self.fvs
        today = self.today

        affiliate_extra_drop_days = fvs.get_affiliate_extra_drop_days()

        scheduled_offers = []

        affiliate_p_drops = fvs.affiliate_p_drops
        affiliate_p_drops_weekend = affiliate_p_drops.copy()

        number_of_drops = affiliate_p_drops[sc_dppub_affiliate]
        number_of_drops_weekend = affiliate_p_drops_weekend[sc_dppub_affiliate]

#         print('Number of drops!!',number_of_drops)
        
        #First Fill any Potential "Placeholder" Offers
        self.fill_placeholders(sc_dppub_affiliate,days_ahead,exclude_offers=exclude_offers,
                          write_to_cobra_bool=update_cobra,print_bool=print_bool)

        latest_date_to_schedule = today + timedelta(days=days_ahead)
        extra_days = fvs.find_days_available(affiliate_extra_drop_days[sc_dppub_affiliate])
       
        ###################
        # Tier One Offers #
        ###################
#         off = offer_selection.inputs(self.fvs, self.lexi, self.cobra, self.offers, self.affiliate_log)
        tier_one_function = getattr(offs, functions_dict['tier_one_production_determination'])
        tier_one_offers = tier_one_function(sc_dppub_affiliate,primary_metric=metric)
        #exclude "exclude_offers" and make sure "manual_offers" do not appear twice
        offers_to_schedule = manual_offers + [x for x in tier_one_offers if x not in exclude_offers and x not in manual_offers]
        
        offers_to_schedule = offs.return_unpaused_offers(sc_dppub_affiliate, offers_to_schedule)
        offers = self.offers


        for hitpath in offers_to_schedule:

            scheduled_date = False

            if len(self.production_empty_indices) > 0:
                scheduled_date = self.schedule_offer(sc_dppub_affiliate,hitpath,days_ahead,manual_offers=manual_offers,
                                                write_to_cobra_bool=update_cobra,selection_confidence='Tier One',
                                                print_bool=print_bool)
                
                if scheduled_date:
                    scheduled_offers.append({'Hitpath':hitpath,'Tier':1,'Date':scheduled_date})
                    if functions_dict['production_drop_preference']=='offer_performance':
                        offers_to_schedule.insert(0,hitpath)

                    else: #offer variety
                        offers_to_schedule.append(hitpath)


        leftover_slots = cobra[(cobra['Affiliate ID']==sc_dppub_affiliate) & 
                           (cobra['Date'].dt.date>=today) &
                           (cobra['Date'].dt.date<=latest_date_to_schedule) &
                           (cobra['Drop Number']>1) &    
                           ( ( (cobra['Drop Number']<=(number_of_drops)) & (cobra['Day of Week'].apply(lambda x: x in range(0,5))) ) |
                             ( (cobra['Drop Number']<=(number_of_drops_weekend)) & (cobra['Day of Week'].apply(lambda x: x in range(5,7))) ) |
                               ( (cobra['Drop Number']<=(number_of_drops+1)) & (cobra['Day of Week'].apply(lambda x: x in extra_days)))) &
                           ((cobra['Offer'].isnull())|( cobra['Offer']==''))]
        if print_bool:
            print('\n*** Completed scheduling tier one;',len(leftover_slots),'spots to be filled with tier two offers ***')

        ###################
        # Tier Two Offers #
        ###################
        
        if len(self.production_empty_indices) > 0:

            tier_two_function = getattr(offs, functions_dict['tier_two_production_determination'])
            tier_two_offers = tier_two_function(sc_dppub_affiliate)

            tier_two_offers = [x for x in tier_two_offers if x not in exclude_offers]
            tier_two_offers = offs.return_unpaused_offers(sc_dppub_affiliate, tier_two_offers)

            for hitpath in tier_two_offers:

                send_strategy = 'P'
                segment = 'A120'

                scheduled_date = False
                if len(self.production_empty_indices) > 0:
                    scheduled_date = self.schedule_offer(sc_dppub_affiliate,hitpath,days_ahead, manual_offers=manual_offers,
                                                    write_to_cobra_bool=update_cobra,
                                  selection_confidence='Tier Two',print_bool=print_bool,
                                                        simple_creative=True)

                if scheduled_date:
                    scheduled_offers.append({'Hitpath':hitpath,'Tier':2,'Date':scheduled_date})

        if print_bool:
            print('\n*** Completed scheduling tier two;',len(self.production_empty_indices),'spots to be filled with tier three offers ***')

        #####################
        # Tier Three Offers #
        #####################
        if 'dynamic_drops' in functions_dict.keys():
            dynamic_drops_bool = eval(functions_dict['dynamic_drops'])
        else:
            dynamic_drops_bool = False
#         print('dynamic_drops_bool',dynamic_drops_bool)
        tier_three_count = 0
        if len(self.production_empty_indices) > 0:

            tier_three_function = getattr(offs, functions_dict['tier_three_production_determination'])
            tier_three_offers = tier_three_function(sc_dppub_affiliate)

            tier_three_offers = [x for x in tier_three_offers if x not in exclude_offers]
            tier_three_offers = offs.return_unpaused_offers(sc_dppub_affiliate, tier_three_offers)
            
            for hitpath in tier_three_offers:

                if (dynamic_drops_bool==False) | (tier_three_count<2):
                    send_strategy = 'P'
                    segment = 'A120'

                    scheduled_date = False
                    if len(self.production_empty_indices) > 0:
                        scheduled_date = self.schedule_offer(sc_dppub_affiliate,hitpath,days_ahead, manual_offers=manual_offers,
                                                    write_to_cobra_bool=update_cobra,
                                  selection_confidence='Tier Three',print_bool=print_bool)

                    if scheduled_date:
                        scheduled_offers.append({'Hitpath':hitpath,'Tier':3,'Date':scheduled_date})
                        tier_three_count+=1
                else:
                    print('\nDynamically reducing drops!')
#                     print(self.production_empty_indices)
#                     cobra.loc[next_drop.index, :] = next_drop[:]
                    #cobra.loc[self.production_empty_indices,'Akshad Notes:'] = 'Please Leave Blank'
                    #cobra.loc[self.production_empty_indices,'Mailer Notes:'] = 'Viper Left Blank'
                    break

        if print_bool:
            print('\nThere are',len(self.production_empty_indices),'spots that remain empty in the next',days_ahead,'days.') 
            
            
        #Alert Mailer of empty slots
        if len(self.production_empty_indices)!=0:
            viper_main.hiss()
        
     
        return scheduled_offers,self.cobra,self.switch_notes
    
    def show_switch_notes(self,switch_notes):
        if len(switch_notes)>0:
            viper_main.rattle()
            print("\n\nWrite below to html-scheduling-issues in Slack:")
            switch_notes = [x for x in switch_notes if x!='']
            for note in switch_notes:
                print(note)


    def find_empty_slots(self,sc_dppub_affiliate, hitpath, days_ahead, send_strategy='P', manual_offers=[], mining_drops = [8],
                         exclude_offers=[], given_empties=[], gap=[], swapping=False, selection_confidence='none'):
        cobra = self.cobra
        functions_dict = self.fvs.find_affiliate_functions(sc_dppub_affiliate)
        offers=self.offers
        offs=self.offs
        today = self.today
        
        fvs = self.fvs
        affiliate_extra_drop_days = fvs.get_affiliate_extra_drop_days()
        affiliate_ot_drop_days = fvs.get_affiliate_ot_drop_days()
        affiliate_e_drop_days = fvs.get_affiliate_e_drop_days()

        mirrors,true_mirrors = fvs.find_mirrors()

        if gap ==[]:
            offer_gap_function = getattr(offs, functions_dict['offer_gap_determination'])
            gap = offer_gap_function(sc_dppub_affiliate,hitpath)
       
        if send_strategy=='MI':
            gap = 2
        ######### we don't consider budget in sms now #########    
        #budget_frame = fvs.budget_frame
      
        #grouped_budget_frame = budget_frame.groupby(['PUB ID','Scheduling Name ']).agg({'Production Segment':'sum'}).reset_index()
        #grouped_budget_frame['Scheduling Name '] = grouped_budget_frame['Scheduling Name '].astype(str)
        #grouped_budget_frame['Hitpath Offer ID'] = grouped_budget_frame['Scheduling Name '].str.split('-').str[0].astype(int)
        #budgeted_offers_affiliate = grouped_budget_frame[grouped_budget_frame['Account']==sc_dppub_affiliate]
        #budgeted_offers_list = budgeted_offers_affiliate['Hitpath Offer ID'].tolist()
        budgeted_offers_list = []
#         day_restrictions = offers[offers['Hitpath Offer ID']==hitpath]['Day Restrictions'].iloc[0]
        day_restrictions = offers.loc[hitpath]['Day Restrictions']
        available_days = fvs.find_days_restrictions(day_restrictions)
        
        if mirrors.get(hitpath) is None: #ensure that mirrors are not sent on same day
            mirror_blackout_dates = []
        else:
            mirror_blackout_dates = self.upcoming_cobras[self.upcoming_cobras['Hitpath Offer ID'].isin(mirrors[hitpath])]['Date']
            
            
        if true_mirrors.get(hitpath) is None:
            true_mirrors[hitpath] = [] 

            
        latest_past_date = self.past_cobras[(self.past_cobras['Offer'].str.contains(str(hitpath)))|
                                            (self.past_cobras['Hitpath Offer ID'].isin(true_mirrors.get(hitpath)))]['Date'].max()

        upcoming_dates = self.upcoming_cobras[(self.upcoming_cobras['Offer'].str.contains(str(hitpath)))|
                                            (self.upcoming_cobras['Hitpath Offer ID'].isin(true_mirrors.get(hitpath)))]['Date'] 
        
        if len(upcoming_dates)>=1:
            latest_scheduled_date = upcoming_dates.max()
        elif pd.isnull(latest_past_date): 
            latest_scheduled_date = pd.Timestamp('2020-01-01')
        else:
            latest_scheduled_date = latest_past_date

        latest_date_to_schedule = today + timedelta(days=days_ahead)
        earliest_next_date = max(latest_scheduled_date.date() + timedelta(days=(gap)), today)
        
        offer_row = offers.loc[hitpath]
        if "paused" in offer_row['Status'].lower():
            #unpause_date = offer_row['Future Unpause Date']  # we don't consider unpause date in sms now
            unpause_date = np.nan
            if not pd.isnull(unpause_date):
                earliest_next_date = max(earliest_next_date, unpause_date)
            else: #prevent from scheduling
                earliest_next_date = today + timedelta(days=days_ahead+1)
        elif offer_row['Status']=="Live":
            #pause_date = offer_row['Future Pause Date'] #  we don't consider pause date in sms now
            pause_date = np.nan
 
            if not pd.isnull(pause_date):
                if pause_date >= today:
                    latest_date_to_schedule = pause_date + timedelta(days=-1)

        
        # Check if we need to find blackout dates for Offer Vertical Gapping
        self.past_cobras['Vertical'] = np.where(self.past_cobras['Hitpath Offer ID'].notnull(),
                                                self.past_cobras['Hitpath Offer ID'].map(offers['Vertical']),'N/A')
        self.upcoming_cobras['Vertical'] = np.where(self.upcoming_cobras['Hitpath Offer ID'].notnull(),
                                                self.upcoming_cobras['Hitpath Offer ID'].map(offers['Vertical']),'N/A')

        both_cobras = pd.concat([self.upcoming_cobras,self.past_cobras])
        cobra_verticals = both_cobras.groupby(['Vertical']).agg({'Vertical':'count'}).rename(columns={'Vertical':'Vertical Count'})

#         vertical_gap_cutoff = cobra_verticals['Vertical Count'].sum()/20
        
        vertical_gap_cutoff = np.percentile(cobra_verticals['Vertical Count'],75)

        rare_verticals = cobra_verticals[cobra_verticals['Vertical Count']<vertical_gap_cutoff].index.tolist()
        
        hitpath_vertical = offers.loc[hitpath]['Vertical']
        if hitpath_vertical in rare_verticals: #find blackout vertical dates
            rare_vertical_dates = self.upcoming_cobras[self.upcoming_cobras['Vertical']==hitpath_vertical]['Date']
        else: #no blackout dates
            rare_vertical_dates = []
        
        affiliate_p_drops = fvs.affiliate_p_drops
        affiliate_p_drops_weekend = affiliate_p_drops.copy()

        number_of_drops = affiliate_p_drops[sc_dppub_affiliate]
        number_of_drops_weekend = affiliate_p_drops_weekend[sc_dppub_affiliate]

            
        if send_strategy=='MI':

            days_to_schedule = fvs.find_mi_drop_days(sc_dppub_affiliate)
            mining_days = fvs.find_days_available(days_to_schedule)
#             if 'x2' in days_to_schedule.lower():
#                 mining_drops = [8,9]
#             else:
#                 mining_drops = [8]
            
            earliest_next_date = today

            return cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
                           (cobra['Date'].dt.date>=earliest_next_date) &
                           (cobra['Date'].dt.date<=latest_date_to_schedule) &
                           (cobra['Drop Number'].isin(mining_drops)) &
                           ((cobra['Offer'].isnull())|( cobra['Offer']=='')) &
                           (cobra['Day of Week'].apply(lambda x: x in available_days)) &
                           (cobra['Day of Week'].apply(lambda x: x in mining_days))
                          ]

        elif (send_strategy=='OT') | (send_strategy=='E'):
#             given_empties = self.ote_empty_indices
            if send_strategy=='E':
                cobra_empties = cobra.loc[self.e_empty_indices]
                days_to_schedule = affiliate_e_drop_days[sc_dppub_affiliate]
            else:
                cobra_empties = cobra.loc[self.ot_empty_indices]
                days_to_schedule = affiliate_ot_drop_days[sc_dppub_affiliate]

            earliest_next_date = max(latest_scheduled_date + timedelta(days=(gap)), today + timedelta(days=1))
            if days_to_schedule is not np.nan:
                if re.search(r'\bevery|other\b',days_to_schedule, re.IGNORECASE):
                    ote_bool = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
                              (cobra['Date'].dt.date>=earliest_next_date) &
                              (cobra['Date'].dt.date<=latest_date_to_schedule) &
                             ~(cobra['Date'].isin(rare_vertical_dates)) &
                             ~(cobra['Date'].isin(mirror_blackout_dates)) &
                              (cobra['Drop Number']==1) &
                             ~((cobra['Offer'].isnull())|( cobra['Offer']==''))]['Date'].max().dayofyear%2==0

                    empty_slots = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
                                     (cobra['Date'].dt.date>=today + timedelta(days=1)) &
                                     (cobra['Date'].dt.date<=latest_date_to_schedule) &
                                     ~(cobra['Date'].isin(rare_vertical_dates)) &
                                     ~(cobra['Date'].isin(mirror_blackout_dates)) &
                                     (cobra['Drop Number']==1) &
                                     ((cobra['Offer'].isnull())|( cobra['Offer']=='')) &
                                    ((cobra['Date'].dt.dayofyear%2==0) == ote_bool) &
                                     (cobra['Day of Week'].apply(lambda x: x in available_days)) ]
                    
                else:
                    
                    ote_days = fvs.find_days_available(days_to_schedule)
                    
                    return cobra_empties.loc[(cobra_empties['Date'].dt.date>=earliest_next_date) &
                          ~(cobra['Date'].isin(rare_vertical_dates)) &
                          ~(cobra['Date'].isin(mirror_blackout_dates)) &
                           (cobra_empties['Offer'].isnull()) &
                           (cobra['Day of Week'].apply(lambda x: x in ote_days)) &
                           (cobra_empties['Day of Week'].apply(lambda x: x in available_days))]
                        
            
        else: #Production Offers
            given_empties = self.production_empty_indices
            extra_days = fvs.find_days_available(affiliate_extra_drop_days[sc_dppub_affiliate])
            log = self.log

            tier_three_drop_indices = self.tier_three_drop_indices
            
            if (len(tier_three_drop_indices)>0) & (selection_confidence!='Tier Three'):

                potential_switches = log.loc[tier_three_drop_indices]                
                
                potential_switches['Day of Week'] = potential_switches['Date'].dt.dayofweek
                potential_switches['Drop Number'] = potential_switches['Drop Number'].astype(int)
                
                current_weekday = today.weekday()
                if current_weekday>=4: #so Akshad does not have last minute switches on weekend
                    days_until_next_monday = 7 - current_weekday #weekdays are numbered starting with Monday at 0
                    next_monday = date.today() + timedelta(days=days_until_next_monday)
                    earliest_next_date = max(earliest_next_date, next_monday)
                
                potential_switches = potential_switches[
                                                (potential_switches['Day of Week'].apply(lambda x: x in available_days))
                                                  & ~(potential_switches['Date'].isin(rare_vertical_dates)) 
                                                  & ~(potential_switches['Date'].isin(mirror_blackout_dates)) 
                                                  &  (potential_switches['Date'].dt.date>=earliest_next_date)
                                                  &  (potential_switches['Drop Number'].isin(range(2,7)))
                                                     ]
                if len(potential_switches)>0:
                    
                    switch = potential_switches.iloc[0]
                
                    cobra_switch = cobra[(cobra['Date']==switch['Date'])
                          &(cobra['Hitpath Offer ID']==switch['Hitpath Offer ID'])
                          &(cobra['Dataset']==sc_dppub_affiliate)
                          ]
                    self.tier_three_drop_indices = self.tier_three_drop_indices.drop(potential_switches.index[0])

                    return cobra_switch
               
            
            if hitpath in manual_offers:
                return cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
                           (cobra['Date'].dt.date>=earliest_next_date) &
                           (cobra['Date'].dt.date>=today) &
                           (cobra['Date'].dt.date<=latest_date_to_schedule) &
                           ~(cobra['Date'].isin(rare_vertical_dates)) &
                           ~(cobra['Date'].isin(mirror_blackout_dates)) &
                           (cobra['Drop Number']>1) & 
                           ( ( (cobra['Drop Number']<=(number_of_drops)) & (cobra['Day of Week'].apply(lambda x: x in range(0,5))) ) |
                             ( (cobra['Drop Number']<=(number_of_drops_weekend)) & (cobra['Day of Week'].apply(lambda x: x in range(5,7))) ) |
                             ( (cobra['Drop Number']<=(number_of_drops+1)) & (cobra['Day of Week'].apply(lambda x: x in extra_days)) ) )  & 
                           ((cobra['Offer'].isnull())|( cobra['Offer']=='')) &
                           (cobra['Day of Week'].apply(lambda x: x in available_days))
                          ] 
            
            elif hitpath in budgeted_offers_list:
                budget_title_month = fvs.get_budget_title_month()
                return cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
                           (cobra['Date'].dt.date>=earliest_next_date) &
                           (cobra['Date'].dt.date<=latest_date_to_schedule) &
                          ~(cobra['Date'].isin(rare_vertical_dates)) &
                          ~(cobra['Date'].isin(mirror_blackout_dates)) &
                           (cobra['Drop Number']>1) & 
                           (((cobra['Drop Number']<=(number_of_drops)) & (cobra['Day of Week'].apply(lambda x: x in range(0,5)))) |
                           ((cobra['Drop Number']<=(number_of_drops_weekend)) &
                           (cobra['Day of Week'].apply(lambda x: x in range(5,7))))  |
                           ((cobra['Drop Number']<=(number_of_drops+1)) & (cobra['Day of Week'].apply(lambda x: x in extra_days)))) &
                           ((cobra['Offer'].isnull())|( cobra['Offer']=='')) &
                           (cobra['Day of Week'].apply(lambda x: x in available_days)) &
                           (cobra['Date'].dt.month_name()==budget_title_month)] 

            else:

                if (swapping==True) | (len(given_empties)==0):
#                     print('Check')
                    return cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
                               (cobra['Date'].dt.date>=earliest_next_date) &
                               (cobra['Date'].dt.date<=latest_date_to_schedule) &
                              ~(cobra['Date'].isin(rare_vertical_dates)) &
                              ~(cobra['Date'].isin(mirror_blackout_dates)) &
                               (cobra['Drop Number']>1) & 
                               ( ( (cobra['Drop Number']<=(number_of_drops)) & (cobra['Day of Week'].apply(lambda x: x in range(0,5))) ) |
                                 ( (cobra['Drop Number']<=(number_of_drops_weekend)) & (cobra['Day of Week'].apply(lambda x: x in range(5,7))) ) | 
                                 ( (cobra['Drop Number']<=(number_of_drops+1)) & (cobra['Day of Week'].apply(lambda x: x in extra_days)))) &
                               ((cobra['Offer'].isnull())|( cobra['Offer']=='')) &
                               (cobra['Day of Week'].apply(lambda x: x in available_days))]
                else:
                    
                    cobra_empties = cobra.loc[given_empties]
                
                    return cobra_empties.loc[
                        (cobra_empties['Date'].dt.date>=earliest_next_date) &
                                          ~(cobra_empties['Date'].isin(rare_vertical_dates)) &
                                          ~(cobra_empties['Date'].isin(mirror_blackout_dates)) &
                               ((cobra_empties['Offer'].isnull())|( cobra_empties['Offer']=='')) &
                               (cobra_empties['Day of Week'].apply(lambda x: x in available_days))]


    def schedule_offer(self,sc_dppub_affiliate,offer,days_ahead,segment='A120',send_strategy='P',manual_offers=[],
                       exclude_offers=[],write_to_cobra_bool=False,selection_confidence='',given_empties=[],
                       print_bool=True,simple_creative=False,mining_drops=[8]):
        #simple_creative will eliminate the randomness from HTML/CC selection
        cobra = self.cobra
        functions_dict = self.fvs.find_affiliate_functions(sc_dppub_affiliate)
        offers = self.offers
        emit = self.emit
        fvs = self.fvs
        offs = self.offs
        ss_offer = self.ss_offer
        switch_notes = self.switch_notes
        sc = sc_dppub_affiliate.split('_')[0]
        past_cobras = self.past_cobras
        
#         fvs = find_viper_settings.inputs(lexi=self.lexi, cobra=self.cobra, offers=self.offers, affiliate_log=self.affiliate_log)
        mirrors,true_mirrors = fvs.find_mirrors()
        
        offer_gap_function = getattr(offs, functions_dict['offer_gap_determination'])
        gap = offer_gap_function(sc_dppub_affiliate,offer)
        empty_slots = self.find_empty_slots(sc_dppub_affiliate,offer,days_ahead,send_strategy=send_strategy,
                                       manual_offers=manual_offers,exclude_offers=exclude_offers,given_empties=given_empties,
                                            gap=gap,selection_confidence=selection_confidence,mining_drops=mining_drops)
        
        if len(empty_slots) > 0:
            
            next_drop = empty_slots[empty_slots['Date']==empty_slots['Date'].min()].sample(n=1)

            next_drop_date = next_drop['Date'].dt.date.values[0].strftime('%-m/%-d/%Y')

            if self.check_debt_vertical_gapping(sc_dppub_affiliate,offer,next_drop['Date'].dt.date.values[0]):


                next_drop_number = next_drop['Drop Number'].values[0]

#                 campaign = offers[offers['Hitpath Offer ID']==offer]['Scheduling Name'].values[0]
                #campaign = offers.loc[offer]['Scheduling Name']
                try: 
                    campaign = ss_offer.loc[(ss_offer['SS Offers (updated)'].str.contains(offer, na = False)) & (ss_offer['SS Offers (updated)'].str[-4:].str.contains(sc, na = False)), 'SS Offers (updated)' ].values[0]
                except: 
                    print("The offer is not added in the SS.")
                
                t3_bool=False
                if next_drop['Offer'].any(): #if this drop if already occupied with a tier three campaign
                    old_offer = str(int(next_drop['Hitpath Offer ID'].iloc[0]))
                    next_drop['Body Content'] = ''
                    t3_bool=True


                next_drop['Offer'] = campaign
                next_drop['Hitpath Offer ID'] = offer

                if send_strategy=='MI':
                    cobra_segment = self.find_segment(sc_dppub_affiliate, next_drop_number)
                else:
                    #dp_pub = emit[emit['Revenue Pub ID']==int(sc_dppub_affiliate)]['DP&Pub'].mode().values[0]
                    #cobra_segment = dp_pub + "_" + segment
                    cobra_segment = past_cobras[(past_cobras['Dataset'] == sc_dppub_affiliate) & (past_cobras['Segment']!='') & (past_cobras['Drop Number'] == 1)].sort_values(by = 'Date', ascending = False)['Segment'].values[0]
                            
                date = next_drop['Date'].dt.date.values[0]
                time = self.find_time_of_day(sc_dppub_affiliate,date)
                next_drop['Time'] = time

                cs = self.cs
                default_content_select = True
                if 'content_split_tests_per_week' in functions_dict.keys(): #Split Testing
                    next_drop_week = datetime.datetime.strptime(next_drop_date,'%m/%d/%Y').isocalendar()[1]
                    weekly_splits_scheduled = len(cobra[(cobra['Date'].dt.isocalendar().week==next_drop_week) 
                          & (cobra['Send Strategy']=='ST') & (cobra['Dataset']==sc_dppub_affiliate)])
                    
                    if functions_dict['content_split_tests_per_week'] - weekly_splits_scheduled > 0:
                        first_selected_creative_function = getattr(cs, functions_dict['first_creative_type_determination'])
                        first_ccid = first_selected_creative_function(sc_dppub_affiliate,offer,
                                                                        print_bool=print_bool,simple=simple_creative)
                        second_selected_creative_function = getattr(cs, functions_dict['second_creative_type_determination'])
                        second_ccid = second_selected_creative_function(sc_dppub_affiliate,offer,
                                                                        print_bool=print_bool,simple=simple_creative)
                        
                        if first_ccid != second_ccid:
                            print("\nSplit Testing!")
                            #if (first_creative_type=='HTML') & (first_ccid==''):
                                #first_ccid = 'Send with HTML'
                            #if (second_creative_type=='HTML') & (second_ccid==''):
                                #second_ccid = 'Send with HTML'
                            #creative_type = first_creative_type + "\n" + second_creative_type
                            ccid = first_ccid + "\n" + second_ccid
                            send_strategy = 'ST'
                            default_content_select = False

                if default_content_select == True:
                    
                    selected_creative_function = getattr(cs, functions_dict['content_id_determination'])
                    ccid = selected_creative_function(sc_dppub_affiliate,offer,print_bool=print_bool,simple=simple_creative,
                                                                    send_date=pd.Timestamp(next_drop['Date'].values[0]))

                #next_drop['Creative Type'] = creative_type
                next_drop['Creative'] = ccid

                next_drop['Send Strategy'] = send_strategy

                #akshad_notes = self.find_akshad_notes(sc_dppub_affiliate,next_drop_number, send_strategy=send_strategy)
                
                if t3_bool:
                    cobra_sheet_df = self.cobra_sheet_df
                    cobra_sheet = self.cobra_sheet
                    pub_row = cobra_sheet_df[cobra_sheet_df['B'].str.contains(sc_dppub_affiliate)].index.values[0]
                    drop_row = int(pub_row + (next_drop_number-1)*11 + 1)
                    date_column = cobra_sheet_df.columns.get_loc(next_drop_date)+1

                    time_row = drop_row + 0
#                     segment_row = drop_row + 1
#                     strategy_row = drop_row + 2
#                     campaign_row = drop_row + 3
#                     creative_row = drop_row + 4
#                     ccid_row = drop_row + 6
#                     akshad_row = drop_row + 8
#                     mailer_row = drop_row + 9
#                     akshad_row = drop_row + 8

                    time_cell = cobra_sheet.cell((time_row,date_column))
    
                    if write_to_cobra_bool:
                        if (time_cell.color == (1,0,0,0)) | (time_cell.color == (1,0.6,0,0)) | (time_cell.color == (0,1,0,0)):
                            time_cell.color = (0,1,1)
                        
                            switch_notes.append("Switched drop {} on {} in {}".format(next_drop_number,next_drop_date,sc_dppub_affiliate))
                    
                    #akshad_notes = 'Switched {} for {}\n\n'.format(old_offer,offer) + akshad_notes

                
                #mailer_notes = 'Viper Scheduled'
                #next_drop['Mailer Notes:'] = mailer_notes
                #next_drop['Akshad Notes:'] = akshad_notes
                next_drop['Segment'] = cobra_segment
                date1 = next_drop['Date'].dt.date.values[0].strftime("%d%b%y") 
                next_drop['Job Name'] = "SS_"+cobra_segment[:3] + "_"+cobra_segment[4:].replace(".",'-').replace("_",'-')+"_"+ offer +"_"+ send_strategy + "_" +   date1

                
                

                cobra.loc[next_drop.index, :] = next_drop[:]

                if print_bool:
                    print("\nScheduling",offer,"on",next_drop_date,"and ccid",ccid,
                      "in drop number",next_drop_number,"at",time,'to',cobra_segment)  
#                     print('Removing', next_drop.index, 'length is',
#                           len(self.production_empty_indices), len(self.ote_empty_indices))

                    if next_drop_number==1:
                        if send_strategy=='OT':
                            self.ot_empty_indices = self.ot_empty_indices.drop(next_drop.index)
                        elif send_strategy=='E':
                            self.e_empty_indices = self.e_empty_indices.drop(next_drop.index)
                            
                    elif next_drop_number<8:
                        if not t3_bool:
                            self.production_empty_indices = self.production_empty_indices.drop(next_drop.index)
#                         self.past_cobras = self.past_cobras.loc[self.past_cobras.index.drop(next_drop.index)]
                        self.past_cobras = cobra.loc[self.past_cobras.index.union(next_drop.index)]
                        self.upcoming_cobras = cobra.loc[self.upcoming_cobras.index.union(next_drop.index)]
                        
#                     print('Done Removing', next_drop.index, 'length is', 
#                           len(self.production_empty_indices),len(self.ote_empty_indices))


                if write_to_cobra_bool:

    #                 write_to_cobra(sc_dppub_affiliate,next_drop_date,next_drop_number,time,cobra_segment,send_strategy,
    #                                campaign,creative_type,ccid,akshad_notes,mailer_notes)

                    self.add_to_log(sc_dppub_affiliate,next_drop_date,next_drop_number,time,cobra_segment,send_strategy,
                             campaign,ccid,
                             days_ahead,manual_offers,exclude_offers,functions_dict,selection_confidence=selection_confidence)

                return next_drop_date

            else:
                if print_bool:
                    print("\nUnable to schedule",offer,'because of Debt vertical gapping restrictions')

        else:
            if true_mirrors.get(offer) is None:
                true_offer_mirrors = [] 
            else:
                true_offer_mirrors = true_mirrors.get(offer)

            last_cobra_df = cobra[((cobra['Send Strategy'].isin(['P','E','CT','OT','MA'])) & (cobra['Hitpath Offer ID']==offer) | (cobra['Hitpath Offer ID'].isin(true_offer_mirrors))) & (cobra['Dataset']==sc_dppub_affiliate)]
            
            if len(last_cobra_df)>0:
                last_cobra = last_cobra_df['Date'].max().strftime('%-m/%-d/%Y')
                
                if print_bool:
                    if len(true_offer_mirrors)>0:
                        interesecting_mirrors = list(set(last_cobra_df['Hitpath Offer ID']).intersection(set(true_offer_mirrors)))
                        if len(interesecting_mirrors)>0:
                            last_cobra_hitpath = int(interesecting_mirrors[0])
                        else:
                            last_cobra_hitpath = offer
                            
                        print(f"\nNo space to schedule {offer} with a {gap} day gap; latest scheduled date is {last_cobra} (mirror offer {last_cobra_hitpath})")
                    else:
                        print("\nNo space to schedule",offer,'with a',gap,'day gap; latest scheduled date is',last_cobra)
            else:
                if print_bool:
                    print("\nNo space to schedule",offer,'with a',gap,'day gap')
            return False   

#     def find_last_ecpm(self,sc_dppub_affiliate,hitpath):
#         lexi = self.lexi
#         last_drop = lexi[(lexi['Affiliate ID']==sc_dppub_affiliate) & (lexi['Hitpath Offer ID']==hitpath)
#                     & (lexi['Send Strategy'].isin(['P','CT','E','OT','PT','RT','IT']))  ].iloc[-1]
#     #     last_ecpm = (last_drop['Revenue'].sum()*1000) / last_drop['Delivered'].sum() 
#         last_ecpm = last_drop['eCPM'] #Using updated payout eCPM here
#         return last_ecpm   


    def check_debt_vertical_gapping(self,sc_dppub_affiliate,hit,date_to_add):
        #Return True if no conflicts
        today = self.today
        
        return True #All Debt restrictions currently lifted
        
        offers = self.offers
        cobra = self.cobra
        emit = self.emit
        
        if hit in [10777,11222,4620]: #exceptions to debt rule
            return True
        
        affiliate_emit = emit[emit['Revenue Pub ID']==int(sc_dppub_affiliate)]
        if (affiliate_emit['Vertical']=='Debt').any():
            return True
        
#         hitpath_vertical = offers[offers['Hitpath Offer ID']==hit]['Vertical'].values[0]
        hitpath_vertical = offers.loc[hit]['Vertical']
        if hitpath_vertical=='Debt':

            cobra_recent = cobra[(cobra['Dataset']==sc_dppub_affiliate)&(cobra['Date'].dt.date >= today + timedelta(days=-14)) & (cobra['Date'].dt.date <= today + timedelta(days=14))]
#             cobra_recent['Offer Vertical'] = cobra_recent['Hitpath Offer ID'].apply(lambda x: offers[offers['Hitpath Offer ID']==x]['Vertical'].values[0] if len(offers[offers['Hitpath Offer ID']==x]['Vertical'].values)>=1 else '')
            cobra_recent = pd.merge(cobra_recent, offers[['Vertical','Status']], how='left',
                                    left_on='Hitpath Offer ID', right_on=offers.index)
            cobra_recent.rename(columns={'Vertical':'Offer Vertical'}, inplace=True)
    
    
#             cobra_recent['Status'] = cobra_recent['Hitpath Offer ID'].apply(lambda x: offers[offers['Hitpath Offer ID']==x]['Status'].values[0] if len(offers[offers['Hitpath Offer ID']==x]['Status'].values)>=1 else '')
#             cobra_recent['Status'] = cobra_recent['Hitpath Offer ID'].apply(lambda x: offers[offers['Hitpath Offer ID']==x]['Status'].values[0] if len(offers[offers['Hitpath Offer ID']==x]['Status'].values)>=1 else '')


            if not pd.isnull(cobra_recent[cobra_recent['Hitpath Offer ID']==hit]['Date'].max().date()):
                latest_debt_date = cobra_recent[cobra_recent['Hitpath Offer ID']==hit]['Date'].max().date()
            else:
                latest_debt_date = date_to_add + timedelta(days=-15)

            if abs((date_to_add - latest_debt_date).days) <= 14: #first check for the same debt offer within two weeks
                return False

            if len(cobra_recent[(cobra_recent['Date'].dt.date <= date_to_add) & (cobra_recent['Date'].dt.date >= date_to_add + timedelta(days=-7)) & (cobra_recent['Offer Vertical']=='Debt') & (cobra_recent['Status']=='Live')]) >= 1: #check for two debt offers in the same week (now excluding CPMs)
                return False

        return True


    def find_segment(self, pub, drop_number, date_string=date.today().strftime('%-m/%-d/%Y')):

        cobra_sheet_df = self.cobra_sheet_df
        
        previous_notes = []

        pub_row = cobra_sheet_df[cobra_sheet_df['B'].str.contains(pub)].index.values[0]
        segment_row = int(pub_row + (drop_number-1)*11 + 1) - 1

        date_column = cobra_sheet_df.columns.get_loc(date_string)+1

        
        for i in range(1,8):
            previous_notes.append(cobra_sheet_df.iloc[segment_row, (date_column-i)])
        previous_notes = [note for note in previous_notes if note!='']
        
        try:
            seg = max(previous_notes, key=previous_notes.count)
        except:
            #seg = pub + '_30DC'
            seg = ''
            print("Please enter the segment by yourself")
        

        return seg


    def schedule_ot_drops(self,sc_dppub_affiliate,days_ahead,manual_offers,exclude_offers,update_cobra):
        cobra = self.cobra
        fvs = self.fvs
        offs = self.offs
        functions_dict = self.fvs.find_affiliate_functions(sc_dppub_affiliate)
        today = self.today
        
        affiliate_ot_drop_days = fvs.get_affiliate_ot_drop_days()

        print('\n**********\nNow Scheduling Offer Testing Drops:\n**********')
        
        ot_selection_function = getattr(offs, functions_dict['ot_determination'])
        ot_offers = ot_selection_function(sc_dppub_affiliate)
        
        ot_offers = [x for x in ot_offers if x not in exclude_offers]
        ot_offers = offs.return_unpaused_offers(sc_dppub_affiliate, ot_offers)

        for hitpath in ot_offers:
            if len(self.ot_empty_indices)>0:
                                              
                days_to_schedule = affiliate_ot_drop_days[sc_dppub_affiliate]
                available_days = fvs.find_days(hitpath)

                if days_to_schedule is not np.nan:

                    if re.search(r'\bevery|other\b',days_to_schedule, re.IGNORECASE):
                        ote_bool = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
                                  (cobra['Date'].dt.date>=today + timedelta(days=1)) &
                                  (cobra['Date'].dt.date<=(today + timedelta(days=days_ahead))) &
                                  (cobra['Drop Number']==1) &
                                 ~((cobra['Offer'].isnull())|( cobra['Offer']==''))]['Date'].max().dayofyear%2==0

                        leftover_slots = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
                                         (cobra['Date'].dt.date>=today + timedelta(days=1)) &
                                         (cobra['Date'].dt.date<=(today + timedelta(days=days_ahead))) &
                                         (cobra['Drop Number']==1) &
                                         ((cobra['Offer'].isnull())|( cobra['Offer']=='')) &
                                        ((cobra['Date'].dt.dayofyear%2==0) == ote_bool) &
                                         (cobra['Day of Week'].apply(lambda x: x in available_days)) ]

                    else:
                        ote_days = fvs.find_days_available(days_to_schedule)

                        
                        leftover_slots = cobra.loc[self.ot_empty_indices]
                        leftover_slots = leftover_slots.loc[(cobra['Day of Week'].apply(lambda x: x in available_days))]
                else:
                    leftover_slots = pd.DataFrame({'A' : []})

                if len(leftover_slots)>0:
                    segment = 'A120' 
                    send_strategy = 'OT'

                    self.schedule_offer(sc_dppub_affiliate,hitpath,days_ahead,segment=segment,given_empties=leftover_slots,
                                   send_strategy=send_strategy,write_to_cobra_bool=update_cobra,simple_creative=True)

    def schedule_e_drops(self,sc_dppub_affiliate,days_ahead,manual_offers,exclude_offers,update_cobra):

        cobra = self.cobra
        offs = self.offs
        functions_dict = self.fvs.find_affiliate_functions(sc_dppub_affiliate)
        fvs = self.fvs
        today = self.today
        
        affiliate_e_drop_days = fvs.get_affiliate_e_drop_days()

        print('\n**********\nNow Scheduling Engagement Drops:\n**********')

        e_selection_function = getattr(offs, functions_dict['e_determination'])
        e_offers = e_selection_function(sc_dppub_affiliate)

    #     ote_offers = [x for x in itertools.chain.from_iterable(itertools.zip_longest(test_hitpaths,high_ctr_offers)) if x]

        e_offers = [x for x in e_offers if x not in exclude_offers]
        e_offers = offs.return_unpaused_offers(sc_dppub_affiliate, e_offers)

        for hitpath in e_offers:
            if len(self.e_empty_indices) > 0:
                
                days_to_schedule = affiliate_e_drop_days[sc_dppub_affiliate]
                available_days = fvs.find_days(hitpath)

                if days_to_schedule is not np.nan:

                    if re.search(r'\bevery|other\b',days_to_schedule, re.IGNORECASE):
                        ote_bool = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
                                  (cobra['Date'].dt.date>=today + timedelta(days=1)) &
                                  (cobra['Date'].dt.date<=(today + timedelta(days=days_ahead))) &
                                  (cobra['Drop Number']==1) &
                                 ~((cobra['Offer'].isnull())|( cobra['Offer']==''))]['Date'].max().dayofyear%2==0

                        leftover_slots = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
                                         (cobra['Date'].dt.date>=today + timedelta(days=1)) &
                                         (cobra['Date'].dt.date<=(today + timedelta(days=days_ahead))) &
                                         (cobra['Drop Number']==1) &
                                         ((cobra['Offer'].isnull())|( cobra['Offer']=='')) &
                                        ((cobra['Date'].dt.dayofyear%2==0) == ote_bool) &
                                         (cobra['Day of Week'].apply(lambda x: x in available_days)) ]

                    else:
                        ote_days = fvs.find_days_available(days_to_schedule)

                        leftover_slots = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
                                                (cobra['Date'].dt.date>=today + timedelta(days=1)) &
                                                (cobra['Date'].dt.date<=(today + timedelta(days=days_ahead))) &
                                                (cobra['Drop Number']==1) &
                                                ((cobra['Offer'].isnull())|( cobra['Offer']=='')) &
                                                (cobra['Day of Week'].apply(lambda x: x in ote_days)) &
                                                (cobra['Day of Week'].apply(lambda x: x in available_days))]
                else:
                    leftover_slots = pd.DataFrame({'A' : []})


                if len(leftover_slots)>0:

                    send_strategy = 'E'
                    segment = 'A7'

        #         def schedule_offer(sc_dppub_affiliate,offer,  days_ahead,segment='A120',send_strategy='P',write_to_cobra_bool=False):
                    self.schedule_offer(sc_dppub_affiliate,hitpath,days_ahead,segment,send_strategy,write_to_cobra_bool=update_cobra)        
    def create_mock_report(self):
        cobra = self.cobra_clean.copy()
        offers = self.offers
        fvs = self.fvs
        
        selection = ''
        while (selection!='one') & (selection!='all'):
            selection = input("To create each individual swap for one account/offer at a time enter 'one'\nTo create swaps for one offer for all accounts in a date range enter 'all':")

        if selection=='one':

            cont = True
            affiliate_inputs = []
            hitpath_inputs = []

            while cont:
                aff_input = input('Enter a Dataset: ')
                affiliate_inputs.append(aff_input)
                hit_input = input('Enter a Hitpath Offer ID: ')
                hitpath_inputs.append(int(hit_input))
                cont_bool = input('Would you like to add more? (Enter "y" to continue) ')

                if cont_bool!='y':
                    cont=False


            mock_report = pd.DataFrame({'Dataset':affiliate_inputs, 'Hitpath Offer ID':hitpath_inputs})
            mock_report = pd.merge(mock_report, cobra[cobra['Date'].dt.date>=date.today()][['Date','Dataset','Affiliate ID','Hitpath Offer ID','Send Strategy','Segment','Drop']], on=['Affiliate ID','Hitpath Offer ID'], how='left')

        else:
            hit_input = int(input('Enter a Hitpath Offer ID: '))
            begin_date_string = input('Enter the first date (format "mm/dd/yyyy"): ')
            end_date_string = input('Enter the last date (format "mm/dd/yyyy"): ')

            mock_report = cobra[(cobra['Date']>=begin_date_string)&(cobra['Date']<=end_date_string)&(cobra['Hitpath Offer ID']==hit_input)]
            
        mock_report = pd.merge(mock_report, offers[['Offer Name','Status','Payout Type','Day Restrictions']], on='Hitpath Offer ID', how='left')
        mock_report['Day Restrictions Numeric'] = mock_report['Day Restrictions'].apply(lambda x: fvs.find_days_restrictions(x))
        mock_report.dropna(subset=['Date'],inplace=True)
        
        mock_report['Day of Week'] = mock_report['Date'].apply(lambda x: int(x.weekday()))
        mock_report['Reason'] = mock_report.apply(lambda row: 'Day Restrictions' if row['Day of Week'] not in row['Day Restrictions Numeric'] else 'Manually Created Swap', axis=1)
        
        mock_report = mock_report[['Affiliate ID', 'Hitpath Offer ID', 'Date', 'Dataset', 'Send Strategy',
       'Segment', 'Drop', 'Offer Name', 'Status', 'Payout Type',
       'Day Restrictions', 'Day Restrictions Numeric', 'Day of Week',
       'Reason']]
        

        return mock_report

    def schedule_swaps(self,swap_name,update_cobra=False,exclude_offers=[],override_cpm=False,create_swaps=False):

        offers = self.offers
        cobra_clean = self.cobra_clean.copy()
        cobra = self.cobra
#         cobra = cobra_clean.copy()
        offs = self.offs
        fvs = self.fvs
        today = self.today
        
        affiliate_p_drops = fvs.affiliate_p_drops

#         cobra=cobra_clean.copy()
        if create_swaps:
            swap_report = self.create_mock_report()
        else:
            print("Retrieving Swap Report...")
            swap_report = viper_main.get_swap_report(swap_name,offers)

            print("Swap Report Loaded!")
        slack_notes = []
        swap_all = False

        print('\n**********\nNow Swapping Drops:\n**********',flush=True)

        for index,row in swap_report.iterrows():

            scheduled_bool = False
            cobra = self.cobra
            
            #Get Swap Report Info
            sc_dppub_affiliate = row['Affiliate ID']
            if sc_dppub_affiliate=='460398':
                if row['Dataset']=='460398_SC.FHA':
                    sc_dppub_affiliate = '160398'
                elif row['Dataset']=='460398_SC.RF':
                    sc_dppub_affiliate = '260398'
            elif sc_dppub_affiliate=='461128':
                if row['Dataset']=='461128_LPG.FHA':
                    sc_dppub_affiliate = '161128'
                elif row['Dataset']=='461128_LPG.RF':
                    sc_dppub_affiliate = '261128'
             
            #Reset cobra indices for each sc_dppub_affiliate in swap report
            self.sc_dppub_affiliate = sc_dppub_affiliate
            self.empties = self.find_initial_empties(self.days_ahead)
        
            self.production_empty_indices = self.empties[self.empties['Drop Number']>1].index

            affiliate_ot_drop_days = fvs.get_affiliate_ot_drop_days()        
            ot_days_to_schedule = affiliate_ot_drop_days[sc_dppub_affiliate]
            ot_days = fvs.find_days_available(ot_days_to_schedule)

            affiliate_e_drop_days = fvs.get_affiliate_e_drop_days()
            e_days_to_schedule = affiliate_e_drop_days[sc_dppub_affiliate]
            e_days = fvs.find_days_available(e_days_to_schedule)

            self.ot_empty_indices = self.empties[(self.empties['Drop Number']==1) &
                                    (self.empties['Day of Week'].apply(lambda x: x in ot_days))].index

            self.e_empty_indices = self.empties[(self.empties['Drop Number']==1) &
                                    (self.empties['Day of Week'].apply(lambda x: x in e_days))].index

            self.past_cobras = self.find_past_cobras()
            self.upcoming_cobras = self.find_upcoming_cobras()
        
                    
                    
            functions_dict = self.fvs.find_affiliate_functions(sc_dppub_affiliate)
            
            send_strategy = row['Send Strategy']
            drop = row['Drop']
            date = row['Date']
            old_offer = row['Hitpath Offer ID']
            reason = row['Reason']
            if old_offer not in exclude_offers:
                exclude_offers.append(old_offer)

            if any( [status in row[col] for col in ['Offer Name','Status','Payout Type'] for status in ['CPM'] ] ) & (override_cpm==False):
                print('Ignoring {} in drop {} of {} since it is a CPM\n'.format(old_offer,drop,sc_dppub_affiliate))
            else:
                
                #Clear Internal Cobra DF
                swap_index = cobra[(cobra['Dataset']==sc_dppub_affiliate) & (cobra['Drop']==drop) 
                                 & (cobra['Hitpath Offer ID']==old_offer) & (cobra['Date']==date)].index
                
                if len(swap_index)>0:
                    a_notes = cobra.loc[swap_index.values[0]]['Akshad Notes:']
                    prev_offers = [int(prev_offer) for prev_offer in a_notes.replace('\n',' ').split(' ') if prev_offer.isdigit()]
                    for prev_offer in prev_offers:
                        if prev_offer not in exclude_offers:
                            exclude_offers.append(prev_offer)

                offer_list = [str(offer) for offer in offers.index.tolist()]

                if swap_all:
                    manual_swap = 'auto'
                    print("\nSwapping for Offer {} in {} of {} on {} ({})".format(old_offer,drop,sc_dppub_affiliate,date.strftime('%-m/%-d/%Y'),reason))
                else:
                        
                        
                    manual_swap = 'dummy'
                    while (manual_swap not in offer_list) & (manual_swap!='auto') & (manual_swap!='ignore') & (manual_swap!='auto all'):

                        manual_swap = input("\n\nSwapping for Offer {} in {} of {} on {} ({}):\n    Enter a Hitpath for Manual Replacement\n    Enter 'auto' to let Viper find a Replacement Offer\n    Enter 'auto all' to let Viper Swap the Rest of the Offers\n    Enter 'ignore' to Leave this Drop on Cobra\n".format(old_offer,drop,sc_dppub_affiliate,date.strftime('%-m/%-d/%Y'),reason)).lower()

                if manual_swap=='auto all':
                    manual_swap = 'auto'
                    swap_all = True
                
                if len(swap_index)>0:

                    if (manual_swap!='ignore') & (int(drop[5:])-1>affiliate_p_drops[sc_dppub_affiliate]) :
                        print('Removing the extra drop from the schedule; no need to replace.')
                        slack_notes.append(self.schedule_swap_offer(swap_index,'',sc_dppub_affiliate,send_strategy,
                                                               old_offer,update_cobra,exclude_offers=exclude_offers,
                                                               manual_swap=manual_swap, just_remove=True))

                    elif manual_swap=='auto':
                        moved_bool = False
                        
                        if reason=='Day Restrictions':
                            
                            hitpath = old_offer
                            day_restrictions = offers.loc[hitpath]['Day Restrictions']
                            available_days = fvs.find_days_restrictions(day_restrictions)
                            given_date = date
                            given_date = pd.to_datetime(given_date, format='%m/%d/%Y')
                            given_date_numeric = int(given_date.weekday())
                            number_of_drops = fvs.affiliate_p_drops[sc_dppub_affiliate]
#                             cobra.drop(cobra.columns[13], axis=1, inplace=True) #drop NaN column

                            df = cobra[(cobra['Dataset']==sc_dppub_affiliate)&(cobra['Date'].dt.date>today)
                                 &(cobra['Day of Week'].apply(lambda x: x in available_days))
                                 &(cobra['Drop Number']>1)
                                 &(cobra['Drop Number']<=(number_of_drops))
                                      ]
                            df = pd.merge(df.reset_index(),offers[['Day Restrictions','Status']],how='left',on='Hitpath Offer ID')
                            df.set_index('index', inplace=True)
#                             df.rename({'index':'Cobra Index'},axis=1,inplace=True)

                            df = df[(df['Status']=='Live')|df['Status'].isna()]
                            df['Day Restrictions Numeric'] = df['Day Restrictions'].apply(lambda x: fvs.find_days_restrictions(x))
                            df = df[df['Day Restrictions Numeric'].apply(lambda x: given_date_numeric in range(x[0], x[-1]))]
                            df['Earliest Next Date'] = df[['Affiliate ID','Hitpath Offer ID']].apply(lambda x: self.find_earliest_next_date(x['Affiliate ID'],x['Hitpath Offer ID'],drops_back=1),axis=1)
                            df = df[df['Earliest Next Date']<=given_date]
                            
                            df['date_diff'] = abs(df['Date'] - given_date)

                            df = df.sort_values(by=['date_diff','Offer'],na_position='first')
                            df = df.drop(columns=['date_diff','Status','Day Restrictions Numeric','Day Restrictions'])
                            
                            swap_drop = pd.DataFrame(df.iloc[0]).T           
                            new_index = swap_drop.index
                            
                            new_index_int = new_index[0]
                            swap_index_int = swap_index[0]
                            
                            
#                             print(new_index_int,swap_index_int)
#                             slack_notes.append(self.schedule_swap_offer(swap_index,swap_drop['Hitpath Offer ID'],
#                                                                         sc_dppub_affiliate,swap_drop['Send Strategy'],
#                                                         old_offer,update_cobra))
#                             print(slack_notes)
    
                            columns_to_exclude = ['Drop', 'Drop Number', 'Date', 'Time', 'Day of Week', 'Segment']
    
                    
                            row1_values = cobra.loc[swap_index_int, cobra.columns.difference(columns_to_exclude)].copy()
                            row2_values = cobra.loc[new_index_int, cobra.columns.difference(columns_to_exclude)].copy()

                            # Swap rows excluding specified columns
                            cobra.loc[swap_index_int, cobra.columns.difference(columns_to_exclude)] = row2_values
                            cobra.loc[new_index_int, cobra.columns.difference(columns_to_exclude)] = row1_values
                
                            new_akshad = cobra.loc[new_index_int]['Akshad Notes:']
                            if len(new_akshad)>0:
                                cobra.loc[new_index_int,'Akshad Notes:'] = new_akshad +'\n\n'+ f"Moved {int(cobra.loc[new_index_int]['Hitpath Offer ID'])} from {cobra.loc[swap_index_int]['Date'].strftime('%m/%-d')}"
                        
                            swap_akshad = cobra.loc[swap_index_int]['Akshad Notes:']
                            if len(swap_akshad)>0:
                                cobra.loc[swap_index_int,'Akshad Notes:'] = swap_akshad +'\n'+ f"Moved {int(cobra.loc[swap_index_int]['Hitpath Offer ID'])} from {cobra.loc[new_index_int]['Date'].strftime('%m/%-d')}"
                    
                    
                            self.past_cobras = cobra.loc[self.past_cobras.index.union(swap_index)]
                            self.past_cobras = cobra.loc[self.past_cobras.index.union(new_index)]
                            self.upcoming_cobras = cobra.loc[self.upcoming_cobras.index.union(swap_index)]
                            self.upcoming_cobras = cobra.loc[self.upcoming_cobras.index.union(new_index)]

                            if update_cobra:
                                slack_notes.append(self.set_swap_df(pd.DataFrame(cobra.loc[new_index_int]).T,sc_dppub_affiliate))

                                if pd.isna(cobra.loc[swap_index_int]['Offer']) & ((cobra.loc[swap_index_int]['Date'].date() - today).days<12): #pick new offer if within 5 days
                                    moved_bool = True
                                
                                else:
                                    moved_bool = False
                                    slack_notes.append(self.set_swap_df(pd.DataFrame(cobra.loc[swap_index_int]).T,sc_dppub_affiliate))

#                                 print(moved_bool)

#                             print(cobra.loc[new_index]['Offer'])
#                             print(self.past_cobras.loc[new_index]['Offer'])
#                             print(self.upcoming_cobras.loc[new_index]['Offer'])
#                             print(cobra.loc[new_index]['Offer'])
                            
                        if ((reason!='Day Restrictions') | (moved_bool==True)):
                        
                            cobra.loc[swap_index,['Offer', 'Creative Type', 'Hitpath Offer ID','Send Strategy']] = np.nan
                            self.cobra = cobra
                            self.ot_empty_indices = cobra.loc[self.ot_empty_indices.union(swap_index)].index

                            #Find Replacement Offers

                            if drop=='Drop 1':
                                ot_selection_function = getattr(offs, functions_dict['ot_determination'])
                                ot_offers = ot_selection_function(sc_dppub_affiliate)

                                offers_to_schedule = [x for x in ot_offers if x not in exclude_offers]

                                selection_confidence = 'Swap - Offer Test'

                            else:

                                tier_one_function = getattr(offs, functions_dict['tier_one_production_determination'])
                                tier_one_offers = tier_one_function(sc_dppub_affiliate)

                                tier_two_function = getattr(offs, functions_dict['tier_two_production_determination'])
                                tier_two_offers = tier_two_function(sc_dppub_affiliate)

                                tier_three_function = getattr(offs, functions_dict['tier_three_production_determination'])
                                tier_three_offers = tier_three_function(sc_dppub_affiliate)

                                offers_to_schedule = [x for x in list(dict.fromkeys(tier_one_offers+tier_two_offers+tier_three_offers)) if x not in exclude_offers]
                                

                            offers_to_schedule = offs.return_unpaused_offers(sc_dppub_affiliate, offers_to_schedule)

                            for offer in offers_to_schedule:
                                
                                if drop!='Drop 1':
                                    
                                    if offer in tier_one_offers:
                                        selection_confidence = 'Swap - Tier One'
                                    elif offer in tier_two_offers:
                                        selection_confidence = 'Swap - Tier Two'
                                    else:
                                        selection_confidence = 'Swap - Tier Three'

                                if scheduled_bool==False:

                                    empty_slots = self.find_empty_slots(sc_dppub_affiliate,offer,days_ahead=100,send_strategy=send_strategy,
                                                                        swapping=True)
    #                                 print('trying to swap with', offer, len(empty_slots) )

                                    if (len(empty_slots) > 0):
    #                                     print(swap_index[0], empty_slots.index)

                                        if swap_index[0] in empty_slots.index:
                                            if self.check_debt_vertical_gapping(sc_dppub_affiliate,offer,date.date()):

                                                slack_notes.append(self.schedule_swap_offer(swap_index,offer,
                                                             sc_dppub_affiliate,send_strategy,old_offer,update_cobra,
                                                             exclude_offers=exclude_offers,manual_swap=manual_swap,
                                                              selection_confidence=selection_confidence))
                                                scheduled_bool=True

                    elif manual_swap!='ignore': #manual
                        selection_confidence = 'Manual'
                        slack_notes.append(self.schedule_swap_offer(swap_index,manual_swap,sc_dppub_affiliate,send_strategy,
                                                               old_offer,update_cobra,exclude_offers=exclude_offers,
                                                  manual_swap=manual_swap))
                        scheduled_bool=True

                else:
                    swapped_drop = cobra[(cobra['Dataset']==sc_dppub_affiliate) & (cobra['Drop']==drop) & (cobra['Date']==date)]['Hitpath Offer ID'].values[0]
                    if not math.isnan(swapped_drop):
                        print('Offer Already Swapped with {}!\n'.format(int(swapped_drop)))
                    else:
                        print('Offer Already Removed!\n')

            #                             if update_cobra:
            #                                 write_to_cobra(sc_dppub_affiliate,next_drop_date,next_drop_number,time,cobra_segment,send_strategy,campaign,creative_type,ccid,akshad_notes,mailer_notes)
        print("Write below to html-scheduling-issues in Slack:")
        slack_notes = [x for x in slack_notes if x!='']
        for note in slack_notes:
            print(note)


    def set_swap_df(self,swap_drop,sc_dppub_affiliate,selection_confidence='Swap',just_remove=False):
        update_cobra = True #right now only calling this function if update_cobra is true
        cobra = self.cobra
        cobra_sheet_df = self.cobra_sheet_df
        cobra_sheet = self.cobra_sheet
        affiliate_log = self.affiliate_log
        affiliate_log_clean = self.affiliate_log_clean
        functions_dict = self.fvs.find_affiliate_functions(sc_dppub_affiliate)
        
        next_drop_number = int(swap_drop['Drop Number'])
        next_drop_date = swap_drop['Date'].dt.date.values[0].strftime('%-m/%-d/%Y')

        pub_row = cobra_sheet_df[cobra_sheet_df['B'].str.contains(sc_dppub_affiliate)].index.values[0]
        drop_row = int(pub_row + (next_drop_number-1)*11 + 1)
        date_column = cobra_sheet_df.columns.get_loc(next_drop_date)+1

        time_row = drop_row + 0
        segment_row = drop_row + 1
        strategy_row = drop_row + 2
        campaign_row = drop_row + 3
        creative_row = drop_row + 4
        ccid_row = drop_row + 6
        akshad_row = drop_row + 8
        mailer_row = drop_row + 9
        akshad_row = drop_row + 8

        time_cell = cobra_sheet.cell((time_row,date_column))
        hitpath = int(swap_drop['Hitpath Offer ID'].values[0])
        
        campaign = swap_drop['Offer'].values[0]
        if pd.isna(campaign):
            campaign = ''
        send_strategy = swap_drop['Send Strategy'].values[0]
        cobra_segment = swap_drop['Segment'].values[0]
        time = swap_drop['Time'].values[0]
        creative_type = swap_drop['Creative Type'].values[0]
        ccid = swap_drop['Creative'].values[0]
        mailer_notes = 'Viper Moved for Day Restrictions'
        swap_drop['Mailer Notes'] = mailer_notes
        akshad_notes = f"Moved in {hitpath}" + swap_drop['Akshad Notes:'].values[0]

        days_ahead = ''
        manual_offers = ''
        exclude_offers = ''
        manual_swap = ''
        
        self.add_to_log(sc_dppub_affiliate,next_drop_date,next_drop_number,time,cobra_segment,send_strategy,
                         campaign,creative_type,ccid,akshad_notes,mailer_notes,
                         days_ahead,manual_offers,exclude_offers,functions_dict,
                   manual_swap=manual_swap,selection_confidence=selection_confidence)

        self.write_to_log(sc_dppub_affiliate,update_cobra)

        swap_index = swap_drop.index
        drop_to_write = cobra.loc[swap_index[0], :]
        swap_df = pd.DataFrame(drop_to_write).drop(index=['Date','Dataset','Drop','Hitpath Offer ID',
                                                                'real seg','Affiliate ID','Drop Number','Day of Week'])

        print(f'\nMoving {hitpath} for Day Restrictions')
        cobra_sheet.set_dataframe(start=(time_row,date_column),df=swap_df,copy_head=False,nan='')
        
        self.affiliate_log = affiliate_log_clean.copy() #clear log for next entry in swap report

        next_drop_date = next_drop_date[:-5] #get rid of the year for formatting notes
        slack_notes = ''
        if (time_cell.color == (1,0,0,0)) | (time_cell.color == (1,0.6,0,0)) | (time_cell.color == (0,1,0,0)) :
            time_cell.color = (0,1,1)
            if just_remove:
                slack_notes = ("Removed drop {} on {} in {}".format(next_drop_number,next_drop_date,sc_dppub_affiliate))
            else:
                slack_notes = ("Swapped drop {} on {} in {}".format(next_drop_number,next_drop_date,sc_dppub_affiliate))
    
        return slack_notes
    
    def schedule_swap_offer(self,swap_index,offer,sc_dppub_affiliate,send_strategy,old_offer,update_cobra,exclude_offers=[],
                            manual_swap='',just_remove=False,selection_confidence='Swap'):

        cobra = self.cobra
        cobra_sheet = self.cobra_sheet
        cobra_sheet_df = self.cobra_sheet_df
        affiliate_log = self.affiliate_log
        affiliate_log_clean = self.affiliate_log_clean
        offers = self.offers
        functions_dict = self.fvs.find_affiliate_functions(sc_dppub_affiliate)
        cs = self.cs
        emit = self.emit

        next_drop = cobra.loc[swap_index]

        if not just_remove:

#             campaign = offers[offers['Hitpath Offer ID']==int(offer)]['Scheduling Name'].values[0]
            campaign = offers.loc[int(offer)]['Scheduling Name']
            next_drop['Offer'] = campaign
            next_drop['Hitpath Offer ID'] = offer
            
            if send_strategy=='ST':
                send_strategy='P'
            else:
                next_drop['Send Strategy'] = send_strategy
            

            selected_creative_function = getattr(cs, functions_dict['creative_type_determination'])
            creative_type,ccid = selected_creative_function(sc_dppub_affiliate,offer)

    #         creative_type = decide_creative_type(sc_dppub_affiliate,offer)

            #next_drop['Creative Type'] = creative_type


            next_drop['Creative'] = ccid

            next_drop_date = next_drop['Date'].dt.date.values[0].strftime('%-m/%-d/%Y')
            next_drop_number = next_drop['Drop Number'].values[0]
            time = next_drop['Time'].values[0]
            mailer_notes = 'Viper Swapped'
            #next_drop['Mailer Notes:'] = mailer_notes

    #         next_drop['Akshad Notes:'] = next_drop['Akshad Notes:'].replace(np.nan,'')
            akshad_notes = 'Swapped {} for {}\n\n'.format(old_offer,offer) + next_drop['Akshad Notes:'].values[0]
            #next_drop['Akshad Notes:'] = akshad_notes

            segment = 'A120'
            dp_pub = emit[emit['Revenue Pub ID']==int(sc_dppub_affiliate)]['DP&Pub'].mode().values[0]
            cobra_segment = dp_pub + "_" + segment

            cobra.loc[swap_index, :] = next_drop[:]
            
            self.past_cobras = cobra.loc[self.past_cobras.index.union(swap_index)]
            self.upcoming_cobras = cobra.loc[self.upcoming_cobras.index.union(swap_index)]

            print("\nScheduling",offer,"on",next_drop_date,"in",sc_dppub_affiliate,"with",creative_type,"and ccid",ccid,
                      "in drop number",next_drop_number,"at",time,'with akshad notes:',akshad_notes) 

        #     print("\nSwapped drop {} on {} in {}".format(next_drop_number,next_drop_date,sc_dppub_affiliate))


        else:
            campaign = ''
            next_drop['Offer'] = ''
            next_drop['Hitpath Offer ID'] = ''
            next_drop['Segment'] = ''
            next_drop['Send Strategy'] = ''

            creative_type = ''
            ccid = ''

            next_drop['Creative Type'] = ''
            next_drop['Creative'] = ''

            next_drop_date = next_drop['Date'].dt.date.values[0].strftime('%-m/%-d/%Y')
            next_drop_number = next_drop['Drop Number'].values[0]
            time = ''
            mailer_notes = 'Viper Removed'
            #next_drop['Mailer Notes:'] = mailer_notes

            next_drop['Akshad Notes:'] = ''
        #     print('akshad notes test',next_drop['Akshad Notes:'].values[0])
            akshad_notes = ''

            segment = ''
            dp_pub = ''
            cobra_segment = ''

            cobra.loc[swap_index, :] = next_drop[:]
            
            self.past_cobras = cobra.loc[self.past_cobras.index.union(swap_index)]
            self.upcoming_cobras = cobra.loc[self.upcoming_cobras.index.union(swap_index)]


        pub_row = cobra_sheet_df[cobra_sheet_df['B'].str.contains(sc_dppub_affiliate)].index.values[0]
        drop_row = int(pub_row + (next_drop_number-1)*11 + 1)
        date_column = cobra_sheet_df.columns.get_loc(next_drop_date)+1

        time_row = drop_row + 0
        segment_row = drop_row + 1
        strategy_row = drop_row + 2
        campaign_row = drop_row + 3
        creative_row = drop_row + 4
        ccid_row = drop_row + 6
        akshad_row = drop_row + 8
        mailer_row = drop_row + 9
        akshad_row = drop_row + 8

        time_cell = cobra_sheet.cell((time_row,date_column))
    #     print(time_row,date_column)

        slack_notes = ''

        if update_cobra:
            days_ahead = ''
            manual_offers = ''
            self.add_to_log(sc_dppub_affiliate,next_drop_date,next_drop_number,time,cobra_segment,send_strategy,
                             campaign,creative_type,ccid,akshad_notes,mailer_notes,
                             days_ahead,manual_offers,exclude_offers,functions_dict,
                       manual_swap=manual_swap,selection_confidence=selection_confidence)
            
            self.write_to_log(sc_dppub_affiliate,update_cobra)
            swap_df = pd.DataFrame(cobra.loc[swap_index[0], :]).drop(index=['Date','Dataset','Drop','Hitpath Offer ID',
                                                                    'real seg','Affiliate ID','Drop Number','Day of Week'])


            cobra_sheet.set_dataframe(start=(time_row,date_column),df=swap_df,copy_head=False,nan='')

            self.affiliate_log = affiliate_log_clean.copy() #clear log for next entry in swap report

            next_drop_date = next_drop_date[:-5] #get rid of the year for formatting notes
            if (time_cell.color == (1,0,0,0)) | (time_cell.color == (1,0.6,0,0)) | (time_cell.color == (0,1,0,0)) :
                time_cell.color = (0,1,1)
                if just_remove:
                    slack_notes = ("Removed drop {} on {} in {}".format(next_drop_number,next_drop_date,sc_dppub_affiliate))
                else:
                    slack_notes = ("Swapped drop {} on {} in {}".format(next_drop_number,next_drop_date,sc_dppub_affiliate))

        return slack_notes


    def write_to_log(self,sc_dppub_affiliate,update_bool=False):
        cobra_sheet_df = self.cobra_sheet_df
        affiliate_log = self.affiliate_log
        gc = self.gc
        
        if (update_bool == True) & (len(affiliate_log)>0):

#             gc = pygsheets.authorize(service_account_file=filepath.service_account_location)
            sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1oKod0EKYfZmrS9MUtmftmEcv6WPESP5-qMJ6DBuJ4kU/edit#gid=0')

            def find_last_col_name(length):
                #will work with column lengths between 1 and 52 inclusive
                #returns last google sheet range

                if length < 27:
                    return chr(ord('A')+(length-1)%26)

                return 'A' + chr(ord('A')+(length-1)%26)

            try:
                aff_sheet = sh.worksheets(sheet_property='title',value=sc_dppub_affiliate)[0]
            except:
                aff_sheet = sh.add_worksheet(sc_dppub_affiliate)
                model_cell = aff_sheet.cell('A1')
                model_cell.set_text_format('bold', True)
                model_cell.set_horizontal_alignment(pygsheets.custom_types.HorizontalAlignment.CENTER)

                template_columns = sh.worksheets(sheet_property='title',value='Template')[0][1]
                aff_sheet.insert_rows(row=0,values=template_columns)

                aff_sheet.adjust_column_width(1, len(template_columns), pixel_size=303)

                end_column_range = find_last_col_name(len(aff_sheet[1])) + "1"

                DataRange('A1',end_column_range, worksheet=aff_sheet).apply_format(model_cell)

            current_log_length = len(aff_sheet.get_as_df())+1   
            aff_sheet.set_dataframe(start='A' + str(current_log_length+1),df=affiliate_log,copy_head=False,fit=True,nan='')
            first_cell = aff_sheet.cell(('A'+str(current_log_length+1)))
            first_cell.set_text_format('bold', False)
            first_cell.set_horizontal_alignment( pygsheets.custom_types.HorizontalAlignment.RIGHT )

            print("\n\nSuccessfully added to Viper Log!")


    def add_to_log(self,sc_dppub_affiliate,next_drop_date,next_drop_number,drop_time,cobra_segment,send_strategy,
                                   campaign,ccid,
                                  days_ahead,manual_offers,exclude_offers,functions_dict,
                   selection_confidence='',manual_swap=""):
        
        cobra_sheet_df = self.cobra_sheet_df
        affiliate_log = self.affiliate_log
        lexi = self.lexi

        #Get existing info
        pub_row = cobra_sheet_df[cobra_sheet_df['B'].str.contains(sc_dppub_affiliate)].index.values[0]
        drop_row = int(pub_row + (next_drop_number-1)*11 + 1)

        
        
#         if campaign=='':
#             hitpath = ''
#         else:
#             hitpath = int(campaign.split('-')[0])
            
        match = re.match(r'^(\d+)\s*-', campaign)
        if match:
            hitpath = match.group(1)
        else:
            hitpath = ''


        #time_row = drop_row + 0
        #segment_row = drop_row + 1
        #strategy_row = drop_row + 2
        #campaign_row = drop_row + 3
        #creative_row = drop_row + 4
        #body_content_row = drop_row + 5
        #ccid_row = drop_row + 6
        #mmid_row = drop_row + 7
        #akshad_row = drop_row + 8
        #mailer_row = drop_row + 9
        
        time_row = drop_row + 0
        segment_row = drop_row + 1
        strategy_row = drop_row + 2
        campaign_row = drop_row + 3 
        limit_row = drop_row + 4 
        offeset_row = drop_row + 5
        creative_row = drop_row+6
        jobname_row = drop_row + 7 

        date_column = cobra_sheet_df.columns.get_loc(next_drop_date)+1

    #     coil_time = cobra_sheet.get_value((time_row,date_column))
    #     coil_segment = cobra_sheet.get_value((segment_row,date_column))
    #     coil_send_strategy = cobra_sheet.get_value((strategy_row,date_column))
    #     coil_campaign = cobra_sheet.get_value((campaign_row,date_column))
    #     coil_creative = cobra_sheet.get_value((creative_row,date_column))
    #     coil_body_content = cobra_sheet.get_value((body_content_row,date_column))
    #     coil_ccid = cobra_sheet.get_value((ccid_row,date_column))
    #     coil_mmid = cobra_sheet.get_value((mmid_row,date_column))
    #     coil_akshad = cobra_sheet.get_value((akshad_row,date_column))
    #     coil_mailer = cobra_sheet.get_value((mailer_row,date_column))


        coil_time = cobra_sheet_df.iloc[time_row-2,date_column-1]
        coil_segment = cobra_sheet_df.iloc[segment_row-2,date_column-1]
        coil_send_strategy = cobra_sheet_df.iloc[strategy_row-2,date_column-1]
        coil_campaign = cobra_sheet_df.iloc[campaign_row-2,date_column-1]
        coil_limit = cobra_sheet_df.iloc[limit_row-2,date_column-1] 
        coil_offset = cobra_sheet_df.iloc[offeset_row-2,date_column-1]
        coil_creative = cobra_sheet_df.iloc[creative_row-2,date_column-1]
        coil_jobname = cobra_sheet_df.iloc[jobname_row-2,date_column-1]
        #coil_creative = cobra_sheet_df.iloc[creative_row-2,date_column]
        #coil_body_content = cobra_sheet_df.iloc[body_content_row-2,date_column-1]
        #coil_ccid = cobra_sheet_df.iloc[ccid_row-2,date_column-1]
        #coil_mmid = cobra_sheet_df.iloc[mmid_row-2,date_column-1]
        #coil_akshad = cobra_sheet_df.iloc[akshad_row-2,date_column-1]
        #coil_mailer = cobra_sheet_df.iloc[mailer_row-2,date_column-1]



        current_time = time.strftime("%I:%M %p", time.localtime())
        current_day = time.strftime("%D", time.localtime())
        latest_lexi = lexi['Date'].max().strftime('%m/%-d/%Y')
        

        function_called = inspect.stack()[2].function
        function_called = function_called if 'sc_dppub_affiliate' not in function_called else inspect.stack()[1].function
        date1 = datetime.datetime.strptime(next_drop_date, "%m/%d/%Y").strftime("%d%b%y")
        jobname = "SS_"+cobra_segment[:3] + "_"+cobra_segment[4:].replace(".",'-').replace("_",'-')+"_"+ campaign +"_"+ send_strategy + "_" +   date1
        affiliate_log.loc[len(affiliate_log)] = [current_day,current_time,function_called,
                                                 next_drop_date,latest_lexi,int(next_drop_number),
                                 days_ahead,', '.join(map(str, manual_offers)),', '.join(map(str, exclude_offers)),
                                 str(functions_dict),selection_confidence,manual_swap,
                                 drop_time,cobra_segment,send_strategy,campaign,
                                 "","",ccid,jobname,coil_time,coil_segment,coil_send_strategy,
                                 coil_campaign,coil_limit,coil_offset,coil_creative,coil_jobname]

    def write_affiliate_to_cobra(self,sc_dppub_affiliate, days_ahead, update_bool=False):

        cobra = self.cobra
        cobra_sheet_df = self.cobra_sheet_df
        cobra_sheet = self.cobra_sheet
#         print('test',sc_dppub_affiliate,days_ahead,update_bool)
        if update_bool:


            df=cobra[(cobra['Dataset']==sc_dppub_affiliate)& (cobra['Date'].dt.date>=date.today()+timedelta(days=0)) &
                                   (cobra['Date'].dt.date<=date.today()+timedelta(days=days_ahead))]

            days=pd.to_datetime(df['Date'].unique())

            drops=['Drop 1','Drop 2','Drop 3','Drop 4','Drop 5','Drop 6']
            cols=['Time','Segment','Send Strategy','Offer','Limit','Offset','Creative','Job Name','Blank']

            write_dict={}

            for index,day in enumerate(pd.to_datetime(df['Date'].unique()).strftime('%D')):
                for drop in drops:
                    for col in cols:
                        if col != 'Blank':
                            write_dict[(drop,col)] = df[(df["Drop"]==drop) & (df['Date']==day)][col].values[0]
                        else:
                            write_dict[(drop,col)] = ''

                if index==0:
                    write_df = pd.DataFrame(write_dict,index=[day]).T
                    write_df1 = pd.DataFrame(write_dict,index=[day]).T

                else:
                    write_df_new = pd.DataFrame(write_dict,index=[day]).T
                    write_df = pd.concat([write_df,write_df_new],axis=1)

            write_df=write_df.replace(np.nan,'')


            cobra_row = int(cobra_sheet_df[cobra_sheet_df['B'].str.contains(sc_dppub_affiliate)].index.values[0]+1)

            date_string = df['Date'].dt.date.values[0].strftime('%-m/%-d/%Y')
            cobra_column = cobra_sheet_df.columns.get_loc(date_string)+1

            
            cobra_sheet.set_dataframe(start=(cobra_row,cobra_column),df=write_df,copy_head=False,nan='')

            print('Completed writing schedule to Cobra!\n\n')

            return(write_df)


    def mock_schedule_estimate(self,hitpath,sc_dppub_affiliate,days_ahead):
#         cobra = self.cobra
        fvs = self.fvs
        offs = self.offs
        functions_dict = self.fvs.find_affiliate_functions(sc_dppub_affiliate)
        
        tier_one_function = getattr(offs, functions_dict['tier_one_production_determination'])
        tier_one_offers = tier_one_function(sc_dppub_affiliate)
        tier_one_offers = offs.return_unpaused_offers(sc_dppub_affiliate, tier_one_offers)
        estimated_drop_count = 0
        
        if hitpath in tier_one_offers:
            empties = self.find_empty_slots(sc_dppub_affiliate, hitpath, days_ahead, 
                         send_strategy='P', manual_offers=[], exclude_offers=[], given_empties=[], gap=[])
            gap = offs.find_offer_gap(sc_dppub_affiliate,hitpath)
            estimated_drop_count = len(empties)/(gap*fvs.affiliate_p_drops[sc_dppub_affiliate])
            
        else:
            tier_two_function = getattr(offs, functions_dict['tier_two_production_determination'])
            tier_two_offers = tier_two_function(sc_dppub_affiliate)
            tier_two_offers = offs.return_unpaused_offers(sc_dppub_affiliate, tier_two_offers)
            
            if hitpath in tier_two_offers:
                estimated_drop_count = 1
                
            else:
                tier_three_function = getattr(offs, functions_dict['tier_three_production_determination'])
                tier_three_offers = tier_three_function(sc_dppub_affiliate)
                tier_three_offers = offs.return_unpaused_offers(sc_dppub_affiliate, tier_three_offers) 
                
                if hitpath in tier_three_offers:
                    estimated_drop_count = 1
                    
        return math.ceil(estimated_drop_count)
                
                               
    def mock_schedule(self,new_hitpath,sc_dppub_affiliate,days_ahead,manual_offers,exclude_offers,update_cobra,print_bool=True):
        if print_bool:
            print('\n**********\nNow Scheduling Production Drops:\n**********')
        cobra = self.cobra
        fvs = self.fvs
        offs = self.offs
        functions_dict = self.fvs.find_affiliate_functions(sc_dppub_affiliate)
        
        affiliate_extra_drop_days = fvs.get_affiliate_extra_drop_days()
        scheduled_offers = []
        
        number_of_drops = fvs.affiliate_p_drops[sc_dppub_affiliate]
        number_of_drops_weekend = number_of_drops
        

        #First Fill any Potential "Placeholder" Offers
    #     print('fillin')
#         self.fill_placeholders(sc_dppub_affiliate,days_ahead,exclude_offers=exclude_offers,write_to_cobra_bool=update_cobra,
#                           print_bool=False,payout_tool=True)

        latest_date_to_schedule = date.today() + timedelta(days=days_ahead)

        extra_days = fvs.find_days_available(affiliate_extra_drop_days[sc_dppub_affiliate])

        
        ###################
        # Tier One Offers #
        ###################

        tier_one_function = getattr(offs, functions_dict['tier_one_production_determination'])
        tier_one_offers = tier_one_function(sc_dppub_affiliate)

        tier_two_function = getattr(offs, functions_dict['tier_two_production_determination'])
        tier_two_offers = tier_two_function(sc_dppub_affiliate)

        tier_two_offers = [x for x in tier_two_offers if x not in exclude_offers]
        tier_two_offers = offs.return_unpaused_offers(sc_dppub_affiliate, tier_two_offers)

        tier_three_function = getattr(offs, functions_dict['tier_three_production_determination'])
        tier_three_offers = tier_three_function(sc_dppub_affiliate)

        tier_three_offers = [x for x in tier_three_offers if x not in exclude_offers]
        tier_three_offers = offs.return_unpaused_offers(sc_dppub_affiliate, tier_three_offers)    

        #exclude "exclude_offers" and make sure "manual_offers" does not appear twice
        offers_to_schedule = manual_offers + [x for x in tier_one_offers if x not in exclude_offers and x not in manual_offers]


        offers_to_schedule = offs.return_unpaused_offers(sc_dppub_affiliate, offers_to_schedule)


        upcoming_offers = (offers_to_schedule + tier_two_offers + tier_three_offers)

        if new_hitpath in upcoming_offers:
           
            initial_leftover_slots = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
                               (cobra['Date'].dt.date>=date.today()) &
                               (cobra['Date'].dt.date<=latest_date_to_schedule) &
                               (cobra['Drop Number']>1) &
                               ( ( (cobra['Drop Number']<=(number_of_drops)) & (cobra['Day of Week'].apply(lambda x: x in range(0,5))) ) |
                                 ( (cobra['Drop Number']<=(number_of_drops_weekend)) & (cobra['Day of Week'].apply(lambda x: x in range(5,7))) )  |
                                 ( (cobra['Drop Number']<=(number_of_drops+1)) & (cobra['Day of Week'].apply(lambda x: x in extra_days)))) & 
                               ((cobra['Offer'].isnull())|( cobra['Offer']==''))]



    #     if new_hitpath in upcoming_offers:
        break_bool = False   
        for index, hitpath in enumerate(offers_to_schedule):
#             print('trying to schedule hitpath',hitpath)
            upcoming_offers = (offers_to_schedule + tier_two_offers + tier_three_offers)[index:]
    #         print(upcoming_offers)
            if new_hitpath not in upcoming_offers:
                break_bool = True
                break
            else:
                
                cobra_empties = cobra.loc[initial_leftover_slots.index]

                final_leftover_slots = cobra_empties[cobra_empties['Offer'].isnull()]

                if len(final_leftover_slots) > 0:

                    scheduled_bool = self.mock_schedule_offer(sc_dppub_affiliate,hitpath,days_ahead,manual_offers=manual_offers,
                                                    write_to_cobra_bool=update_cobra,selection_confidence='Tier One',
                                                    given_empties=cobra_empties,print_bool=print_bool)
                   
        #             print(scheduled_bool)
                    if scheduled_bool:
                        scheduled_offers.append(hitpath)
                        if functions_dict['production_drop_preference']=='offer_performance':
                            offers_to_schedule.insert(index+1,hitpath)
                        else: #offer variety
                            offers_to_schedule.insert(index+1,hitpath)


            leftover_slots = cobra[(cobra['Dataset']==sc_dppub_affiliate) & 
                               (cobra['Date'].dt.date>=date.today()) &
                               (cobra['Date'].dt.date<=latest_date_to_schedule) &
                               (cobra['Drop Number']>1) &    
                               ( ( (cobra['Drop Number']<=(number_of_drops)) & (cobra['Day of Week'].apply(lambda x: x in range(0,5))) ) |
                                 ( (cobra['Drop Number']<=(number_of_drops_weekend)) & (cobra['Day of Week'].apply(lambda x: x in range(5,7))) ) |
                                   ( (cobra['Drop Number']<=(number_of_drops+1)) & (cobra['Day of Week'].apply(lambda x: x in extra_days)))) &
                               ((cobra['Offer'].isnull())|( cobra['Offer']==''))]
            if print_bool:
                print('\n*** Completed scheduling tier one;',len(leftover_slots),'spots to be filled with tier two offers ***')

        ###################
        # Tier Two Offers #
        ###################
        if not break_bool:
            for index, hitpath in enumerate(tier_two_offers):
                upcoming_offers = (tier_two_offers + tier_three_offers)[index:]
                if new_hitpath not in upcoming_offers:
                    break_bool = True
                    break
                else:

                    send_strategy = 'P'
                    #segment = 'A120'
                    cobra_empties = cobra.loc[initial_leftover_slots.index]     
                    scheduled_bool = self.mock_schedule_offer(sc_dppub_affiliate,hitpath,days_ahead, manual_offers=manual_offers,
                                                    write_to_cobra_bool=update_cobra,
                                  selection_confidence='Tier Two',given_empties=cobra_empties,print_bool=print_bool)

                    if scheduled_bool:
                            scheduled_offers.append(hitpath)

                cobra_empties = cobra.loc[initial_leftover_slots.index]

                final_leftover_slots = cobra_empties[cobra_empties['Offer'].isnull()]

        #####################
        # Tier Three Offers #
        #####################
        if break_bool == False:
            if ('final_leftover_slots' in locals()) and (len(final_leftover_slots)>0):

                for index, hitpath in enumerate(tier_three_offers):
                    upcoming_offers = (tier_three_offers)[index:]

                    if new_hitpath not in upcoming_offers:
                        break
                    else:

                        send_strategy = 'P'
                        #segment = 'A120'

                        cobra_empties = cobra.loc[initial_leftover_slots.index]

                        final_leftover_slots = cobra_empties[cobra_empties['Offer'].isnull()]


                        if len(final_leftover_slots) > 0:
                            scheduled_bool = self.mock_schedule_offer(sc_dppub_affiliate,hitpath,days_ahead, manual_offers=manual_offers,
                                                            write_to_cobra_bool=update_cobra,
                                          selection_confidence='Tier Three',given_empties=cobra_empties,print_bool=print_bool)

                        if scheduled_bool:
                            scheduled_offers.append(hitpath)

                cobra_empties = cobra.loc[initial_leftover_slots.index]

                final_leftover_slots = cobra_empties[cobra_empties['Offer'].isnull()]

        return scheduled_offers

    def mock_schedule_offer(self,sc_dppub_affiliate,offer,days_ahead,segment='A120',send_strategy='P',manual_offers=[],
                       exclude_offers=[],write_to_cobra_bool=False,selection_confidence='',given_empties=[],print_bool=True):
        cobra = self.cobra
        offs = self.offs
        ss_offer = self.ss_offer
        functions_dict = self.fvs.find_affiliate_functions(sc_dppub_affiliate)
        offers = self.offers
        sc = sc_dppub_affiliate.split('_')[0]
        past_cobras = self.past_cobras

        offer_gap_function = getattr(offs, functions_dict['offer_gap_determination'])
        gap = offer_gap_function(sc_dppub_affiliate,offer)

        empty_slots = self.find_empty_slots(sc_dppub_affiliate,offer,days_ahead,send_strategy=send_strategy,
                                       manual_offers=manual_offers,exclude_offers=exclude_offers,given_empties=given_empties)
       
        if len(empty_slots) > 0:
            
            next_drop = empty_slots[empty_slots['Date']==empty_slots['Date'].min()].sample(n=1)

            next_drop_date = next_drop['Date'].dt.date.values[0].strftime('%-m/%-d/%Y')

            if self.check_debt_vertical_gapping(sc_dppub_affiliate,offer,next_drop['Date'].dt.date.values[0]):

    #             if send_strategy=='MI':
    #                 next_drop_number = 8
    #             else:
                next_drop_number = next_drop['Drop Number'].values[0]

#                 campaign = offers[offers['Hitpath Offer ID']==offer]['Scheduling Name'].values[0]
                #campaign = offers.loc[offer]['Scheduling Name']
                try: 
                    campaign = ss_offer.loc[(ss_offer['SS Offers (updated)'].str.contains(offer, na = False)) & (ss_offer['SS Offers (updated)'].str[-4:].str.contains(sc, na = False)), 'SS Offers (updated)' ].values[0]
                except: 
                    print("The offer is not added in the SS.")
                
                cobra_segment = past_cobras[(past_cobras['Dataset'] == sc_dppub_affiliate) & (past_cobras['Segment']!='') & (past_cobras['Drop Number'] == 1)].sort_values(by = 'Date', ascending = False)['Segment'].values[0]
                next_drop['Offer'] = campaign
                next_drop['Hitpath Offer ID'] = offer
                next_drop['Send Strategy'] = 'P'
                date1 = next_drop['Date'].dt.date.values[0].strftime("%d%b%y")  
                next_drop['Segment'] = cobra_segment
                next_drop['Job Name'] = "SS_"+cobra_segment[:3] + "_"+cobra_segment[4:].replace(".",'-').replace("_",'-')+"_"+ offer +"_"+ send_strategy + "_" +   date1


                cobra.loc[next_drop.index, :] = next_drop[:]

                return True

        
    def compare_payouts(self,sc_dppub_affiliate,new_payout_hitpath,new_payout,days_ahead):
        lexi = self.lexi
        fvs = self.fvs
        #lexi_conversions = self.lexi_conversions
        lexi_segment_sizes = self.lexi_segment_sizes

        def run_normal(aff, new_hitpath):
            offers_scheduled = []
            self.lexi = self.lexi_clean.copy()
            self.offs.lexi = self.lexi.copy()

            affiliates = [aff]

            normal_counts_affiliate = ()
            for sc_dppub_affiliate in affiliates:

#                 hitpath_affiliate_count = self.mock_schedule(new_hitpath,sc_dppub_affiliate,days_ahead,[],[],
#                                           update_cobra=False,print_bool=False).count(new_hitpath)
                
                hitpath_affiliate_count = self.mock_schedule_estimate(new_hitpath,sc_dppub_affiliate,days_ahead)
    
                try:
                    est_conversions_per_drop = lexi_conversions.loc[sc_dppub_affiliate,'P',new_hitpath].values[0] 
                except:
                    est_conversions_per_drop = 0

                   
                segment_size = lexi_segment_sizes.loc[sc_dppub_affiliate].values[0]
                delivered_estimate = segment_size * hitpath_affiliate_count
                conversion_estimate = est_conversions_per_drop * hitpath_affiliate_count    
                    
                current_payout = lexi[(lexi['Affiliate ID']==sc_dppub_affiliate)&
                                  (lexi['Hitpath Offer ID']==new_hitpath)].sort_values(by='Date').iloc[-1]['Updated Payout']

                if np.isnan(current_payout): #replace with most recent calculated payout
                    try:
                        current_payout = lexi[(lexi['Affiliate ID']==sc_dppub_affiliate)& (lexi['Hitpath Offer ID']==new_hitpath)
                            ].sort_values(by='Date')['Payout'].replace([np.inf, -np.inf], np.nan).dropna().iloc[-1]
                    except:
                        current_payout = 1
                normal_counts_affiliate += (sc_dppub_affiliate, hitpath_affiliate_count, 
                                            delivered_estimate, conversion_estimate, current_payout)
                
                
            return normal_counts_affiliate

        def run_updated_payout(sc_dppub_affiliate,new_hitpath,new_payout,days_ahead):

            normal_counts_affiliate = run_normal(sc_dppub_affiliate, new_hitpath)
            offers = self.offers
            fvs = self.fvs
            self.cobra = self.cobra_clean.copy()

            updated_counts_list = []
            
            lexi_payouts_complete = viper_main.complete_lexi_payouts(self.lexi_payouts, sc_dppub_affiliate, new_hitpath, new_payout)
            self.lexi = lexi_payouts_complete
            self.offs.lexi = self.lexi.copy()
        
    
            updated_counts_affiliate = ()

#             hitpath_affiliate_count = self.mock_schedule(new_hitpath,sc_dppub_affiliate,days_ahead,[],[],
#                                       update_cobra=False,print_bool=False).count(new_hitpath)
            hitpath_affiliate_count = self.mock_schedule_estimate(new_hitpath,sc_dppub_affiliate,days_ahead)
        
            try:
                est_conversions_per_drop = lexi_conversions.loc[sc_dppub_affiliate,'P',new_hitpath].values[0] 
            except:
                est_conversions_per_drop = 0
                
            segment_size = lexi_segment_sizes.loc[sc_dppub_affiliate].values[0]
            delivered_estimate = segment_size * hitpath_affiliate_count
            conversion_estimate = est_conversions_per_drop * hitpath_affiliate_count    

            updated_counts_affiliate += (sc_dppub_affiliate, hitpath_affiliate_count, delivered_estimate, conversion_estimate)
     
    
            return(normal_counts_affiliate,updated_counts_affiliate)

    #         print("New payout drop count for {}: {} (in the next {} days)"
    #               .format(new_payout_entries[1],updated_counts.get(new_payout_entries[1],0), days_ahead))

        return run_updated_payout(sc_dppub_affiliate,new_payout_hitpath,new_payout,days_ahead)
    
