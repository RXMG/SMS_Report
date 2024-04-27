import numpy as np
import re
import datetime
import pandas as pd
from datetime import date
from datetime import timedelta
import json
from nltk import flatten
import inspect 
import viper_main
import pygsheets
import infrastructure
# from multiprocessing import Pool
import sys
# import viper_main
# del sys.modules["viper_main"]
# import viper_main
# import multiprocessing
import filepath
import time
import os

class inputs:
    def __init__(self, lexi=None, cobra=None, cobra_sheet=None, cobra_sheet_df=None, offers=None):
#         self.lock = threading.Lock()
        self.cobra = cobra
        self.cobra_clean = cobra.copy()
        self.cobra_sheet = cobra_sheet
        self.cobra_sheet_df = cobra_sheet_df
        self.offers = offers
        self.lexi = lexi
        
        viper_settings_url = 'https://docs.google.com/spreadsheets/d/104t7KyOxfQvZH318MKxT0m7hMRRPvDMFVLIMd2YUVco/edit#gid=1354838607'
#         if os.environ.get('DOCKER_RUNNING'):
#             auth_path='host_data/' + filepath.service_account_location.split('/')[-1]
#         else:
#             auth_path=filepath.service_account_location

#         self.gc = pygsheets.authorize(service_account_file=auth_path)
        self.gc = infrastructure.get_gc()

        self.viper_settings_sheet = self.gc.open_by_url(viper_settings_url)
        self.aff_settings_frame = self.viper_settings_sheet.worksheet_by_title('Affiliate Settings').get_as_df()

        self.cpam_frame = self.viper_settings_sheet.worksheet_by_title('CPAM Scheduling').get_as_df()
        self.cpam_frame = self.cpam_frame.replace('',np.nan).ffill()
        
        self.affiliate_p_drops = {}  
        for index, row in self.aff_settings_frame.iterrows():
            self.affiliate_p_drops[row['Account']] = int(row['Production Drops per Day'])
            
        ## Budget Sheet
        budget_url = 'https://docs.google.com/spreadsheets/d/1mDS54-C_OtHhD6MVch_PUHAOtWjlfXVZ1OorEkrlSQg/edit#gid=1004364787'
#         sheets = Sheets.from_files(filepath.gsheets)
#         self.budget_sheet = sheets.get(budget_url)
        self.budget_sheet = self.gc.open_by_url(budget_url)
        
            
            
        
  
        ## Direct Agents Payout Sheet
        direct_agents_url = 'https://docs.google.com/spreadsheets/d/1olJVhGiRqx6oTUAW556EGft7zPgEW6e8dZabh5uLKH4/edit#gid=1899204545'
        # self.direct_agents_spreadsheet = sheets.get(direct_agents_url)
        self.direct_agents_spreadsheet = self.gc.open_by_url(direct_agents_url)
        
        
        #self.lexi_payouts = viper_main.update_lexi_payouts(lexi.copy(),offers)

#         self.lexi = self.update_lexi_history(self.lexi)

        #self.budget_frame = self.get_budget_frame()
        
#         self.lexi.sort_values(by=['Date'],inplace=True)
        self.lexi['EMA Revenue'] = self.lexi.groupby(['SC_DP&Pub','Hitpath Offer ID'])['Revenue'].transform(lambda x: x.ewm(alpha=0.4).mean())
        self.lexi['EMA Clicks'] = self.lexi.groupby(['SC_DP&Pub','Hitpath Offer ID'])['Clicks'].transform(lambda x: x.ewm(alpha=0.4).mean())
        self.lexi_running = self.set_lexi_running(self.lexi)
        
        
#         self.lexi_p = lexi[(lexi['Date'].dt.date>=date.today()+timedelta(days=-90))&(lexi['Send Strategy']=='P')][['SC_DP&Pub','Hitpath Offer ID','Super Segment','Date']]
#         self.lexi_p = lexi_p[~lexi_p.duplicated()]
#         self.avg_drops = lexi_p.groupby(['SC_DP&Pub']).agg({'Hitpath Offer ID':'count','Date':'nunique'})
#         self.avg_drops['Average Drops per Day'] = avg_drops['Hitpath Offer ID'] / avg_drops['Date']
#         self.avg_drops['Average Drops per Day'] = avg_drops['Average Drops per Day'].round()
      
    def set_lexi_running(self,lexi):
#         lexi_running = lexi.sort_values(by=['SC_DP&Pub', 'Date'])
#         lexi_running = lexi_running.groupby(['SC_DP&Pub','Date']).agg({'Revenue':'sum','Delivered':'sum','EMA Revenue':'sum'}).reset_index()
#         lexi_running['Running Revenue'] = lexi_running.groupby('SC_DP&Pub')['Revenue'].cumsum()
#         lexi_running['Running Delivered'] = lexi_running.groupby('SC_DP&Pub')['Delivered'].cumsum()
#         lexi_running['Running EMA Revenue'] = lexi_running.groupby('SC_DP&Pub')['EMA Revenue'].cumsum()
        
#         lexi_running.set_index(['SC_DP&Pub','Date'],inplace=True)
#         lexi_running = lexi_running.unstack().fillna(0).stack()
        
        lexi_running = lexi.sort_values(by=['SC_DP&Pub', 'Date'])
        lexi_running = lexi_running[lexi_running['SC_DP&Pub']!='nan']
        lexi_running = lexi_running[lexi_running['Send Strategy']=='P']
#         lexi_running = lexi_running[lexi_running['Operational Status']=='Live']

        sc_dppub_affiliate = lexi_running['SC_DP&Pub'].unique()
        date_range = pd.date_range(lexi_running['Date'].min(),lexi_running['Date'].max())

        empty_index = pd.MultiIndex.from_product([sc_dppub_affiliate,date_range],names=['SC_DP&Pub','Date'])
        empty_template_df = pd.DataFrame(index=empty_index).reset_index()

        lexi_running = pd.merge(empty_template_df, lexi_running[['SC_DP&Pub','Date','Revenue','Delivered','EMA Revenue','Clicks','EMA Clicks']], how='left', on=['SC_DP&Pub','Date'])

        lexi_running = lexi_running.groupby(['SC_DP&Pub','Date'],).agg({'Revenue':'sum','Delivered':'sum','EMA Revenue':'sum','Clicks':'sum','EMA Clicks':'sum'}).reset_index()
        lexi_running['Running Revenue'] = lexi_running.groupby('SC_DP&Pub')['Revenue'].cumsum()
        lexi_running['Running Clicks'] = lexi_running.groupby('SC_DP&Pub')['Clicks'].cumsum()
        lexi_running['Running Delivered'] = lexi_running.groupby('SC_DP&Pub')['Delivered'].cumsum()
        lexi_running['Running EMA Revenue'] = lexi_running.groupby('SC_DP&Pub')['EMA Revenue'].cumsum()
        lexi_running['Running EMA Clicks'] = lexi_running.groupby('SC_DP&Pub')['EMA Clicks'].cumsum()
        
        lexi_running['30 Day Running Revenue'] = lexi_running.groupby('SC_DP&Pub')['Running Revenue'].diff(periods=30)
        lexi_running['30 Day Running EMA Revenue'] = lexi_running.groupby('SC_DP&Pub')['Running EMA Revenue'].diff(periods=30)
        lexi_running['30 Day Running EMA Clicks'] = lexi_running.groupby('SC_DP&Pub')['Running EMA Clicks'].diff(periods=30)
        lexi_running['30 Day Running Delivered'] = lexi_running.groupby('SC_DP&Pub')['Running Delivered'].diff(periods=30)
        lexi_running['30 Day EMA eCPM'] = lexi_running['30 Day Running EMA Revenue']*1000 / lexi_running['30 Day Running Delivered']
        lexi_running['30 Day EMA CTR'] = lexi_running['30 Day Running EMA Clicks']*1000 / lexi_running['30 Day Running Delivered']
        lexi_running['30 Day eCPM'] = lexi_running['30 Day Running Revenue']*1000 / lexi_running['30 Day Running Delivered']

        lexi_running.set_index(['SC_DP&Pub','Date'],inplace=True)


        
        return lexi_running
        
        
    def add_new_column(self, new_col_name, new_col_position):
        from pygsheets.datarange import DataRange
        #Add a new column to all worksheets in Viper Log
        #Position is how many columns will be to the left
        gc = self.gc

#         gc = pygsheets.authorize(service_account_file=filepath.service_account_location)
        sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1oKod0EKYfZmrS9MUtmftmEcv6WPESP5-qMJ6DBuJ4kU/edit?usp=sharing')

        def find_last_col_name(length):
            #will work with column lengths between 1 and 52 inclusive
            #returns last google sheet range

            if length < 27:
                return chr(ord('A')+(length-1)%26)

            return 'A' + chr(ord('A')+(length-1)%26)

        for ws in sh.worksheets():
            ws_with_new_col = ws.get_as_df().copy()

            if new_col_name not in ws_with_new_col.columns:
                ws_with_new_col.insert(new_col_position, column=new_col_name, value="")
                ws.set_dataframe(start='A1',df=ws_with_new_col)
                #Reformat Column Headers
                template_columns = sh.worksheets(sheet_property='title',value='Template')[0][1]
                ws.adjust_column_width(1, len(template_columns), pixel_size=303)

                end_column_range = find_last_col_name(len(ws[1])) + "1"

#                 DataRange('A1',end_column_range, worksheet=ws).apply_format(model_cell)

        
        
#     def update_lexi_history(self,lexi):
#         aff_settings_frame = self.aff_settings_frame
#         aff_settings_frame.replace('',np.nan,inplace=True)
#         history = aff_settings_frame[~aff_settings_frame['Previous Affiliate IDs'].isnull()]
#         history['Previous Affiliate IDs'] = history['Previous Affiliate IDs'].astype(int)
#         history[['Affiliate', 'Previous Affiliate IDs']] = history[['Affiliate', 'Previous Affiliate IDs']].astype(str)
        
#         mapping = history.set_index('Previous Affiliate IDs')['Affiliate'].to_dict()
#         for original_id, new_id in mapping.items():
#             new_df = lexi[lexi['SC_DP&Pub']==new_id]
#             original_df = lexi[lexi['SC_DP&Pub']==original_id]
#             history_rows = original_df[original_df['Date']<new_df['Date'].min()].copy()
#             history_rows['SC_DP&Pub'] = new_id
#             lexi = pd.concat([lexi,history_rows],ignore_index=True)
            
            

#         lexi['SC_DP&Pub'].replace(mapping,inplace=True)

#         mapping = history.set_index('Affiliate')['Previous Affiliate IDs'].to_dict()

#         lexi['Affiliate ID History'] = lexi['SC_DP&Pub']
#         lexi['Affiliate ID History'].replace(mapping,inplace=True)


#        return lexi

    def get_affiliate_extra_drop_days(self):
        affiliate_extra_drop_days = {}
        for index, row in self.aff_settings_frame.iterrows():
            affiliate_extra_drop_days[row['Account']] = row['Extra Drop Days']
        return affiliate_extra_drop_days

    def get_all_affiliates(self):
        aff_settings_frame = self.aff_settings_frame
        all_affiliates = [str(x) for x in list(self.viper_settings_sheet.worksheet_by_title('Affiliate Settings').get_as_df()['Account'].unique())]
        return all_affiliates
    
    def get_recent_affiliates(self):
        lexi = self.lexi
        lexi_affiliate = lexi[lexi['Date']>=lexi['Date'].max() + timedelta(days=-30)]['SC_DP&Pub'].unique().tolist()
        sheet_affiliates = self.get_all_affiliates()
        sc_dppub_affiliate = [x for x in sheet_affiliates if x in lexi_affiliate]
        return sc_dppub_affiliate
    
    def get_budget_title_month(self):

        budget_sheet = self.budget_sheet
        budget_titles = [worksheet.title for worksheet in budget_sheet.worksheets()]
        budget_title = [title for title in budget_titles if re.search(r'\bBudget\b',title) ][0]
        budget_title_month = budget_title.split(' ')[0].split('/')[0]

        return budget_title_month

    def get_budget_frame(self):

        budget_sheet = self.budget_sheet
        budget_titles = [worksheet.title for worksheet in budget_sheet.worksheets()]
        budget_title = [title for title in budget_titles if re.search(r'\bBudget\b',title) ][0]

        budget_frame = budget_sheet.worksheet_by_title(budget_title).get_as_df(dtype={'PUB ID':str})
        budget_frame[['DMA','PUB ID','Scheduling Name ']] = budget_frame[['DMA','PUB ID','Scheduling Name ']].fillna(method='ffill')
        budget_frame['Production Segment'].fillna(1, inplace=True)

        budget_frame.loc[budget_frame['PUB ID'].str.lower().str.contains('sc.fha',na=False), 'PUB ID'] = '160398'
        budget_frame.loc[budget_frame['PUB ID'].str.lower().str.contains('sc.rf',na=False), 'PUB ID'] = '260398'
        budget_frame.loc[budget_frame['PUB ID'].str.lower().str.contains('lpg.fha',na=False), 'PUB ID'] = '161128'
        budget_frame.loc[budget_frame['PUB ID'].str.lower().str.contains('lpg.rf',na=False), 'PUB ID'] = '261128'
        
        budget_frame['Scheduling Name '].replace('',np.nan,inplace=True)
        budget_frame.dropna(subset=['Scheduling Name '],inplace=True)

        return budget_frame

    def get_budgeted_offers_list(self,sc_dppub_affiliate):
        budget_frame = self.get_budget_frame()
        grouped_budget_frame = budget_frame.groupby(['PUB ID','Scheduling Name ']).agg({'Production Segment':'sum'}).reset_index()
        grouped_budget_frame['Scheduling Name '] = grouped_budget_frame['Scheduling Name '].astype(str)
        grouped_budget_frame['Hitpath Offer ID'] = grouped_budget_frame['Scheduling Name '].str.split('-').str[0].astype(int)
        budgeted_offers_affiliate = grouped_budget_frame[grouped_budget_frame['PUB ID']==sc_dppub_affiliate]
        budgeted_offers_list = budgeted_offers_affiliate['Hitpath Offer ID'].tolist()
        return budgeted_offers_list

    def get_affiliate_ot_drop_days(self):
        affiliate_ot_drop_days = {}
        for index, row in self.aff_settings_frame.iterrows():
            affiliate_ot_drop_days[row['Account']] = row['Affiliate OT Schedule']
        return affiliate_ot_drop_days

    def get_affiliate_e_drop_days(self):
        affiliate_e_drop_days = {}
        for index, row in self.aff_settings_frame.iterrows():
            affiliate_e_drop_days[row['Account']] = row['Affiliate Engagement Schedule']
        return affiliate_e_drop_days

    def find_mirrors(self):
        mirror_frame = self.viper_settings_sheet.worksheet_by_title('Mirror Offers').get_as_df()
        mirror_frame['Hitpaths'] = mirror_frame['Hitpaths'].astype(str)
        mirrors={}
        true_mirrors={}
        offers = self.offers
                
        def find_hitpaths(hitpath):
            hitpath = hitpath.strip()
            try:
                return int(hitpath)
            except:
                if hitpath[0]=='s':
                    return offers[(offers['Super Hitpath']==hitpath)].index.tolist()

        for index, row in mirror_frame.iterrows():
            if row['True Mirrors'].lower()=='true':
                true_mirror = True
            else:
                true_mirror = False
            hit_list = [find_hitpaths(hitpath) for hitpath in row['Hitpaths'].split(",")]
            hit_list = flatten(hit_list)

            for hit in hit_list:
                hit_list_copy = hit_list.copy()
                hit_list_copy.remove(hit)
                if true_mirror:
                    true_mirrors[hit] = hit_list_copy
                else:
                    mirrors[hit] = hit_list_copy

        return mirrors,true_mirrors


    def find_mi_drop_days(self,sc_dppub_affiliate):
        affiliate_mi_drop_days = {}

        for index, row in self.aff_settings_frame.iterrows():
            affiliate_mi_drop_days[row['Account']] = row['Affiliate Mining Schedule']
        return affiliate_mi_drop_days[sc_dppub_affiliate]


    def find_days_restrictions(self,day_restrictions):
        # Take in an offer and find the string of restrictions (e.g. "Monday-Friday", "MON-SAT"). Then return these days as numbers
        # 0=monday, 1=tuesday, etc...
#         print('day restrictions', day_restrictions, type(day_restrictions), day_restrictions is np.nan, pd.isnull(day_restrictions))
        if type(day_restrictions)==int or (day_restrictions=='None'):
            return range(0,7)
        if day_restrictions is np.nan or (pd.isnull(day_restrictions)):
            return range(0,7)
        elif '-' not in day_restrictions:
            return range(0,7)
        else:

            first_day = day_restrictions.split('-')[0]
            last_day = day_restrictions.split('-')[1]

            if re.findall("mon", first_day, re.IGNORECASE):
                first = 0
            elif re.findall("tue", first_day, re.IGNORECASE):
                first = 1
            elif re.findall("wed", first_day, re.IGNORECASE):
                first = 2
            elif re.findall("thu", first_day, re.IGNORECASE):
                first = 3
            elif re.findall("fri", first_day, re.IGNORECASE):
                first = 4
            elif re.findall("sat", first_day, re.IGNORECASE):
                first = 5
            elif re.findall("sun", first_day, re.IGNORECASE):
                first = 6

            if re.findall("mon", last_day, re.IGNORECASE):
                last = 0
            elif re.findall("tue", last_day, re.IGNORECASE):
                last = 1
            elif re.findall("wed", last_day, re.IGNORECASE):
                last = 2
            elif re.findall("thu", last_day, re.IGNORECASE):
                last = 3
            elif re.findall("fri", last_day, re.IGNORECASE):
                last = 4
            elif re.findall("sat", last_day, re.IGNORECASE):
                last = 5
            elif re.findall("sun", last_day, re.IGNORECASE):
                last = 6

            if first>last:
                return [x for x in range(0, 7) if x not in range(last+1,first)]
            else:
                return range(first, last+1) #range is exlcusive on the end
        

    def find_days_available(self,days_to_schedule):
    #Take user input e.g. "Monday - Friday" and return a range e.g. (0,4)

        if type(days_to_schedule)==int:
            return range(0,7)
        if days_to_schedule is np.nan or pd.isnull(days_to_schedule) or days_to_schedule=='':
            return range(8,8)
        elif ',' in days_to_schedule:
            day_range = []

            for comma_counter in range(days_to_schedule.count(',')+1):
                days = days_to_schedule.split(',')[comma_counter].strip()
                if re.findall("mon", days, re.IGNORECASE):
                    day = 0
                elif re.findall("tue", days, re.IGNORECASE):
                    day = 1
                elif re.findall("wed", days, re.IGNORECASE):
                    day = 2
                elif re.findall("thu", days, re.IGNORECASE):
                    day = 3
                elif re.findall("fri", days, re.IGNORECASE):
                    day = 4
                elif re.findall("sat", days, re.IGNORECASE):
                    day = 5
                elif re.findall("sun", days, re.IGNORECASE):
                    day = 6
                day_range.append(day)

            return day_range
        else:

            first_day = days_to_schedule.split('-')[0]
            if len(days_to_schedule.split('-')) > 1:
                last_day = days_to_schedule.split('-')[1]
            else:
                last_day = first_day

            if re.findall("mon", first_day, re.IGNORECASE):
                first = 0
            elif re.findall("tue", first_day, re.IGNORECASE):
                first = 1
            elif re.findall("wed", first_day, re.IGNORECASE):
                first = 2
            elif re.findall("thu", first_day, re.IGNORECASE):
                first = 3
            elif re.findall("fri", first_day, re.IGNORECASE):
                first = 4
            elif re.findall("sat", first_day, re.IGNORECASE):
                first = 5
            elif re.findall("sun", first_day, re.IGNORECASE):
                first = 6

            if re.findall("mon", last_day, re.IGNORECASE):
                last = 0
            elif re.findall("tue", last_day, re.IGNORECASE):
                last = 1
            elif re.findall("wed", last_day, re.IGNORECASE):
                last = 2
            elif re.findall("thu", last_day, re.IGNORECASE):
                last = 3
            elif re.findall("fri", last_day, re.IGNORECASE):
                last = 4
            elif re.findall("sat", last_day, re.IGNORECASE):
                last = 5
            elif re.findall("sun", last_day, re.IGNORECASE):
                last = 6

            return range(first, last+1) #range is exlcusive on the end


    def find_days(self,hitpath):
        #Return the available days an offer is able to be dropped
        offers = self.offers
        day_restrictions = offers.loc[hitpath]['Day Restrictions']
#         fvs = self.fvs
        available_days = self.find_days_restrictions(day_restrictions)
        return available_days


    def find_AM_requests(self,sc_dppub_affiliate,redrop_or_test):
        #Return a list of offers to be put to the front of either Redrops or Testing Queues

        lexi = self.lexi
        offers = self.offers

        requests_redrops_list = []
        requests_tests_list = []
        requests_frame = self.viper_settings_sheet.worksheet_by_title('Offer Scale Requests').get_as_df()

        year_ago_aff_df = lexi[(lexi['SC_DP&Pub']==sc_dppub_affiliate) & (lexi['Date'].dt.date>=(date.today() + timedelta(days=-395))) & 
                     (lexi['Date'].dt.date<=(date.today() + timedelta(days=-335))) & (lexi['Send Strategy']=='P')] 

        year_ago_p_ecpm = year_ago_aff_df['Revenue'].sum()*1000 / year_ago_aff_df['Delivered'].sum()
            
        for index, row in requests_frame.iterrows():
            hitpath = row['Hitpath Offer ID']

            request_vertical = offers.loc[hitpath]['Vertical']

            if lexi[lexi['SC_DP&Pub']==sc_dppub_affiliate]["Date"].min() <= date.today() + timedelta(days=-395): #if this account is at least 13 months old

                year_ago_vertical = year_ago_aff_df[year_ago_aff_df['Vertical']==request_vertical]
                year_ago_vertical_ecpm = year_ago_vertical['Revenue'].sum()*1000 / year_ago_vertical['Delivered'].sum()

                year_ago_hitpath = year_ago_aff_df[year_ago_aff_df['Vertical']==hitpath]
                year_ago_hitpath_ecpm = year_ago_hitpath['Revenue'].sum()*1000 / year_ago_hitpath['Delivered'].sum()


                if year_ago_hitpath_ecpm>=year_ago_p_ecpm*0.8:
                    requests_redrops_list.append(hitpath)
                elif year_ago_vertical_ecpm>=year_ago_p_ecpm*1.2:
                    requests_tests_list.append(hitpath)
                else:
                    pass

        if redrop_or_test=='redrop':
            return requests_redrops_list
        else:
            return requests_tests_list


    def find_affiliate_functions(self,sc_dppub_affiliate,print_bool=True):

        functions_frame = self.aff_settings_frame.copy()
        functions_frame.set_index('Account',inplace=True)
        functions_frame.index = functions_frame.index.astype(str)

        try:
            dict_string = functions_frame.loc[sc_dppub_affiliate]['Affiliate Functions'].replace('\n','').replace("functions_dict = ", "").replace(" #enter \"offer_variety\" or \"offer_performance\"","").replace("\'","\"")
            return json.loads(dict_string)

        except:
            dict_string = "{\"tier_one_production_determination\" : \"find_top_p_offers\",\"tier_two_production_determination\" : \"find_tier_two_offers\",\"tier_three_production_determination\" : \"get_last_resort_offers\",\"creative_type_determination\" : \"choose_creative\",\"content_id_determination\" : \"choose_ccid\",\"offer_gap_determination\" : \"find_offer_gap\",\"mining_offer_determination\" : \"find_top_mi_offers\",\"ot_determination\" : \"find_offer_tests\",\"e_determination\" : \"find_e_offers\",\"placeholder_offer_determination\" : \"find_top_advertiser_offers\",\"production_drop_preference\" :\"offer_variety\" }"

        if print_bool:
            print("Please check your sc_dppub_affiliate functions dictionary!  Returning default values")

        return json.loads(dict_string)
        
        
    def load_log_old(self,sc_dppub_affiliate='all'):
        gc = self.gc
#         gc = pygsheets.authorize(service_account_file=filepath.service_account_location)
        sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1oKod0EKYfZmrS9MUtmftmEcv6WPESP5-qMJ6DBuJ4kU/edit#gid=0')
        log = pd.DataFrame({'A' : []})
        all_sheet_titles = [x.title for x in sh.worksheets()]
        
        if sc_dppub_affiliate=='all':
            sc_dppub_affiliate = all_sheet_titles
        
        for aff in sc_dppub_affiliate:
            if aff in all_sheet_titles:
                aff_df = sh.worksheets(sheet_property='title',value=aff)[0].get_as_df()
                aff_df['SC_DP&Pub'] = aff #clean up for CPAM
                log = pd.concat([log,aff_df])
            else:
                aff_df = sh.worksheets(sheet_property='title',value='Template')[0].get_as_df()
                aff_df['SC_DP&Pub'] = aff #clean up for CPAM
                log = pd.concat([log,aff_df])
                

        log['Date'] = pd.to_datetime(log['Date'])
        log['Drop Number'] = log['Drop Number'].astype(int).astype(str)
        log['Hitpath Offer ID'] = log['Campaign ID - Strike'].str.extract(r'^(\d+)\s*-', expand=False)
        log['Hitpath Offer ID'] = pd.to_numeric(log['Hitpath Offer ID'], errors='coerce')
        
        log.drop('A',axis=1,inplace=True)

        log['Viper Execution Date'] = pd.to_datetime(log['Viper Execution Date'])
        
        def extract_pattern(text):
            if text is np.nan:
                return np.nan
            pattern_match = re.search(r'(46\d{4})', text)
            if pattern_match:
                matched_value = pattern_match.group(1)
                if matched_value=='460398':
                    if "FHA" in text:
                        return '160398'
                    elif "RF" in text:
                        return '260398'
                elif matched_value=='461128':
                    if "FHA" in text:
                        return '161128'
                    elif "RF" in text:
                        return '261128'
                else:
                    return matched_value
            else:
                return 'CPAM'

        # Create the new column using the custom function
        #         log['SC_DP&Pub'] = log['Segment - Strike'].apply(extract_pattern)        
        mask = (log['SC_DP&Pub'] == 'CPAM')
        log.loc[mask, 'SC_DP&Pub'] = log.loc[mask, 'Segment - Strike'].apply(extract_pattern)
        
        log = log[~log.duplicated(keep='first')]
        
        return log
    
    def find_ecpm_from_date(self,sc_dppub_affiliate,last_date_string,days_back=30,ema=False,metric='Revenue'):   
        lexi=self.lexi
        lexi_running = self.lexi_running
#         print(sc_dppub_affiliate)
        if type(last_date_string)==str:
            last_date = datetime.datetime.strptime(last_date_string, "%m/%d/%Y").date()
        else:
            last_date = last_date_string.date()
            
        if last_date > lexi_running.loc[sc_dppub_affiliate].index.max():
            last_date = lexi_running.loc[sc_dppub_affiliate].index.max()
            last_date_string = last_date.strftime('%m-%d-%Y')
            
        first_date = last_date + timedelta(days=-days_back)
        
        if first_date < lexi_running.loc[sc_dppub_affiliate].index.min():
            first_date = lexi_running.loc[sc_dppub_affiliate].index.min()
        first_date_string = first_date.strftime('%m-%d-%Y')
#         print(sc_dppub_affiliate,first_date,first_date_string)

        if ema==True:
            metric_col = f'Running EMA {metric}'
        else:
            metric_col = f'Running {metric}'
            
            
            
        metric1 = lexi_running.loc[sc_dppub_affiliate].loc[first_date_string][metric_col]
        metric2 = lexi_running.loc[sc_dppub_affiliate].loc[last_date_string][metric_col]
        del1 = lexi_running.loc[sc_dppub_affiliate].loc[first_date_string]['Running Delivered']
        del2 = lexi_running.loc[sc_dppub_affiliate].loc[last_date_string]['Running Delivered']
        
#         if metric=='Revenue':
#             factor = 1000
#         else:
#             factor = 1
        factor = 1000
            
        production_ecpm = ((metric2-metric1)*factor)/(del2-del1)
        return production_ecpm
                                    

    
    def check_for_drop_changes(self, sc_dppub_affiliate='all', update_cobra=False, log=None):
        cobra = self.cobra
        gc = self.gc
        
        #Make sure fvs is up to date
        self.affiliate_p_drops = {}  
        for index, row in self.aff_settings_frame.iterrows():
            self.affiliate_p_drops[row['Account']] = int(row['Production Drops per Day'])
            
        print('Checking for Accounts to Expand/Reduce Production Drops:')
        
        if log is None:
            log = self.load_log(sc_dppub_affiliate)
           
        if len(log) > 0:
            sc_counts = log[(log['Viper Execution Date'].dt.date>=date.today()+timedelta(days=-21))&(log['Viper Function Called']=='schedule_production_drops')].groupby(['SC_DP&Pub','Viper Execution Date','Selection Confidence']).agg({'Selection Confidence':'count'}).unstack(fill_value=0).rename(columns={'Selection Confidence':'Count'})
            sc_counts.columns = sc_counts.columns.to_flat_index()

            to_drop = [col for col in sc_counts.columns if 'tier' not in str(col).lower()]
            sc_counts.drop(to_drop, axis=1, inplace=True)

            all_cols_str = ' '.join([' ' .join(t) for t in sc_counts.columns]).lower()
            if 'one' not in all_cols_str:
                sc_counts['Tier One Count'] = 0
            if 'two' not in all_cols_str:
                sc_counts['Tier Two Count'] = 0
            if 'three' not in all_cols_str:
                sc_counts['Tier Three Count'] = 0

            for col in sc_counts.columns:
                if 'one' in str(col).lower():
                    sc_counts.rename(columns={col:'Tier One Count'},inplace=True)
                elif 'two' in str(col).lower():
                    sc_counts.rename(columns={col:'Tier Two Count'},inplace=True)
                if 'three' in str(col).lower():
                    sc_counts.rename(columns={col:'Tier Three Count'},inplace=True)


            sc_counts.reset_index(inplace=True)
            sc_counts = sc_counts.groupby('SC_DP&Pub').agg({'Viper Execution Date':'count','Tier One Count':'sum','Tier Two Count':'sum','Tier Three Count':'sum'})

            accounts_to_expand_drops = list(sc_counts[(sc_counts['Viper Execution Date']>1)&(sc_counts['Tier Two Count']==0)&(sc_counts['Tier Three Count']==0)&(sc_counts['Tier One Count']>=10)].index)
            accounts_to_reduce_drops = list(sc_counts[(sc_counts['Viper Execution Date']>1)&((sc_counts['Tier Two Count']>2)|(sc_counts['Tier Three Count']>2))&(sc_counts['Tier One Count']<10)].index)

            if (len(accounts_to_expand_drops)==0) and (len(accounts_to_reduce_drops))==0:
                print('No Suggestions Found for a Change in Drop Number!')

            to_modify = {}
            akshad_notice = 4

            for sc_dppub_affiliate in(accounts_to_expand_drops):
                current_drops = self.affiliate_p_drops[sc_dppub_affiliate]
                exec_dates = sc_counts.loc[sc_dppub_affiliate]['Viper Execution Date']
                tier_one_counts = sc_counts.loc[sc_dppub_affiliate]['Tier One Count']
                tier_two_counts = sc_counts.loc[sc_dppub_affiliate]['Tier Two Count']
                tier_three_counts = sc_counts.loc[sc_dppub_affiliate]['Tier Three Count']
                if current_drops<3:
                    boolean = input("\nWould you like to expand drops from {} to {} in {}?\n\tLast 3 Week Summary:\n\tViper Executions: {}\n\tTier Ones Scheduled: {}\n\tTier Twos Scheduled: {}\n\tTier Threes Scheduled: {}\n(yes/no)".format(current_drops, current_drops+1, sc_dppub_affiliate,exec_dates,tier_one_counts,tier_two_counts,tier_three_counts))

                    if boolean.lower()=="yes":
                        to_modify[sc_dppub_affiliate] = current_drops+1

            for sc_dppub_affiliate in(accounts_to_reduce_drops):
                current_drops = self.affiliate_p_drops[sc_dppub_affiliate]
                exec_dates = sc_counts.loc[sc_dppub_affiliate]['Viper Execution Date']
                tier_one_counts = sc_counts.loc[sc_dppub_affiliate]['Tier One Count']
                tier_two_counts = sc_counts.loc[sc_dppub_affiliate]['Tier Two Count']
                tier_three_counts = sc_counts.loc[sc_dppub_affiliate]['Tier Three Count']
                if current_drops>2:
                    boolean = input("\nWould you like to reduce drops from {} to {} in {}?\n\tLast 3 Week Summary:\n\tViper Executions: {}\n\tTier Ones Scheduled: {}\n\tTier Twos Scheduled: {}\n\tTier Threes Scheduled: {}\n(yes/no)".format(current_drops, current_drops-1, sc_dppub_affiliate,exec_dates,tier_one_counts,tier_two_counts,tier_three_counts))

                    if boolean.lower()=="yes":
                        to_modify[sc_dppub_affiliate] = current_drops-1

            if (len(to_modify)>0):

                cobra_sheet_df = self.cobra_sheet_df
                cobra_sheet = self.cobra_sheet

                if update_cobra:
#                     gc = pygsheets.authorize(service_account_file=filepath.service_account_location)
                    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/104t7KyOxfQvZH318MKxT0m7hMRRPvDMFVLIMd2YUVco/edit#gid=1354838607')

                    aff_settings = sh.worksheets(sheet_property='title',value='Affiliate Settings')[0]

                    for sc_dppub_affiliate,new_drops in to_modify.items():

                        #Update Viper Settings Sheet
                        aff_cell = aff_settings.find(sc_dppub_affiliate, matchCase=True, matchEntireCell=True, cols=(1,1))[0]
                        row = aff_cell.row
                        aff_settings.update_value('B{}'.format(row),new_drops)

                        if self.affiliate_p_drops[sc_dppub_affiliate] < new_drops: 

                            #ensure we don't schedule extra drops too soon for Akshad
                            skip_slots = cobra[(cobra['Dataset']==sc_dppub_affiliate)&(cobra['Date'].dt.date>=date.today())
              &(cobra['Date'].dt.date<=date.today()+timedelta(days=akshad_notice))&(cobra['Drop Number']==new_drops+1)&(cobra['Offer'].isna())].index
                            cobra.loc[skip_slots,'Offer'] = ' '



                        self.affiliate_p_drops[sc_dppub_affiliate] = new_drops

                else: #if only "mock scheduling" this will update for the current round of scheduling then reset each time
                    for sc_dppub_affiliate,new_drops in to_modify.items():

                        if self.affiliate_p_drops[sc_dppub_affiliate] < new_drops:
                            #ensure we don't schedule extra drops too soon for Akshad
                            skip_slots = cobra[(cobra['Dataset']==sc_dppub_affiliate)&(cobra['Date'].dt.date>=date.today())
              &(cobra['Date'].dt.date<=date.today()+timedelta(days=akshad_notice))&(cobra['Drop Number']==new_drops+1)&(cobra['Offer'].isna())].index
                            cobra.loc[skip_slots,'Offer'] = ' '

                        self.affiliate_p_drops[sc_dppub_affiliate] = new_drops
        else:
            print('New Account - No Drop Count Changes!')
            
        return cobra
    
    
    
    def return_prime_accounts(self, prime_fraction=0.4):
        #Return accounts that have at least "prime_fraction" of their revenue coming from prime verticals
        #This will likely return ~20 accounts
        
        lexi_prime = self.lexi.copy()
        prime_verticals = ['Home Financing','Home Improvement','Home Warranty','Home Sale']
        
        lexi_prime['Prime Offer'] = np.where(lexi_prime['Vertical'].isin(prime_verticals), 'Prime Offer', 'Regular Offer')
        
        #df will create percentage of revenue that is prime
        df = lexi_prime.groupby(['SC_DP&Pub','Prime Offer']).agg({'Revenue':'sum'}).reset_index()
        
        #df2 will create total amount of revenue that is prime.  at the moment it is not actually used
        df2 = lexi_prime[lexi_prime['Prime Offer']=='Prime Offer'].groupby(['SC_DP&Pub']).agg({'Revenue':'sum'}).sort_values(by='Revenue',ascending=False)
        
        pivot_df = df.pivot_table(index='SC_DP&Pub', columns='Prime Offer', values='Revenue', aggfunc='sum')
        pivot_df.columns = ['Prime Offer Revenue', 'Regular Offer Revenue']

        pivot_df['Prime Percent'] = pivot_df['Prime Offer Revenue'] / (pivot_df['Prime Offer Revenue']+pivot_df['Regular Offer Revenue'])
        pivot_df.sort_values(by='Prime Percent',ascending=False)
        
        all_primes = pd.merge(df2, pivot_df[['Prime Percent']], how='left', on='SC_DP&Pub', validate="1:1")
        primes = all_primes[all_primes['Prime Percent']>=prime_fraction]

        return primes.index.tolist()
                    

#     def load_log_mp(self,sc_dppub_affiliate='all'):
# #         print('load1')
#         gc = pygsheets.authorize(service_account_file=filepath.service_account_location)
#         sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1oKod0EKYfZmrS9MUtmftmEcv6WPESP5-qMJ6DBuJ4kU/edit#gid=0')
#         all_sheet_titles = [x.title for x in sh.worksheets()]
#         if sc_dppub_affiliate=='all':
#             sc_dppub_affiliate = all_sheet_titles

#         matrix = [ (tuple([aff]) + tuple([sh]) + tuple([all_sheet_titles]) ) for aff in sc_dppub_affiliate]
        
# #         def log_aff_to_df(aff, sh, all_sheet_titles):
# #             if __name__ == 'viper_main':
# #                 if aff in all_sheet_titles:
# #                     aff_df = sh.worksheets(sheet_property='title',value=aff)[0].get_as_df()
# #                     aff_df['SC_DP&Pub'] = aff 
# #                 else:
# #                     aff_df = sh.worksheets(sheet_property='title',value='Template')[0].get_as_df()
# #                     aff_df['SC_DP&Pub'] = aff 

# #             return aff_df

#         if __name__ == 'find_viper_settings':
            
#             pool = Pool()
#             results = pool.starmap(viper_main.log_aff_to_df, [(aff, sh, all_sheet_titles) for aff, sh, all_sheet_titles in matrix])
    

#         log = pd.concat(results)
#         log['Date'] = pd.to_datetime(log['Date'])
#         log['Drop Number'] = log['Drop Number'].astype(int).astype(str)
#         log['Hitpath Offer ID'] = log['Campaign ID - Strike'].str.extract(r'^(\d+)\s*-', expand=False)
#         log['Hitpath Offer ID'] = pd.to_numeric(log['Hitpath Offer ID'], errors='coerce')

#         log['Viper Execution Date'] = pd.to_datetime(log['Viper Execution Date'])
        
        
#         def extract_pattern(text):
#             pattern_match = re.search(r'(46\d{4})', text)
#             if pattern_match:
#                 matched_value = pattern_match.group(1)
#                 if matched_value=='460398':
#                     if "FHA" in text:
#                         return '160398'
#                     elif "RF" in text:
#                         return '260398'
#                 elif matched_value=='461128':
#                     if "FHA" in text:
#                         return '161128'
#                     elif "RF" in text:
#                         return '261128'
#                 else:
#                     return matched_value
#             else:
#                 return 'CPAM'

#         # Create the new column using the custom function
# #         log['SC_DP&Pub'] = log['Segment - Strike'].apply(extract_pattern)        
#         mask = (log['SC_DP&Pub'] == 'CPAM')
#         log.loc[mask, 'SC_DP&Pub'] = log.loc[mask, 'Segment - Strike'].apply(extract_pattern)

#         log = log[~log.duplicated(keep='first')]

#         return log  
    


    def clear_duplicates_from_log(self,sc_dppub_affiliate='all'):
        gc = self.gc
#         gc = pygsheets.authorize(service_account_file=filepath.service_account_location)
        sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1oKod0EKYfZmrS9MUtmftmEcv6WPESP5-qMJ6DBuJ4kU/edit#gid=0')
        log = pd.DataFrame({'A' : []})
        all_sheet_titles = [x.title for x in sh.worksheets()]

        if sc_dppub_affiliate=='all':
            sc_dppub_affiliate = all_sheet_titles

        for aff in sc_dppub_affiliate:
            if aff in all_sheet_titles:
                aff_df = sh.worksheets(sheet_property='title',value=aff)[0].get_as_df()
                aff_df['SC_DP&Pub'] = aff #clean up for CPAM
                log = pd.concat([log,aff_df])
            else:
                aff_df = sh.worksheets(sheet_property='title',value='Template')[0].get_as_df()
                aff_df['SC_DP&Pub'] = aff #clean up for CPAM
                log = pd.concat([log,aff_df])


        log['Date'] = pd.to_datetime(log['Date'])
        log['Drop Number'] = log['Drop Number'].astype(int).astype(str)
        log['Hitpath Offer ID'] = log['Offer - Strike'].str.extract(r'^(\d+)\s*-', expand=False)
        #log['Hitpath Offer ID'] = pd.to_numeric(log['Hitpath Offer ID'], errors='coerce')

        log.drop('A',axis=1,inplace=True)

        log['Viper Execution Date'] = pd.to_datetime(log['Viper Execution Date'])
        
        for aff in sc_dppub_affiliate:
            aff_df = sh.worksheets(sheet_property='title',value=aff)[0]
            df = log[(log['SC_DP&Pub']==aff)]
            duplicated_rows = (df[df.duplicated()].index+2).values
            print('Deleting',len(duplicated_rows),'from',aff)
            aff_delete_count = 0
            for row_num in duplicated_rows:
                row_to_delete = int(row_num) - aff_delete_count
        #         print(row_num,row_to_delete)
                aff_df.delete_rows(row_to_delete)
                aff_delete_count = aff_delete_count+1



    def clear_mismatches_from_log(self):
        gc = self.gc
#         gc = pygsheets.authorize(service_account_file=filepath.service_account_location)
        sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1oKod0EKYfZmrS9MUtmftmEcv6WPESP5-qMJ6DBuJ4kU/edit#gid=0')
        
        def extract_affiliate(text):
            if text is np.nan:
                return np.nan
        
            pattern_match = re.search(r'(46\d{4})', text)
            if pattern_match:
                matched_value = pattern_match.group(1)
                if matched_value=='460398':
                    if "FHA" in text:
                        return '160398'
                    elif "RF" in text:
                        return '260398'
                elif matched_value=='461128':
                    if "FHA" in text:
                        return '161128'
                    elif "RF" in text:
                        return '261128'
                else:
                    return matched_value
            else:
                return np.nan

        log = self.load_log()
        log['Extracted SC_DP'] = log['Segment - Strike'].split('_')[0] + '_'+ log['Segment - Strike'].split('_')[1]
        log['SC_DP']  = log['SC_DP&Pub'].strip()[:-7]
        log['Mismatch'] = (log['Extracted SC_DP'] != log['SC_DP']) & (~log['Extracted SC_DP'].isna())

        sc_dppub_affiliate = log[(log['Mismatch']==True)]['SC_DP&Pub'].unique()

        for aff in sc_dppub_affiliate:
            aff_df = sh.worksheets(sheet_property='title',value=aff)[0]
            df = log[(log['SC_DP&Pub']==aff)].reset_index()
            mismatch_rows = df[df['Mismatch']==True].index.values[::-1]+2

            print('Deleting',len(mismatch_rows),'rows from',aff)

            for row_num in mismatch_rows:
                row_to_delete = int(row_num)
                aff_df.delete_rows(row_to_delete)
                
                
    def load_log(self,sc_dppub_affiliate='all',download=True):
        
#         from google.oauth2.credentials import Credentials
        from oauth2client.service_account import ServiceAccountCredentials
        from googleapiclient.discovery import build
        import re 
        
        start = time.time()
        
        if download==True:
            print('Loading Viper Log:')

            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            
            if os.environ.get('DOCKER_SCRIPT_RUNNING'):
                drive_service = build('drive', 'v3', credentials=self.gc.oauth)
                viper_path = '/tmp/viper-log-download.xlsx'
            elif os.environ.get('DOCKER_RUNNING'):
                auth_path='host_data/' + filepath.service_account_location.split('/')[-1]
                credentials = ServiceAccountCredentials.from_json_keyfile_name(auth_path, scope)
                drive_service = build('drive', 'v3', credentials=credentials)
#                 viper_path = '/tmp/viper-log-download.xlsx'
            else:
                auth_path=filepath.service_account_location
                credentials = ServiceAccountCredentials.from_json_keyfile_name(auth_path, scope)
                drive_service = build('drive', 'v3', credentials=credentials)
                viper_path = '/tmp/viper-log-download.xlsx'

            spreadsheet_id = '1oKod0EKYfZmrS9MUtmftmEcv6WPESP5-qMJ6DBuJ4kU'

            # Download the spreadsheet as an Excel file
            request = drive_service.files().export(fileId=spreadsheet_id, mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response = request.execute()

            # Download locally
            with open(viper_path, 'wb') as f:
                f.write(response)

            print(f'Downloaded local copy in {round(time.time()-start,2)} seconds, now formatting to dataframe:')

        else:
            viper_path = 'viper-log-download.xlsx'
     
        if len(sc_dppub_affiliate)==1:
            try:
                log = pd.read_excel(viper_path, sheet_name=sc_dppub_affiliate[0])
                log['SC_DP&Pub'] = sc_dppub_affiliate[0]
            except:
                sc_dppub_affiliate='all'
        if len(sc_dppub_affiliate)!=1:
            excel_file = pd.ExcelFile(viper_path)
            if sc_dppub_affiliate == 'all':
                sheet_names = excel_file.sheet_names
            else:
                sheet_names = sc_dppub_affiliate

            dfs = []
            for sheet_name in sheet_names:
                df = excel_file.parse(sheet_name)
                if sheet_name != 'CPAM':
                    df['SC_DP&Pub'] = sheet_name
                dfs.append(df)

            log = pd.concat(dfs, ignore_index=True)


        log['SC_DP&Pub'] = log['SC_DP&Pub'].astype(str)
        log['Date'] = pd.to_datetime(log['Date'])
        log['Drop Number'] = log['Drop Number'].astype(int).astype(str)
        log['Hitpath Offer ID'] = log['Offer - Strike'].str.extract(r'^(\d+)\s*-', expand=False)
        log['Hitpath Offer ID'] = pd.to_numeric(log['Hitpath Offer ID'], errors='coerce')

        log['Viper Execution Date'] = pd.to_datetime(log['Viper Execution Date'])

        log = log[~log.duplicated(keep='first')]

        end = time.time()

        print(f"Viper Log ready after {round(end - start,2)} seconds")
        
        return log

                
                
    

