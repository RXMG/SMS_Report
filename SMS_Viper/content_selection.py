import numpy as np
import pandas as pd
from datetime import timedelta
from datetime import date
import sys
import viper_main
# del sys.modules["viper_main"]
# import viper_main
from os import listdir
import re
import infrastructure
import filepath 
from scipy.stats import percentileofscore

 
class inputs:
    def __init__(self, fvs=None, cobra=None, lexi=None, offers=None, emit = None):
#         self.lock = threading.Lock()
        self.fvs = fvs
#         print('cs access')
        self.cobra = cobra
        self.cobra_clean = cobra.copy()
#         self.cobra_sheet = cobra_sheet
#         self.cobra_sheet_df = cobra_sheet_df
        self.emit = emit
        self.offers = offers
        self.lexi = lexi
        #self.content_report = viper_main.get_content_report()

        #For Content Selection:
        lexi['Time Adjusted Revenue'] = np.maximum(-np.log(.005* (1+ (pd.Series(lexi['Date'].max() - lexi['Date'], index=lexi.index)).dt.days)), pd.Series(1, index=lexi.index))*lexi['Revenue']
        lexi['Creative'] = np.where(lexi['Creative Type']=='HTML','HTML','Custom Content')
        self.pubs_df = lexi.groupby(['Affiliate ID','Hitpath Offer ID','Creative']).agg({'Time Adjusted Revenue':'sum','Delivered':'sum','Date':'nunique'})
        self.pubs_df.rename(columns={'Date':'Drop Count'},inplace=True)
        self.pubs_df['eCPM'] = self.pubs_df['Time Adjusted Revenue']*1000 / self.pubs_df['Delivered']
        
        self.content_df, self.all_offers_df = get_content_df(lexi)
        
        self.cw = viper_main.load_el_nino()

    
    def test_creative_selection(self,pub,hitpath):
        creative_type = 'HTML'
        ccid = 'test'

        cobra = dfs.get_cobra()
        lexi = dfs.get_lexi()

        return creative_type,ccid

    def choose_html_or_cc(self,pub,hitpath,simple=False,print_bool=False):
        cobra = self.cobra
        lexi = self.lexi
        functions_dict = self.fvs.find_affiliate_functions(pub)

        # Return either 'HTML' or 'CC' for a given affiliate and offer

        # Methodology: (html weight for pub) = (html ecpm for pub) * (html drops for pub) 
        # (cc weight for pub) = (cc ecpm for pub) * (cc drops for pub) 
        # (html pub prob) = (html weight for pub) / ( (html weight for pub) + (cc weight for pub) )
        # repeat above to find (html non pub prob) for all other affiliates
        # weight (html pub prob) twice as much as (html non pub prob) for (final html probability)
        # make final decision by picking randomly html with (final html probability)

        #Determine html vs cc for affiliate
        pub_df = lexi[(lexi['Affiliate ID']==pub) & (lexi['Hitpath Offer ID']==hitpath)]

        pub_html_ecpm = pub_df[pub_df['Creative Type']=='HTML']['Revenue'].sum()*1000 / (pub_df[pub_df['Creative Type']=='HTML']['Delivered'].sum()+1)
        pub_cc_ecpm = pub_df[pub_df['Creative Type']!='HTML']['Revenue'].sum()*1000 / (pub_df[pub_df['Creative Type']!='HTML']['Delivered'].sum()+1)
        pub_html_drops = pub_df[pub_df['Creative Type']=='HTML']['Date'].nunique()
        pub_cc_drops = pub_df[pub_df['Creative Type']!='HTML']['Date'].nunique()

        pub_html_weight = pub_html_ecpm*pub_html_drops
        pub_cc_weight = pub_cc_ecpm*pub_cc_drops

        pub_html_prob = pub_html_weight / (pub_html_weight+pub_cc_weight)

        #Determine html vs cc for non affiliate
        non_pub_df = lexi[(lexi['Affiliate ID']!=pub) & (lexi['Hitpath Offer ID']==hitpath)]

        non_pub_html_ecpm = non_pub_df[non_pub_df['Creative Type']=='HTML']['Revenue'].sum()*1000 / (non_pub_df[non_pub_df['Creative Type']=='HTML']['Delivered'].sum()+1)
        non_pub_cc_ecpm = non_pub_df[non_pub_df['Creative Type']!='HTML']['Revenue'].sum()*1000 / (non_pub_df[non_pub_df['Creative Type']!='HTML']['Delivered'].sum()+1)
        non_pub_html_drops = non_pub_df[non_pub_df['Creative Type']=='HTML']['Date'].nunique()
        non_pub_cc_drops = non_pub_df[non_pub_df['Creative Type']!='HTML']['Date'].nunique()

        non_pub_html_weight = non_pub_html_ecpm*non_pub_html_drops
        non_pub_cc_weight = non_pub_cc_ecpm*non_pub_cc_drops

        non_pub_html_prob = non_pub_html_weight / (non_pub_html_weight+non_pub_cc_weight)

        final_html_prob = (pub_html_prob*2 + non_pub_html_prob)/3 #weight pub performance twice as much as all other affiliates
#         print(pub_html_prob,final_html_prob)
        try:
            creative_type = np.random.choice(['HTML', 'CC'],p=[final_html_prob, 1-final_html_prob])
        except:
            creative_type = 'HTML'

        if creative_type == 'CC':
#             try:
            if 'content_id_determination' in functions_dict.keys():
                chosen_content_selection_function = getattr(self,functions_dict['content_id_determination'])
            else:
                chosen_content_selection_function = getattr(self,'choose_ccid')
            ccid = chosen_content_selection_function(hitpath,pub)

#             except:
#                 if print_bool:
#                     print('There was an error in finding a content id, defaulting to HTML creative type')
#                 creative_type='HTML'
#                 ccid = ''
        else:
            ccid = ''
#         print('Old html prob =',final_html_prob)
        return creative_type,ccid

    def choose_creative(self,pub,hitpath,simple=False,print_bool=False,find_id=True,send_date=date.today()):
#         cobra = self.cobra
#         lexi = self.lexi
        functions_dict = self.fvs.find_affiliate_functions(pub)
#         pubs_df = self.pubs_df

        creative_type = self.lookup_creative_choice(pub,hitpath)
        
        if find_id:
            if creative_type == 'CC':
    #             try:
                if 'content_id_determination' in functions_dict.keys():
                    chosen_content_selection_function = getattr(self,functions_dict['content_id_determination'])
                else:
                    chosen_content_selection_function = getattr(self,'choose_ccid')
                ccid = chosen_content_selection_function(hitpath,pub)
                if ccid=='Change to HTML':
                    print('Potential Error finding a CCID - defaulting to HTML')
                    creative_type = 'HTML'
                    ccid = ''

    #             except:
    #                 print('There was an error in finding a content id, defaulting to HTML creative type')
    #                 creative_type='HTML'
    #                 ccid = ''
            else:
                ccid = ''
        else:
            ccid = ''
        
#         print('New html prob =',final_html_prob)
        return creative_type,ccid
    
    
    def choose_creative_old(self,pub,hitpath,simple=False,print_bool=False,find_id=True):
        cobra = self.cobra
        lexi = self.lexi
        functions_dict = self.fvs.find_affiliate_functions(pub)
        pubs_df = self.pubs_df
        
        
        non_pubs_df = pubs_df.drop(pub,level=0).reset_index().groupby(['Hitpath Offer ID','Creative']).agg({'Time Adjusted Revenue':'sum','Delivered':'sum','Drop Count':'sum'})
        non_pubs_df['eCPM'] = non_pubs_df['Time Adjusted Revenue']*1000 / non_pubs_df['Delivered']

        # Return either 'HTML' or 'CC' for a given affiliate and offer

        #Determine html vs cc for affiliate
#         pub_df = lexi[(lexi['Affiliate ID']==pub) & (lexi['Hitpath Offer ID']==hitpath)]


#         pub_html_ecpm = pub_df[pub_df['Creative Type']=='HTML']['Time Adjusted Revenue'].sum()*1000 / (pub_df[pub_df['Creative Type']=='HTML']['Delivered'].sum()+1)
#         pub_cc_ecpm = pub_df[pub_df['Creative Type']!='HTML']['Time Adjusted Revenue'].sum()*1000 / (pub_df[pub_df['Creative Type']!='HTML']['Delivered'].sum()+1)
        
        
#         pub_html_drops = pub_df[pub_df['Creative Type']=='HTML']['Date'].nunique()+1
#         pub_cc_drops = pub_df[pub_df['Creative Type']!='HTML']['Date'].nunique()+1
    
        try:
            pub_hitpath = pubs_df.loc[pub].loc[hitpath]
            
            pub_html_drops = pub_hitpath.loc['HTML']['Drop Count']
            pub_cc_drops = pub_hitpath.loc['Custom Content']['Drop Count']
        except:
            pub_html_drops = 0
            pub_cc_drops = 0


        if (pub_html_drops>0) and (pub_cc_drops>0):
            
            
            pub_html_ecpm = pub_hitpath.loc['HTML']['eCPM']
            pub_cc_ecpm = pub_hitpath.loc['Custom Content']['eCPM']
            
            pub_html_ecpm_pct = pub_html_ecpm / (pub_html_ecpm + pub_cc_ecpm)
            pub_cc_ecpm_pct = pub_cc_ecpm / (pub_html_ecpm + pub_cc_ecpm)

            pub_html_drop_pct = pub_html_drops / (pub_html_drops + pub_cc_drops)
            pub_cc_drop_pct = pub_cc_drops / (pub_html_drops + pub_cc_drops)

            pub_html_weight = (9*pub_html_ecpm_pct + pub_html_drop_pct)/10
            pub_cc_weight = (9*pub_cc_ecpm_pct + pub_cc_drop_pct)/10

            pub_html_prob = pub_html_weight / (pub_html_weight+pub_cc_weight)
            pub_weight = 1
            
        else:
            pub_html_prob = 0.5
            pub_weight = 0

        #Determine html vs cc for non affiliate
#         non_pub_df = lexi[(lexi['Affiliate ID']!=pub) & (lexi['Hitpath Offer ID']==hitpath)]
#         non_pub_html_ecpm = non_pub_df[non_pub_df['Creative Type']=='HTML']['Time Adjusted Revenue'].sum()*1000 / (non_pub_df[non_pub_df['Creative Type']=='HTML']['Delivered'].sum()+1)
#         non_pub_cc_ecpm = non_pub_df[non_pub_df['Creative Type']!='HTML']['Time Adjusted Revenue'].sum()*1000 / (non_pub_df[non_pub_df['Creative Type']!='HTML']['Delivered'].sum()+1)
#         non_pub_html_drops = non_pub_df[non_pub_df['Creative Type']=='HTML']['Date'].nunique()+1
#         non_pub_cc_drops = non_pub_df[non_pub_df['Creative Type']!='HTML']['Date'].nunique()+1
        
#         print(pub,hitpath)

        non_pub_hitpath = non_pubs_df.loc[hitpath]
        non_pub_html_ecpm = non_pub_hitpath['eCPM'].get('HTML',0)
        non_pub_cc_ecpm = non_pub_hitpath['eCPM'].get('Custom Content',0)
        non_pub_html_drops = non_pub_hitpath['Drop Count'].get('HTML',0)
        non_pub_cc_drops = non_pub_hitpath['Drop Count'].get('Custom Content',0)


        non_pub_html_ecpm_pct = non_pub_html_ecpm / (non_pub_html_ecpm + non_pub_cc_ecpm)
        non_pub_cc_ecpm_pct = non_pub_cc_ecpm / (non_pub_html_ecpm + non_pub_cc_ecpm)
        
        non_pub_html_drop_pct = non_pub_html_drops / (non_pub_html_drops + non_pub_cc_drops)
        non_pub_cc_drop_pct = non_pub_cc_drops / (non_pub_html_drops + non_pub_cc_drops)
        
        non_pub_html_weight = (4*non_pub_html_ecpm_pct*non_pub_html_drops + non_pub_html_drop_pct)/5
        non_pub_cc_weight = (4*non_pub_cc_ecpm_pct*non_pub_cc_drops + non_pub_cc_drop_pct)/5

        non_pub_html_prob = non_pub_html_weight / (non_pub_html_weight+non_pub_cc_weight)
    
        final_html_prob = (pub_weight*pub_html_prob*3 + non_pub_html_prob)/4 #weight pub performance more than all other affiliates
        if pub_weight==0:
            final_html_prob = non_pub_html_prob
            
#         print(pub_html_prob,non_pub_html_prob,final_html_prob)
    
        try:
            if simple: #take the more likely outcome (deterministic)
#                 print('Simple Creative Determination!')
                if final_html_prob>=0.5:
                    creative_type = 'HTML'
                else:
                    creative_type = 'CC'
            else:
                creative_type = np.random.choice(['HTML', 'CC'],p=[final_html_prob, 1-final_html_prob])
        except:
            creative_type = 'HTML'
            
            
        # Ensure that there is at least a 30% chance of the selected Creative Type being chosen; otherwise reverse
        if (creative_type=='HTML') & (final_html_prob<0.3):
            creative_type = 'CC'
        if (creative_type=='CC') & (final_html_prob>0.7):
            creative_type = 'HTML'
                

        if find_id:
            if creative_type == 'CC':
    #             try:
                chosen_content_selection_function = getattr(self,functions_dict['content_id_determination'])
                ccid = chosen_content_selection_function(hitpath,pub)
                if ccid=='Change to HTML':
                    creative_type = 'HTML'
                    ccid = ''

    #             except:
    #                 print('There was an error in finding a content id, defaulting to HTML creative type')
    #                 creative_type='HTML'
    #                 ccid = ''
            else:
                ccid = ''
        else:
            ccid = ''
        
#         print('New html prob =',final_html_prob)
        return creative_type,ccid





    def choose_creative_output_test(self,pub,hitpath,date,simple=False,print_bool=False,find_id=True):
        cobra = self.cobra
        lexi = self.lexi
        functions_dict = self.fvs.find_affiliate_functions(pub)
#         pubs_df = self.pubs_df
        
        lexi_at_time = lexi[lexi['Date']<=date]
        
#         non_pubs_df = pubs_df.drop(pub,level=0).reset_index().groupby(['Hitpath Offer ID','Creative']).agg({'Time Adjusted Revenue':'sum','Delivered':'sum','Drop Count':'sum'})
#         non_pubs_df['eCPM'] = non_pubs_df['Time Adjusted Revenue']*1000 / non_pubs_df['Delivered']

        # Return either 'HTML' or 'CC' for a given affiliate and offer

        #Determine html vs cc for affiliate
        pub_df = lexi_at_time[(lexi_at_time['Affiliate ID']==pub) & (lexi_at_time['Hitpath Offer ID']==hitpath)]


        pub_html_ecpm = pub_df[pub_df['Creative Type']=='HTML']['Time Adjusted Revenue'].sum()*1000 / (pub_df[pub_df['Creative Type']=='HTML']['Delivered'].sum()+1)
        pub_cc_ecpm = pub_df[pub_df['Creative Type']!='HTML']['Time Adjusted Revenue'].sum()*1000 / (pub_df[pub_df['Creative Type']!='HTML']['Delivered'].sum()+1)
        
        
        pub_html_drops = pub_df[pub_df['Creative Type']=='HTML']['Date'].nunique()+1
        pub_cc_drops = pub_df[pub_df['Creative Type']!='HTML']['Date'].nunique()+1
    
#         try:
#             pub_html_drops = pubs_df.loc[pub].loc[hitpath].loc['HTML']['Drop Count']
#             pub_cc_drops = pubs_df.loc[pub].loc[hitpath].loc['Custom Content']['Drop Count']
#         except:
#             pub_html_drops = 0
#             pub_cc_drops = 0


        if (pub_html_drops>0) and (pub_cc_drops>0):
            
#             pub_html_ecpm = pubs_df.loc[pub].loc[hitpath].loc['HTML']['eCPM']
#             pub_cc_ecpm = pubs_df.loc[pub].loc[hitpath].loc['Custom Content']['eCPM']
            
            pub_html_ecpm_pct = pub_html_ecpm / (pub_html_ecpm + pub_cc_ecpm)
            pub_cc_ecpm_pct = pub_cc_ecpm / (pub_html_ecpm + pub_cc_ecpm)

            pub_html_drop_pct = pub_html_drops / (pub_html_drops + pub_cc_drops)
            pub_cc_drop_pct = pub_cc_drops / (pub_html_drops + pub_cc_drops)

            pub_html_weight = (9*pub_html_ecpm_pct + pub_html_drop_pct)/10
            pub_cc_weight = (9*pub_cc_ecpm_pct + pub_cc_drop_pct)/10

            pub_html_prob = pub_html_weight / (pub_html_weight+pub_cc_weight)
            pub_weight = 1
            
        else:
            pub_html_prob = 0.5
            pub_weight = 0

        #Determine html vs cc for non affiliate
        non_pub_df = lexi_at_time[(lexi_at_time['Affiliate ID']!=pub) & (lexi_at_time['Hitpath Offer ID']==hitpath)]
        non_pub_html_ecpm = non_pub_df[non_pub_df['Creative Type']=='HTML']['Time Adjusted Revenue'].sum()*1000 / (non_pub_df[non_pub_df['Creative Type']=='HTML']['Delivered'].sum()+1)
        non_pub_cc_ecpm = non_pub_df[non_pub_df['Creative Type']!='HTML']['Time Adjusted Revenue'].sum()*1000 / (non_pub_df[non_pub_df['Creative Type']!='HTML']['Delivered'].sum()+1)
        non_pub_html_drops = non_pub_df[non_pub_df['Creative Type']=='HTML']['Date'].nunique()+1
        non_pub_cc_drops = non_pub_df[non_pub_df['Creative Type']!='HTML']['Date'].nunique()+1
        
#         print(pub,hitpath)
#         non_pub_html_ecpm = non_pubs_df.loc[hitpath]['eCPM'].get('HTML',0)
#         non_pub_cc_ecpm = non_pubs_df.loc[hitpath]['eCPM'].get('Custom Content',0)
#         non_pub_html_drops = non_pubs_df.loc[hitpath]['Drop Count'].get('HTML',0)
#         non_pub_cc_drops = non_pubs_df.loc[hitpath]['Drop Count'].get('Custom Content',0)


        non_pub_html_ecpm_pct = non_pub_html_ecpm / (non_pub_html_ecpm + non_pub_cc_ecpm)
        non_pub_cc_ecpm_pct = non_pub_cc_ecpm / (non_pub_html_ecpm + non_pub_cc_ecpm)
        
        non_pub_html_drop_pct = non_pub_html_drops / (non_pub_html_drops + non_pub_cc_drops)
        non_pub_cc_drop_pct = non_pub_cc_drops / (non_pub_html_drops + non_pub_cc_drops)
        
        non_pub_html_weight = (4*non_pub_html_ecpm_pct*non_pub_html_drops + non_pub_html_drop_pct)/5
        non_pub_cc_weight = (4*non_pub_cc_ecpm_pct*non_pub_cc_drops + non_pub_cc_drop_pct)/5
        
        non_pub_html_prob = non_pub_html_weight / (non_pub_html_weight+non_pub_cc_weight)
    
        final_html_prob = (pub_weight*pub_html_prob*3 + non_pub_html_prob)/4 #weight pub performance more than all other affiliates
        if pub_weight==0:
            final_html_prob = non_pub_html_prob
            
#         print(pub_html_prob,non_pub_html_prob,final_html_prob)
    
        try:
            if simple: #take the more likely outcome (deterministic)
#                 print('Simple Creative Determination!')
                if final_html_prob>=0.5:
                    creative_type = 'HTML'
                else:
                    creative_type = 'CC'
            else:
                creative_type = np.random.choice(['HTML', 'CC'],p=[final_html_prob, 1-final_html_prob])
        except:
            creative_type = 'HTML'

        if find_id:
            if creative_type == 'CC':
    #             try:
                chosen_content_selection_function = getattr(self,functions_dict['content_id_determination'])
                ccid = chosen_content_selection_function(hitpath,pub)
                if ccid=='Change to HTML':
                    creative_type = 'HTML'
                    ccid = ''

    #             except:
    #                 print('There was an error in finding a content id, defaulting to HTML creative type')
    #                 creative_type='HTML'
    #                 ccid = ''
            else:
                ccid = ''
        else:
            ccid = ''
        
#         print('New html prob =',final_html_prob)
        return creative_type,final_html_prob



    def choose_ccid(self,hitpath,affiliate,send_date=date.today()):
        #Get the top CCID from the Content Report for this affiliate that is not on Cobra in the last 7 days
        cobra = self.cobra
        lexi = self.lexi
#         print(hitpath,affiliate)

        content_report = self.content_report
        hitpath = str(hitpath) 

        offset = 0
        if re.search('[1-2]60398', affiliate):
            affiliate = '460398'
        elif re.search('[1-2]61128', affiliate):
            affiliate = '461128'
        
        try: 
            affiliate_customs = pd.read_excel(content_report, affiliate)
            affiliate_hitpath = affiliate_customs[hitpath].dropna()
        except: #in case affiliate does not appear on report
            affiliate_hitpath = []

            
        def find_any_affiliate_id():
            chosen_id = 'Custom Content'
            
            lexi_hitpath_cc = lexi[(lexi['Hitpath Offer ID']==int(float(hitpath))) & (lexi['Creative Type']!='HTML') 
                                   & ~(lexi['Creative Type'].str.startswith('300',na=False))
                                   & (lexi['Creative Type']!='Custom Content')]
            
            recent_id_drops = lexi_hitpath_cc[(lexi_hitpath_cc['Date']>=lexi_hitpath_cc['Date'].max() - timedelta(days=90))]
            
            attempts = 0
            while (chosen_id=='Custom Content') or (len(chosen_id)<2):
                attempts+=1
                if attempts>5:
                    return'Change to HTML'

                if len(recent_id_drops)>0:
                    chosen_id = recent_id_drops.sort_values(by='Payout eCPM')['Creative Type'].dropna()[:5].sample(1).values[0]
                else:
                    potential_ids = lexi_hitpath_cc.sort_values(by='Payout eCPM')['Creative Type'].dropna()[:5]
                    if len(potential_ids)==0:
                        chosen_id = 'Change to HTML'
                    else:
                        chosen_id = potential_ids.sample(1).values[0]
                if 'HTML' not in chosen_id:
                    if not self.check_ccid(hitpath,chosen_id,affiliate,send_date=send_date):
                        chosen_id='Custom Content'
                
            return chosen_id
         
        if len(affiliate_hitpath)==0:  #if the content scheduling report is empty for the affiliate then pick a random ccid from the top 5 across all affiliates (preferably) in the last 90 days
            return find_any_affiliate_id()
        
        else:
            chosen_id = affiliate_customs[hitpath].dropna().iloc[0] 
#             print(chosen_id)
            while not self.check_ccid(hitpath,chosen_id,affiliate,send_date=send_date):
                offset+=1
                if offset < len(affiliate_hitpath):
                    chosen_id = affiliate_customs[hitpath].iloc[offset]
                else:
                    return find_any_affiliate_id()
                
            if offset < len(affiliate_hitpath):
                return affiliate_hitpath.iloc[offset]
            else:
                chosen_id = 'Custom Content'
                lexi_hitpath_cc = lexi[(lexi['Hitpath Offer ID']==int(hitpath)) & (lexi['Creative Type']!='HTML')
                                      & ~(lexi['Creative Type'].str.startswith('300',na=False))]
                
                recent_id_drops = lexi_hitpath_cc[(lexi_hitpath_cc['Date']>=lexi_hitpath_cc['Date'].max() - timedelta(days=90))]
                
                attempts = 0
                while (chosen_id=='Custom Content') or (len(chosen_id)<2):
                    attempts+=1
                    if attempts>5:
                        return'Change to HTML'
                    if len(recent_id_drops)>0:
                        potential_ids = recent_id_drops.sort_values(by='Payout eCPM')['Creative Type'].dropna()[:5]
                        if len(potential_ids)==0:
                            chosen_id = 'Change to HTML'
                        else:
                            chosen_id = potential_ids.sample(1).values[0]
                    else:
                        chosen_id = lexi_hitpath_cc.sort_values(by='Payout eCPM')['Creative Type'].dropna()[:5].sample(1).values[0]
                
                    if not self.check_ccid(hitpath,chosen_id,affiliate,send_date=send_date):
                        chosen_id='Custom Content'
                    
                return chosen_id

    def check_ccid(self,hitpath,ccid,affiliate,send_date=date.today(),print_bool=False):
        offers = self.offers
        hitpath = int(float(hitpath))
        cobra = self.cobra
        cw = self.cw.set_index('Content ID')

        #Make sure ccid is not being sent to affiliate within 7 days
        if len(cobra[(cobra['Affiliate ID']==affiliate) & (cobra['Date'].dt.date<=date.today()+timedelta(days=7))
              & (cobra['Date'].dt.date>=date.today()-timedelta(days=7)) & (cobra['Send Strategy'].isin(['P','MA','E']))
             & ( (cobra['CC ID (mailers) / Reporting ID (Akshad)']==ccid) | (cobra['Body Content']==ccid  ))]) >0:
            if print_bool==True:
                print('Content Scheduled within 7 days')
            return False

        if (cw.loc[ccid]['Graveyard']!='No') | (not cw.loc[ccid]['Individual Content Status'] in (['Blanket Approval','Approved'])):
            if print_bool==True:
                print('Content in Graveyard')
            return False

        def is_in_season(text,send_date):
            if text is np.nan:
                return True
            if '-' not in text:
                return False
            pattern = r'^(.*?)-(.*)-(.*)$'
            replacement = r'\1-\2/\3'
            range_string = re.sub(pattern, replacement, text)
            range_string = range_string.split('-')
            start_date = range_string[0].strip().replace('/','-')
            end_date = range_string[1].strip().replace('/','-')
            start_date_month = start_date.split('-')[0]
            end_date_month = end_date.split('-')[0]
            if start_date_month < end_date_month:
                end_year = 2000
            else:
                end_year = 2001
            start_date = '2000-' + start_date
            end_date = f'{end_year}-' + range_string[1].strip().replace('/','-')
            date_range = pd.date_range(start=start_date, end=end_date, freq='D').strftime('%m-%d')
            return send_date.strftime('%m-%d') in date_range
        
        if not is_in_season(cw.loc[ccid]['Seasonal/Holiday Date Restrictions'],send_date):
            if print_bool==True:
                print('Content has Seasonal Restriction')
            return False

        if ccid is np.nan:
            return False
        if str(hitpath) in ccid:
            return True
        else:
            ps5786_hitpaths = [5786,8025]
            ps5300_hitpaths = [5297,5300,5301,5303,5304]
            ps7878_hitpaths = [7878,8131]
            if hitpath in ps5786_hitpaths:
                for hit in ps5786_hitpaths:
                    if str(hit) in ccid:
                        return True
            if hitpath in ps5300_hitpaths:
                for hit in ps5300_hitpaths:
                    if str(hit) in ccid:
                        return True
            if hitpath in ps7878_hitpaths:
                for hit in ps7878_hitpaths:
                    if str(hit) in ccid:
                        return True

        supe = offers.loc[hitpath]['Super Hitpath']
        dupes = offers[offers['Super Hitpath']==supe].index.tolist()
        for dupe in dupes:
            if str(dupe) in ccid:
                return True
        if 'Nepka' in offers.loc[int(hitpath)]['Advertiser Name']:
            if 'Nepka' in ccid:
                return True
        return False

    def choose_content(self,pub,hitpath,simple=False,print_bool=False,send_date=date.today()):
        cobra = self.cobra
        lexi = self.lexi
        offers = self.offers
        cw = self.cw
        html_nino = viper_main.get_htm_el_nino()

        print('')
        print('Selecting content for', hitpath)

        # Methodology:
        # If content has an eCPM at or above the aggregate for the offer in the affiliate or in the case
        # where it has not been dropped in the past 90 an eCPM at or above the aggregate in all other accounts
        # and the content has a median eCPM at or abovethe median eCPM for the offer in can be selected
        # It's selection is weighted as (2 * median eCPM + aggregate eCPM in affiliate) / 3 or
        # if not dropped in the affiliate in the past 90 days (2 * median eCPM + aggregate eCPM) / 3

        # Create dataframe of all drops of given hitpath in past 90 days and past year
        lexi_content = lexi[(lexi['Date'] > pd.Timestamp.today() - pd.to_timedelta(90, unit='d')) & (lexi['Hitpath Offer ID']==int(hitpath))]
        content_90_day = lexi_content[['Creative Group', 'Creative Type']].drop_duplicates()
        lexi_content_year = lexi[(lexi['Date'] > pd.Timestamp.today() - pd.to_timedelta(365, unit='d')) & (lexi['Hitpath Offer ID']==int(hitpath))]
        content_1_year = lexi_content_year[['Creative Group', 'Creative Type']].drop_duplicates()
        lexi_all = lexi[lexi['Hitpath Offer ID']==int(hitpath)]
        # Use 90 days data unless there is none, in that case use the 1 year data
        if lexi_content.empty:
            print('Not enough content for', hitpath, '. Using 1 year data.')
            lexi_content = lexi_content_year
        # If 90 days data has few than 20% of the content in the 1 year data, use the 1 year date except for Nepka offers
        if len(content_90_day) < 10 and len(content_90_day)/len(content_1_year)*100 < 20 and 'Nepka' not in str(lexi_content['Scheduling Name'].iloc[0]):
            print('Not enough content for', hitpath, '. Using 1 year data.')
            lexi_content = lexi_content_year

        html_count = len(lexi_content[lexi_content['Creative Group']=='HTML'])
        cc_count = len(lexi_content[lexi_content['Creative Group']=='CC'])
        if cc_count>0:
            html_percent = html_count / len(lexi_content)
        else:
            html_percent = 1

        cw = cw.dropna(subset=['OfferIDs'])
        if 'Nepka' in offers.loc[int(hitpath)]['Advertiser Name']:
            more_content = list(cw[cw['OfferIDs'].str.contains('Nepka')]['Content ID'])
            more_content = lexi[(lexi['Date'] > pd.Timestamp.today() - pd.to_timedelta(90, unit='d')) & (lexi['Creative Type'].isin(more_content))]
        else:
            more_content = list(cw[cw['OfferIDs'].str.contains(str(hitpath),na=False)]['Content ID'])
            more_content = lexi[(lexi['Date'] > pd.Timestamp.today() - pd.to_timedelta(90, unit='d')) & (lexi['Creative Type'].isin(more_content))]
        lexi_content = pd.concat([lexi_content,more_content]).drop_duplicates()

        lexi_content['Opp Cost eCPM'] = lexi_content['Opportunity Cost'] / lexi_content['Delivered'] * 1000

        lexi_content_by_drop = lexi_content.copy()
        lexi_content['Affiliate'] = lexi_content['Affiliate ID'] == pub
        offer_median_eCPM = lexi_content['Opp Cost eCPM'].median()

        lexi_content_drops = lexi[(lexi['Date'] > pd.Timestamp.today() - pd.to_timedelta(90, unit='d')) &
                                  (lexi['Creative Type'].isin(lexi_content['Creative Type']))]
        lexi_content_drops = lexi_content_drops.groupby(['Creative Type'], as_index=False).count()
        lexi_content_drops = lexi_content_drops[['Creative Type', 'Hitpath Offer ID']]
        lexi_content_drops.columns = ['Creative Type','Drops']

        lexi_content = lexi_content.groupby(['Affiliate','Creative Type','Hitpath Offer ID','Creative Group'], as_index=False).sum(numeric_only=True)

        lexi_content['Calculated Opp Cost eCPM'] = lexi_content['Opportunity Cost'] / lexi_content['Delivered'] * 1000
        lexi_content = lexi_content.merge(lexi_content_drops, on=['Creative Type'])

        # Get the drops for the affiliate account
        lexi_content_affliate = lexi_content[lexi_content['Affiliate']==True]
        lexi_content_affliate_eCPM = lexi_content_affliate['Opportunity Cost'].sum() / lexi_content_affliate['Delivered'].sum() * 1000
        lexi_content_affliate['Calculated Opp Cost eCPM'] = lexi_content_affliate['Opportunity Cost'] / lexi_content_affliate['Delivered'] * 1000
        lexi_content_affliate = lexi_content_affliate[['Affiliate','Creative Type','Calculated Opp Cost eCPM']]
        lexi_content_affliate['Meets Threshold'] =  lexi_content_affliate['Calculated Opp Cost eCPM'] >= lexi_content_affliate_eCPM

        # Get the drops for all other accounts
        lexi_content_other = lexi_content[lexi_content['Affiliate']==False]
        lexi_content_other_eCPM = lexi_content_other['Opportunity Cost'].sum() / lexi_content_other['Delivered'].sum() * 1000
        lexi_content_other = lexi_content_other[['Affiliate','Creative Type','Calculated Opp Cost eCPM','Drops','Creative Group']]
        lexi_content_other['Meets Threshold'] =  lexi_content_other['Calculated Opp Cost eCPM'] >= lexi_content_other_eCPM

        # Get date last dropped in affiliate account and whether it is in the past 30 days or not
        lexi_content_dates = lexi[(lexi['Date'] > pd.Timestamp.today() - pd.to_timedelta(90, unit='d')) & \
                                  (lexi['Hitpath Offer ID']==int(hitpath)) & \
                                  (lexi['Affiliate ID'] == pub)]
        lexi_content_dates = lexi_content_dates[['Creative Group','Creative Type','Date']]
        lexi_content_dates = lexi_content_dates.groupby(['Creative Type','Creative Group'],as_index=False).max()
        cobra_dates = cobra[(cobra['Hitpath Offer ID']==hitpath) & (cobra['Affiliate ID']==pub)][['Date','CC ID (mailers) / Reporting ID (Akshad)']]
        cobra_dates.columns = ['Date','Creative Type']
        lexi_content_dates = pd.concat([lexi_content_dates,cobra_dates],ignore_index=True).drop_duplicates()
        lexi_content_dates['Recently Dropped'] = lexi_content_dates['Date'] > pd.Timestamp.today() - pd.to_timedelta(30, unit='d')

        # Merge the above dataframes together
        lexi_content = lexi_content_other.merge(lexi_content_affliate, how='outer', on='Creative Type').merge(lexi_content_dates, how='outer', on='Creative Type')
        lexi_content['Meets Threshold'] = (lexi_content['Meets Threshold_y'] == True) | ((lexi_content['Meets Threshold_y'].isnull()) & (lexi_content['Meets Threshold_x'] == True))
        rank_threshold = (lexi_content_other_eCPM + offer_median_eCPM * 2) / 3
        if print_bool==True:
            print('Rank Threshold: ', round(rank_threshold,2))
            print('Offer eCPM for Other Accounts: ', round(lexi_content_other_eCPM,2))
            print('Offer eCPM for Affiliate Account: ', round(lexi_content_affliate_eCPM,2))
            print('Offer median eCPM: ',round(offer_median_eCPM,2))
        lexi_content = lexi_content[['Creative Type', 'Creative Group_x', 'Calculated Opp Cost eCPM_x', 'Drops',
                                     'Calculated Opp Cost eCPM_y', 'Date', 'Recently Dropped', 'Meets Threshold']]
        lexi_content.columns = ['Creative Type','Creative Group','eCPM in Other Accounts','Drops',
                                'eCPM in Affiliate Account','Date','Recently Dropped',
                                'Meets Mean Threshold']

        # check if the content has a median eCPM higher than the offer median eCPM
        lexi_content_by_drop = lexi_content_by_drop.groupby(['Creative Type'], as_index=False)['Opp Cost eCPM'].median()
        lexi_content = lexi_content.merge(lexi_content_by_drop, how='left', on='Creative Type')
        lexi_content = lexi_content.rename(columns = {'Opp Cost eCPM':'median eCPM'})
        lexi_content['Meets Median Threshold'] = lexi_content['median eCPM'] >= offer_median_eCPM
        lexi_content.loc[~lexi_content['eCPM in Affiliate Account'].isna(),'Meets Median Threshold'] = True

        # check if the content has an aggregate eCPM in the affiliate account higher than the offer aggregate eCPM in the affiliate account
        lexi_content.loc[lexi_content['eCPM in Affiliate Account'] >= lexi_content_affliate_eCPM,'Meets Median Threshold'] = True

        # rank is calculated as the average of the aggregate eCPM of the content piece and two times the median eCPM of the content piece
        lexi_content['rank'] = (lexi_content['eCPM in Other Accounts'] + 2 * lexi_content['median eCPM']) / 3
        # if the content piece has been dropped in the affiliate account in the past 90 days, that aggregate eCPM will be used in the ranking instead of the aggregate for all accounts
        lexi_content.loc[~lexi_content['eCPM in Affiliate Account'].isna(),['rank']] = lexi_content['eCPM in Affiliate Account']
        lexi_content = lexi_content.sort_values('rank', ascending=False)

        html_nino = html_nino.iloc[:, :5]
        html_nino.rename(columns={'Reporting ID':'Creative Type'},inplace=True)
        html_nino = html_nino[['Hitpath Offer ID','Creative Type','HTML Status']].astype(str)

        all_content = list(lexi_all[lexi_all['Creative Group']=='HTML']['Creative Type'].unique())
        new_html = html_nino[(html_nino['Hitpath Offer ID']==str(hitpath)) & (html_nino['HTML Status']!='Paused')]
        new_html = new_html[~new_html['Creative Type'].isin(all_content)]
        new_html['Creative Group'] = 'HTML'
        new_html[['Drops', 'eCPM in Affiliate Account','Drops in Affiliate Accounts', 'Date', 'Recently Dropped']] = np.nan
        new_html['Date'] = pd.to_datetime(new_html['Date'])
        new_html['Recently Dropped'] = new_html['Recently Dropped'].astype(str)
        new_html['eCPM in Other Accounts'] = lexi_content_other_eCPM
        new_html['median eCPM'] = offer_median_eCPM
        new_html['Meets Mean Threshold'] = True
        new_html['Meets Median Threshold'] = True
        new_html['rank'] = (new_html['eCPM in Other Accounts'] + 2 * new_html['median eCPM']) / 3
        new_html['HTML Status'] = 'New'
        new_html = new_html[['Creative Type', 'Creative Group', 'eCPM in Other Accounts', 'Drops', 'eCPM in Affiliate Account', 'Date', 'Recently Dropped', 'Meets Mean Threshold', 'median eCPM', 'Meets Median Threshold', 'rank', 'HTML Status']]

        lexi_content = lexi_content.merge(html_nino[['Creative Type','HTML Status']],on='Creative Type',how='left')
        lexi_content = lexi_content.dropna(subset=['Creative Type'])
        lexi_content = lexi_content[lexi_content['HTML Status']!='Paused']
        lexi_content['Meets Drop Threshold'] = ((lexi_content['Drops']>2)) | (~lexi_content['eCPM in Affiliate Account'].isna()) | (lexi_content['Creative Group']=='HTML')

        check_dict = dict()
        for ccid in lexi_content[lexi_content['Creative Group']=='CC']['Creative Type'].unique():
            #check_dict[ccid] = cs.check_ccid(hitpath,ccid,pub,send_date)
            try:
                check_dict[ccid] = self.check_ccid(hitpath,ccid,pub,send_date)
            except:
                print('Could not check', ccid, ', removing from selection')
                check_dict[ccid] = False
        lexi_content['Check'] = lexi_content['Creative Type'].replace(check_dict)
        lexi_content = lexi_content[(lexi_content['Creative Group']=='HTML') | (lexi_content['Check']==True)]

        lexi_content['Meets Rank Threshold'] = lexi_content['rank'] >= rank_threshold
        lexi_content = lexi_content.sort_values(['rank'],ascending=False).reset_index()
        lexi_content.loc[~lexi_content['eCPM in Affiliate Account'].isna(),'Meets Rank Threshold'] = lexi_content['eCPM in Affiliate Account'] >= lexi_content_affliate_eCPM
        lexi_content_all = lexi_content.copy()
        lexi_content = lexi_content[lexi_content['Meets Drop Threshold']==True]
        lexi_content = lexi_content[lexi_content['Meets Rank Threshold']==True].sort_values('rank', ascending=False)

        # if no content meets threshold, remove drop number requirement
        if lexi_content.empty:
            lexi_content = lexi_content_all.copy()
            lexi_content = lexi_content[lexi_content['Meets Rank Threshold']==True].sort_values('rank', ascending=False)

        # if after remove number of drop restriction no content meets threshold, reduce mean and median threshold until content is found
        k=0
        while lexi_content.empty:
            k = k + 1
            print('No offers, decreasing thresholds.')
            lexi_content = lexi_content_all.copy()
            lexi_content.loc[lexi_content['eCPM in Affiliate Account'].isna(),['Meets Rank Threshold']] =  lexi_content['rank'] >= rank_threshold - k * 0.1
            lexi_content.loc[~lexi_content['eCPM in Affiliate Account'].isna(),['Meets Rank Threshold']] =  lexi_content['eCPM in Affiliate Account'] >= lexi_content_affliate_eCPM - k * 0.1
            lexi_content = lexi_content[lexi_content['Meets Rank Threshold']==True].sort_values('rank', ascending=False)
            if k > 20:
                lexi_content = lexi_content_all.copy()
                lexi_content = lexi_content.sort_values('rank', ascending=False)
                break

        if lexi_content.empty:
            print('No offers, selecting top 3.')
            lexi_content = lexi_content_all.sort_values(['rank'], ascending=False).iloc[0:3]


        lexi_content.dropna(subset=['Creative Type'],inplace=True)
        lexi_content = lexi_content.reset_index(drop=True)
        if print_bool==True:
            print(lexi_content[['Creative Type', 'eCPM in Other Accounts', 'Drops', 'eCPM in Affiliate Account', 'Recently Dropped', 'median eCPM', 'Meets Mean Threshold', 'Meets Median Threshold', 'rank', 'Meets Drop Threshold', 'Meets Rank Threshold','HTML Status']].sort_values(['rank'],ascending=False))

        # Select a random content piece that has not dropped in the past 30 days
        content = lexi_content[lexi_content['Recently Dropped']!=True]

        # if content is empty and the offer send at least 10% HTML add in the new HTML
        if content.empty and html_percent >= 0.05:
            if print_bool == True:
                print(round(html_percent*100), '% HTML, adding',len(new_html), 'in new html')
            content = new_html

        # Content selection will be weighted by the rank or 4 times the rank if opp cost is positive
        content.loc[content['rank']>0,'rank'] = content['rank'] * 4

        # if content is still empty select the one least recently dropped if all have been dropped in the past 30 days
        if content.empty:
            content = lexi_content.copy()
            selected_creative = content[content['Date'] == content['Date'].min()][['Creative Type','Creative Group']]
        else:
            selected_creative = content[['Creative Type','Creative Group']].sample(n=1,weights=pd.to_numeric(content['rank']) + abs(2*min(pd.to_numeric(content['rank'])))+1)

        ccid = selected_creative['Creative Type'].iloc[0]
        if selected_creative['Creative Group'].iloc[0] in ['HTML','CC']:
            creative_type = selected_creative['Creative Group'].iloc[0]
        elif ccid=='HTML' or ccid.isdigit():
            creative_type = 'HTML'
        else:
            creative_type = 'CC'

        if ccid=='HTML':
            creative_type = ''

        if ccid in list(new_html['Creative Type']):
            print('     New html selected', ccid)
        else:
            print('     ',creative_type, ccid, 'ranked', lexi_content.index[lexi_content['Creative Type'] == ccid][0] + 1, 'out of', len(lexi_content), 'pieces that meet thresholds')

        if ccid=='HTML':
            creative_type = 'HTML'
            ccid = ''

        return creative_type,ccid

    def choose_content_id(self,pub,hitpath,simple=False,print_bool=False,send_date=date.today()):
        cobra = self.cobra
        lexi = self.lexi
        offers = self.offers
        cw = self.cw
        html_nino = viper_main.get_htm_el_nino()

        print_bool=False

        print('')
        print('Selecting content for', hitpath)

        # Methodology:
        # If content has an eCPM at or above the aggregate for the offer in the affiliate or in the case
        # where it has not been dropped in the past 90 an eCPM at or above the aggregate in all other accounts
        # and the content has a median eCPM at or abovethe median eCPM for the offer in can be selected
        # It's selection is weighted as (2 * median eCPM + aggregate eCPM in affiliate) / 3 or
        # if not dropped in the affiliate in the past 90 days (2 * median eCPM + aggregate eCPM) / 3

        # Create dataframe of all drops of given hitpath in past 90 days and past year
        lexi_content = lexi[(lexi['Date'] > pd.Timestamp.today() - pd.to_timedelta(90, unit='d')) & (lexi['Hitpath Offer ID']==int(hitpath))]
        content_90_day = lexi_content[['Creative Group', 'Creative Type']].drop_duplicates()
        lexi_content_year = lexi[(lexi['Date'] > pd.Timestamp.today() - pd.to_timedelta(365, unit='d')) & (lexi['Hitpath Offer ID']==int(hitpath))]
        content_1_year = lexi_content_year[['Creative Group', 'Creative Type']].drop_duplicates()
        lexi_all = lexi[lexi['Hitpath Offer ID']==int(hitpath)]
        # Use 90 days data unless there is none, in that case use the 1 year data
        if lexi_content.empty:
            print('Not enough content for', hitpath, '. Using 1 year data.')
            lexi_content = lexi_content_year
        # If 90 days data has few than 20% of the content in the 1 year data, use the 1 year date except for Nepka offers
        if len(content_90_day) < 10 and len(content_90_day)/len(content_1_year)*100 < 20 and 'Nepka' not in str(lexi_content['Scheduling Name'].iloc[0]):
            print('Not enough content for', hitpath, '. Using 1 year data.')
            lexi_content = lexi_content_year

        html_count = len(lexi_content[lexi_content['Creative Group']=='HTML'])
        cc_count = len(lexi_content[lexi_content['Creative Group']=='CC'])
        if cc_count>0:
            html_percent = html_count / len(lexi_content)
        else:
            html_percent = 1

        cw = cw.dropna(subset=['OfferIDs'])
        if 'Nepka' in offers.loc[int(hitpath)]['Advertiser Name']:
            more_content = list(cw[cw['OfferIDs'].str.contains('Nepka')]['Content ID'])
            more_content = lexi[(lexi['Date'] > pd.Timestamp.today() - pd.to_timedelta(90, unit='d')) & (lexi['Creative Type'].isin(more_content))]
        else:
            more_content = list(cw[cw['OfferIDs'].str.contains(str(hitpath),na=False)]['Content ID'])
            more_content = lexi[(lexi['Date'] > pd.Timestamp.today() - pd.to_timedelta(90, unit='d')) & (lexi['Creative Type'].isin(more_content))]
        lexi_content = pd.concat([lexi_content,more_content]).drop_duplicates()

        lexi_content['Opp Cost eCPM'] = lexi_content['Opportunity Cost'] / lexi_content['Delivered'] * 1000

        lexi_content_by_drop = lexi_content.copy()
        lexi_content['Affiliate'] = lexi_content['Affiliate ID'] == pub
        offer_median_eCPM = lexi_content['Opp Cost eCPM'].median()

        lexi_content_drops = lexi[(lexi['Date'] > pd.Timestamp.today() - pd.to_timedelta(90, unit='d')) &
                                  (lexi['Creative Type'].isin(lexi_content['Creative Type']))]
        lexi_content_drops = lexi_content_drops.groupby(['Creative Type'], as_index=False).count()
        lexi_content_drops = lexi_content_drops[['Creative Type', 'Hitpath Offer ID']]
        lexi_content_drops.columns = ['Creative Type','Drops']

        lexi_content = lexi_content.groupby(['Affiliate','Creative Type','Hitpath Offer ID','Creative Group'], as_index=False).sum(numeric_only=True)

        lexi_content['Calculated Opp Cost eCPM'] = lexi_content['Opportunity Cost'] / lexi_content['Delivered'] * 1000
        lexi_content = lexi_content.merge(lexi_content_drops, on=['Creative Type'])

        # Get the drops for the affiliate account
        lexi_content_affliate = lexi_content[lexi_content['Affiliate']==True]
        lexi_content_affliate_eCPM = lexi_content_affliate['Opportunity Cost'].sum() / lexi_content_affliate['Delivered'].sum() * 1000
        lexi_content_affliate['Calculated Opp Cost eCPM'] = lexi_content_affliate['Opportunity Cost'] / lexi_content_affliate['Delivered'] * 1000
        lexi_content_affliate = lexi_content_affliate[['Affiliate','Creative Type','Calculated Opp Cost eCPM']]
        lexi_content_affliate['Meets Threshold'] =  lexi_content_affliate['Calculated Opp Cost eCPM'] >= lexi_content_affliate_eCPM

        # Get the drops for all other accounts
        lexi_content_other = lexi_content[lexi_content['Affiliate']==False]
        lexi_content_other_eCPM = lexi_content_other['Opportunity Cost'].sum() / lexi_content_other['Delivered'].sum() * 1000
        lexi_content_other = lexi_content_other[['Affiliate','Creative Type','Calculated Opp Cost eCPM','Drops','Creative Group']]
        lexi_content_other['Meets Threshold'] =  lexi_content_other['Calculated Opp Cost eCPM'] >= lexi_content_other_eCPM

        # Get date last dropped in affiliate account and whether it is in the past 30 days or not
        lexi_content_dates = lexi[(lexi['Date'] > pd.Timestamp.today() - pd.to_timedelta(90, unit='d')) & \
                                  (lexi['Hitpath Offer ID']==int(hitpath)) & \
                                  (lexi['Affiliate ID'] == pub)]
        lexi_content_dates = lexi_content_dates[['Creative Group','Creative Type','Date']]
        lexi_content_dates = lexi_content_dates.groupby(['Creative Type','Creative Group'],as_index=False).max()
        cobra_dates = cobra[(cobra['Hitpath Offer ID']==hitpath) & (cobra['Affiliate ID']==pub)][['Date','CC ID (mailers) / Reporting ID (Akshad)']]
        cobra_dates.columns = ['Date','Creative Type']
        lexi_content_dates = pd.concat([lexi_content_dates,cobra_dates],ignore_index=True).drop_duplicates()
        lexi_content_dates['Recently Dropped'] = lexi_content_dates['Date'] > pd.Timestamp.today() - pd.to_timedelta(30, unit='d')

        # Merge the above dataframes together
        lexi_content = lexi_content_other.merge(lexi_content_affliate, how='outer', on='Creative Type').merge(lexi_content_dates, how='outer', on='Creative Type')
        lexi_content['Meets Threshold'] = (lexi_content['Meets Threshold_y'] == True) | ((lexi_content['Meets Threshold_y'].isnull()) & (lexi_content['Meets Threshold_x'] == True))
        rank_threshold = (lexi_content_other_eCPM + offer_median_eCPM * 2) / 3
        if print_bool==True:
            print('Rank Threshold: ', round(rank_threshold,2))
            print('Offer eCPM for Other Accounts: ', round(lexi_content_other_eCPM,2))
            print('Offer eCPM for Affiliate Account: ', round(lexi_content_affliate_eCPM,2))
            print('Offer median eCPM: ',round(offer_median_eCPM,2))
        lexi_content = lexi_content[['Creative Type', 'Creative Group_x', 'Calculated Opp Cost eCPM_x', 'Drops',
                                     'Calculated Opp Cost eCPM_y', 'Date', 'Recently Dropped', 'Meets Threshold']]
        lexi_content.columns = ['Creative Type','Creative Group','eCPM in Other Accounts','Drops',
                                'eCPM in Affiliate Account','Date','Recently Dropped',
                                'Meets Mean Threshold']

        # check if the content has a median eCPM higher than the offer median eCPM
        lexi_content_by_drop = lexi_content_by_drop.groupby(['Creative Type'], as_index=False)['Opp Cost eCPM'].median()
        lexi_content = lexi_content.merge(lexi_content_by_drop, how='left', on='Creative Type')
        lexi_content = lexi_content.rename(columns = {'Opp Cost eCPM':'median eCPM'})
        lexi_content['Meets Median Threshold'] = lexi_content['median eCPM'] >= offer_median_eCPM
        lexi_content.loc[~lexi_content['eCPM in Affiliate Account'].isna(),'Meets Median Threshold'] = True

        # check if the content has an aggregate eCPM in the affiliate account higher than the offer aggregate eCPM in the affiliate account
        lexi_content.loc[lexi_content['eCPM in Affiliate Account'] >= lexi_content_affliate_eCPM,'Meets Median Threshold'] = True

        # rank is calculated as the average of the aggregate eCPM of the content piece and two times the median eCPM of the content piece
        lexi_content['rank'] = (lexi_content['eCPM in Other Accounts'] + 2 * lexi_content['median eCPM']) / 3
        # if the content piece has been dropped in the affiliate account in the past 90 days, that aggregate eCPM will be used in the ranking instead of the aggregate for all accounts
        lexi_content.loc[~lexi_content['eCPM in Affiliate Account'].isna(),['rank']] = lexi_content['eCPM in Affiliate Account']
        lexi_content = lexi_content.sort_values('rank', ascending=False)

        html_nino = html_nino.iloc[:, :5]
        html_nino.rename(columns={'Reporting ID':'Creative Type'},inplace=True)
        html_nino = html_nino[['Hitpath Offer ID','Creative Type','HTML Status']].astype(str)

        all_content = list(lexi_all[lexi_all['Creative Group']=='HTML']['Creative Type'].unique())
        new_html = html_nino[(html_nino['Hitpath Offer ID']==str(hitpath)) & (html_nino['HTML Status']!='Paused')]
        new_html = new_html[~new_html['Creative Type'].isin(all_content)]
        new_html['Creative Group'] = 'HTML'
        new_html[['Drops', 'eCPM in Affiliate Account','Drops in Affiliate Accounts', 'Date', 'Recently Dropped']] = np.nan
        new_html['Date'] = pd.to_datetime(new_html['Date'])
        new_html['Recently Dropped'] = new_html['Recently Dropped'].astype(str)
        new_html['eCPM in Other Accounts'] = lexi_content_other_eCPM
        new_html['median eCPM'] = offer_median_eCPM
        new_html['Meets Mean Threshold'] = True
        new_html['Meets Median Threshold'] = True
        new_html['rank'] = (new_html['eCPM in Other Accounts'] + 2 * new_html['median eCPM']) / 3
        new_html['HTML Status'] = 'New'
        new_html = new_html[['Creative Type', 'Creative Group', 'eCPM in Other Accounts', 'Drops', 'eCPM in Affiliate Account', 'Date', 'Recently Dropped', 'Meets Mean Threshold', 'median eCPM', 'Meets Median Threshold', 'rank', 'HTML Status']]

        lexi_content = lexi_content.merge(html_nino[['Creative Type','HTML Status']],on='Creative Type',how='left')
        lexi_content = lexi_content.dropna(subset=['Creative Type'])
        lexi_content = lexi_content[lexi_content['HTML Status']!='Paused']
        lexi_content['Meets Drop Threshold'] = ((lexi_content['Drops']>2)) | (~lexi_content['eCPM in Affiliate Account'].isna()) | (lexi_content['Creative Group']=='HTML')

        check_dict = dict()
        for ccid in lexi_content[lexi_content['Creative Group']=='CC']['Creative Type'].unique():
            #check_dict[ccid] = cs.check_ccid(hitpath,ccid,pub,send_date)
            try:
                check_dict[ccid] = self.check_ccid(hitpath,ccid,pub,send_date)
            except:
                print('Could not check', ccid, ', removing from selection')
                check_dict[ccid] = False
        lexi_content['Check'] = lexi_content['Creative Type'].replace(check_dict)
        lexi_content = lexi_content[(lexi_content['Creative Group']=='HTML') | (lexi_content['Check']==True)]

        lexi_content['Meets Rank Threshold'] = lexi_content['rank'] >= rank_threshold
        lexi_content = lexi_content.sort_values(['rank'],ascending=False).reset_index()
        lexi_content.loc[~lexi_content['eCPM in Affiliate Account'].isna(),'Meets Rank Threshold'] = lexi_content['eCPM in Affiliate Account'] >= lexi_content_affliate_eCPM
        lexi_content_all = lexi_content.copy()
        lexi_content = lexi_content[lexi_content['Meets Drop Threshold']==True]
        lexi_content = lexi_content[lexi_content['Meets Rank Threshold']==True].sort_values('rank', ascending=False)

        # if no content meets threshold, remove drop number requirement
        if lexi_content.empty:
            lexi_content = lexi_content_all.copy()
            lexi_content = lexi_content[lexi_content['Meets Rank Threshold']==True].sort_values('rank', ascending=False)

        # if after remove number of drop restriction no content meets threshold, reduce mean and median threshold until content is found
        k=0
        while lexi_content.empty:
            k = k + 1
            print('No offers, decreasing thresholds.')
            lexi_content = lexi_content_all.copy()
            lexi_content.loc[lexi_content['eCPM in Affiliate Account'].isna(),['Meets Rank Threshold']] =  lexi_content['rank'] >= rank_threshold - k * 0.1
            lexi_content.loc[~lexi_content['eCPM in Affiliate Account'].isna(),['Meets Rank Threshold']] =  lexi_content['eCPM in Affiliate Account'] >= lexi_content_affliate_eCPM - k * 0.1
            lexi_content = lexi_content[lexi_content['Meets Rank Threshold']==True].sort_values('rank', ascending=False)
            if k > 20:
                lexi_content = lexi_content_all.copy()
                lexi_content = lexi_content.sort_values('rank', ascending=False)
                break

        if lexi_content.empty:
            print('No offers, selecting top 3.')
            lexi_content = lexi_content_all.sort_values(['rank'], ascending=False).iloc[0:3]


        lexi_content.dropna(subset=['Creative Type'],inplace=True)
        lexi_content = lexi_content[lexi_content['Creative Group']=='HTML']
        lexi_content = lexi_content.reset_index(drop=True)
        if print_bool==True:
            print(lexi_content[['Creative Type', 'eCPM in Other Accounts', 'Drops', 'eCPM in Affiliate Account', 'Recently Dropped', 'median eCPM', 'Meets Mean Threshold', 'Meets Median Threshold', 'rank', 'Meets Drop Threshold', 'Meets Rank Threshold','HTML Status']].sort_values(['rank'],ascending=False))


        # Select a random content piece that has not dropped in the past 30 days
        content = lexi_content[lexi_content['Recently Dropped']!=True]

        # if content is empty and the offer send at least 10% HTML add in the new HTML
        if content.empty and html_percent >= 0.05:
            if print_bool == True:
                print(round(html_percent*100), '% HTML, adding',len(new_html), 'in new html')
            content = new_html

        # Content selection will be weighted by the rank or 4 times the rank if opp cost is positive
        content.loc[content['rank']>0,'rank'] = content['rank'] * 4

        # if content is still empty select the one least recently dropped if all have been dropped in the past 30 days
        if content.empty:
            content = lexi_content.copy()
            selected_creative = content[content['Date'] == content['Date'].min()][['Creative Type','Creative Group']]
        else:
            selected_creative = content[['Creative Type','Creative Group']].sample(n=1,weights=pd.to_numeric(content['rank']) + abs(2*min(pd.to_numeric(content['rank'])))+1)

        ccid = selected_creative['Creative Type'].iloc[0]

        if selected_creative['Creative Group'].iloc[0] in ['HTML','CC']:
            creative_type = selected_creative['Creative Group'].iloc[0]
        elif ccid=='HTML' or ccid.isdigit():
            creative_type = 'HTML'
        else:
            creative_type = 'CC'

        if ccid=='HTML':
            creative_type = ''

        if ccid in list(new_html['Creative Type']):
            print('     New html selected', ccid)
        else:
            print('     ',creative_type, ccid, 'ranked', lexi_content.index[lexi_content['Creative Type'] == ccid][0] + 1, 'out of', len(lexi_content), 'pieces that meet thresholds')

        if ccid=='HTML':
            creative_type = 'HTML'
            ccid = ''

        return ccid

    def lookup_creative_choice(self, affiliate, hitpath):
        content_df = self.content_df
        all_offers_df = self.all_offers_df
        hitpath = int(hitpath)
        aff_df = content_df.loc[affiliate]
        if hitpath in aff_df.index.get_level_values(0):
            aff_hit_df = aff_df.loc[hitpath]
            if 'Custom Content' in aff_hit_df.index.get_level_values(0):
                cc_pct = aff_hit_df.loc['Custom Content']['Final Weight']
            else:
                cc_pct = 0
            if 'HTML' in aff_hit_df.index.get_level_values(0):
                html_pct = aff_hit_df.loc['HTML']['Final Weight']
            else:
                html_pct = 0
        else:
            hit_df = all_offers_df.loc[hitpath]
            if 'Custom Content' in hit_df.index.get_level_values(0):
                cc_pct = hit_df.loc['Custom Content']['Final Weight']
            else:
                cc_pct = 0
            if 'HTML' in hit_df.index.get_level_values(0):
                html_pct = hit_df.loc['HTML']['Final Weight']
            else:
                html_pct = 0

        if html_pct>=0.7:
            return 'HTML'
        elif cc_pct>=0.7:
            return 'CC'
        else:
            return np.random.choice(['HTML', 'CC'],p=[html_pct, 1-html_pct])

    def lookup_creative_choice(self, affiliate, hitpath):
        content_df = self.content_df
        all_offers_df = self.all_offers_df
        hitpath = int(hitpath)
        aff_df = content_df.loc[affiliate]
        if hitpath in aff_df.index.get_level_values(0):
            aff_hit_df = aff_df.loc[hitpath]
            if 'Custom Content' in aff_hit_df.index.get_level_values(0):
                cc_pct = aff_hit_df.loc['Custom Content']['Final Weight']
            else:
                cc_pct = 0
            if 'HTML' in aff_hit_df.index.get_level_values(0):
                html_pct = aff_hit_df.loc['HTML']['Final Weight']
            else:
                html_pct = 0
        else:
            hit_df = all_offers_df.loc[hitpath]
            if 'Custom Content' in hit_df.index.get_level_values(0):
                cc_pct = hit_df.loc['Custom Content']['Final Weight']
            else:
                cc_pct = 0
            if 'HTML' in hit_df.index.get_level_values(0):
                html_pct = hit_df.loc['HTML']['Final Weight']
            else:
                html_pct = 0

        if html_pct>=0.7:
            return 'HTML'
        elif cc_pct>=0.7:
            return 'CC'
        else:
            return np.random.choice(['HTML', 'CC'],p=[html_pct, 1-html_pct])
    ####################### Content Selection for SMS #######################


    def content_select_sms(self,sc_dppub_affiliate,hitpath,simple=False,print_bool=False,send_date=date.today()):
        mamba = self.cobra
        df = self.lexi
        offers = self.offers
        lanina = self.cw
        ccid=''
        # according to pubid and sc to find out sc_dppub from df
        publisher = infrastructure.get_publisher()
        pubid = sc_dppub_affiliate.split('_')[2]
        sc = sc_dppub_affiliate.split('_')[0]
        #dppub = publisher[publisher['PUBID']==int(pubid)]['DP.DS or DP.sV'].values[0]
        dv = publisher[publisher['PUBID']==int(pubid)]['Sub Vertical'].values[0]
        sc_dppub = sc_dppub_affiliate.strip()[:-7]
        
        
        df['Opp Cost eCPM'] = df['Opportunity Cost'] / df['Delivered'] * 1000
        # convert old content ID to new content ID 
        df = df.merge(lanina[['Content ID','Old Content ID']], how = 'left', left_on = 'Creative Type', right_on = 'Old Content ID')
        df.loc[df['Content ID'].isna() == False,'Creative Type'] = df['Content ID']
        df.drop(columns=['Content ID','Old Content ID'], inplace=True)
        # Create dataframe of all drops of given hitpath in past 90 days
        df_content = df[(df['Date'] > pd.Timestamp.today() - pd.to_timedelta(90, unit='d')) & (df['Hitpath Offer ID']==hitpath) & (df['Shortcode Name']==sc)]
        df_content = df_content[(df_content['Creative Type'].isin(lanina['Content ID'].unique().tolist())) ]

        
        df_content['shortcode_DP.SV'] = df_content['shortcode_DP.SV'] == sc_dppub 
        df_content['data_vertical'] = df_content['Data Vertical'] == dv
        df_content_by_drop = df_content.copy()
        ## get the opportunity CTR50 for each content piece in the same data vertical 
        df_content_other_affliate = df_content[(df_content['shortcode_DP.SV']==False) & (df_content['data_vertical'] == True)]
        df_content_other_affliate = df_content_other_affliate.groupby(['data_vertical','Creative Type','Hitpath Offer ID'], as_index=False).agg({'Delivered': 'sum', 'Opportunity Cost': 'sum','Opportunity Clicks': 'sum','Clicks': 'sum','Revenue':'sum'})
        df_content_other_affliate = viper_main.calculate_oppo_ctr(df_content_other_affliate)
        df_content_other_affliate = df_content_other_affliate[['Creative Type','opportunity CTR50']]
        df_content_other_affliate = df_content_other_affliate.rename(columns = {'opportunity CTR50':'opportunity CTR50_data_vertical_level'})


        #offer_median_eCPM = df_content['Opp Cost eCPM'].median()
        offer_median_eCPM = df_content['opportunity CTR50'].median()
        df_content_drops = df_content.groupby(['shortcode_DP.SV','Creative Type'], as_index=False).count()
        df_content_drops = df_content_drops[['shortcode_DP.SV','Creative Type', 'Hitpath Offer ID']]
        df_content_drops.columns = ['shortcode_DP.SV','Creative Type','Drops']
        df_content = df_content.groupby(['shortcode_DP.SV','Creative Type','Hitpath Offer ID'], as_index=False).agg({'Delivered': 'sum', 'Opportunity Cost': 'sum','Opportunity Clicks': 'sum','Clicks': 'sum','Revenue':'sum'})
        df_content = viper_main.calculate_oppo_ctr(df_content)
        df_content['Calculated Opp Cost eCPM'] = df_content['Opportunity Cost'] / df_content['Delivered'] * 1000
        df_content['Calculated CTR'] = df_content['Clicks'] / df_content['Delivered']
        df_content['Calculated opp CTR'] = df_content['Opportunity Clicks'] / df_content['Delivered']
        #df_content['Calculated Opp Cost eCPM'] = df_content['Opportunity Cost'] / df_content['Delivered'] * 1000
        df_content = df_content.merge(df_content_drops, on=['shortcode_DP.SV','Creative Type'])

        # Get the drops for the affiliate account
        df_content_affliate = df_content[df_content['shortcode_DP.SV']==True]
        df_content_affliate_eCPM = df_content_by_drop[df_content_by_drop['shortcode_DP.SV']==True]['opportunity CTR50'].median()
        df_content_drops = df_content_drops[df_content_drops['shortcode_DP.SV']==True]
        df_content_affliate_drops = df_content_affliate.groupby('Creative Type', as_index=False).count()
        #df_content_affliate_eCPM = df_content_affliate['Opportunity Cost'].sum() / df_content_affliate['Delivered'].sum() * 1000
        df_content_affliate = viper_main.calculate_oppo_ctr(df_content_affliate)
        #df_content_affliate['Calculated Opp Cost eCPM'] = df_content_affliate['Opportunity Cost'] / df_content_affliate['Delivered'] * 1000
        df_content_affliate = df_content_affliate[['shortcode_DP.SV','Creative Type','opportunity CTR50','Calculated Opp Cost eCPM','Calculated CTR','Calculated opp CTR','Drops']]
        df_content_affliate['Meets Threshold'] =  df_content_affliate['opportunity CTR50'] >= df_content_affliate_eCPM

        # Get date last dropped in affiliate account and whether it is in the past 30 days or not
        df_content_dates = df[(df['Date'] > pd.Timestamp.today() - pd.to_timedelta(90, unit='d')) & \
                            (df['Hitpath Offer ID']==hitpath) & \
                            (df['shortcode_DP.SV'] == sc_dppub)]
        df_content_dates = df_content_dates[['Creative Type','Date']]
        df_content_dates = df_content_dates.groupby(['Creative Type'],as_index=False).max()
        cobra_dates = mamba[(mamba['Hitpath Offer ID']==hitpath) & (mamba['shortcode_DP.SV']==sc_dppub) & (mamba['Creative']!='') & (mamba['Date']> df['Date'].max() ) ][['Date','Creative']]
        cobra_dates['Creative'] = cobra_dates['Creative'].str.replace('NEW','')
        cobra_dates['Creative'] = cobra_dates['Creative'].str.replace('*','')
        cobra_dates['Creative'] = cobra_dates['Creative'].str.split('\n')
        cobra_dates = cobra_dates.explode('Creative')
        cobra_dates = cobra_dates.reset_index(drop=True)
        cobra_dates['Creative'] = cobra_dates['Creative'].str.strip()
        cobra_dates = cobra_dates[cobra_dates['Creative']!='']
        #cobra_dates = mamba[(mamba['Hitpath Offer ID']==hitpath) & (mamba['shortcode_DP.SV']==sc_dppub)][['Date','Creative']]
        cobra_dates.columns = ['Date','Creative Type']
        df_content_dates = pd.concat([df_content_dates,cobra_dates],ignore_index=True).drop_duplicates()
        df_content_dates = df_content_dates.groupby(['Creative Type'],as_index=False).max()
        df_content_dates['Recently Dropped'] = df_content_dates['Date'] > pd.Timestamp.today() - pd.to_timedelta(30, unit='d')

        # Get the drops for all other accounts
        df_content_other = df_content[df_content['shortcode_DP.SV']==False]
        df_content_other_eCPM = df_content_by_drop[df_content_by_drop['shortcode_DP.SV']==False]['opportunity CTR50'].median()
        df_content_drops = df_content_drops[df_content_drops['shortcode_DP.SV']==False]
        #df_content_other_eCPM = df_content_other['Opportunity Cost'].sum() / df_content_other['Delivered'].sum() * 1000
        df_content_other = viper_main.calculate_oppo_ctr(df_content_other)
        df_content_other = df_content_other.merge(df_content_other_affliate, on = 'Creative Type', how = 'left') 
        df_content_other.loc[df_content_other['opportunity CTR50_data_vertical_level'].isna() == False, 'opportunity CTR50'] = df_content_other['opportunity CTR50_data_vertical_level'] * 0.7 +   df_content_other['opportunity CTR50'] * 0.3
        
        #df_content_other_median_eCPM = df_content_other['Revenue CPM (eCPM)'].median()
        df_content_other = df_content_other[['shortcode_DP.SV','Creative Type','opportunity CTR50','Calculated Opp Cost eCPM','Calculated CTR','Calculated opp CTR','Drops']]
        df_content_other['Meets Threshold'] =  df_content_other['opportunity CTR50'] >= df_content_other_eCPM
    
    


        # Merge the above dataframes together 
        df_content = df_content_other.merge(df_content_affliate, how='outer', on='Creative Type').merge(df_content_dates, how='outer', on='Creative Type')
        df_content['Meets Threshold'] = (df_content['Meets Threshold_y'] == True) | ((df_content['Meets Threshold_y'].isnull()) & (df_content['Meets Threshold_x'] == True))
        print('Offer eCPM for Other Accounts: ', round(df_content_other_eCPM,2))
        print('Offer eCPM for Affiliate Account: ', round(df_content_affliate_eCPM,2))
        print('Offer median eCPM: ',round(offer_median_eCPM,2))

        df_content = df_content[['Creative Type', 'opportunity CTR50_x', 'Drops_x',
                                    'opportunity CTR50_y', 'Drops_y', 'Date', 'Recently Dropped', 'Meets Threshold']]
        df_content.columns = ['Creative Type','eCPM in Other Accounts','Drops in Other Accounts',
                                'eCPM in Affiliate Account', 'Drops in Affiliate Accounts','Date','Recently Dropped',
                                'Meets Mean Threshold']
        # check if the content has a median eCPM higher than the offer median eCPM
        df_content_by_drop = df_content_by_drop.groupby(['Creative Type'], as_index=False)['opportunity CTR50'].median()
        df_content = df_content.merge(df_content_by_drop, how='left', on='Creative Type')
        df_content = df_content.rename(columns = {'opportunity CTR50':'median eCPM'})
        df_content['Meets Median Threshold'] = df_content['median eCPM'] >= offer_median_eCPM

        # check if the content has an aggregate eCPM in the affiliate account higher than the offer aggregate eCPM in the affiliate account 
        df_content.loc[df_content['eCPM in Affiliate Account'] >= df_content_affliate_eCPM,'Meets Median Threshold'] = True

        # rank is calculated as the average of the aggregate eCPM of the content piece and two times the median eCPM of the content piece 
        df_content['rank'] = (df_content['eCPM in Other Accounts'] + 2 * df_content['median eCPM']) / 3
        # if the content piece has been dropped in the affiliate account in the past 90 days, that aggregate eCPM will be used in the ranking instead of the aggregate for all accounts
        df_content.loc[~df_content['eCPM in Affiliate Account'].isna(),['rank']] = (df_content['eCPM in Affiliate Account'] + 2*df_content['median eCPM'])/3
        df_content = df_content.sort_values('rank', ascending=False)
        df_content = df_content.merge(lanina[['Content ID','Content Approval Status','Content']], how='left', left_on='Creative Type',right_on = 'Content ID', copy = False )
        # drop reporting content id column
        df_content = df_content.drop(columns=['Content ID'])
        # filter out content that is not approved and failed testing 
        df_content = df_content[~(df_content['Content Approval Status'].str.contains('Paused|Not Approved|Failed Testing', na = False)) ]
        df_content_all = df_content.copy()
        if df_content.empty:
            
                        # find Reporting Content ID from la nina based on offer IDs and Type 
            lanina1 =  lanina[~(lanina['Content Approval Status'].str.contains('Paused|Not Approved|Failed Testing', na = False)) ]
            df_content= lanina1[(lanina1['OfferIDs'] == hitpath) & (lanina1['Type'] == sc) & (lanina1['Channel'] =='SC')][['Content ID','Content Approval Status', 'Content']]
            if len(df_content) > 0: 
                print('No content available from content selection script, we choose 1 random content from Lanina')
                ccid  = df_content['Content ID'].values[0]
                df_content_all = df_content.copy()
                print(ccid)
            else:
                
                print('No content available from lanina')
        else:
            # select only content that has been dropped more than 2 times in any account, or has been dropped at least once in the affiliate account it
            df_content['Meets Drop Threshold'] = (df_content['Drops in Other Accounts']>=2) 
            df_content = df_content[((df_content['Meets Drop Threshold']==True) & (df_content['eCPM in Affiliate Account'].isna())) | (~df_content['eCPM in Affiliate Account'].isna())]
            df_content = df_content[(df_content['Meets Mean Threshold']==True) & (df_content['Meets Median Threshold']==True)].sort_values('rank', ascending=False)

        # if no content meets threshold, remove drop number requirement
            if df_content.empty:
                df_content = df_content_all.copy()
                df_content = df_content[(df_content['Meets Mean Threshold']==True) & (df_content['Meets Median Threshold']==True)].sort_values('rank', ascending=False)

            # if after remove number of drop restriction no content meets threshold, reduce mean and median threshold until content is found
            k=0
            while df_content.empty:
                k = k + 1
                print('No offers, decreasing thresholds.')
                df_content = df_content_all.copy()
                df_content_other['Meets Threshold'] =  df_content_other['opportunity CTR50'] >= df_content_other_eCPM - k * 0.1
                df_content_affliate['Meets Threshold'] =  df_content_affliate['opportunity CTR50'] >= df_content_affliate_eCPM - k * 0.1
                df_content['Meets Median Threshold'] = df_content['median eCPM'] >= offer_median_eCPM - k * 0.1
                df_content = df_content[(df_content['Meets Mean Threshold']==True) & (df_content['Meets Median Threshold']==True)].sort_values('rank', ascending=False)

                if k > 20:
                    df_content = df_content_all.copy()
                    df_content['Meets Drop Threshold'] = (df_content['Drops in Other Accounts'] > 2) 
                    df_content = df_content.sort_values('rank', ascending=False)
                    break

            df_content = df_content.reset_index(drop=True)
            # If not specifying HTML, remove dates from HTML so gapping is not considered
            
            # Select content, either a random content piece that has not dropped in the past 30 days weighted by the rank or 4 times the rank if opp cost is positive
            # or the one least recently dropped if all have been dropped in the past 30 days
            content = df_content[df_content['Recently Dropped']!=True] # didn't drop in the past 30 days 
            #content.loc[content['rank']>0,'rank'] = content['rank'] * 4 
                
            if content.empty:
                content = df_content[(df_content['Meets Mean Threshold']==True) & (df_content['Meets Median Threshold']==True)]
                selected_creative = content[content['Date'] == content['Date'].min()][['Creative Type']]
            else:
                selected_creative = content[['Creative Type']].sample(n=1,weights=pd.to_numeric(content['rank']) + abs(2*min(pd.to_numeric(content['rank']))))
                #selected_creative = content[['Creative Type']]
            try: 
                ccid = selected_creative['Creative Type'].iloc[0]
            except: 
                ccid = df_content_all['Creative Type'].iloc[0]
            

            
            print('')
            
            print(ccid)
            print('Ranked', df_content.index[df_content['Creative Type'] == ccid][0] + 1, 'out of', len(df_content), 'pieces that meet thresholds')

        return ccid 

def get_content_df(lexi):
    lexi['Time Adjusted Revenue'] = np.maximum(-np.log(.005* (1+ (pd.Series(lexi['Date'].max() - lexi['Date'], index=lexi.index)).dt.days)), pd.Series(1, index=lexi.index))*lexi['Revenue']
    lexi['Creative'] = np.where(lexi['Creative Type']=='HTML','HTML','Custom Content')

    content_df = lexi.groupby(['Affiliate ID','Hitpath Offer ID','Creative']).agg({'Time Adjusted Revenue':'sum','Delivered':'sum','Date':'nunique'})
    content_df.rename(columns={'Date':'Prime Drop Count','Delivered':'Prime Delivered'},inplace=True)

    content_df['Prime eCPM'] = content_df['Time Adjusted Revenue']*1000 / content_df['Prime Delivered']
    content_df.reset_index(inplace=True)

    hitpath_creative_df = content_df.groupby(['Hitpath Offer ID','Creative']).agg({'Time Adjusted Revenue':'sum','Prime Delivered':'sum','Prime Drop Count':'sum'})
    hitpath_creative_df.columns = ['Total Adjusted Revenue','Total Delivered', 'Total Drop Count']
    hitpath_creative_df['Total eCPM'] = hitpath_creative_df['Total Adjusted Revenue']*1000 / hitpath_creative_df['Total Delivered']

    content_df = pd.merge(content_df,hitpath_creative_df, how='left', on=['Hitpath Offer ID','Creative'])

    content_df['Non Prime Adjusted Revenue'] = content_df['Total Adjusted Revenue'] - content_df['Time Adjusted Revenue']
    content_df['Non Prime Delivered'] = content_df['Total Delivered'] - content_df['Prime Delivered']
    content_df['Non Prime eCPM'] = content_df['Non Prime Adjusted Revenue']*1000 / content_df['Non Prime Delivered']


    creative_sums = content_df.groupby(['Affiliate ID','Hitpath Offer ID']).agg({'Prime eCPM':'sum','Non Prime eCPM':'sum','Prime Drop Count':'sum','Total eCPM':'sum','Total Drop Count':'sum'}).rename(columns={'Prime eCPM':'Prime eCPM Sum','Non Prime eCPM':'Non Prime eCPM Sum','Prime Drop Count':'Prime Drop Count Sum','Total eCPM':'Total eCPM Sum','Total Drop Count':'Total Drop Count Sum'})
    content_df = pd.merge(content_df, creative_sums, how='left', on=['Affiliate ID','Hitpath Offer ID']).set_index(['Affiliate ID','Hitpath Offer ID','Creative'])


    content_df['Non Prime Drop Count'] = content_df['Total Drop Count'] - content_df['Prime Drop Count']
    content_df['Non Prime Drop Count Sum'] = content_df['Total Drop Count Sum'] - content_df['Prime Drop Count Sum']

    content_df['Prime eCPM Pct'] = content_df['Prime eCPM'] / content_df['Prime eCPM Sum']
    content_df['Prime Drop Pct'] = content_df['Prime Drop Count'] / content_df['Prime Drop Count Sum']
    content_df['Prime Weight'] = (content_df['Prime eCPM Pct']*3 + content_df['Prime Drop Pct'])/4

    content_df['Non Prime eCPM Pct'] = content_df['Non Prime eCPM'] / content_df['Non Prime eCPM Sum']
    content_df['Non Prime Drop Pct'] = content_df['Non Prime Drop Count'] / content_df['Non Prime Drop Count Sum']
    content_df['Non Prime Weight'] = (content_df['Non Prime eCPM Pct']*3 + content_df['Non Prime Drop Pct'])/4

    content_df['Final Weight'] = (content_df['Prime Weight']*3 + content_df['Non Prime Weight'])/4
    content_df['Final Weight'].fillna(0,inplace=True)
    
    #Create alternative df in case some affiliate has never dropped an offer
    all_offers_df = content_df.reset_index().groupby(['Hitpath Offer ID','Creative']).agg({'Final Weight':'mean'})

    return content_df, all_offers_df





