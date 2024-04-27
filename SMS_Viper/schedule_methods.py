from gsheets import Sheets
import pandas as pd
import numpy as np
import datetime as dt
import time
import filepaths
import smartsheet
import os
from datetime import timedelta
from datetime import date
import pygsheets
import json

def get_smartsheet(sheet):
    options=['offers','emit','tests','content']
    if sheet not in options:
        raise ValueError("Avaliable Smartsheets are: %s" % options)
    at='r7jiyc0it16akkdrv1bo8d4r79'
    sheet_id='4063260640077700'
    emit_id='6899759718918020'
    test_id='3553756253054852'
    content_id='1807162503653252'
    
    if os.environ.get('DOCKER_SCRIPT_RUNNING'):
        os.makedirs('host_data/', exist_ok=True)
        download_path='host_data/'
    elif os.environ.get('DOCKER_RUNNING'):
        download_path='host_data/'
    else:
        download_path=filepaths.smartsheet_folder

    smartsheet_client=smartsheet.Smartsheet(at)
    offer_sheet=smartsheet_client.Sheets.get_sheet_as_excel(sheet_id,download_path,'Offer Data Workbook.xlsx')
    emit_sheet=smartsheet_client.Sheets.get_sheet_as_csv(emit_id,download_path)
    test_sheet=smartsheet_client.Sheets.get_sheet_as_csv(test_id,download_path)
    content_sheet=smartsheet_client.Sheets.get_sheet_as_csv(content_id,download_path)
    
    if sheet=='offers':
        return pd.read_excel(os.path.join(download_path,"Offer Data Workbook.xlsx"))
    elif sheet=='emit':
        return pd.read_csv(os.path.join(download_path,"Email Identifier Mapping Tracker.csv"))
    elif sheet=='tests':
        return pd.read_csv(os.path.join(download_path,"Testing Pipeline.csv"))
    elif sheet=='content':
        return pd.read_csv(os.path.join(download_path,"Custom Content Submission.csv"))
    else:
        print('Sheet name much be offers,emit,tests, or content.')

def get_cobra(days_ahead=1):
    start_time=time.time()
    
    url='https://docs.google.com/spreadsheets/d/1CXgHLu5HML6fabjSK5mk66PzbRbU_6lH7Ws-FiuHzbc/edit?ts=5fdbaa9e#gid=2104287081&range=B5'
    sheets=Sheets.from_files(filepaths.gsheets)
    cobra=sheets.get(url)
    schedule=cobra.find('Cobra').to_frame()
    
    api_time=time.time()-start_time
    # print('Pull Time {}'.format(api_time))
    
    schedule2=schedule.transpose()
    schedule2['Date']=pd.to_datetime(schedule2[0],errors='coerce')
    schedule2.index=schedule2['Date']
    c=schedule2.iloc[0]
    d=c.where(c=='x').notna()
    f=c.where(c=='Enter Search Text:').notna()# we dont need it now.. can keep it. comes as blank.f can be empty
    e=list(d[d].index)+(list(f[f].index))
    schedule2=schedule2.drop(columns=e)
    
    last_cobra_date = schedule2.index[-1]

    start_date = last_cobra_date + timedelta(days=1)
    end_date = date.today() + timedelta(days=days_ahead)

    new_date_list = list(pd.date_range(start_date,end_date,freq='d'))
    new_df = pd.DataFrame(np.nan, new_date_list, schedule2.columns)
    schedule2 = pd.concat([schedule2, new_df])
    col=1
    
    # seven_drops=['460804','460743']
    
    #change5: change the while loop:
    pubids=[]
    while col<(len(schedule2.columns)-1):
        pub=schedule2.iloc[1,col]
        if pub!='Test':
            aff=pub.split('_')[0]
            dpds=pub.split('_')[1]
            mini=(pub,col+1,col+100)
            pubids.append(mini)
        col+=101 #changed to 101
    
    drops=['Drop 1','Drop 2','Drop 3','Drop 4','Drop 5','Drop 6','Drop 7','Drop 8','Drop 9','Drop 10']
    variables=10 # change4: change variables to 8
    
    frames={}
    
    for i in pubids:
        mini_frame=schedule2.iloc[1:,i[1]:i[2]]
        col2=0
        for j in drops:
            df=mini_frame.iloc[:,col2:col2+variables]
            df.columns=df.iloc[0]
            df['dataset']=i[0]
            df['drop']=j
            df=df.set_index([df.index,'dataset','drop'])
            col2+=variables
            frames[(i[0],j)]=df
    
    columns=list(frames[(i[0],j)].columns)
    snakes=pd.DataFrame(columns=columns)
    
    #dp2id={'SC.FHA':'460398','LPG.FH':'460758','SC.RF ':'460398'}#change2:no need of this dict.
    
    for i in frames.values():
        snakes=pd.concat([snakes,i])
    snakes=snakes.sort_index(level=0)
    
    index=pd.MultiIndex.from_tuples(snakes.index,names=['Date','Dataset','Drop'])
    snakes.index=index
    snakes=snakes[~snakes.index.duplicated()]
    
    snakes['Hitpath Offer ID']=pd.to_numeric(snakes['Campaign ID'].str[:4],errors='coerce')
    snakes['real seg']=snakes['Segment '].str.split('_').str[2]
    snakes=snakes.reset_index()
    return snakes

def df_stats(df):
    gpby=df.groupby(by='Unique ID')
    rev=gpby.agg({'Revenue':'sum'})
    rev['eCPM']=gpby.apply(lambda x:x.Revenue.sum()*1000/x.Delivered.sum())
    rev['var']=gpby.apply(df_var)
    rev['clicks']=gpby.apply(click_count)
    
    return rev

def df_var(df,nodmin=4):
    var=[]
    for i in list(df['Hitpath Offer ID'].unique()):
        var_frame=df[df['Hitpath Offer ID']==i]
        if len(var_frame)>=nodmin:
            var_variable=var_frame['RPC'].var()
            var.append(var_variable)
    var_mean=np.mean(var)
    return var_mean

def click_count(df):
    o1=df[df['Segment']==df['proper_segment']]['Delivered'].median()
    if np.isnan(o1):
        o1=df[df['Segment']=='O30']['Delivered'].median()*.3
    clicks=o1*df['Clicks'].sum()/df['Delivered'].sum()
    return clicks

def segment_size(df,payout): 
    # This function is to find proper segment size for offer testing 
    # use unique id 
    df1= df.groupby(['Affiliate ID','Segment'])['Delivered'].median().reset_index()
    df2 = df1.loc[(df1['Segment']== 'O1') |(df1['Segment']== 'O7') |(df1['Segment']== 'O14') |(df1['Segment']== 'O30')  ,]
    df3 = df2.pivot(index = 'Affiliate ID', columns  = 'Segment', values = 'Delivered').reset_index()
    eCPM_df = df.groupby('Affiliate ID')['Revenue CPM (eCPM)'].median().reset_index()
    df4 = df3.merge(eCPM_df,how = 'right', on = 'Affiliate ID')
    # we assume we don't have account with 0 eCPM medium, especially in tier x 
    df4['segment_size'] = (payout * 1000) / df4['Revenue CPM (eCPM)']
    df4.loc[df4['segment_size'].isnull(),'proper_segment']= 'O1'# if not given any payout, we use O1 as default 
    df4.loc[df4['segment_size'].isnull(),'proper_segment_size'] = df4['O1']
    df4.loc[df4['O30']< df4['segment_size'],'proper_segment']= 'no choice' # if all the segment size is smaller than we expected, we will suggest change another account 
    df4.loc[df4['O30']< df4['segment_size'],'proper_segment_size']= np.nan
    df4.loc[df4['O30']>= df4['segment_size'],'proper_segment']= 'O30' 
    df4.loc[df4['O30']>= df4['segment_size'],'proper_segment_size'] = df4['O30']
    df4.loc[df4['O14']>= df4['segment_size'],'proper_segment']= 'O14'
    df4.loc[df4['O14']>= df4['segment_size'],'proper_segment_size']= df4['O14']
    df4.loc[df4['O7']>= df4['segment_size'],'proper_segment']= 'O7'
    df4.loc[df4['O7']>= df4['segment_size'],'proper_segment_size']= df4['O7']
    df4.loc[df4['O1']>= df4['segment_size'],'proper_segment']= 'O1'
    df4.loc[df4['O1']>= df4['segment_size'],'proper_segment_size']= df4['O1']
    df4.loc[df4['proper_segment'] == np.nan,'proper_segment']= 'no choice'
    df4.loc[df4['proper_segment'] == np.nan,'proper_segment_size']= np.nan
    
    return df4

def active_account(df): 
    # This function is filter out those account that is not active in the past two days. 
    most_recent_date = df.groupby('Affiliate ID')['Date'].max().reset_index()
    most_recent_date['last_two_date'] = pd.Timestamp(df['Date'].max()-dt.timedelta(days=2)) 
    most_recent_date.loc[most_recent_date['Date']>= most_recent_date['last_two_date'], 'active'] = True 
    most_recent_date.loc[most_recent_date['Date']< most_recent_date['last_two_date'], 'active'] = False
  
    
    return most_recent_date 

def rank_cleaner(rdf,overlap,olap):
    rdf=rdf.sort_values('Ranked Total')
    rdf=rdf.drop_duplicates(subset='Affiliate ID',keep='first')
    rdf=rdf.set_index('Affiliate ID')
    drop_it=set()
    for i in rdf.index:
        drop_this=list(overlap[overlap['Affiliate 1']==i][overlap['% overlap']>=olap][overlap['% overlap']!=1]['Affiliate 2'].unique())
        comp={}
        cont=rdf.loc[i]['Total Rank']
        for j in drop_this:
            if j in rdf.index:
                comp[j]=rdf.loc[j]['Total Rank']
        if len(comp)>0:
            if cont>min(comp.values()):
                drop_it=drop_it.union(set([i]))
                # print(i)
            else:
                drop_it=drop_it.union(set(comp.keys()))
                # print(comp.keys())
    return drop_it
def dynamic_tiers(hitpath,period=60,ignore=None,conversion=3,olap=0.1):
    df=pd.read_excel(filepaths.lexi3,dtype={'Hitpath Offer ID':str,'Affiliate ID':str},parse_dates=['Date']) 
    df['Revenue CPM (eCPM)']=pd.to_numeric(df['Revenue CPM (eCPM)'],errors='coerce')
    df['Affiliate ID']=df['Affiliate ID'].str[:6]
    df=df[~df['Segment'].isin(['W','TR'])]
    df['Unique ID']=df['Affiliate ID']+'_'+df['data_provider'].str.upper()
    # lower case vertical
    
    offer_sheet=pd.read_excel(filepaths.offers,dtype={'Hitpath Offer ID':str})
    offer_sheet['Payout']=pd.to_numeric(offer_sheet['Payout'],errors='coerce')
    df=df.merge(offer_sheet[['Hitpath Offer ID','Payout Type']],on='Hitpath Offer ID',how='left')
    df=df[df['Payout Type']!='CPM']
    
    today=dt.date.today()
    time_period=pd.Timestamp(today-dt.timedelta(days=period))
    
    overlap=pd.read_excel(filepaths.omatrix,'Stacked overlap-O30 files')
    overlap['Affiliate 1']=overlap['pubid 1'].str[:6]
    overlap['Affiliate 2']=overlap['pubid 2'].str[:6]
    

    overt=[offer_sheet.loc[offer_sheet['Hitpath Offer ID']==hitpath,'Vertical'].values.item()]
    always_ignore=['460750','460632']
    if ignore==None:
        ignore=[]
    else:
        ignore=[lambda x: x for x in ignore.split(',').strip()]
    ignore.extend(always_ignore)
    # networks=['Massive (c3 Data)']
    #add payout and conversions
    payout = offer_sheet.loc[offer_sheet['Hitpath Offer ID']==hitpath,'Payout'].values.item()
    if payout==np.nan:
        total_payout=25
    else: 
        total_payout = payout * conversion
    
    # filter out the account that was not active in the past 2 days. 
    most_recent_date = active_account(df)
    non_active_list = most_recent_date.loc[most_recent_date['active']== False, 'Affiliate ID'].values.tolist()
    df=df[~df['Affiliate ID'].isin(non_active_list)]
    
    df=df[df['Date']>=time_period]
    # find out the proper segment
    segment_size_choice = segment_size(df, total_payout)
    proper_segment = segment_size_choice[['Affiliate ID','proper_segment','proper_segment_size']]
    # merge the proper segment to df
    df = df.merge(proper_segment, how  = 'left',  on = 'Affiliate ID')
    df=df[df['Vertical'].isin(overt)]
    df=df[~df['Affiliate ID'].isin(ignore)]
    
    
    # df=df[df['Hitpath Offer ID'].isin(hitpaths)]
    # df=df[df['Network'].isin(networks)]
    
    small=['var']
    ranked_list=[]
    
    x=df_stats(df)
    # x['unique_user']=overlap.groupby('pubid 1')['pubid 1 size'].mean()
    
    for i in x:
        if i in small:
            x['Ranked {}'.format(i)]=x['{}'.format(i)].rank(na_option='bottom')
        else:
            x['Ranked {}'.format(i)]=x['{}'.format(i)].rank(na_option='bottom',ascending=False)
        ranked_list.append('Ranked {}'.format(i))
    
    x['Affiliate ID']=x.index.str[:6]
    x['Total Rank']=x[ranked_list].sum(axis=1)
    x['Ranked Total']=x['Total Rank'].rank()
    if olap>0:
        z=rank_cleaner(x, overlap,olap)
        x=x[~x['Affiliate ID'].isin(z)]
    
    o30s=overlap[['Affiliate 1','pubid 1 size']].drop_duplicates('Affiliate 1')
    x=x.merge(o30s,left_on='Affiliate ID',right_on='Affiliate 1',how='left')
    x=x.merge(proper_segment, on ='Affiliate ID', how = 'left' )
    return x

def get_cw():
    
    url='https://docs.google.com/spreadsheets/d/1beoDjrKwv8_oXkfC2VhDorAHXeEkQ9GkFccI0poeVAk/edit?usp=sharing'
    if os.environ.get('DOCKER_RUNNING'):
        cobra_auth_path='host_data/' + filepaths.service_account_location.split('/')[-1]
    else:
        cobra_auth_path=filepaths.service_account_location
#     gc = pygsheets.authorize(service_account_file = cobra_auth_path)
    gc = get_gc()
    cw_spreadsheet = gc.open_by_url(url)
    
    contentdf = cw_spreadsheet.worksheet_by_title('Content Warehouse').get_as_df()
    contentdf['Reporting Content ID']=contentdf['Reporting Content ID'].astype(str)
    contentdf['Reporting Content ID']=contentdf['Reporting Content ID'].str[:7]
    contentdf['Content Pitch']=np.where(contentdf['Type (Pitch)'].isna(),contentdf['Pitch Resolution'],contentdf['Type (Pitch)'])
    contentdf.replace('',np.nan,inplace=True)
    
#     old
#     url='https://docs.google.com/spreadsheets/d/1beoDjrKwv8_oXkfC2VhDorAHXeEkQ9GkFccI0poeVAk/edit?usp=sharing'
#     sheets=Sheets.from_files(filepaths.gsheets)
#     contentdf=sheets.get(url)
#     contentdf=contentdf.find('Content Warehouse').to_frame(dtype={'Reporting Content ID':str})
#     contentdf['Reporting Content ID']=contentdf['Reporting Content ID'].str[:7]
#     contentdf['Content Pitch']=np.where(contentdf['Type (Pitch)'].isna(),contentdf['Pitch Resolution'],contentdf['Type (Pitch)'])
    return contentdf


def homogeneous_type(seq):
    """Checks to see if all elements of a list are a single type,
    returns data type of first element if true."""
    iseq = iter(seq)
    first_type = type(next(iseq))
    return first_type if all( (type(x) is first_type) for x in iseq ) else False

def get_gsheet(url,ignore=None,bad_lines='warn'):
    """Imports gsheets and combines into a dataframe,
    with optional ignore statement for eliminating sheets by index.
    Function default is to warn and not error on bad lines"""
    
    sheets = Sheets.from_files(filepaths.gsheets) #Import API keys 
    my_sheets = sheets.get(url) #Import all gooogle sheets by url
    
    sheet_dict = {} #used for dataframe storage
    
    is_list = False 
    
    
    for i in range(len(my_sheets)):
        try:
            sheet_dict[i] = my_sheets.sheets[i].to_frame(on_bad_lines=bad_lines) #creates master dict of all gsheets
        except:
            print(f"Sheet in position {i} has been skipped")
    """Following code block eliminates ignored sheets and returns completed dataframe"""
    if ignore is None:
        return pd.concat(sheet_dict.values(),ignore_index=True)
    else: #below code removes values in ignore before combining the dataframes
        ignore_t = type(ignore)
        removes = {}
        if ignore_t is list:
            is_list = True
            ignore_t = homogeneous_type(ignore)
        if ignore_t is int:
            if is_list:
                for i in ignore:
                    removes[i] = sheet_dict.pop(i,"Bad Key, no item removed")
            else:
                removes[ignore] = sheet_dict.pop(ignore,"Bad Key, no item removed")
            for a,b in removes.items(): #prints out line items that were not removed
                if type(b)!=pd.DataFrame:
                    print(f"The index {a} could not be located and was not removed.")
        else:
            raise ValueError("Must provide gsheet indexes as integers through single item or list.")
    return pd.concat(sheet_dict.values(),ignore_index=True)
    

def get_cobra_secure():
    
    start_time=time.time()

    url='https://docs.google.com/spreadsheets/d/1CXgHLu5HML6fabjSK5mk66PzbRbU_6lH7Ws-FiuHzbc/edit?ts=5fdbaa9e#gid=2104287081&range=B5'
    
    
    if os.environ.get('DOCKER_SCRIPT_RUNNING'):
        import tempfile
        from google.cloud import secretmanager
        
        secret_name = "projects/573977519394/secrets/viper_secret/versions/latest"
        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(name=secret_name)
        payload = response.payload.data.decode("UTF-8")

        service_dict = json.loads(payload)
        
        def _google_creds_as_file():
            temp = tempfile.NamedTemporaryFile()
            temp.write(json.dumps(
            service_dict

            ).encode('utf-8'))
            temp.flush()
            return temp

        creds_file = _google_creds_as_file()
        
        gc = get_gc()
        
    schedule = gc.open_by_url(url)[1].get_as_df()
    s = schedule.columns.to_series()
    s.iloc[0] = 'viper'
    s.iloc[1] = 'mongoose'
    schedule.columns = s
    api_time=time.time()-start_time
#     print('Pull Time {}'.format(api_time))

    schedule2=schedule.transpose()
    schedule2['Date']=pd.to_datetime(schedule2[0],errors='coerce')
    schedule2.index=schedule2['Date']

    drops=['Drop 1','Drop 2','Drop 3','Drop 4','Drop 5','Drop 6','Drop 7','Drop 8','Drop 9','Drop 10']
    frames={}

    topdexes = schedule[schedule['viper']=="Drop 1"].index.to_list()
    indexes = schedule[schedule['viper'].isin(drops)].index.to_list()
    endexes = schedule[schedule['mongoose']=="Mailer Notes:"].index.to_list()


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

    #dp2id={'SC.FHA':'460398','LPG.FH':'460758','SC.RF ':'460398'}#change2:no need of this dict.

    for i in frames.values():
        snakes=pd.concat([snakes,i])
    snakes=snakes.sort_index(level=0)

    index=pd.MultiIndex.from_tuples(snakes.index,names=['Date','Dataset','Drop'])
    snakes.index=index
    snakes=snakes[~snakes.index.duplicated()]

    snakes['Hitpath Offer ID']=pd.to_numeric(snakes['Campaign ID'].str.split('-').str[0],errors='coerce')
    snakes['real seg']=snakes['Segment '].str.split('_').str[2]
    snakes=snakes.reset_index()
    
#     os.remove(local_auth_path)

    return snakes

def get_gc():
    if os.environ.get('DOCKER_SCRIPT_RUNNING'):
        import tempfile
        from google.cloud import secretmanager

        secret_name = "projects/573977519394/secrets/viper_secret/versions/latest"
        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(name=secret_name)
        payload = response.payload.data.decode("UTF-8")

        service_dict = json.loads(payload)

        def _google_creds_as_file():
            temp = tempfile.NamedTemporaryFile()
            temp.write(json.dumps(
            service_dict
            ).encode('utf-8'))
            temp.flush()
            return temp

        creds_file = _google_creds_as_file()
        cobra_auth_path = creds_file.name

    elif os.environ.get('DOCKER_RUNNING'):
        cobra_auth_path='host_data/' + filepaths.service_account_location.split('/')[-1]
    else:
        cobra_auth_path=filepaths.service_account_location
        
            
    gc = pygsheets.authorize(service_account_file=cobra_auth_path)

    return gc

