from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import storage
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from functools import reduce
from unidecode import unidecode
import csv



SCOPES = ['https://www.googleapis.com/auth/webmasters']
SERVICE_ACCOUNT_FILE = 'C:\\Users\\an210f\\Downloads\\GSC MK spry spanner.json'
credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

service = build(
    'webmasters',
    'v3',
    credentials=credentials
)

site_urls = ["sc-domain:boeingdistribution.com"
             ,"sc-domain:jeppesen.com"
             ,"https://shop.boeing.com/"
             ,"https://shop.boeing.com/aviation-supply/"
            ]

start_date = "2022-01-31"
end_date = "2022-01-31"

def date_range(start_date, end_date, delta=timedelta(days=1)):
        """
        The range is inclusive, so both start_date and end_date will be returned
        Args:
            start_date: The datetime object representing the first day in the range.
            end_date: The datetime object representing the second day in the range.
            delta: A datetime.timedelta instance, specifying the step interval. Defaults to one day.
        Yields:
            Each datetime object in the range.
        """
        start_date_datetime = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_datetime = datetime.strptime(end_date, '%Y-%m-%d')
        current_date = start_date_datetime
        while current_date <= end_date_datetime:
            yield current_date
            current_date += delta

#Since there is a chance of duplicates occuring over the same page with different case sensitivity. 
#We need a function to aggregate the CTR - Click Through Rate
def agg_ctr(series):
    sum_clicks = 0
    sum_impression = 0
    row_num = 0
    for index, value in series.items():
        sum_clicks = sum_clicks + df_result.iloc[index, 5]
        sum_impression = sum_impression + df_result.iloc[index, 6]
    return sum_impression / sum_clicks if sum_clicks else 0

def get_sc_df(site_url,start_date,end_date):
    """Grab Search Console data for the specific property."""
    output_rows = []
    maxRows = 25000
    
    
    for date in date_range(start_date, end_date):
        #output_rows = []
        date = date.strftime("%Y-%m-%d")
        print(date)
        i = 0
        while True:

            request = {
              'startDate': date,
              'endDate': date,
              'dimensions': ["date","query","page","country","device"], 
              'rowLimit': maxRows,
#              'dimensionFilterGroups': [{'filters': 
#                                         [
#                                             {'dimension': 'query'
#                                              ,'operator': 'contains'
#                                              ,'expression': 'distribution'}
#                                             ,{'dimension': 'country'
#                                               ,'operator': 'contains'
#                                               ,'expression': 'USA'}
#                                         ]
#                                        }
#                                       ],
              'startRow': i * maxRows
                
               }
            
            response = service.searchanalytics().query(siteUrl=site_url, body=request).execute()
            #print(response)

            if response is None:
                #print("there is no response")
                break
            if 'rows' not in response:
                #print("row not in response")
                break
            else:
                for row in response['rows']:
                    #print(row)
                    date = row['keys'][0]
                    #BigQuery can hanlde the unicode values, will need a fork in the logic
                    query = unidecode(row['keys'][1].upper()).upper()#.strip()
                    #query = row['keys'][1]
                    page = row['keys'][2].lower()
                    country = row['keys'][3].upper()
                    device = row['keys'][4]
                    output_row = [date, query, page, country, device, row['clicks'], row['impressions'], row['ctr'], row['position'],site_url]
                    output_rows.append(output_row)
                i = i + 1  

    #print(output_rows)              
    df = pd.DataFrame(output_rows, columns=['date','query','page', 'country', 'device', 'clicks', 'impressions', 'ctr', 'avg_position', 'site'])
    return df

df_result = pd.DataFrame()

df = pd.DataFrame()
#Iterate through all the sites
for site_url in site_urls:
    print(site_url)
    df_temp = get_sc_df(site_url,start_date,end_date)
    #print(df_temp)
    df_result = pd.concat([df_result, df_temp],ignore_index = 'True')
    

#print(df_result)
column_rename = {'date':'TD_DATE'
                 ,'query':'TD_QUERY'
                 ,'page':'PAGE'
                 ,'country':'COUNTRY'
                 ,'device':'DEVICE'
                 ,'clicks':'CLICKS'
                 ,'impressions':'IMPRESSION'
                 ,'ctr':'CTR'
                 ,'avg_position':'AVG_POSITION'
                 ,'site':'SITE'    
}
df_result.rename(columns=column_rename, inplace=True)

#print(df_result)

grouped_result = df_result.groupby(['TD_DATE', 'TD_QUERY','PAGE','COUNTRY','DEVICE', 'SITE']).aggregate({
    'CLICKS': 'sum',
    'IMPRESSION': 'sum',
    'CTR': agg_ctr, #custom aggregate function
    'AVG_POSITION': 'min'
}
)
df_result = grouped_result.reset_index()
df_result = df_result[['TD_DATE','TD_QUERY','PAGE','COUNTRY','DEVICE','CLICKS','IMPRESSION','CTR','AVG_POSITION','SITE']]

filename = 'GOOGLE_SEARCH_CONSOLE_API_' + end_date + '.csv'
filename = filename.replace('-','')
df_result.to_csv(filename,index=False,quotechar='"',quoting=csv.QUOTE_ALL)