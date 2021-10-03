import pickle
import pandas as pd
# Imports Python standard library logging
import logging
from google.cloud import storage
from datetime import datetime, timedelta
import datetime as dt
from dateutil.relativedelta import relativedelta
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

def start_processing(request):

    #URL of the site that I am analyzing
	SITE_URL = "https://enter your url here"
    OAUTH_SCOPE = ('https://www.googleapis.com/auth/webmasters.readonly', 'https://www.googleapis.com/auth/webmasters')
    REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'

	#Setup new storage client, initialize bucket, and prepare the file for download
    client = storage.Client()
    bucket = client.get_bucket('your bucket name')
    blob = bucket.get_blob('your file name')
    blob_as_string = blob.download_as_string()
    
	#Download the credential file
    credentials = pickle.loads(blob_as_string)

    # Connect to Search Console Service using the credentials
    webmasters_service = build('webmasters', 'v3', credentials=credentials)

	#GSC only allows 25k rows per request
    maxRows = 25000
    i = 0
    output_rows = []
    #Get results for the last day that has data
    end_date_string = (dt.datetime.today() + relativedelta(days=-1)).strftime ('%Y-%m-%d')
    start_date_string = (dt.datetime.today() + relativedelta(days=-2)).strftime ('%Y-%m-%d')
    start_date = datetime.strptime(start_date_string, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_string, "%Y-%m-%d")

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
        current_date = start_date
        while current_date <= end_date:
            yield current_date
            current_date += delta


    for date in date_range(start_date, end_date):
        date = date.strftime("%Y-%m-%d")
        i = 0
        while True:

            request = {
                'startDate' : date,
                'endDate' : date,
                'dimensions' : ["query","page","country","device"],
                "searchType": "Web",
                'rowLimit' : maxRows,
                'startRow' : i * maxRows

            }
			#Query the API for the current data
            response = webmasters_service.searchanalytics().query(siteUrl = SITE_URL, body=request).execute()

			#Most of this code is only useful when debugging locally
            if response is None:
                print("there is no response")
                break
            if 'rows' not in response:
                print("row not in response")
                break
            else:
                for row in response['rows']:
                    keyword = row['keys'][0]
                    page = row['keys'][1]
                    country = row['keys'][2]
                    device = row['keys'][3]
                    output_row = [date, keyword, page, country, device, row['clicks'], row['impressions'], row['ctr'], row['position'],SITE_URL]
                    output_rows.append(output_row)
                i = i + 1

    #A filename for each day this is run
    filename = "gsc_output_" + str(end_date)[0:10] + ".csv"
    
    bucket2 = client.get_bucket('your bucket for output')
    
    blob2 = bucket2.blob(filename)

    #Create a dataframe of the results
    df = pd.DataFrame(output_rows, columns=['date','query','page', 'country', 'device', 'clicks', 'impressions', 'ctr', 'avg_position', 'site'])
    
    #Save the file to a bucket, ignoring the first column, and formatted as text
    blob2.upload_from_string(df.to_csv(index=False), 'text/csv')

    return f'Success!'

