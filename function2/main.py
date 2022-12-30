import time
import pandas as pd
import re
import datetime
from google.cloud import bigquery
from deep_translator import GoogleTranslator
import pandas_gbq

def translateandSA(fromdate,todate):
    
    #this function will 
    # 1 - query the data from BigQuery
    # 2 - remove features and normalize tweets
    # 3 - translate all tweets to english
    # 4 - run sentiment analysis and
    # 5 - store result in anwaribrahim_en table
    
    #requirements
    #pip install pandas
    #pip install pandas_gbq
    #pip install google-cloud-bigquery
    #pip install 'google-cloud-bigquery[pandas]'
    #pip install db-dtypes
    #pip install deep_translator
    #pip instasll textblob
    
    # get the start time
    st = time.time()

    def resolve_emoticon(line):
        emoticon = {
            ':-)' : 'smile',
            ':))' : 'very happy',
            ':)'  : 'happy',
            ':((' : 'very sad',
            ':('  : 'sad',
            ':-P' : 'tongue',
            ':-o' : 'gasp',
            '>:-)': 'angry'
       }   
        for key in emoticon:
            line = line.replace(key, emoticon[key])
        return line
    def remove_features(data_str):
        url_re = re.compile(r'https?://(www.)?\w+\.\w+(/\w+)*/?')    
        #mention_re = re.compile(r'@|#(\w+)') 
        mention_re = re.compile(r"@[A-Za-z0-9_]+") 
        RT_re = re.compile(r'RT(\s+)')
        num_re = re.compile(r'(\d+)')

        data_str = str(data_str)
        data_str = RT_re.sub(' ', data_str)  
        data_str = data_str.lower()  
        data_str = url_re.sub(' ', data_str)   
        data_str = mention_re.sub(' ', data_str)  
        data_str = num_re.sub(' ', data_str)
        return data_str
    def sentiment(text):
        if text > 0:
            return 'Positive'
        elif text < 0:
            return 'Negative'
        else: return 'Neutral'

    #query from BigQuery
    #print('Authenticating Google account')
    #from google.oauth2 import service_account
    #project_id = 'eltsa-371008'
    #credentials = service_account.Credentials.from_service_account_file('eltsa-371008-0f01b403d047.json')
    print(f'Querying from BigQuery {fromdate} to {todate}')    
    #client=bigquery.Client(credentials=credentials)
    client=bigquery.Client()
    sql = f'SELECT Date_Created,Tweet from `eltsa-371008.alltweets.anwaribrahim` where Date_Created between "{fromdate}" AND "{todate}" ORDER BY DATE_CREATED ASC'
    df = client.query(sql).to_dataframe()
    print('Querying complete. removing features')
    print(df.info())
    #remove feature
    df['Tweet'] = df['Tweet'].map(lambda x: remove_features(x)).map(lambda x: resolve_emoticon(x))
    df1 = df
    print('Translating the text')
    df1['Tweet'] = df['Tweet'].apply(lambda x: GoogleTranslator(source='ms', target='en').translate(text=x))
    print('Translation completed')
    print('Analyzing sentiment')
    from textblob import TextBlob
    tweettextpolarity = df1['Tweet'].map(lambda x:TextBlob(str(x)).sentiment.polarity)
    tweettextpolarity.name='Polarity'
    SA = tweettextpolarity.map(lambda x:sentiment(x))
    SA.name='Sentiment'
    result = pd.concat([df1, tweettextpolarity,SA], axis=1)
    print('Sentiment analysis completed')
    #store translated tweets at bigquery  
    print('Exporting to BigQuery')
    project_id = "eltsa-371008"
    table_id = 'eltsa-371008.alltweets.anwaribrahim_en'
    #table_id = 'eltsa-371008.alltweets.testtableforVM'
    

    pandas_gbq.to_gbq(result, table_id, project_id=project_id, if_exists='append')
    print('Export completed')

    # get the end time
    et = time.time()

    # get the execution time
    elapsed_time = et - st
    print('Execution time:', elapsed_time, 'seconds')
   
#get todays date
today_date = datetime.datetime.utcnow()
today_date_str = today_date.strftime("%Y-%m-%d")
request_date = datetime.datetime.utcnow() - datetime.timedelta(days=1)
request_date_str = request_date.strftime("%Y-%m-%d")

translateandSA(f'{request_date_str} 00:00:00 UTC',f'{request_date_str} 02:00:00 UTC')
translateandSA(f'{request_date_str} 02:00:00 UTC',f'{request_date_str} 04:00:00 UTC')
translateandSA(f'{request_date_str} 04:00:00 UTC',f'{request_date_str} 06:00:00 UTC')
translateandSA(f'{request_date_str} 06:00:00 UTC',f'{request_date_str} 08:00:00 UTC')
translateandSA(f'{request_date_str} 08:00:00 UTC',f'{request_date_str} 10:00:00 UTC')
translateandSA(f'{request_date_str} 10:00:00 UTC',f'{request_date_str} 12:00:00 UTC')
translateandSA(f'{request_date_str} 12:00:00 UTC',f'{request_date_str} 14:00:00 UTC')
translateandSA(f'{request_date_str} 14:00:00 UTC',f'{request_date_str} 16:00:00 UTC')
translateandSA(f'{request_date_str} 16:00:00 UTC',f'{request_date_str} 18:00:00 UTC')
translateandSA(f'{request_date_str} 18:00:00 UTC',f'{request_date_str} 20:00:00 UTC')
translateandSA(f'{request_date_str} 20:00:00 UTC',f'{request_date_str} 22:00:00 UTC')
translateandSA(f'{request_date_str} 22:00:00 UTC',f'{today_date_str} 00:00:00 UTC')