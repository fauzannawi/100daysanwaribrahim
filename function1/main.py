import base64

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)

import datetime
import pandas as pd
import snscrape.modules.twitter as sntwitter

today_date = datetime.datetime.utcnow()
today_date_str = today_date.strftime("%Y-%m-%d")
request_date = datetime.datetime.utcnow() - datetime.timedelta(days=1)
request_date_str = request_date.strftime("%Y-%m-%d")
y = datetime.datetime(int(request_date.strftime("%Y")),  int(request_date.strftime("%m")),  int(request_date.strftime("%d")), hour=00, minute=00, second=00, microsecond=0, tzinfo=datetime.timezone.utc, fold=0)
#define keyword
keyword = 'anwaribrahim'

# Creating list to append tweet data to
attributes_container = []

# Using TwitterSearchScraper to scrape data and append tweets to list
print("Aggregating tweets. This may take a while.")
for i,tweet in enumerate(sntwitter.TwitterSearchScraper(keyword+' since:'+request_date_str+' until:'+today_date_str).get_items()):
    if i>100000 or tweet.date ==y:
        break
    attributes_container.append([tweet.date, tweet.content])
    
# Creating a dataframe to load the list
tweets_df = pd.DataFrame(attributes_container, columns=["Date Created", "Tweet"])
#reverse row order
tweets_df = tweets_df.loc[::-1]

#export to google cloud bucket
from google.cloud import storage
client = storage.Client()
source_file_name=f'{keyword}-{request_date_str}.csv'
destination_blob_name='alltweets-bucket'
bucket = client.bucket(destination_blob_name)
blob = bucket.blob(source_file_name)
blob.upload_from_string(tweets_df.to_csv(index=False), 'text/csv')
print('File {} uploaded to {}.'.format(
      source_file_name,
      destination_blob_name))

#copy data from google storage bucket to bigquery
from google.cloud import bigquery
client = bigquery.Client()
dataset_id = 'alltweets'
dataset_ref = client.dataset(dataset_id)
table_id = 'anwaribrahim'
#### COPY FROM GOOGLE SORAGE TO BIGQUERY, DUNIT
job_config = bigquery.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
job_config.schema = [
    bigquery.SchemaField("Date_Created", "TIMESTAMP"),
    bigquery.SchemaField("Tweet", "STRING"),
]
job_config.skip_leading_rows = 1
job_config.allow_quoted_newlines = True
# The source format defaults to CSV, so the line below is optional.
job_config.source_format = bigquery.SourceFormat.CSV
uri = f'gs://{destination_blob_name}/{source_file_name}'

load_job = client.load_table_from_uri(
    uri, dataset_ref.table(table_id), job_config=job_config
)  # API request
print("Starting job {}".format(load_job.job_id))

load_job.result()  # Waits for table load to complete.
print("Job finished.")

destination_table = client.get_table(dataset_ref.table(table_id))
print("Loaded {} rows.".format(destination_table.num_rows))
