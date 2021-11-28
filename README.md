### gservice-api
Personal project to create a python package that makes working with Google APIs simpler.

### Features
- Support oauth and service account authentication for host of g-suite services- Sheets, BigQuery, GoogleAnalytics, Gmail, Drive, etc. <br>
- Read, edit and delete data in a google spreadsheet. <br>
- Fetch query results, create & manage datasets, tables and tabledata in BigQuery. <br>
- Send and manage email from gmail.<br>
- Fetch and store google analytics reports.<br>


### Usage
pip install git+https://github.com/raichandanisagar/gservice-api

Once installed, import methods/functions from the different packages to autheticate g-suite services and utilize the aforementioned featues.


### Example

from gservice_api_tools import gservice_authenticate as gsa, gservice_bigquery as gbq

bigquery_scopes = ['https://www.googleapis.com/auth/bigquery']<br>
bigquery_service = GService('bigquery','v2',bigquery_scopes).oauth('OAuthCredentials.json')

bigquery_project = gbq().Bigquery(bigquery_service,'PROJECT_NAME')<br>
query = #Query string; example- 'SELECT * FROM bigquery-public-data.usa_names.usa_1910_2013 limit 10'<br>
query_response = bq.fetch_query_results(query)
