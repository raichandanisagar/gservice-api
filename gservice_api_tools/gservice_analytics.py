#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: sagarraichandani
"""

import datetime as dt
import pandas as pd

google_analytics_scopes = ['https://www.googleapis.com/auth/analytics.readonly']
analytics_service = GService('analyticsreporting','v4',google_analytics_scopes).oauth('OAuthCredentials.json')

class GoogleAnalytics:
    """Fetch and store google analytics reports"""
    
    def __init__(self, analytics_service):
        """Args:
            analytics_service: API discovery resource object for google analytics"""
            
        self._analytics = analytics_service
    
    @staticmethod
    def _parse_headers(column_header):
        """Returns a list of requested dimension and metric headers from columnHeader object
        https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet#ColumnHeader"""
        headers = []
        headers.extend(column_header['dimensions'])
        headers.extend([metric_header['name'] for metric_header in column_header['metricHeaderEntries']])
        return headers
    
    @staticmethod
    def _parse_report_rows(report_rows):
        """Returns a 2d list of metrics from rows object
        https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet#ReportData"""
        data_rows = []
        for row in report_rows:
            data_row = []
            data_row.extend(row['dimensions'])
            data_row.extend([metric_vals for metric_vals in row['metrics'][0]['values']])
            data_rows.append(data_row)
        return data_rows
    
    def _fetch_report(self, request_body):
        """Makes the API call and returns a tuple of headers and rows"""
        return self._analytics.reports.batchGet(body=request_body).execute()
        
    
    def fetch_report(self, view_id, report_request_data, date_range=None, fetch_by_day=True,
                     sampling='LARGE', page_size=100000):
        """Fetches report data from google analytics and returns a pandas dataframe
        Args:
            view_id: str, GA view id to pull data from
            report_request_data: dict, information of the dimensions, metrics, filters, segments to be queried
            date_range: tuple of datetime objects (start,end); GA assumes 7 days by default
            fetch_by_day: bool, fetch data sequentially by days to avoid sampling
            sampling: str, report's sample size. Lower threshold furnishes report data faster
                https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet#Sampling
            page_size: int, number of rows to fetch
            """
        
        
        ga_data_params = ['dimensions', 'dimensionFilterClauses', 'metrics', 'metricFilterClauses', 
                             'filtersExpression', 'orderBys', 'segments', 'pivots', 'cohortGroup']
        
        if(not(set(report_request_data.keys()).issubset(ga_data_params))):
            unexpected_params_str = ','.join(set(report_request_data.keys())-set(ga_data_params))
            ga_params_str = ','.join(ga_data_params)
            raise ValueError("Data params %s not in expected %s" %(unexpected_params_str,ga_params_str))
            
        if(date_range!=None and (not(isinstance(date_range[0],dt.datetime) and isinstance(date_range[1],dt.datetime)))):
            raise ValueError("Improper date range value! Check values: {}, {}".format(date_range[0],date_range[1]))
        
        if(sampling not in ('LARGE','SMALL','DEFAULT')):
            raise ValueError("Improper sampling value. Expected in ('LARGE','SMALL','DEFAULT')")
        
        start_date = date_range[0]
        end_date = date_range[1]
        assert start_date<=end_date, "Start date must be prior to end date" 
        num_days = (end_date-start_date).days+1
        
        report_request_data['viewId']= view_id
        report_request_data['samplingLevel']= sampling
        report_request_data['pageSize']= page_size
        report_request = {'reportRequests':[report_request_data]}
        
        result_rows = []
        
        if(fetch_by_day):
            for i in range(num_days):
                query_date = (start_date+dt.timedelta(i)).strftime('%Y-%m-%d')
                report_request['reportRequests'][0]['dateRanges']=[{'startDate':query_date,'endDate':query_date}]
                
                while True:
                    report_response = self._fetch_report(report_request)
                    report_rows = self._parse_rows(report_response['reports'][0]['data']['rows'])
                    result_rows.extend(report_rows)
                    
                    if(bool(report_response.get('nextPageToken'))):
                        report_request['reportRequests'][0]['pageToken'] = report_response['nextPageToken']
                    else:
                        break
                print("Completed data pull for {}".format(query_date))
                    
            #Report headers can be accessed from the last iteration's response object    
            report_headers = self._parse_headers(report_response['reports'][0]['columnHeader'])
            result_dataframe = pd.DataFrame(data=result_rows,columns=report_headers)
            
        
        else:
            report_request['reportRequests'][0]['dateRanges']=[{'startDate':start_date.strftime('%Y-%m-%d'),
                                                                'endDate':end_date.strftime('%Y-%m-%d')}]
            
            while True:
                report_response = self._fetch_report(report_request)
                report_rows = self._parse_rows(report_response['reports'][0]['data']['rows'])
                result_rows.extend(report_rows)
                
                if(bool(report_response.get('nextPageToken'))):
                    report_request['reportRequests'][0]['pageToken'] = report_response['nextPageToken']
                else:
                    break
            print("Completed data pull for {}-{}".format(start_date.strftime('%Y-%m-%d'),end_date.strftime('%Y-%m-%d')))
                
            #Report headers can be accessed from the last iteration's response object    
            report_headers = self._parse_headers(report_response['reports'][0]['columnHeader'])
            result_dataframe = pd.DataFrame(data=result_rows,columns=report_headers)
            
        return result_dataframe
    
            