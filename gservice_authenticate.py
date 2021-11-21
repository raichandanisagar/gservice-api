#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: sagarraichandani
"""

import pickle
import os
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2 import service_account


class GService:
    """Build & authenticate object to access different g-suite services--
    Sheets, BigQuery, GoogleAnalytics, Gmail, Drive, etc."""
    
    def __init__(self,service_name,version,scopes):
        """Args:
            service_name: string, name of the request service API
            version: string, version of request service API
            scopes: list, authentication scopes requested"""
        
        self.service_name = service_name
        self.version = version
        self.scopes = scopes
        
    def oauth(self,credentials_filepath,store_refresh_token=True):
        """Args:
            credentials_filepath: JSON file that stores Google's OAuth token
            store_refresh_token: Bool to create a pickle file that stores 
            refresh token to eliminate repetitive logins"""
        
        if(store_refresh_token):
            refresh_token_filepath='{}{}_refresh_token.pickle'.format(self.service_name,self.version)
        else:
            refresh_token_filepath=None
        
        creds = None
        if (refresh_token_filepath!=None and os.path.exists(refresh_token_filepath)):
            with open(refresh_token_filepath, 'rb') as token:
                creds = pickle.load(token)
        
        #If there are no valid credentials in the pickle file
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(credentials_filepath,self.scopes)
                creds = flow.run_local_server(port=0)
            
            # Save the credentials for the next run
            if(refresh_token_filepath!=None):
                with open(refresh_token_filepath, 'wb') as token:
                    pickle.dump(creds, token)
                    
        google_service = build(self.service_name,self.version,credentials=creds)
        return google_service
    
    def service_account(self,credentials_filepath):
        """Args:
            credentials_filepath: JSON file that stores Service Account credentials"""
            
        creds = service_account.Credentials.from_service_account_file(credentials_filepath,scopes=self.scopes)
        google_service = build(self.service_name,self.version,credentials=creds)
        return google_service
    
if __name__=='__main__':
    
    sheets_scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    gmail_scopes = ["https://mail.google.com/"]
    
    sheets = GService('sheets','v4',sheets_scopes).ouuth('OAuthCredentials.json')
    gmail = GService('gmail','v1',gmail_scopes).serviceaccount('ServiceAccountCredentials.json')
    
    
    
    