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
  """Build & authenticate service object to access g-suite--
  Sheets, BigQuery, GoogleAnalytics, Gmail, Drive, etc.
  Args:
      service_name: string, name of the request service API
      version: string, version of request service API
      scopes: list, authentication scopes requested"""
  
  def __init__(self,service_name,version,scopes):
    self.service_name = service_name
    self.version = version
    self.scopes = scopes
    
  def oauth(self,credentials,store_refresh_token=True):
    """Args:
      credentials: JSON file that stores Google's OAuth token
      store_refresh_token: Bool to create a pickle file that stores 
      refresh token to avoid repetitive logins"""
      
    refresh_token=None
    if(store_refresh_token):
      refresh_token='{}{}_rt.pickle'.format(self.service_name,self.version)
    creds = None
    if (refresh_token!=None and os.path.exists(refresh_token)):
      with open(refresh_token, 'rb') as token:
        creds = pickle.load(token)
        
    #If there are no valid credentials in the pickle file
    if not creds or not creds.valid:
      if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
      else:
        flow = InstalledAppFlow.from_client_secrets_file(credentials,self.scopes)
        creds = flow.run_local_server(port=0)
      # Save credentials to pickle for the next run
      if(refresh_token!=None):
        with open(refresh_token, 'wb') as token:
          pickle.dump(creds, token)
    google_service = build(self.service_name,self.version,credentials=creds)
    return google_service
  
  def service_account(self,credentials):
    """credentials: JSON file that stores Service Account credentials"""

    creds = service_account.Credentials.from_service_account_file(credentials,scopes=self.scopes)
    google_service = build(self.service_name,self.version,credentials=creds)
    return google_service
  
  