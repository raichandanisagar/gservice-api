#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: sagarraichandani
"""

from googleapiclient.errors import HttpError
import pandas as pd
import socket

#Set default timeout to 5 mins
socket.setdefaulttimeout(300)

class Spreadsheet:
    """Read, edit and delete data in a google spreadsheet"""
    
    def __init__(self,sheets_service,spreadsheet_id):
        """Args:
            sheets_service: API discovery resource object for google sheets
            spreadsheet_id: str, id of the target spreadsheet"""
        
        self.sheets_service = sheets_service
        self.spreadsheet_id = spreadsheet_id
        
    def _get_column_name(self,num_columns):
        letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        result=[]
        while num_columns:
            num_columns,rem = divmod(num_columns-1,26)
            result[:0] = letters[rem]
        return ''.join(result)
    
    def _build_range(self,row_start,num_columns):
        #column_start assumed to be A
        col_end = self._get_column_name(num_columns)
        return 'A{}:{}'.format(row_start,col_end)
    
        
    def fetch_values(self,sheet_name,range_a1Notation=None):
        """Args:
            sheet_name: str, sheet name of the target file
            range_a1Notation: str, range of values to fetch; default pulls all values from sheet
        Returns:
            response: dict object with the range and values in the sheet"""
        
        if(range_a1Notation==None):
            data_range = sheet_name
        else:
            data_range = '{}!{}'.format(sheet_name,range_a1Notation)
        response = self.sheets_service.spreadsheets().values().get(spreadsheetId=self.spreadsheet_id, range=data_range).execute()
        return response
    
    def create_new_sheet(self,sheet_name):
        body={'requests':[{'addSheet':{'properties':{'title':sheet_name}}}]}
        self.sheets_service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheet_id,body=body).execute()
        print("Created sheet: {}".format(sheet_name))
    
    
    def post_values(self,sheet_name,data,clear_all=False,create_sheet=True):
    
        """Post values passed as an array of arrays to a spreadsheet. Returns None
        Args:
            sheet_name: str, sheet name of the target file
            data: 2d list which contains the information to post
            clear_all: bool, False by default. If set to True clears all the data in sheet"""
            
        if(create_sheet==True):
            try:
                self.sheets_service.spreadsheets().get(spreadsheetId=self.spreadsheet_id,ranges=sheet_name).execute()
            except HttpError:
                self.create_new_sheet(sheet_name)
        
        current_data = self.fetch_values(sheet_name)
        current_data_range = current_data['range']
        try:
            current_num_rows = len(current_data['values'])
        except KeyError:
            current_num_rows = 0
        
        max_num_data_columns = max([len(row) for row in data])
        min_num_data_columns = min([len(row) for row in data])
        
        assert max_num_data_columns==min_num_data_columns, "Irregular data rows. Recheck lengths of each row"
        
        if(clear_all==True):
            self.sheets_service.spreadsheets().values().clear(spreadsheetId=self.spreadsheet_id, 
                                            range=current_data_range).execute()
            target_data_range = '{}!{}'.format(sheet_name,self._build_range(1,max_num_data_columns))
        else:
            target_data_range = '{}!{}'.format(sheet_name,self._build_range(current_num_rows+1,max_num_data_columns))
        
        body =  {'values':data}
        self.sheets_service.spreadsheets().values().update(spreadsheetId=self.spreadsheet_id,body=body,
                                        range=target_data_range,valueInputOption='USER_ENTERED').execute()   


    def push_dataframe(self,sheet_name,dataframe,clear_all=False,create_sheet=True,df_headers=True):
        """Push dataframe object to sheet. Returns None
            Args:
                sheet_name: str, sheet name of the target file
                dataframe: dataframe which contains the information to post
                clear_all: bool, False by default. If set to True clears all the data in sheet
                create_sheet: bool, True by default. Auto create sheet if it does not exist
                df_headers: list, dataframe header control- if set to False, will not add headers to sheet"""
        
        
        data_dict=dataframe.astype(str).fillna('').to_dict('split')
        if(df_headers==False):
            data = data_dict['data']
        else:
            data = [data_dict['columns']]+data_dict['data']
        
        self.post_values(sheet_name,data,clear_all=clear_all,create_sheet=create_sheet)