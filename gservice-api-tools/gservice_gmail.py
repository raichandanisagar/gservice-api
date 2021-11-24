#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: sagarraichandani
"""

import base64
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import mimetypes
from email import encoders

from gservice_authenticate import GService
import os


class Message(object):
    
    """Create a message for an email.
    
    Arguments:
        email_sender: Sender's address
        email_receiver: Receiver's address
        email_subject: Email subject
        email_body: Email's content body (text only)"""
    
    def __new__(cls, email_sender, email_receiver, email_subject, email_body):
        self = object.__new__(cls)
        self._message = MIMEMultipart()
        self._message['to'] = email_receiver
        self._message['from'] = email_sender
        self._message['subject'] = email_subject
        self._message.attach(MIMEText(email_body))
        return self
    
    def with_attachment(self, email_attachment):
        """Args:
            email_attachment: Path of the file to be attached"""
        
        contentType, encoding = mimetypes.guess_type(email_attachment)
        if contentType is None or encoding is not None:
            contentType = 'application/octet-stream'
        main_type, sub_type = contentType.split('/', 1)
        
        if main_type == 'text':
            fp = open(email_attachment, 'r')
            attach_msg = MIMEText(fp.read(), _subtype=sub_type)
            fp.close()
        elif main_type == 'image':
            fp = open(email_attachment, 'rb')
            attach_msg = MIMEImage(fp.read(), _subtype=sub_type)
            fp.close()
        elif main_type == 'audio':
            fp = open(email_attachment, 'rb')
            attach_msg = MIMEAudio(fp.read(), _subtype=sub_type)
            fp.close()
        else:
            fp = open(email_attachment, 'rb')
            attach_msg = MIMEBase(main_type, sub_type)
            attach_msg.set_payload(fp.read())
            encoders.encode_base64(attach_msg)
            fp.close()
        
        filename = os.path.basename(email_attachment)
        attach_msg.add_header('Content-Disposition', 'attachment', filename=filename)
        self._message.attach(attach_msg)
        return self
    
    def with_html(self, HTML):
        """Args:
            HTML: HTML to add to the email"""
        self._message.attach(MIMEText(HTML,'html'))
        return self
    
    def b64_encode(self):
        """Returns:
            An object containing a base64url encoded email object."""
            
        return {'raw': base64.urlsafe_b64encode(self._message.as_string().encode()).decode()}

class Gmail:
    def __init__(self, gmail_service, user_id):
        """Args:
            gmail_service: API discovery resource object for gmail
            user_id: string, user's email; can take special value 'me'"""
        
        self._gmail_service = gmail_service
        self._user_id = user_id
    
    def send_email(self, message):
        """Args:
            message: MIME base64 encoded message"""
        self._gmail_service.users().messages().send(userId=self._user_id, body=message).execute()
        

if __name__=='__main__':
    gmail_scopes = ["https://mail.google.com/"]
    gmail_service = GService('gmail','v1',gmail_scopes).oauth('OAuthCredentials.json')
    msg = Message('raichandanisagar@gmail.com','sagar.raichandani@blackbuck.com','ABCD','ABCD').\
    with_attachment('/Users/sagarraichandani/Documents/Private/PythonPractice/GoogleAPI_RS/gservice_sheets.py').b64_encode()
    
    py_email = Gmail(gmail_service,'me')
    py_email.send_email(msg)
    
    
    