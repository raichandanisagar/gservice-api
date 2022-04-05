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
      with open(email_attachment, 'r') as fp:
        attach_msg = MIMEText(fp.read(), _subtype=sub_type)
        
    elif main_type == 'image':
      with open(email_attachment, 'rb') as fp:
        attach_msg = MIMEImage(fp.read(), _subtype=sub_type)
        
    elif main_type == 'audio':
      with open(email_attachment, 'rb') as fp:
          attach_msg = MIMEAudio(fp.read(), _subtype=sub_type)
    else:
      with open(email_attachment, 'rb') as fp:
        attach_msg = MIMEBase(main_type, sub_type)
        attach_msg.set_payload(fp.read())
        encoders.encode_base64(attach_msg)
    
    filename = os.path.basename(email_attachment)
    attach_msg.add_header('Content-Disposition', 'attachment', filename=filename)
    self._message.attach(attach_msg)
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
    send_resp = self._gmail_service.users().messages().send(userId=self._user_id, body=message).execute()
    print("Email sent- id: {}".format(send_resp['id']))
    
  