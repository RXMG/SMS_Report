#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 15 11:16:49 2022

@author: liliguo
"""

import pandas as pd
import smtplib 
from email.mime.multipart import MIMEMultipart 
from email.mime.text import MIMEText 
from email.mime.base import MIMEBase 
from email import encoders 
from email.mime.application import MIMEApplication


def send_email(filename,subject,body,toaddr, format = 'plain'):
    
    fromaddr = "schedulealartsrxmg@gmail.com"
    
    # creates SMTP session 
    s = smtplib.SMTP('smtp.gmail.com', 587) 
          
        # start TLS for security 
    s.starttls() 
          
        # Authentication 
    s.login(fromaddr, "yyjetejtjhjhzxxk") 
    
    # instance of MIMEMultipart 
    msg = MIMEMultipart() 
      
    # storing the senders email address   
    msg['From'] = fromaddr 
      
    # storing the receivers email address  
    msg['To'] = toaddr 
      
    # storing the subject  
    msg['Subject'] = subject
      
    # string to store the body of the mail 
    if body == "":
        body ="\n Please see attached file for details. \n Have a fanastic day! \n Bests, \n Lili Guo \n"
      
    # attach the body with the msg instance 
    msg.attach(MIMEText(body, format)) 
      
    # open the file to be sent  


       
    for f in filename or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=f.split('/')[-1]
                )
            # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % f.split('/')[-1]
        msg.attach(part)

      
    # Converts the Multipart msg into a string 
    text = msg.as_string() 
      
    # sending the mail 
    s.sendmail(fromaddr, toaddr, text) 
      
    # terminating the session 
    s.quit() 