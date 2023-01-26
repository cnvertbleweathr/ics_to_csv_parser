#IMPORT LIBS, SET VARS
import boto3 as boto
import requests
from icalendar import Calendar, Event
import pandas as pd
from pathlib import Path

s3_bucket = 'mtln-partnersuccess'
s3_filename = 'kgCalendar'
s3 = boto.client('s3')
s3_r = boto.resource('s3')
bucket = s3_r.Bucket(s3_bucket)
ec2_user = 'matillion_user'
ec2_pass = 'Matillion_123'


#Download ICS file with calendar info to tmp directory
for key in bucket.objects.all():
  if s3_filename+".ics" in key.key:
    s3dir_s3file = key.key
    s3file = s3dir_s3file
    localdir_s3file = '/tmp/' + s3file
    s3.download_file(s3_bucket, s3dir_s3file, localdir_s3file)
    print('File: '+s3file+ ' saved to: '+ localdir_s3file)

    
#Parse ICR file, load to list

subject_list = []
date_list = []
attendee_list = []

with open(localdir_s3file, mode='r', encoding='utf-8') as f:
  gcal = Calendar.from_ical(f.read())
  for component in gcal.walk():
    if component.name == "VEVENT":
      #print (component) #GET ALL DATA FROM EACH EVENT
      summary = component.get('summary')
      attendees = component.get('attendee')
      date = component.get('dtstart').dt
      if type(attendees) == list:
        for each in attendees:
          ind_attendee = each.split(':')[1]#[:-3]
          attendee_list.append(ind_attendee)
          subject_list.append(summary)
          date_list.append(date)


# Create dataframe with info, populate CSV, load to S3

data = {'Subject':subject_list, 'Date': date_list, 'Attendees':attendee_list}
df = pd.DataFrame(data)

filename = s3_filename+'.csv'

print(df)
filepath = Path('/tmp/'+filename)
df.to_csv(filepath,index=False)

s3 = boto.client('s3')
s3.upload_file('/tmp/'+filename,s3_bucket, filename)
f.close()
