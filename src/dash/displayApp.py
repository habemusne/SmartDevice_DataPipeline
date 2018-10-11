'''
The file displays the information on the front-end 
of the pipeline
'''
import time
import datetime
import boto3
import json
import random
import sys
import pandas as pd
import io
import dash
import dash_core_components as dcc
import dash_html_components as html
from datetime import date, timedelta

#low-level functional API
client = boto3.client('s3')

#high-level object-oriented API
resource = boto3.resource('s3')

#Today's data
now = datetime.datetime.now()
date = now.strftime("%Y-%m-%d")


#object of your s3 bucket
my_bucket = resource.Bucket('anshu-insight')

#Go to today's riskedUserData folder
prefix = 'riskedUserData_' + date  + '/'


print(prefix)

#Initialize DASH application
app = dash.Dash()

#Data format
#Sr.  ID    Latitude  Longitude  TimeStamp             CurrentHeartRate      
#178  5556  42.1623   -74.1024   1.538841762341346E9   187

x_1 = []
y_1 = []
x_2 = []
y_2 = []
y_min_1 = []
y_max_1 = []
y_min_2 = []
y_max_2 = []
deviceID = 0

riskedPatients = []
df = pd.DataFrame(columns =('DeviceID','Count','Latitude','Longitude','Timestamp','CurrentHeartRate'))
result = pd.DataFrame(columns=('DeviceID','Count','Latitude','Longitude','Timestamp','CurrentHeartRate'))

count = 1;
for obj in my_bucket.objects.filter(Prefix=prefix).all():
    if '.csv' in obj.key:
        df = pd.read_csv(io.BytesIO(obj.get()['Body'].read()))
        if df.empty:
	    continue
        df.columns = ['DeviceID','Count','Latitude','Longitude','Timestamp','CurrentHeartRate']
        #print(df)
        result = result.append(df)
	newValues = df.DeviceID.unique().tolist() 
        riskedPatients = list(set(riskedPatients+newValues))
        #print(len(df))
        

#Find the count of patients having abnormal count
countR = len(riskedPatients)

if result.empty:
    pass
else:
    result_1 = result.loc[result['DeviceID']==riskedPatients[0]]
    result_2 = result.loc[result['DeviceID']==riskedPatients[1]]

print(len(result))
print("p1 is ",riskedPatients[0])
print("p2 is ",riskedPatients[1])
#Get the results for patient 1 for previous 7 days
prev = datetime.datetime.now() - timedelta(1)
date = prev.strftime("%Y-%m-%d")
prefix_prev1 = 'rawData_' + date  + '/'

prev = datetime.datetime.now() - timedelta(2)
date = prev.strftime("%Y-%m-%d")
prefix_prev2 = 'rawData_' + date  + '/'


prev = datetime.datetime.now() - timedelta(3)
date = prev.strftime("%Y-%m-%d")
prefix_prev3 = 'rawData_' + date  + '/'

prev = datetime.datetime.now() - timedelta(4)
date = prev.strftime("%Y-%m-%d")
prefix_prev4 = 'rawData_' + date  + '/'

prev = datetime.datetime.now() - timedelta(5)
date = prev.strftime("%Y-%m-%d")
prefix_prev5 = 'rawData_' + date  + '/'

prev = datetime.datetime.now() - timedelta(6)
date = prev.strftime("%Y-%m-%d")
prefix_prev6 = 'rawData_' + date  + '/'


prev = datetime.datetime.now() - timedelta(7)
date = prev.strftime("%Y-%m-%d")
prefix_prev7 = 'rawData_' + date  + '/'


print(prefix_prev1)
result_prev1_p1 = pd.DataFrame(columns=('DeviceID','Latitude','Longitude','Timestamp','CurrentHeartRate'))
result_prev1_p2 = pd.DataFrame(columns=('DeviceID','Latitude','Longitude','Timestamp','CurrentHeartRate'))
for obj in my_bucket.objects.filter(Prefix=prefix_prev1).all():
    if '.csv' in obj.key:
        df = pd.read_csv(io.BytesIO(obj.get()['Body'].read()))
        if df.empty:
	    continue
        #print(df)
        df.columns = ['DeviceID','Latitude','Longitude','Timestamp','CurrentHeartRate']
        result_p1 = df.loc[df['DeviceID']==riskedPatients[0]]
	print("result_p1",len(result_p1))
        result_p2 = df.loc[df['DeviceID']==riskedPatients[1]]
	print("result_p2",len(result_p2))
        
        result_prev1_p1 = result_prev1_p1.append(result_p1)
        result_prev1_p2 = result_prev1_p2.append(result_p2)
        print(1,len(result_prev1_p1))
        print(1,len(result_prev1_p2))



result_prev1_p1['CurrentHeartRate'].to_csv("patient1_mon.csv", encoding='utf-8', index=False)
result_prev1_p2['CurrentHeartRate'].to_csv("patient2_mon.csv", encoding='utf-8', index=False)

result_prev2_p1 = pd.DataFrame(columns=('DeviceID','Latitude','Longitude','Timestamp','CurrentHeartRate'))
result_prev2_p2 = pd.DataFrame(columns=('DeviceID','Latitude','Longitude','Timestamp','CurrentHeartRate'))
for obj in my_bucket.objects.filter(Prefix=prefix_prev2).all():
    if '.csv' in obj.key:
        df = pd.read_csv(io.BytesIO(obj.get()['Body'].read()))
        if df.empty:
	    continue
        #print(df)
        df.columns = ['DeviceID','Latitude','Longitude','Timestamp','CurrentHeartRate']
        result_p1 = df.loc[df['DeviceID']==riskedPatients[0]]
        print("result_p1",len(result_p1))
        result_p2 = df.loc[df['DeviceID']==riskedPatients[1]]
        print("result_p2",len(result_p2))

        result_prev2_p1 = result_prev2_p1.append(result_p1)
        result_prev2_p2 = result_prev2_p2.append(result_p2)
        print(2,len(result_prev2_p1))
        print(2,len(result_prev2_p2))

result_prev2_p1['CurrentHeartRate'].to_csv("patient1_tues.csv", encoding='utf-8', index=False)
result_prev2_p2['CurrentHeartRate'].to_csv("patient2_tues.csv", encoding='utf-8', index=False)

result_prev3_p1 = pd.DataFrame(columns=('DeviceID','Latitude','Longitude','Timestamp','CurrentHeartRate'))
result_prev3_p2 = pd.DataFrame(columns=('DeviceID','Latitude','Longitude','Timestamp','CurrentHeartRate'))
for obj in my_bucket.objects.filter(Prefix=prefix_prev3).all():
    if '.csv' in obj.key:
        df = pd.read_csv(io.BytesIO(obj.get()['Body'].read()))
        if df.empty:
	    continue
        df.columns = ['DeviceID','Latitude','Longitude','Timestamp','CurrentHeartRate']
        result_p1 = df.loc[df['DeviceID']==riskedPatients[0]]
        print("result_p1",len(result_p1))
        result_p2 = df.loc[df['DeviceID']==riskedPatients[1]]
        print("result_p2",len(result_p2))

        result_prev3_p1 = result_prev3_p1.append(result_p1)
        result_prev3_p2 = result_prev3_p2.append(result_p2)
        print(3,len(result_prev3_p1))
        print(3,len(result_prev3_p2))

result_prev3_p1['CurrentHeartRate'].to_csv("patient1_wed.csv", encoding='utf-8', index=False)
result_prev3_p2['CurrentHeartRate'].to_csv("patient2_wed.csv", encoding='utf-8', index=False)

result_prev4_p1 = pd.DataFrame(columns=('DeviceID','Latitude','Longitude','Timestamp','CurrentHeartRate'))
result_prev4_p2 = pd.DataFrame(columns=('DeviceID','Latitude','Longitude','Timestamp','CurrentHeartRate'))
for obj in my_bucket.objects.filter(Prefix=prefix_prev4).all():
    if '.csv' in obj.key:
        df = pd.read_csv(io.BytesIO(obj.get()['Body'].read()))
        if df.empty:
	    continue
        df.columns = ['DeviceID','Latitude','Longitude','Timestamp','CurrentHeartRate']
        result_p1 = df.loc[df['DeviceID']==riskedPatients[0]]
        print("result_p1",len(result_p1))
        result_p2 = df.loc[df['DeviceID']==riskedPatients[1]]
        print("result_p2",len(result_p2))

        result_prev4_p1 = result_prev4_p1.append(result_p1)
        result_prev4_p2 = result_prev4_p2.append(result_p2)
        print(4,len(result_prev4_p1))
        print(4,len(result_prev4_p2))

result_prev4_p1['CurrentHeartRate'].to_csv("patient1_thu.csv", encoding='utf-8', index=False)
result_prev4_p2['CurrentHeartRate'].to_csv("patient2_thu.csv", encoding='utf-8', index=False)

result_prev5_p1 = pd.DataFrame(columns=('DeviceID','Latitude','Longitude','Timestamp','CurrentHeartRate'))
result_prev5_p2 = pd.DataFrame(columns=('DeviceID','Latitude','Longitude','Timestamp','CurrentHeartRate'))
for obj in my_bucket.objects.filter(Prefix=prefix_prev5).all():
    if '.csv' in obj.key:
        df = pd.read_csv(io.BytesIO(obj.get()['Body'].read()))
        if df.empty:
	    continue
        df.columns = ['DeviceID','Latitude','Longitude','Timestamp','CurrentHeartRate']
        result_p1 = df.loc[df['DeviceID']==riskedPatients[0]]
        print("result_p1",len(result_p1))
        result_p2 = df.loc[df['DeviceID']==riskedPatients[1]]
        print("result_p2",len(result_p2))

        result_prev5_p1 = result_prev5_p1.append(result_p1)
        result_prev5_p2 = result_prev5_p2.append(result_p2)
        print(5,len(result_prev5_p1))
        print(5,len(result_prev5_p2))

result_prev5_p1['CurrentHeartRate'].to_csv("patient1_fri.csv", encoding='utf-8', index=False)
result_prev5_p2['CurrentHeartRate'].to_csv("patient2_fri.csv", encoding='utf-8', index=False)



result_prev6_p1 = pd.DataFrame(columns=('DeviceID','Latitude','Longitude','Timestamp','CurrentHeartRate'))
result_prev6_p2 = pd.DataFrame(columns=('DeviceID','Latitude','Longitude','Timestamp','CurrentHeartRate'))
for obj in my_bucket.objects.filter(Prefix=prefix_prev6).all():
    if '.csv' in obj.key:
        df = pd.read_csv(io.BytesIO(obj.get()['Body'].read()))
        if df.empty:
	    continue
        df.columns = ['DeviceID','Latitude','Longitude','Timestamp','CurrentHeartRate']
        result_p1 = df.loc[df['DeviceID']==riskedPatients[0]]
        print("result_p1",len(result_p1))
        result_p2 = df.loc[df['DeviceID']==riskedPatients[1]]
        print("result_p2",len(result_p2))

        result_prev6_p1 = result_prev6_p1.append(result_p1)
        result_prev6_p2 = result_prev6_p2.append(result_p2)
        print(6,len(result_prev6_p1))
        print(6,len(result_prev6_p2))

result_prev6_p1['CurrentHeartRate'].to_csv("patient1_sat.csv", encoding='utf-8', index=False)
result_prev6_p2['CurrentHeartRate'].to_csv("patient2_sat.csv", encoding='utf-8', index=False)

result_prev7_p1 = pd.DataFrame(columns=('DeviceID','Latitude','Longitude','Timestamp','CurrentHeartRate'))
result_prev7_p2 = pd.DataFrame(columns=('DeviceID','Latitude','Longitude','Timestamp','CurrentHeartRate'))
for obj in my_bucket.objects.filter(Prefix=prefix_prev7).all():
    if '.csv' in obj.key:
        df = pd.read_csv(io.BytesIO(obj.get()['Body'].read()))
        if df.empty:
	    continue
        df.columns = ['DeviceID','Latitude','Longitude','Timestamp','CurrentHeartRate']
        result_p1 = df.loc[df['DeviceID']==riskedPatients[0]]
        print("result_p1",len(result_p1))
        result_p2 = df.loc[df['DeviceID']==riskedPatients[1]]
        print("result_p2",len(result_p2))

        result_prev7_p1 = result_prev7_p1.append(result_p1)
        result_prev7_p2 = result_prev7_p2.append(result_p2)
        print(7,len(result_prev7_p1))
        print(7,len(result_prev7_p2))


result_prev7_p1['CurrentHeartRate'].to_csv("patient1_sun.csv", encoding='utf-8', index=False)
result_prev7_p2['CurrentHeartRate'].to_csv("patient2_sun.csv", encoding='utf-8', index=False)

for i in range(10 * len(result_1)):
    x_1.append(i)

y_1 = result_1['CurrentHeartRate'].tolist()

for i in range(10):
    y_1.extend(y_1)


#Sunday
y_sunday = []
hr = 70
for i in range(10 * len(result_1)):
    if hr < 73:
       y_sunday.append(hr)
       hr = hr + 1
    else:
	hr = 70
	y_sunday.append(hr)

#Monday
y_monday = []
hr = 71
for i in range(8 * len(result_1)):
    if hr < 73:
       y_monday.append(hr)
       hr = hr + 1
    else:
        hr = 70
        y_monday.append(hr)

hr=150
for i in range(2 * len(result_1)):
    if hr < 152:
       y_monday.append(hr)
       hr = hr + 1
    else:
        hr = 150
        y_monday.append(hr)
hr=80
for i in range(10 * len(result_1)):
    if hr < 83:
       y_monday.append(hr)
       hr = hr + 1
    else:
        hr = 83
        y_monday.append(hr)


for i in range(10 * len(result_2)):
    x_2.append(i)


#Tuesday
for i in range(7 * len(result_1)):
    if hr < 83:
       y_2.append(hr)
       hr = hr + 1
    else:
        hr = 83
        y_2.append(hr)


for i in range(10):
    y_2.extend(result_2['CurrentHeartRate'].tolist())

for i in range(3 * len(result_1)):
    if hr < 83:
       y_2.append(hr)
       hr = hr + 1
    else:
        hr = 83
        y_2.append(hr)



#Min Max range
for i in range(10 *len(result_1)):
    y_min_1.append(60)

for i in range(10 * len(result_1)):
    y_max_1.append(150)


for i in range(10 * len(result_2)):
    y_min_2.append(70)

for i in range(10 * len(result_2)):
    y_max_2.append(140)


#print(len(x))
#print(len(y))


#Convert the list into string
patients = '\n'.join(str(x) for x in riskedPatients)

def display_risked_users():
    return html.Div(className='col',
             children=[
		dcc.Textarea(
		id='List-patient',
    		value= patients,
    		disabled=True,
                style={'width': '100%','height': '100%'},
		
		)])



def display_data_1():
    return html.Div(className='col',
             children=[
                 dcc.Graph(
                     id='minute-graph_1',
                     figure={
                         'data': [
                             {'x': x_1,
                              'y': y_1,
                              'type': 'line', 'name': 'heart rate in real time'},
                             {'x': x_1,
                              'y': y_min_1,
                              'type': 'line', 'name': 'min heart rate of the patient with id ' + str(riskedPatients[0])},
                             {'x': x_1,
                              'y': y_max_1,
                              'type': 'line', 'name': 'max heart rate of the patient with id ' + str(riskedPatients[0])},
                         ],
                         'layout': {
                             'title': 'Real time v/s Desired state of the heart'
                         }
                     }
                 )]) 

def display_data_71():
    return html.Div(className='col',
             children=[
                 dcc.Graph(
                     id='graph_7',
                     figure={
                         'data': [
                             {'x': x_1,
                              'y': result_prev1_p1['CurrentHeartRate'].tolist(),
                              'type': 'line', 'name': 'heart rate pattern on Monday'},
                             {'x': x_1,
                              'y': result_prev2_p1['CurrentHeartRate'].tolist(),
                               'type': 'line', 'name': 'heart rate pattern on Tuesday'},
                             {'x': x_1,
                              'y': result_prev3_p1['CurrentHeartRate'].tolist(),
                               'type': 'line', 'name': 'heart rate pattern on Wednesday'},
                             {'x': x_1,
                              'y': result_prev4_p1['CurrentHeartRate'].tolist(),
                               'type': 'line', 'name': 'heart rate pattern on Thursday'},
                             {'x': x_1,
                              'y': result_prev5_p1['CurrentHeartRate'].tolist(),
                               'type': 'line', 'name': 'heart rate pattern on Friday'},
                             {'x': x_1,
                              'y': result_prev6_p1['CurrentHeartRate'].tolist(),
                               'type': 'line', 'name': 'heart rate pattern on Saturday'},
                             {'x': x_1,
                              'y': result_prev7_p1['CurrentHeartRate'].tolist(),
                               'type': 'line', 'name': 'heart rate pattern on Sunday'},
                             {'x': x_1,
                              'y': y_min_1,
                              'type': 'line', 'name': 'min heart rate of the patient with id ' + str(riskedPatients[0])},
                             {'x': x_1,
                              'y': y_max_1,
                              'type': 'line', 'name': 'max heart rate of the patient with id ' + str(riskedPatients[0])},
                         ],
                         'layout': {
                             'title': 'Real time v/s Desired state of the heart'
                         }
                     }
                 )]) 


def display_data_72():
    return html.Div(className='col',
             children=[
                 dcc.Graph(
                     id='graph_72',
                     figure={
                         'data': [
                             {'x': x_1,
                              'y': result_prev1_p2['CurrentHeartRate'].tolist(),
                              'type': 'line', 'name': 'heart rate pattern on Monday'},
                             {'x': x_1,
                              'y': result_prev2_p2['CurrentHeartRate'].tolist(),
                               'type': 'line', 'name': 'heart rate pattern on Tuesday'},
                             {'x': x_1,
                              'y': result_prev4_p2['CurrentHeartRate'].tolist(),
                               'type': 'line', 'name': 'heart rate pattern on Wednesday'},
                             {'x': x_1,
                              'y': result_prev3_p2['CurrentHeartRate'].tolist(),
                               'type': 'line', 'name': 'heart rate pattern on Thursday'},
                             {'x': x_1,
                              'y': result_prev5_p2['CurrentHeartRate'].tolist(),
                               'type': 'line', 'name': 'heart rate pattern on Friday'},
                             {'x': x_1,
                              'y': result_prev6_p2['CurrentHeartRate'].tolist(),
                               'type': 'line', 'name': 'heart rate pattern on Saturday'},
                             {'x': x_1,
                              'y': result_prev7_p2['CurrentHeartRate'].tolist(),
                               'type': 'line', 'name': 'heart rate pattern on Sunday'},
                             {'x': x_1,
                              'y': y_min_1,
                              'type': 'line', 'name': 'min heart rate of the patient with id ' + str(riskedPatients[1])},
                             {'x': x_1,
                              'y': y_max_1,
                              'type': 'line', 'name': 'max heart rate of the patient with id ' + str(riskedPatients[1])},
                         ],
                         'layout': {
                             'title': 'Real time v/s Desired state of the heart'
                         }
                     }
                 )])



def display_data_2():
    return html.Div(className='col',
             children=[
                 dcc.Graph(
                     id='minute-graph_2',
                     figure={
                         'data': [
                             {'x': x_2,
                              'y': y_2,
                              'type': 'line', 'name': 'heart rate in real time'},
                             {'x': x_2,
                              'y': y_min_2,
                              'type': 'line', 'name': 'min heart rate of the patient with id '+ str(riskedPatients[1])},
                             {'x': x_2,
                              'y': y_max_2,
                              'type': 'line', 'name': 'max heart rate of the patient with id'+ str(riskedPatients[1])},
                         ],
                         'layout': {
                             'title': 'Real time v/s Desired state of the heart'
                         }
                     }
                 )]) 

'''
def display_data_for_a_day():
     return html.Div(className='col',
             children=[
                 dcc.Graph(
                     id='day-graph',
                     figure={
                         'data': [
                             {'x': x,
                              'y': y_mon,
                              'type': 'line', 'name': 'heart rate values on monday'},
                             {'x': x,
                              'y': y_min,
                              'type': 'line', 'name': 'min heart rate  the patient with id ' + },
                             {'x': x,
                              'y': y_max,
                              'type': 'line', 'name': 'max heart rate value of the patient'},
                             {'x': x,
                              'y': y_tues,
                              'type': 'line', 'name': 'heart rate values on tuesday'}
                         ],
                         'layout': {
                             'title': 'Real time v/s Desired state of the heart'
                         }
                     }
                 )])

'''

app.layout = html.Div(children=[
    html.H1(children='HEART WATCH'),

    html.Div(children=['''
        List of the patients with anomalies detected in their heart rates currently 
    ''']),

   html.Div(className='flex-grid',
             children=[
                 display_risked_users(),
                 display_data_71(),
                 display_data_72()
             ]),

    #dcc.Dropdown(
     #   id='my-dropdown',
      #  options=[
       #     {'label': str(patients[0]), 'value': str(patients[0])},
        #    {'label': str(patients[1]), 'value': str(patients[1])},
         #   {'label': str(patients[2]), 'value': str(patients[2])}
        #],
        #value= str(patients[0])
    #),
    #html.Div(id='output-container'),

])


#@app.callback(
 #   dash.dependencies.Output('output-container', 'children'),
  #  [dash.dependencies.Input('my-dropdown', 'value')])
#def update_output(value):
    #return display_data_7(value)
#    return 'You have selected "{}"'.format(value)


if __name__ == '__main__':
    app.run_server(debug=True,host='ec2-34-192-63-158.compute-1.amazonaws.com',port=80)
