import pandas as pd
import numpy as np
import datetime
from datetime import datetime
import math
from sklearn.impute import KNNImputer



flow1 = pd.read_excel (r'C:\\Users\\313516\\Documents\\Non work\\Other\\MM\\technical-test\\flow\\flow_1.xlsx')

imputer = KNNImputer(n_neighbors=4, weights = 'distance')
imputed = imputer.fit_transform(flow1)
flow1_imputed = pd.DataFrame(imputed, columns=flow1.columns)

flow1['imputedValues'] = flow1_imputed['value']
flow1['imputedTime'] = flow1_imputed['time']

flow1 = flow1.dropna(subset=['time'])

flow1.drop("imputedTime", inplace = True, axis = 1)

readableDateList = []
for i in range(len(flow1)):
	readableDateList.append(datetime.utcfromtimestamp(float(flow1['time'].iloc[i])).strftime('%Y-%m-%d %H:%M:%S'))

flow1['readableTime'] = np.array(readableDateList)

flow1.drop("time", inplace = True, axis = 1)
flow1.drop("value", inplace = True, axis = 1)

# print("Highest allowed",flow1['value'].mean() + 3.5*flow1['value'].std())

flow1 = flow1[(flow1['imputedValues'] < flow1['imputedValues'].mean() + 3.5*flow1['imputedValues'].std()) & (flow1['imputedValues'] >= 0)]
flow1.rename(columns={'imputedValues':'Values'}, inplace=True)
flow1['file'] = "flow1"
# print(flow1.head())

# flow1.to_csv(r'C:\\Users\\313516\\Documents\\Non work\\Other\\MM\\technical-test\\flow\\flow1Fixed.csv', index = False)





flow2 = pd.read_excel (r'C:\\Users\\313516\\Documents\\Non work\\Other\\MM\\technical-test\\flow\\flow_2.xlsx')


imputer = KNNImputer(n_neighbors=4, weights = 'distance')
imputed = imputer.fit_transform(flow2)
flow2_imputed = pd.DataFrame(imputed, columns=flow2.columns)

flow2['imputedValues'] = flow2_imputed['value']
flow2['imputedTime'] = flow2_imputed['time']

# flow2 = flow2.dropna(subset=['time'])

flow2.drop("imputedTime", inplace = True, axis = 1)

readableDateList = []
for i in range(len(flow2)):
	readableDateList.append(datetime.utcfromtimestamp(float(flow2['time'].iloc[i])).strftime('%Y-%m-%d %H:%M:%S'))

flow2['readableTime'] = np.array(readableDateList)

flow2.drop("time", inplace = True, axis = 1)
flow2.drop("value", inplace = True, axis = 1)

# print("Highest allowed",flow2['value'].mean() + 3.6*flow2['value'].std())

flow2 = flow2[(flow2['imputedValues'] >= 0)]

# print(flow2.describe())
flow2['file'] = "flow2"
flow2.rename(columns={'imputedValues':'Values'}, inplace=True)
# print(flow2.head())
# flow2.to_csv(r'C:\\Users\\313516\\Documents\\Non work\\Other\\MM\\technical-test\\flow\\flow2Fixed.csv', index = False)


flow3 = pd.read_excel (r'C:\\Users\\313516\\Documents\\Non work\\Other\\MM\\technical-test\\flow\\flow_3.xlsx')

imputer = KNNImputer(n_neighbors=4, weights = 'distance')
imputed = imputer.fit_transform(flow3)
flow3_imputed = pd.DataFrame(imputed, columns=flow3.columns)


flow3['imputedValues'] = flow3_imputed['value']
flow3['imputedTime'] = flow3_imputed['time']

flow3 = flow3.dropna(subset=['time'])

flow3.drop("imputedTime", inplace = True, axis = 1)

readableDateList = []
for i in range(len(flow3)):
	readableDateList.append(datetime.utcfromtimestamp(float(flow3['time'].iloc[i])).strftime('%Y-%m-%d %H:%M:%S'))

flow3['readableTime'] = np.array(readableDateList)

flow3.drop("time", inplace = True, axis = 1)
flow3.drop("value", inplace = True, axis = 1)

# print("Highest allowed",flow3['value'].mean() + 3.6*flow3['value'].std())

flow3 = flow3[(flow3['imputedValues'] >= 0)]
flow3['file'] = "flow3"
flow3.rename(columns={'imputedValues':'Values'}, inplace=True)
# print(flow3.head())
# flow3.to_csv(r'C:\\Users\\313516\\Documents\\Non work\\Other\\MM\\technical-test\\flow\\flow3Fixed.csv', index = False)



flow4 = pd.read_excel (r'C:\\Users\\313516\\Documents\\Non work\\Other\\MM\\technical-test\\flow\\flow_4.xlsx')


## Missing dates
datesDropped = flow4.dropna()
dateCount = {}
for i in datesDropped['time']:
	if i in dateCount:
		dateCount[i] = dateCount[i] + 1
	else:
		dateCount[i] = 1

dateFrequencyCount = {}
for key, value in dateCount.items():
	if value not in dateFrequencyCount:
		dateFrequencyCount[value] = 1
	else:
		dateFrequencyCount[value] = dateFrequencyCount[value] + 1  

# print(dateFrequencyCount)


flow4 = flow4.dropna(subset=['time'])

# flow4.drop("imputedTime", inplace = True, axis = 1)

readableDateList = []
for i in range(len(flow4)):
	readableDateList.append(datetime.utcfromtimestamp(float(flow4['time'].iloc[i])).strftime('%Y-%m-%d %H:%M:%S'))

flow4['readableTime'] = np.array(readableDateList)

flow4.drop("time", inplace = True, axis = 1)
# flow4.drop("value", inplace = True, axis = 1)

# print("Highest allowed",flow4['value'].mean() + 3.6*flow4['value'].std())

flow4 = flow4[(flow4['value'] >= 0)]

# print(flow4.describe())
flow4['file'] = "flow4"
flow4.rename(columns={'value':'Values'}, inplace=True)
# print(flow4.head())
# flow4.to_csv(r'C:\\Users\\313516\\Documents\\Non work\\Other\\MM\\technical-test\\flow\\flow4Fixed.csv', index = False)


flowdf = pd.concat([flow1, flow2, flow3, flow4], ignore_index = True)
# flowdf.to_csv(r'C:\\Users\\313516\\Documents\\Non work\\Other\\MM\\technical-test\\flow\\flowdf.csv', index = False)



rain1 = pd.read_excel (r'C:\\Users\\313516\\Documents\\Non work\\Other\\MM\\technical-test\\rain\\rain_1.xlsx')

RainList = []

for i in rain1['rain']:
	if i < 0:
		RainList.append(0)
	else:
		RainList.append(i)

rain1['rain'] = RainList

readableDateList = []
for i in range(len(rain1)):
	readableDateList.append(datetime.utcfromtimestamp(float(rain1['time'].iloc[i])).strftime('%Y-%m-%d %H:%M:%S'))

rain1['readableTime'] = np.array(readableDateList)
rain1.drop("time", inplace = True, axis = 1)
# rain1.rename(columns={'readableTime':'time'}, inplace=True)
rain1['file'] = "rain1"

# rain1.to_csv(r'C:\\Users\\313516\\Documents\\Non work\\Other\\MM\\technical-test\\rain\\rain1.csv', index = False)



rain2 = pd.read_excel (r'C:\\Users\\313516\\Documents\\Non work\\Other\\MM\\technical-test\\rain\\rain_2.xlsx')

readableDateList = []
for i in range(len(rain2)):
	readableDateList.append(datetime.utcfromtimestamp(float(rain2['time'].iloc[i])).strftime('%Y-%m-%d %H:%M:%S'))

rain2['readableTime'] = np.array(readableDateList)
rain2.drop("time", inplace = True, axis = 1)
# rain2.rename(columns={'readableTime':'time'}, inplace=True)
rain2 = rain2[(rain2['rain'] < rain2['rain'].mean() + 3*rain2['rain'].std()) & (rain2['rain'] >= 0)]
rain2['file'] = "rain2"

rain2.to_csv(r'C:\\Users\\313516\\Documents\\Non work\\Other\\MM\\technical-test\\rain\\rain2.csv', index = False)

raindf = pd.concat([rain1, rain2], ignore_index = True)
# raindf.to_csv(r'C:\\Users\\313516\\Documents\\Non work\\Other\\MM\\technical-test\\rain\\raindf.csv', index = False)


# joinDF = flowdf.merge(raindf, how='outer', on='readableTime')
# joinDF.to_csv(r'C:\\Users\\313516\\Documents\\Non work\\Other\\MM\\technical-test\\joinDF.csv', index = False)


# print(flowdf['readableTime'].iloc[0][0:10])
flowdf['Date'] = flowdf['readableTime'].str.slice(0, 10)
raindf['Date'] = raindf['readableTime'].str.slice(0, 10)
# ndf = flowdf.groupby([flowdf['readableTime'][0:10]]).mean()
# print(flowdf.head())

newFlowDF = flowdf.groupby(['Date','file']).agg({'Values':['mean','sum']})
newRainDF = raindf.groupby(['Date','file']).agg({'rain':['mean','sum']})

# print(newFlowDF.head())
# print(newRainDF.head())

# joinDF = pd.concat([newFlowDF, newRainDF], join='outer')

# joinDF = newFlowDF.merge(newRainDF, on='Date', how='outer', suffixes=('_f','_r'), left_index = True )
joinDF = newFlowDF.merge(newRainDF,  how='outer', suffixes=('_f','_r'), left_on = 'Date', right_on = 'Date')
# joinDF = newFlowDF.join(newRainDF, on='Date', how='outer')

print(joinDF.head())
# print(joinDF.columns)

# joinDF.drop("file", inplace = True, axis = 1)
joinDF.columns = ['flowMean', 'flowSum', 'rainMean', 'rainSum']
# print(joinDF.head())

# joinDF['flowMean'] = joinDF['flowMean'].replace(np.nan, 0)
# joinDF['flowSum'] = joinDF['flowSum'].replace(np.nan, 0)
# joinDF['rainMean'] = joinDF['rainMean'].replace(np.nan, 0)
# joinDF['rainSum'] = joinDF['rainSum'].replace(np.nan, 0)

# print(joinDF.shape)
joinDF = joinDF.dropna()
# print(joinDF.shape)


joinDF['flowMean'] = joinDF['flowMean'] - joinDF['rainMean']
joinDF['flowSum'] = joinDF['flowSum'] - joinDF['rainSum']

print(joinDF['flowMean'].corr(joinDF['rainMean']))
print(joinDF['flowSum'].corr(joinDF['rainSum']))

# print(joinDF.head())

# print(joinDF.head())
# joinDF['flowMean'] = joinDF['flowMean'] - joinDF['rainMean']
# joinDF['flowSum'] = joinDF['flowSum'] - joinDF['rainSum']

# print(joinDF.head())
# joinDF.to_csv(r'C:\\Users\\313516\\Documents\\Non work\\Other\\MM\\technical-test\\joinDF.csv', index = False)

	