from tkinter import *
import pyodbc
import pandas as pd
import collections, numpy
from geopy import geocoders
from geopy import distance
from collections import defaultdict
from collections import Counter
import collections
import pymssql
import os


g = geocoders.GoogleV3('')
file4D = 'icd10cm-codes-2019.txt'

#compName='UNKNOWN\SQLEXPRESS'
#dbName='SB_IMPORT_TEST'
#ID = ''
#PWD = ''
#IPAddress = '192.168.123.456'
#Port = '1433'
#conntextText = 'Driver={SQL Server Native Client 11.0};Server='+ dbName + ';Database='+dbName+';trusted_connection=yes;'
#cnxnText = 'DRIVER={SQL Server};SERVER='+IPAddress+';DATABASE='+dbName+';UID='+ID+';PWD='+PWD+';'
#print(conntextText)
#cnxnText = 'DRIVER={SQL Server};SERVER=192.168.123.456;PORT=1433;DATABASE=yourdb;UID=your_user;PWD=your_pw;'PORT='+Port+';
#cnxn = pyodbc.connect()
#print("hello")

configD = {}
with open("config.txt") as f:
	for line in f:
		(key, val) = line.split('=')
		configD[key] = str(val)


geoAPIKEY = configD['geoAPIKEY'].rstrip()
g = geocoders.GoogleV3(geoAPIKEY)


print(configD)
host = configD['IPAddress'].rstrip()
username = configD['username'].rstrip()
password = configD['password'].rstrip()
database = configD['dbName'].rstrip()
rule1ThresholdDistance=configD['rule1ThresholdDistance'].rstrip()
rule1ThresholdDistance = int(rule1ThresholdDistance)
rule1ThresholdPercent=configD['rule1ThresholdPercent'].rstrip()
rule1ThresholdPercent = float(rule1ThresholdPercent)
rule2ThresholdNumLoc=configD['rule2ThresholdNumLoc'].rstrip()
rule2ThresholdNumLoc = int(rule2ThresholdNumLoc)
rule3ThresholdNum=configD['rule3ThresholdNum'].rstrip()
rule3ThresholdNum=int(rule3ThresholdNum)
rule4Threshold=configD['rule4Threshold'].rstrip()
rule4Threshold=float(rule4Threshold)

rule6a = int(configD['rule6a'].rstrip())
rule6b = int(configD['rule6b'].rstrip())
rule6c = int(configD['rule6c'].rstrip())
rule6d = int(configD['rule6d'].rstrip())
rule6e = int(configD['rule6e'].rstrip())
rule6f = int(configD['rule6f'].rstrip())



conn = pymssql.connect(host, username, password, database)
cursor = conn.cursor()
print("connection established")

f = open("Rule3CPTPopulus.txt", "r")
Rule3CPTCodeList = []
for x in f:
	for code in x.split(','):
		Rule3CPTCodeList.append(code)
print(Rule3CPTCodeList)
f.close()

f = open("Rule4CPTPopulus.txt", "r")
populusCount = 0
CPTpopulations4B = defaultdict(list)
CPTpopulationsCount = 1


for x in f:
	codeList = []
	for code in x.split(','):
		codeList.append(code.strip('\n'))
	CPTpopulations4B['populus' + str(CPTpopulationsCount)] = codeList
	CPTpopulationsCount+=1
print(CPTpopulations4B)

f.close()




#conntextText = 'Driver={SQL Server Native Client 11.0};Server='+ compName + ';Database='+dbName+';trusted_connection=yes;'
'''
conn = pyodbc.connect(
	"Driver={SQL Server Native Client 11.0};"
	"Server=UNKNOWN\SQLEXPRESS;"#change for your pc
	"Database=SB_IMPORT_TEST;"#change for your pc
	"trusted_connection=yes;"
)
'''
#conn =pyodbc.connect(conntextText)

#cursor = conn.cursor()

def print_rules(paramArgRule):
	#DBnamePrompt['text'] = str(rule1())
	
	global startDate
	global endDate
	startDate =DBStartEntry.get()
	endDate = DBEndEntry.get()
	print(startDate)
	print(endDate)


	print(paramArgRule)
	print("apiKey = " + geoAPIKEY)




	testsql = "Select * From t_Claim WHERE CAST(DOSFrom1 as date) BETWEEN '"+str(startDate)+"' and '"+str(endDate) + "' and isnull(parentclaimid,'')=''"


	test = pd.read_sql_query(testsql, conn)

	test.drop(columns = ['InsuID','InsuTypeCode' ,'ReferLName','ReferFName'	,'Signature',	'FacName','FacCity','FacState','FacPhone','PsyCity','PsyState','PsyZip','PatFName','PatMI','PatLName','PatSSN','PatAddress1','PatAddress2','PatPhone','PatEmployerName','PatEmployerAddress','PatEmployerPhone','PHFName','PHLName','PHSSN','DoctorLName','DoctorFName','DoctorSSN','DoctorPIN','DoctorTaxID','DoctorTaxonomyCode','LocatorCode','EmergencyCode','OrderAddressID','PatientStatus','InsuAddressID','OutstandingNotes'])
	print("drop collumn success")
	
	

	if paramArgRule == 7:
		if geoAPIKEY == '':
			print("its blank")
			dfrule1 = rule1()
			test =pd.concat([dfrule1,test], axis=1)
			print("rule1 complete")

		dfrule2 = rule2()
		print("rule2 complete")
		dfrule3 = rule3()
		print("rule3 complete")
		dfrule5 = rule5()
		print("rule5 complete")
		rule4()
		print("rule4 complete")
		rule6()
		print("rule6 complete")
		
		test =pd.concat([dfrule2,test ], axis=1)
		test =pd.concat([dfrule3,test], axis=1)
		test =pd.concat([dfrule5,test], axis=1)
		
	elif paramArgRule == 1:
		if geoAPIKEY == '':
			print("api is blank")
		else:
			dfrule1 = rule1()
			test =pd.concat([dfrule1,test], axis=1)
			print("rule1 complete")
	elif paramArgRule == 2:
		dfrule2 = rule2()
		print("rule2 complete")
		test =pd.concat([dfrule2,test ], axis=1)
	elif paramArgRule == 3:
		rule3()
		print("rule3 complete")
	elif paramArgRule == 4:
		rule4()
		print("rule4 complete")
	elif paramArgRule == 5:
		dfrule5 = rule5()
		print("rule5 complete")
		test =pd.concat([dfrule5,test], axis=1)
	elif paramArgRule == 6:
		rule6()
		print("rule6 complete")
	print(test)





	
	test.to_csv("ResultML.csv", sep=',', encoding='utf-8')
	
	
	
	

#add functionaliy for time frames

def rule1():
	print("rule 1")
	print(startDate)
	print(endDate)
	rule1sql = "Select PatientID, DoctorID,DoctorLName,DoctorFName,DoctorGroupID,PracticeID,GroupID, PatAddress1, PatAddress2, PatZip, FacilityID,FacName, FacAddress1, FacCity, FacState, FacZip, DOSFrom1 From t_Claim WHERE CAST(DOSFrom1 as date) BETWEEN '"+str(startDate)+"' and '"+str(endDate) + "' and isnull(parentclaimid,'')=''"
	df_rule1 = pd.read_sql_query(rule1sql, conn)
	df_rule1["distance"] = 0
	df_rule1["rule1Flag"] = "False"
	df_rule1["Patlat"] = ""
	df_rule1["Patlon"] = ""
	df_rule1["Faclat"] = ""
	df_rule1["Faclon"] = ""
	
	
	for index, row in df_rule1.iterrows():
		
		print(str(row.name) + " "+ row['PatZip'][:5] + " " + row['FacZip'][:5])
		#take first five characters of zip
		PatZip, (lat, lng) = g.geocode(row['PatZip'][:5])
		FacZip, (lat1, lng1) = g.geocode(row['FacZip'][:5])
		print("%s: %.5f, %.5f" % (PatZip, lat, lng))
		print("%s: %.5f, %.5f" % (FacZip, lat1, lng1))

		df_rule1.at[index, "Patlat"] = lat
		df_rule1.at[index, "Patlon"] = lng
		df_rule1.at[index, "Faclat"] = lat1
		df_rule1.at[index, "Faclon"] = lng1
		
		df_rule1.at[index, "distance"] = distance.distance((lat, lng), (lat1, lng1)).miles
		row["distance"] = df_rule1.at[index, "distance"]
		print("dist: ", row["distance"])
		
		if(row["distance"] > rule1ThresholdDistance):
			df_rule1.at[index, "rule1Flag"] = "True"
		print(row["rule1Flag"])
	print(df_rule1['distance'])
		
	

	FacilitiesRule1 = list(dict.fromkeys(df_rule1["FacName"]))
	DoctorsRule1 = list(dict.fromkeys(df_rule1["DoctorID"]))

	flagRowsList = {}
	for i in FacilitiesRule1:
		tempdf = df_rule1[df_rule1.FacName == i].copy()
		result = tempdf['rule1Flag'].value_counts(normalize=True) * 100
		#print(tempdf['rule1Flag'])
		
		falseResult = -1
		trueResult = -1

		try:			
			falseResult = result['False']
		except Exception as e:
			print("no false")
		try:			
			trueResult = result['True']
		except Exception as e:
			print("no true")


		if trueResult > rule1ThresholdPercent:
			tempdf = tempdf[tempdf.rule1Flag != 'False']
			print(tempdf['rule1Flag'])
			flagRowsList[i] = tempdf
	print(flagRowsList)

	for key,value in flagRowsList.items():
		flagRowsList[key].to_csv("Rule1"+key+" "+startDate+"-"+endDate+".csv", sep='\t', encoding='utf-8')
	
	return df_rule1[["distance","rule1Flag","Patlat","Patlon","Faclat","Faclon"]]
	

	

def rule2():
	print("rule 2")
	rule2sql = "Select DoctorID,DoctorLName,DoctorFName,DoctorGroupID,PracticeID,GroupID,FacilityID,FacName,FacAddress1,DOSFrom1,DOSFrom2,DOSFrom3,DOSFrom4,DOSFrom5,DOSFrom6 From t_Claim WHERE CAST(DOSFrom1 as date) BETWEEN '"+str(startDate)+"' and '"+str(endDate) + "' and isnull(parentclaimid,'')=''"
	df_rule2 = pd.read_sql_query(rule2sql, conn)

	rule2sql = "Select DoctorGroupID,NPI From t_DoctorGroup"
	df_npilookup = pd.read_sql_query(rule2sql, conn)

	df_rule2['NPI']  = ""
	df_rule2['rule2Flag'] = False

	df_npilookup = df_npilookup.set_index('DoctorGroupID')
	#print(df_npilookup)

	for index, row in df_rule2.iterrows():
		try:
			print(df_npilookup.loc[row['DoctorID']])
			NPI = df_npilookup.loc[row['DoctorID']].values[0]
			df_rule2.at[index, "NPI"] = NPI
		except Exception as e:
			df_rule2.at[index, "NPI"] = 'NO ID'
		
	NPIlist = list(dict.fromkeys(df_rule2["NPI"]))
	NPIFlagDictionary = {}

	for NPIindex in NPIlist:
		tempdf = df_rule2[df_rule2.NPI == NPIindex].copy()

		datesDictionary = defaultdict(list)

		for index, row in tempdf.iterrows():

			dates = [row['DOSFrom1'],row['DOSFrom2'],row['DOSFrom3'],row['DOSFrom4'],row['DOSFrom5'],row['DOSFrom6']]
			
			removelist = []
			count = 0
			for i in dates:
				if pd.isnull(i):
					removelist.append(count)
				count+=1
					
			for i in sorted(removelist, reverse=True):
				del dates[i]
			dates = list(dict.fromkeys(dates))
			
			for i in dates:
				tempdict = { 
					'DoctorID': row["DoctorID"],  
					'DoctorLName' : row["DoctorLName"], 
					'DoctorFName' : row["DoctorFName"],
					'DoctorGroupID' : row["DoctorGroupID"],
					'PracticeID' : row["PracticeID"],			
					'GroupID' : row["GroupID"],
					'FacilityID' : row["FacilityID"],
					'FacName' : row["FacName"],
					'FacAddress1' : row["FacAddress1"],
					'thedate': i
				}
				datesDictionary[i].append(tempdict)
				#datesDictionary[i]= 


		for key, value in datesDictionary.items():
			LocationList = []
			for i in value:
				LocationList.append(i['FacAddress1']) # or PracticeID
			LocationList = list(dict.fromkeys(LocationList))
			#print(NPIindex)
			#print(LocationList)
			if len(LocationList) > rule2ThresholdNumLoc:
				#print(NPIindex)
				#print("flag")
				NPIFlagDictionary[key] = {
					'LocationList': LocationList,
					'NPI': NPIindex,
					'DoctorID': i['DoctorID'],
					'DoctorLName': i['DoctorLName'],
					'DoctorFName': i['DoctorFName'],
					'DoctorGroupID': i['DoctorGroupID'],
					'PracticeID': i['PracticeID'],
					'GroupID': i['GroupID'],
					'FacilityID' :i['FacilityID'],
					'FacName': i['FacName'],
					'FacAddress1': i['FacAddress1'],
					'date': i['thedate']

				}
			#append dates with row FacAddress1
			# FacName FacAddress1 DoctorFName DoctorLName
	
	rule2file = "Rule2.txt"
	rule2String = []
	for key, value in NPIFlagDictionary.items():
		valueString = "Date: " + str(key) + " NPI: " + str(value['NPI'])+"\nDoctor: " + str(value['DoctorFName']) + " " +str(value['DoctorLName']) + "\nFacName: " + str(value['FacName']) + " " + str(value['FacAddress1']) +"\nNPI: " + str(value['NPI']) + "\n"
		for i in value['LocationList']:
			valueString += "Address: " + str(i) + "\n"
		rule2String.append(valueString)
		print(key)
		df_rule2.loc[(df_rule2['NPI'] == value['NPI']) & (df_rule2['DOSFrom1'] == key),'rule2Flag'] = True
	
	print(df_rule2['rule2Flag'])

	with open('Rule2 '+startDate+' '+endDate +'.txt', 'w') as f:
		for item in rule2String:
			f.write("%s\n" % item)
	
	return df_rule2[["NPI","rule2Flag"]]

def rule3():
	print("rule 3")

	rule3sql = "Select PatientID,DoctorID,DoctorLName,DoctorFName,PracticeID,FacName,Proc1,Proc2,Proc3,Proc4,Proc5,Proc6,DOSFrom1,DOSFrom2,DOSFrom3,DOSFrom4,DOSFrom5,DOSFrom6 From t_Claim WHERE CAST(DOSFrom1 as date) BETWEEN '"+str(startDate)+"' and '"+str(endDate) + "' and isnull(parentclaimid,'')=''  and Modifier1 NOT like '%26%' and Modifier2 NOT like '%26%' and Modifier3 NOT like '%26%' and Modifier4 NOT like '%26%' and Modifier5 NOT like '%26%' and Modifier6 NOT like '%26%'"
	df_rule3 = pd.read_sql_query(rule3sql, conn)
	print(df_rule3)

	rule3sql = "Select PracticeID,PracticeName From t_Practice"
	df_PractiveLookup = pd.read_sql_query(rule3sql, conn)
	print(df_PractiveLookup)
	df_rule3['Practice Name']  = ""
	df_rule3['rule3Flag'] = False

	df_PractiveLookup = df_PractiveLookup.set_index('PracticeID')

	for index, row in df_rule3.iterrows():
		try:
			print(df_npilookup.loc[row['PracticeID']])
			FacName = df_npilookup.loc[row['PracticeID']].values[0]
			df_rule3.at[index, "Practice Name"] = FacName
		except Exception as e:
			df_rule3.at[index, "Practice Name"] = 'NO ID'	

	#CPT_codes = [95921, 95922, 95923, 95924, 95943, 93880, 93882, 93886, 93888, 93890, 93892, 93893, 93922, 93923, 93924, 93925, 93926, 93930, 93931, 93970, 93971, 93975, 93976, 93978, 93979, 93970, 93971, 93990, 'G0365', 76881, 76882, 93306, 76536, 76604, 76700, 76705, 76770, 76775, 76856, 76857,76830, 76870, 76872, 95907, 95908, 95909, 95910, 95911, 95912, 95913, 95925, 95926, 93980, 93981,  95938]
	#CPT_codes = [str(i) for i in CPT_codes] 
	CPT_codes = list(Rule3CPTCodeList)
	PatientDictionary = defaultdict(list)

	PatientList = list(dict.fromkeys(df_rule3["PatientID"]))

	open("rule3 "+startDate+" "+endDate+ ".txt", 'w').close()
	f = open("rule3 "+startDate+" "+endDate+  ".txt", "a")

	for Patient in PatientList:
		print("proccesing part 1")
		for index, row in df_rule3.iterrows():
			if row['PatientID'] == Patient:
				
				Dates = [row['DOSFrom1'],row['DOSFrom2'],row['DOSFrom3'],row['DOSFrom4'],row['DOSFrom5'],row['DOSFrom6']]
				Codes = [row['Proc1'],row['Proc2'],row['Proc3'],row['Proc4'],row['Proc5'],row['Proc6']]
				Info = [[row['DoctorID'],row['DoctorLName'],row['DoctorLName'],row['PracticeID'],row['Practice Name']]]
				
				count = 0
				removelist = []
				for i in Codes:
					if i is None:
						removelist.append(count)
					elif not i.strip() or pd.isnull(i) or not i in CPT_codes:
						removelist.append(count)
					count +=1
		
				for i in sorted(removelist, reverse=True):
					del Dates[i]
					del Codes[i]
				PatientDictionary[Patient,'Codes'] = PatientDictionary[Patient,'Codes'] + Codes
				PatientDictionary[Patient,'Dates'] = PatientDictionary[Patient,'Dates'] + Dates
				PatientDictionary[Patient,'Info'] = PatientDictionary[Patient,'Info'] + Info*len(Codes)
	
	
	#check to see if 5 dates are the same
	FlaggedPatients = defaultdict(list)
	print("proccesing part 2")
	for Patient in PatientList:
		Dates = PatientDictionary.get((Patient,'Dates'))
		Dates = numpy.array(Dates)
		unique, counts = numpy.unique(Dates, return_counts=True)
		cunique = dict(zip(unique, counts))
		#print(cunique)
		for key in cunique:
			if(cunique[key]>=rule3ThresholdNum):
				codeIndex = 0
				codematchlist = []
				for i in Dates:
					if key == i:
						codematchlist.append(codeIndex)
						codeIndex +=1
				Codes = PatientDictionary.get((Patient,'Codes'))
				Info = PatientDictionary.get((Patient,'Info'))
				CodesMatched = list(Codes[i] for i in codematchlist)
				InfoMatched = list(Info[i] for i in codematchlist)
				print(InfoMatched)

				f.write("\n"+"Patient: "+Patient+"\n")
				f.write("Date: "+ str(key)+"\n")

				count = 0
				for i in CodesMatched:
					f.write("CPT Code: "+i+"\n")
					f.write('DoctorID: '+InfoMatched[count][0]+"\n")
					f.write('Doctor Name: '+InfoMatched[count][1]+" "+InfoMatched[count][2]+"\n")
					f.write('PracticeID: '+InfoMatched[count][3]+"\n")
					f.write('Practice Name: '+InfoMatched[count][4]+"\n")
					FlaggedPatients[Patient].append(InfoMatched[count][3])
					print("test")
					count+=1

	for key, value in FlaggedPatients.items():
		print(key)
		print(value)
		for index, row in df_rule3.iterrows():
			if (row['PracticeID'] in value) & (row['PatientID'] == key):
				print("flagged")
				df_rule3.at[index, "rule3Flag"] = True
	print(df_rule3[['Practice Name',"rule3Flag"]])
	f.close()
	return
	'''
	flagRowsList = []
	for index, row in df_rule3.iterrows():
		print(row.name)
		dates = [row['DOSFrom1'],row['DOSFrom2'],row['DOSFrom3'],row['DOSFrom4'],row['DOSFrom5'],row['DOSFrom6']]
		Codes = [row['Proc1'],row['Proc2'],row['Proc3'],row['Proc4'],row['Proc5'],row['Proc6']]

		count = 0
		removelist = []
		for i in Codes:
			if not i.strip() or pd.isnull(i):
				removelist.append(count)
			count +=1
		
		for i in sorted(removelist, reverse=True):
			del dates[i]
			del Codes[i]
		print(Codes)		

		dates = numpy.array(dates)
		unique, counts = numpy.unique(dates, return_counts=True)
		cunique = dict(zip(unique, counts))
		for key in cunique:
			if(cunique[key]>=5):
				codeCounter = 0
				codeIndex = 0
				for i in dates:
					if key == i:
						if Codes[codeIndex] in CPT_codes:
							codeCounter +=1
							print("match")
						codeIndex +=1
					if codeCounter >= 5:
						print("FLAG")
						flagRowsList.append(row.name)

				codeIndex = 0
				codeCounter = 0
	flagRowsList = list(dict.fromkeys(flagRowsList))
	print(flagRowsList)

	rule3sql = "Select * From t_Claim WHERE CAST(DOSFrom1 as date) BETWEEN '"+str(startDate)+"' and '"+str(endDate) + "'"
	df_rule3 = pd.read_sql_query(rule3sql, conn)
	

	#print df to csv
	df_rule3["rule3Flag"] = "False"
	for i in flagRowsList:
		df_rule3.at[i, "rule3Flag"] = "True"
	
	df_rule3.to_csv("df_with_rule3", encoding='utf-8', index=False)
	
	return df_rule3[["rule3Flag"]]
	'''

def rule4():
	print("rule4")

	rule4sql = "Select PracticeID, DoctorID,DoctorLName,DoctorFName, Proc1,Proc2,Proc3,Proc4,Proc5,Proc6,DOSFrom1,DOSFrom2,DOSFrom3,DOSFrom4,DOSFrom5,DOSFrom6,DiagCode1,DiagCode2,DiagCode3,DiagCode4,DiagCode5,DiagCode6 From t_Claim WHERE CAST(DOSFrom1 as date) BETWEEN '"+str(startDate)+"' and '"+str(endDate) + "' and isnull(parentclaimid,'')=''"
	df_rule4a = pd.read_sql_query(rule4sql, conn)

	open("rule4 "+startDate+" "+endDate+ ".txt", 'w').close()
	f1 = open("rule4 "+startDate+" "+endDate+  ".txt", "a")

	PracticeList = list(dict.fromkeys(df_rule4a["PracticeID"]))
	#print(df_rule4a)
	#print(PracticeList)
	'''
	CPTpopulations4B = defaultdict(list)

	CPTpopulations4B["populus1"] = ['99201','99202','99203','99204','99205']
	CPTpopulations4B["populus2"] = ['90832', '90833', '90834', '90836', '90837', '90838']
	CPTpopulations4B["populus3"] =['99211', '99212', '99213', '99214', '99215'] 
	CPTpopulations4B["populus4"] = ['99304', '99305', '99306']
	CPTpopulations4B["populus5"] = ['99307', '99308', '99309', '99310']
	CPTpopulations4B["populus6"] = ['99221', '99222', '99223']
	CPTpopulations4B["populus7"] = ['99231', '99232', '99233']
	CPTpopulations4B["populus8"] = ['99281', '99282', '99283',  '99284',  '99285']
	CPTpopulations4B["populus9"] = ['99341', '99342', '99343', '99344', '99345']
	CPTpopulations4B["populus10"] = ['99347', '99348', '99349', '99350']
	CPTpopulations4B["populus11"] = ['97165', '97166','97167']
	CPTpopulations4B["populus12"] = ['97161', '97162', '97163'] 
	'''
	


	for Practice in PracticeList:
		tempdf = df_rule4a[df_rule4a.PracticeID == Practice].copy()
		#print(tempdf)


		allPracticeCodes4a =[]
		for index, row in tempdf.iterrows():
			Codes = [row['Proc1'],row['Proc2'],row['Proc3'],row['Proc4'],row['Proc5'],row['Proc6']]
			doctorinfo = str(row['DoctorID']) + ' ' + str(row['DoctorLName']) + ' ' + str(row['DoctorFName'])
			count = 0
			removelist = []
			for i in Codes:
				if i is None:
					removelist.append(count)
				elif not i.strip() or pd.isnull(i):
					removelist.append(count)
				count +=1
		
			for i in sorted(removelist, reverse=True):
				del Codes[i]
			#print(Codes)
			for Code in Codes:
				allPracticeCodes4a.append((Code,doctorinfo))
		
		CodeCount = Counter(Tuple[0] for Tuple in allPracticeCodes4a)
		#print(CodeCount)
		MaxN = max(CodeCount.values())
		SumN = sum(CodeCount.values())
		Percentage = MaxN/SumN
		
		if Percentage > rule4Threshold:
			MCode = max(CodeCount, key=CodeCount.get)
			DoctorsWithMax = {}
			for Tuple in allPracticeCodes4a:
				if Tuple[0] == MCode:
					if not Tuple[1] in DoctorsWithMax:
						DoctorsWithMax[Tuple[1]] = 1
					else:
						DoctorsWithMax[Tuple[1]] += 1
			f1.write("\n")
			f1.write("\nCPT Code with the hightest Occurence in PracticeID " +Practice+ " was CPT Code: " + str(MCode))
			f1.write("\nCode has this many instances: "+ str(CodeCount[MCode]))
			f1.write("\nPercent: "+ str(Percentage*100))
			for Doctor in DoctorsWithMax:
				f1.write("\nDoctor: "+ str(Doctor) + " assigned to "+str(DoctorsWithMax[Doctor]/MaxN*100) +"% of CPT code instances")
				#4a Done Here


		for Populus, List in CPTpopulations4B.items():
			#print(Populus)
			#print(List)
			allPracticeCodes4b=[]
			for index, row in tempdf.iterrows():
				Codes = [row['Proc1'],row['Proc2'],row['Proc3'],row['Proc4'],row['Proc5'],row['Proc6']]
				doctorinfo = str(row['DoctorID']) + ' ' + str(row['DoctorLName']) + ' ' + str(row['DoctorFName'])
				count = 0
				removelist = []
				for i in Codes:
					if i is None:
						removelist.append(count)
					elif not i.strip() or pd.isnull(i):
						removelist.append(count)
					elif not i in List:
						removelist.append(count)
					count +=1
		
				for i in sorted(removelist, reverse=True):
					del Codes[i]
				#print(Codes)
				for Code in Codes:
					allPracticeCodes4b.append((Code,doctorinfo))
			
			#print(List)
			#print(allPracticeCodes4b)
			if allPracticeCodes4b:
				CodeCountb = Counter(Tuple[0] for Tuple in allPracticeCodes4b)
				#print(CodeCountb)
				#print(List)

				MaxNb = max(CodeCountb.values())
				SumNb = sum(CodeCountb.values())
				Percentage = MaxNb/SumNb
			
				if Percentage > rule4Threshold:
					MCodeb = max(CodeCountb, key=CodeCountb.get)
					DoctorsWithMaxb = {}
				
					for Tuple in allPracticeCodes4b:
						if Tuple[0] == MCodeb:
							if not Tuple[1] in DoctorsWithMaxb:
								DoctorsWithMaxb[Tuple[1]] = 1
							else:
								DoctorsWithMaxb[Tuple[1]] += 1
					f1.write("\n")
					f1.write("\nIn CPTCode Population: "+str(List))
					f1.write("\nCPT Code with the hightest Occurence in PracticeID " +Practice+ " was CPT Code: " + str(MCodeb))
					f1.write("\nCode has this many instances: "+ str(CodeCount[MCodeb]))
					f1.write("\nPercent: "+ str(Percentage*100))
					for Doctor in DoctorsWithMaxb:
						f1.write("\nDoctor: "+ str(Doctor) + " assigned to "+str(DoctorsWithMaxb[Doctor]/MaxNb*100) +"% of CPT code instances")

	f1.close()
	#rule4b
	
				
	#print(len(allPracticeCodes4a))
		




			
	'''
	for Practice in PracticeList:
		tempdf = df_rule4a[df_rule4a.PracticeID == Practice].copy()



		allPracticeCodes = []
		paralelDoctors = []
		for index, row in tempdf.iterrows():
			#print(Practice)
			#print(index)
			Codes = [row['Proc1'],row['Proc2'],row['Proc3'],row['Proc4'],row['Proc5'],row['Proc6']]
			#print(row[''])
			doctorinfo = str(row['DoctorID']) + ' ' + str(row['DoctorLName']) + ' ' + str(row['DoctorFName'])

			count = 0
			removelist = []
			for i in Codes:
				if not i.strip() or pd.isnull(i):
					removelist.append(count)
				count +=1
		
			for i in sorted(removelist, reverse=True):
				del Codes[i]
			#print(Codes)
			allPracticeCodes += Codes
			for i in range(len(Codes)):
				paralelDoctors += [doctorinfo]
		
		
		codeCountOverall = Counter(allPracticeCodes)
		
		mostCounted = codeCountOverall.most_common(1)[0][0] if codeCountOverall else None
		mostCountedPercent = codeCountOverall[mostCounted]/ len(allPracticeCodes)
		
		

		if mostCountedPercent >= .010:
			tempparalelDoctors = list(paralelDoctors)
			
			removelist = []
			count = 0
			for Code in allPracticeCodes:
				if Code != mostCounted:
					removelist.append(count)
				count+=1

			for i in sorted(removelist, reverse=True):
				del tempparalelDoctors[i]
			docCount = Counter(tempparalelDoctors)
			print(docCount)
			print(Practice + ' ' + mostCounted + ' '+  str(mostCountedPercent))
			for key in docCount:
				print(key)
				#print(tempparalelDoctors)
				#print(docCount[key]/len(tempparalelDoctors) * 100)
				print(docCount[key])


		DoctorsList = list(dict.fromkeys(paralelDoctors))	
		for Doctor in DoctorsList:
			tempparalelDoctors = list(paralelDoctors)
			tempCorespondingCodes = list(allPracticeCodes)
			
			count = 0
			removelist = []
			for md in tempparalelDoctors:
				if md != Doctor:
					removelist.append(count)
				count +=1
			
			for i in sorted(removelist, reverse=True):
				del tempparalelDoctors[i]
				del tempCorespondingCodes[i]
			docCount = Counter(tempparalelDoctors)

			#print(len(tempparalelDoctors))
			#print(Doctor)
			#print(tempCorespondingCodes)

			codeCountDocOverall = Counter(tempCorespondingCodes)
			mostDocCounted = codeCountDocOverall.most_common(1)[0][0] if codeCountDocOverall else None
			mostCountedDocPercent = codeCountDocOverall[mostDocCounted]/ len(tempCorespondingCodes)

	#rules 4a
	'''






	
	
	


def rule5():
	print("Rule 5: Comparison of CPT codes different providers with the same specialty (provider billing activities for this specialty are significantly higher than peer group) ")

	rule5sql = "Select Proc1,Proc2,Proc3,Proc4,Proc5,Proc6,DoctorID,DoctorLName,DoctorFName,DoctorGroupID,PracticeID,GroupID,FacilityID,FacName,FacAddress1,DOSFrom1,DOSFrom2,DOSFrom3,DOSFrom4,DOSFrom5,DOSFrom6 From t_Claim WHERE CAST(DOSFrom1 as date) BETWEEN '"+str(startDate)+"' and '"+str(endDate) + "' and isnull(parentclaimid,'')=''"
	df_rule5 = pd.read_sql_query(rule5sql, conn)
	print("its working")
	rule5sql = "Select DoctorGroupID,Specialty From t_DoctorGroup"
	df_SpecialtyLookup = pd.read_sql_query(rule5sql, conn)

	df_rule5['Specialty']  = ""


	df_SpecialtyLookup = df_SpecialtyLookup.set_index('DoctorGroupID')
	

	

	FacultyDictionary = {}

	CodesDictionlaryofLists = defaultdict(list)
	CodesList = []

	for index, row in df_rule5.iterrows():

		try:
			Specialty = df_SpecialtyLookup.loc[row['DoctorID']].values[0]
			df_rule5.at[index, "Specialty"] = Specialty
		except Exception as e:
			df_rule5.at[index, "Specialty"] = 'NO ID'

		Codes = [row['Proc1'],row['Proc2'],row['Proc3'],row['Proc4'],row['Proc5'],row['Proc6']]

		count = 0
		removelist = []
		for i in Codes:
			if i is None:
				removelist.append(count)
			elif not i.strip() or pd.isnull(i):
				removelist.append(count)
			count +=1
		
		for i in sorted(removelist, reverse=True):
			del Codes[i]
		CodesDictionlaryofLists[row['DoctorID']] = CodesDictionlaryofLists[row['DoctorID']] + Codes
		CodesList = CodesList + Codes
		
	WholeDictionary = {}


	print("proccesing rule 5")
	#key  = Faculty ID
	for key,value in CodesDictionlaryofLists.items():

		try:
			SpecialtyAssignment = df_SpecialtyLookup.loc[key].values[0],
		except Exception as e:
			SpecialtyAssignment = "No Key"

		WholeDictionary[key] = {
			'Specialty': SpecialtyAssignment,
			'CodeStats': Counter(value),
			'Codes': value
		}
	
	SpecialtyList = list(dict.fromkeys(df_rule5['Specialty']))
	SpecialtyList = [str(i) for i in SpecialtyList]
	
	CodesList = list(dict.fromkeys(CodesList))
	CodesList = [str(i) for i in CodesList]
	
	DoctorIDList =  list(dict.fromkeys(df_rule5['DoctorID']))
	DoctorIDList = [str(i) for i in DoctorIDList]

	#print(WholeDictionary)

	SpecialtyDF = {}
	#key code
	open("rule5 "+startDate+" "+endDate + ".txt", 'w').close()
	f = open("rule5 "+startDate+" "+endDate+  ".txt", "a")
	for Specialty in SpecialtyList:
		#calculate average volume
		#lowest volume
		#highest volume
		#SpecialtyAverageVolumes
		facultyDic = {}
		for Code in CodesList:

			for key,value in WholeDictionary.items():

				if(value['Specialty'] == Specialty):
					if value.get('CodeStats').get(Code) != None:
						facultyDic[Code,key] = value.get('CodeStats').get(Code)
					else:
						facultyDic[Code,key] = 0
		removeKeys = []

		for key,value in facultyDic.items():
			if value == 0:
				removeKeys.append(key)

		for key in removeKeys:
			if key in facultyDic :
				del facultyDic[key]
		
		
		for Code in CodesList:
			codeRecord = {}
			for Doctor in DoctorIDList:
				if facultyDic.get((Code,Doctor)) != None:
					codeRecord[Doctor]= facultyDic.get((Code,Doctor))

			maxN = None
			minN = None
			standardDiviation = None
			mean = None
			sumN = 0
			tempList = []
			if any(codeRecord):
				for key,value in codeRecord.items():
					
					tempList.append(value)
					if maxN == None:
						maxN = key
					elif value > codeRecord[maxN]:
						maxN = key
					if minN == None:
						minN = key
					elif value < codeRecord[minN]:
						minN = key
					sumN += codeRecord[key]


				average = numpy.mean(tempList)
				std = numpy.std(tempList)

				

				f.write("\n\nSpecialty: " + Specialty)
				f.write("\nCode: "+ Code+ " "+ str(codeRecord))
				f.write("\naverage: "+str(average) + " standard diviation: " + str(std))
				f.write("\nMax: "+str(maxN) +" "+str(codeRecord[maxN])+" Min: "+ str(minN)+" "+str(codeRecord[minN]))
				for key,value in codeRecord.items():
					if (value <= average - std):
						f.write("\nOutliar below Standard Deviation: " + key +": "+ str(value))
					if (value >= average + std):
						f.write("\nOutliar above Standard Deviation: " + key +": "+ str(value))

	f.close()
	return df_rule5[["Specialty"]]
		#print(facultyDic)
		
	

def rule6():
	print("Rule 6: Number of patients per day (excessive provider activities within a day")

	rule6sql = "Select DoctorID,PatientID,FacState,DoctorLName,DoctorFName,DoctorGroupID,PracticeID,GroupID,FacilityID,FacName,FacAddress1,DOSFrom1,DOSFrom2,DOSFrom3,DOSFrom4,DOSFrom5,DOSFrom6,PlaceOfService1,PlaceOfService2,PlaceOfService3,PlaceOfService4,PlaceOfService5,PlaceOfService6 From t_Claim WHERE CAST(DOSFrom1 as date) BETWEEN '"+str(startDate)+"' and '"+str(endDate) + "'and isnull(parentclaimid,'')=''"
	df_rule6 = pd.read_sql_query(rule6sql, conn)

	rule6sql = "Select DoctorGroupID,Specialty,NPI From t_DoctorGroup"
	df_SpecialtyLookup = pd.read_sql_query(rule6sql, conn)

	df_rule6['Specialty']  = ""
	df_rule6['NPI'] = ""
	df_rule6['dayListSG1Home'] = False
	df_rule6['dayListSG1NotHome'] = False
	df_rule6['dayListSG2'] = False
	df_rule6['dayListPTNJ'] = False
	df_rule6['dayListSG3'] = False
	df_rule6['dayListSG4'] = False

	df_SpecialtyLookup = df_SpecialtyLookup.set_index('DoctorGroupID')

	SpecialtyGroup1 = ['Ment', 'Podia', 'Derm', 'IntRa', 'Surg']
	SpecialtyGroup2 = ['PT', 'Occup']
	SpecialtyGroup3 = ['SLP']
	SpecialtyGroup4 = ['Chir']
	

	for index, row in df_rule6.iterrows():
					
		Specialty = df_SpecialtyLookup.loc[row['DoctorID']].values[0]
		NPI = df_SpecialtyLookup.loc[row['DoctorID']].values[1]
		df_rule6.at[index, "Specialty"] = Specialty
		df_rule6.at[index, "NPI"] = NPI
		
	NPIList = list(dict.fromkeys(df_rule6["NPI"]))
	print(NPIList)

	open("rule6 "+startDate+" "+endDate + ".txt", 'w').close()
	f = open("rule6 "+startDate+" "+endDate+  ".txt", "a")
	for NPI in NPIList:

		datesDictionary = defaultdict(list)
		df_NPISub = df_rule6[df_rule6['NPI']==NPI].copy()

		for index, row in df_rule6.iterrows():
			Dates = [row['DOSFrom1'],row['DOSFrom2'],row['DOSFrom3'],row['DOSFrom4'],row['DOSFrom5'],row['DOSFrom6']]
			PlaceOfService = [row['PlaceOfService1'],row['PlaceOfService2'],row['PlaceOfService3'],row['PlaceOfService4'],row['PlaceOfService5'],row['PlaceOfService6']]
			State = row['FacState']

			count = 0
			removelist = []
			for i in Dates:
				if not (str(i)).strip() or pd.isnull(i):
					removelist.append(count)
				count +=1
		
			for i in sorted(removelist, reverse=True):
				del Dates[i]
				del PlaceOfService[i]
			
			for Date, Place in zip(Dates, PlaceOfService):
				datesDictionary[Date].append((row['PatientID'], Place, State,row['Specialty'],row['DoctorLName'],row['DoctorFName'],row['PracticeID']))

			for key,value in datesDictionary.items():
				value = list(dict.fromkeys(value))
		#print(len(datesDictionary))

		for Date, value in datesDictionary.items():
			#print(Date)

			#append to dictionaries
			dayListSG1Home = []
			dayListSG1NotHome = []
			dayListSG2 = []
			dayListPTNJ = []
			dayListSG3 = []
			dayListSG4 = []
			#check lengths of lists
			for PatientTuple in value:
				if PatientTuple[3] in SpecialtyGroup1:
					if PatientTuple[1] == str(12):
						dayListSG1Home.append( (PatientTuple[0],PatientTuple[4],PatientTuple[5],PatientTuple[6],PatientTuple[3]) )
					else:
						dayListSG1NotHome.append( (PatientTuple[0],PatientTuple[4],PatientTuple[5],PatientTuple[6],PatientTuple[3]) )
				elif PatientTuple[3] in SpecialtyGroup2:
					dayListSG2.append( (PatientTuple[0],PatientTuple[4],PatientTuple[5],PatientTuple[6],PatientTuple[3]) )
					if (PatientTuple[3] == 'PT') & (PatientTuple[2]=="NJ"):
						dayListPTNJ.append( (PatientTuple[0],PatientTuple[4],PatientTuple[5],PatientTuple[6],PatientTuple[3]) )
				elif PatientTuple[3] in SpecialtyGroup3:
					dayListSG3.append( (PatientTuple[0],PatientTuple[4],PatientTuple[5],PatientTuple[6],PatientTuple[3]) )
				elif PatientTuple[3] in SpecialtyGroup4:
					dayListSG4.append( (PatientTuple[0],PatientTuple[4],PatientTuple[5],PatientTuple[6],PatientTuple[3]) )

			dayListSG1Home = list(dict.fromkeys(dayListSG1Home))
			dayListSG1NotHome = list(dict.fromkeys(dayListSG1NotHome))
			dayListSG2 = list(dict.fromkeys(dayListSG2))
			dayListPTNJ = list(dict.fromkeys(dayListPTNJ))
			dayListSG3 = list(dict.fromkeys(dayListSG3))
			dayListSG4 = list(dict.fromkeys(dayListSG4))

			if len(dayListSG1Home) > rule6a:
				f.write("\n")
				f.write(str(len(dayListSG1Home))+" patients on " + str(Date) + " Place of Service Home")
				for Patient in dayListSG1Home:
					f.write("\n")
					f.write("Patient ID: " + str(Patient[0]) + " Doctor: " + str(Patient[1]) + " " + str(Patient[2]) + " PracticeID: " +str(Patient[3]))
			if len(dayListSG1NotHome) > rule6b:
				f.write("\n")
				f.write(str(len(dayListSG1NotHome))+" patients on " + str(Date) + " Place of Service Not Home")
				for Patient in dayListSG1NotHome:
					f.write("\n")
					f.write("Patient ID: " + str(Patient[0]) + " Doctor: " + str(Patient[1]) + " " + str(Patient[2])  + " PracticeID: " +str(Patient[3]))
			if len(dayListSG2) > rule6c:
				f.write("\n")
				f.write(str(len(dayListSG2))+" patients on " + str(Date))
				for Patient in dayListSG2:
					f.write("\n")
					f.write("Patient ID: " + str(Patient[0]) + " Doctor: " + str(Patient[1]) + " " + str(Patient[2])  + " PracticeID: " +str(Patient[3]))
			if len(dayListPTNJ)> rule6d:
				f.write("\n")
				f.write(str(len(dayListPTNJ))+" patients on " + str(Date)+" Location State NJ")
				for Patient in dayListPTNJ:
					f.write("\n")
					f.write("Patient ID: " + str(Patient[0]) + " Doctor: " + str(Patient[1]) + " " + str(Patient[2])  + " PracticeID: " +str(Patient[3]))
			if len(dayListSG3)> rule6e:
				f.write("\n")
				f.write(str(len(dayListSG3))+" patients on " + str(Date))
				for Patient in dayListSG3:
					f.write("\n")
					f.write("Patient ID: " + str(Patient[0]) + " Doctor: " + str(Patient[1]) + " " + str(Patient[2])  + " PracticeID: " +str(Patient[3]))
			if len(dayListSG4)> rule6f:
				f.write("\n")
				f.write(str(len(dayListSG4))+" patients on " + str(Date))
				for Patient in dayListSG4:
					f.write("\n")
					f.write("Patient ID: " + str(Patient[0]) + " Doctor: " + str(Patient[1]) + " " + str(Patient[2])  + " PracticeID: " +str(Patient[3]))
						
	#print(PatientTuple)

	f.close()
	#sort the values into sub dictionaries
			



root = Tk()
DBStartPrompt = Label(root, text="Enter start Date")
DBStartPrompt.grid(row=0,column=0)
DBStartEntry = Entry(root)
DBStartEntry.grid(row=0,column=1)

DBEndPrompt = Label(root, text="Enter end Date")
DBEndPrompt.grid(row=1,column=0)
DBEndEntry = Entry(root)
DBEndEntry.grid(row=1,column=1)

button0 = Button(root, text="All Rules", fg="red", 
	command=lambda: print_rules(7))
button0.grid(row=2,column=0)

button1 = Button(root, text="Rule1", fg="red", 
	command=lambda: print_rules(1))
button1.grid(row=2,column=2)

button2 = Button(root, text="Rule2", fg="red", 
	command=lambda: print_rules(2))
button2.grid(row=2,column=3)

button3 = Button(root, text="Rule3", fg="red", 
	command=lambda: print_rules(3))
button3.grid(row=2,column=4)

button4 = Button(root, text="Rule4", fg="red", 
	command=lambda: print_rules(4))
button4.grid(row=2,column=5)

button5 = Button(root, text="Rule5", fg="red", 
	command=lambda: print_rules(5))
button5.grid(row=2,column=6)

button6 = Button(root, text="Rule6", fg="red", 
	command=lambda: print_rules(6))
button6.grid(row=2,column=7)

root.mainloop()

