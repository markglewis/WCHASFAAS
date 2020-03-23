from cx_Freeze import setup, Executable

base = None    

executables = [Executable("app.py", base=base)]

packages = ["idna"]
options = {
    'build_exe': {    
        'packages':packages,
    },    
}

setup(
    name = "<WCHAuto>",
    options = options,
    version = "0.11",
    description = '<test build>',
    executables = executables
)
#pyinstaller --onefile -w app.py

"Select PatientID,DoctorID,DoctorLName,DoctorFName,PracticeID,FacName,Proc1,Proc2,Proc3,Proc4,Proc5,Proc6,Modifier1,Modifier2,Modifier3,Modifier4,Modifier5,Modifier6,DOSFrom1,DOSFrom2,DOSFrom3,DOSFrom4,DOSFrom5,DOSFrom6 From t_Claim WHERE CAST(DOSFrom1 as date) BETWEEN '"+str(startDate)+"' and '"+str(endDate) + "' and isnull(parentclaimid,'')=''"

where modifier1 like '%26%'

59 26 LT
50 TX LP

f = open("demofile.txt", "r")
for x in f:
  for code in x.split(','):
    print (word)