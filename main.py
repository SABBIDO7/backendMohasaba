import math
import os
import time
from unicodedata import decimal
from urllib import response
import uuid
from datetime import datetime


from dataclasses import Field
from pydantic import BaseModel,Field
from typing import List,Annotated
import mysql.connector as mariadb

import uvicorn
from fastapi import FastAPI, File, Request, UploadFile, Form, Response, status,Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware



import time

import json

# import SqlQueries as sql

app = FastAPI()



origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# app.add_middleware(
#     TrustedHostMiddleware, allowed_hosts=["pssapi.net", "*.pssapi.net"]
# )
# app.add_middleware(HTTPSRedirectMiddleware)


dbHost='80.81.158.76'


# oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# uptime = psutil

# templates = Jinja2Templates(directory="templates")

#ip1 = "192.168.1.129"
#port1 = 9000

# with open(f"C:\\scripts\\qr\\ip.txt",'r',encoding="utf-8") as ip_txt:
#     ip2 = ip_txt.readlines()

# ip1 = ip2[0].split("|")[0]
# port1 = ip2[0].split("|")[1]



activelist = [
    "paradox|MySuperSecureToken|12/03/22:11:31|hkm|"
]

def addList(username,uid,compname):
    # i = 0
    # for i in range(0,len(activelist)):
    #     if username == activelist[i].split("|")[0]:
    #         removeList(i)

    now = datetime.now()
    current_time = now.strftime("%D:%H:%M")
    final = str(compname) + "|" + str(uid) + "|" + str(current_time) + "|" + str(username) + "|"
    print(final)
    activelist.append(final)



def removeList(index):   
    activelist.remove(activelist[index]) 

    
            

  
        

# @app.get("/")    
# async def mohHome(req:Request):
#     return templates.TemplateResponse("index.html",{"request":req})

# @app.get("/Accounting")
# async def mohAcc(req:Request):
#     return templates.TemplateResponse("index.html",{"request":req}) 

# @app.get("/Inventory")
# async def mohInv(req:Request):
#     return templates.TemplateResponse("index.html",{"request":req})

# @app.get("/Invoice")
# async def mohInvoice(req:Request):
#     return templates.TemplateResponse("index.html",{"request":req})

@app.post("/moh/login/")
async def login(compname:str = Form() ,username:str = Form(), password:str = Form() ):
       
    # with open("C:\\scripts\\qr\\users.txt") as info2:
    #     userlist = info2.readlines()
    
    try:
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = "python") 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            return {"Info":"unauthorized",
                        "msg":"Internal Error"
                        }     
    cur = conn.cursor()

    cur.execute("""select * from users""")
    userlist = list(cur)

#('paradox', 'hkm', '123', 'owner', 1)
    for users in userlist:
            if users[0].lower() == compname.lower() and users[1].lower() == username.lower() and users[2] == password:
                if str(users[4]) == "0": 
                    print("problem var")
                    return{"Info":"unauthorized",
                            "msg":"Please Check Your Service Provider"
                            }         
                else:  
                    uid = uuid.uuid1()
                    print(uid)
                    addList(username=username,uid=uid,compname=compname)
                    return{
                        "Info":"authorized",
                        "compname":users[0],
                        "name":users[1],
                        "token":uid,
                        "password":users[2],
                    }
    return{"Info":"unauthorized",
            "msg":"Invalid Username or Password",
         }

@app.post("/INVOICE_DATA_SELECT/")
async def getAccounts(data:dict):
    username=data["username"]

    try:
        conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            
            return({"Info":"unauthorized",
                    "msg":{e}})
    
    cur = conn.cursor()

    print(data)
    
    baseQuary = "select * from "
    
    if data["option"] == "Accounts":
        baseQuary = baseQuary +" listhisab "
        
        baseQuary = baseQuary +" WHERE accno not like '%ALLDATA%' "
        if data["value"] != "":
            baseQuary = baseQuary + f"""  and (accname LIKE '{data["value"]}%' or accname LIKE '%{data["value"]}' or accname LIKE '%{data["value"]}%' or accno LIKE '{data["value"]}%' or accno LIKE '%{data["value"]}' or accno LIKE '%{data["value"]}%' or tel LIKE '{data["value"]}%' or tel LIKE '%{data["value"]}' or tel LIKE '%{data["value"]}%')  """
    
    if data["option"] == "Items":
        baseQuary = baseQuary +" goods "
        
        baseQuary = baseQuary +" WHERE itemno not like '%ALLDATA%' "
        if data["value"] != "":
            baseQuary = baseQuary + f"""  and (itemname LIKE '{data["value"]}%' or itemname LIKE '%{data["value"]}' or itemname LIKE '%{data["value"]}%' or itemno LIKE '{data["value"]}%' or itemno LIKE '%{data["value"]}' or itemno LIKE '%{data["value"]}%' or itemname2 LIKE '{data["value"]}%' or itemname2 LIKE '%{data["value"]}' or itemname2 LIKE '%{data["value"]}%')  """
            
    baseQuary = baseQuary + " limit 100 "
    print(baseQuary)
    cur.execute(baseQuary)
    r = list(cur)
    
    return{
        "Info":"authorized",
        "opp":r
    }
        
        
        
        
        
        
# @app.get("/moh/{uid}/login/")
# async def logintoken(uid:str):
#     val = checkList(uid)
#     if val == "unauthorized":
#         return{"Info":"unauthorized"}

#     elif val.split("|")[0] == "authorized":
#         compname = val.split("|")[1]
#         username = val.split("|")[2]
#         return{
#                 "Info":"authorized",
#                 "compname":compname,
#                 "name":username,
#                     }

@app.get("/moh/{uid}/Accounting/{limit}/",status_code=200)
async def Accounting(uid:str,limit:int):
    # if checkList(uid) == "unauthorized":
    #     return{"Info":"unauthorized"}

    # elif checkList(uid).split("|")[0] == "authorized":
    #     username = checkList(uid).split("|")[1]
    username=uid
    print(username)
    try:
        conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        conn2 = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            
            return({"Info":"unauthorized",
                    "msg":{e}})
    
    cur = conn.cursor()
    cur2 = conn2.cursor()
    
    # cur.execute("SELECT DISTINCT(Cur) FROM listhisab")
    # distinctCurrency = list(cur)

    # totalBalance = []
    # for c in distinctCurrency:
    #     cur.execute(f"SELECT sum(Balance) FROM listhisab where Cur = '{c[0]}';")
    #     temp = list(cur)[0][0]
    #     totalBalance.append({c[0]:round(temp,3)})

    cur.execute(f"""SELECT 
    lh.*, 
   COALESCE(ld.balance,0) AS balance
FROM 
    listhisab lh
LEFT JOIN (
    SELECT 
        AccNo, 
        SUM(DB - CR) AS balance
    FROM 
        listdaily
    GROUP BY 
        AccNo
   
) ld ON lh.AccNo = ld.AccNo
WHERE 
    lh.AccNo IS NOT NULL AND ld.balance>0 limit {limit}""") #honn
    hisab = []
    ind = 0
    for x in cur:
        # if ind !=0:
        #     cur2.execute(f"SELECT SUM(DB-CR) FROM listdaily WHERE AccNo={x[0]} GROUP BY AccNo;")

        #     result = cur2.fetchone()
        #     if result:
        #         balance = result[0]
        #         print(balance)
        #     else:
        #         balance = 0  # Handle the case when there is no result
        # else:
        #     balance=0
        hisab.append({
            "key":ind,
            "AccNo":x[0],
            "AccName":x[1],
            "Cur":x[2],
            "Balance":x[14],
            "set":x[6],
            "category":x[7],
            "Price":x[8],
            "Contact":x[9],
            "TaxNo":x[10],
            "Address":x[12],
            "tel":x[13],
            "Mobile":x[14],
            "AccName2":x[12],
            "Fax":x[13],
        })
        ind = ind +1


    cur.execute("SELECT DISTINCT `BR`,`BRName` FROM `goodsqty` WHERE br is not null and brname is not null order by br asc;")   
    branches = []
    branches.append({
        "key":"Any",
        "split":"",
        "val":"",
    })
    for l in cur:
        branches.append({
            "key":l[0],
            "split":" - ",
            "val":l[1],
        })

    # cur.execute(f"""
    # select * from hisabbr limit {limit} ;
    # """)
    # cur.execute(f""" limit {limit}""") 
#DB CR  Branch calculate balance
    hisabBranches = []
    for x in list(cur):
        # hisabBranches.append({
        #     "AccNo":x[0],
        #     "Branch":x[17],
        #     "Balance":x[20],
        #     "Cur":x[2],
        #     "AccName":x[1],
        #     "tel":x[13],
        #     "Address":x[12],
        #     "Fax":x[16],
        #     "Mobile":x[14],
        #     "Contact":x[9],
        #     "set":x[6],
        #     "category":x[7],
        #     "Price":x[8],
        #     "TaxNo":x[10],
        #     "AccName2":x[15],
        #     "DB":x[18],
        #     "CR":x[19],
        # })
             hisabBranches.append({
          
        })
    print(hisab[0])
    return{
    "Info":"authorized",
    "hisab":hisab,
    "hisabBranches":hisabBranches,
    "branches":branches,
    }
        

@app.post("/accounting/filter/{limit}/")
async def accFilter(data:dict,limit):
    # if checkList(data["token"]) == "unauthorized":
    #     return{"Info":"unauthorized"}
    # elif checkList(data["token"]).split("|")[0] == "authorized":
    #     username = checkList(data["token"]).split("|")[1]
    username = data["username"]
    

    try:
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            response.status_code = status.HTTP_409_CONFLICT
            return({"Info":"unauthorized",
                    "msg":{e}})
    cur = conn.cursor()
    cur2 = conn.cursor()
             
    hisab = []
    temphisab = []
    hisabBranches = []
    branches = []
    mydata = data["data"]["data"]
    filters = data["data"]["filters"]
    
    if mydata["branch"]:
        vdepdetail = " , D.Dep "
    else:
        vdepdetail = ", Cast('' AS CHAR(4)) AS D.DEP"
        
    if mydata["vAny"]:
        pass
    
    

    baseQuary = ""
    
    if not mydata["branch"]:
        prefixWithWithoutBranch="lh."
        baseQuary = """SELECT
    lh.*, 
   COALESCE(ld.balance,0) AS balance
FROM 
    listhisab lh
LEFT JOIN (
    SELECT 
        AccNo, 
        SUM(DB - CR) AS balance
    FROM 
        listdaily
    GROUP BY
        AccNo
) ld ON lh.AccNo = ld.AccNo
WHERE 
    lh.AccNo IS NOT NULL """
   
    elif mydata["branch"]:
        prefixWithWithoutBranch="ld."
        if mydata["selectedBranch"] == "Any":
            #baseQuary = "SELECT * FROM hisabbr WHERE AccNo IS NOT NULL "
            baseQuary="""SELECT SUM(ld.DB - ld.CR) AS Balance, ld.Dep,SUM(ld.DB),SUM(ld.CR),lh.* FROM listdaily ld LEFT JOIN( SELECT * FROM listhisab) lh ON ld.AccNo = lh.AccNo WHERE ld.AccNo IS NOT NULL """
        elif mydata["selectedBranch"] == "":
            #baseQuary = "SELECT * FROM hisabbr WHERE AccNo IS NOT NULL "
            print("SALAM ALEKOM")
            baseQuary="""SELECT SUM(ld.DB - ld.CR) AS Balance, ld.Dep,SUM(ld.DB),SUM(ld.CR),lh.* FROM listdaily ld LEFT JOIN( SELECT * FROM listhisab) lh ON ld.AccNo = lh.AccNo WHERE ld.AccNo IS NOT NULL AND ld.Dep IS NULL """
        else:
            #baseQuary = f"SELECT * FROM hisabbr WHERE Branch = \'{mydata['selectedBranch']}\' "
            baseQuary=f"""SELECT SUM(ld.DB - ld.CR) AS Balance, ld.Dep,SUM(ld.DB),SUM(ld.CR),lh.* FROM listdaily ld LEFT JOIN( SELECT * FROM listhisab) lh ON ld.AccNo = lh.AccNo WHERE ld.AccNo IS NOT NULL AND ld.Dep={mydata['selectedBranch']} """
        
        
        
    if mydata["vAny"] != "":
        if mydata["sAny"] == "Start":
            baseQuary = baseQuary + str(f" AND ( {prefixWithWithoutBranch+"AccNo"} like \'{mydata['vAny']}%\' OR {prefixWithWithoutBranch +"AccName"} like \'{mydata['vAny']}%\' OR {prefixWithWithoutBranch+"Contact"} like \'{mydata['vAny']}%\' OR {prefixWithWithoutBranch +"Address"} like \'{mydata['vAny']}%\' OR {prefixWithWithoutBranch +"tel"} like \'{mydata['vAny']}%\' OR {prefixWithWithoutBranch+"AccName2"} like \'{mydata['vAny']}%\' OR {prefixWithWithoutBranch+"Fax"} like \'{mydata['vAny']}%\')  ")
        elif mydata["sAny"] == "Contains":
            baseQuary = baseQuary + str(f" AND ( {prefixWithWithoutBranch+"AccNo"} like \'%{mydata['vAny']}%\' OR  {prefixWithWithoutBranch+"AccNo"} like \'{mydata['vAny']}%\' OR  {prefixWithWithoutBranch+"AccNo"} like \'%{mydata['vAny']}\' OR {prefixWithWithoutBranch +"AccName"} like \'%{mydata['vAny']}%\'  OR {prefixWithWithoutBranch +"AccName"} like \'{mydata['vAny']}%\'  OR {prefixWithWithoutBranch +"AccName"} like \'%{mydata['vAny']}\' OR {prefixWithWithoutBranch+"Contact"} like \'%{mydata['vAny']}%\'  OR {prefixWithWithoutBranch+"Contact"} like \'{mydata['vAny']}%\'  OR {prefixWithWithoutBranch+"Contact"} like \'%{mydata['vAny']}\' OR {prefixWithWithoutBranch +"Address"}  like \'%{mydata['vAny']}%\'  OR {prefixWithWithoutBranch +"Address"}  like \'{mydata['vAny']}%\'  OR {prefixWithWithoutBranch +"Address"}  like \'%{mydata['vAny']}\' OR {prefixWithWithoutBranch +"tel"} like \'%{mydata['vAny']}%\'  OR {prefixWithWithoutBranch +"tel"} like \'{mydata['vAny']}%\'  OR {prefixWithWithoutBranch +"tel"} like \'%{mydata['vAny']}\' OR {prefixWithWithoutBranch+"AccName2"} like \'%{mydata['vAny']}%\'  OR {prefixWithWithoutBranch+"AccName2"} like \'{mydata['vAny']}%\'  OR {prefixWithWithoutBranch+"AccName2"} like \'%{mydata['vAny']}\' OR {prefixWithWithoutBranch+"Fax"} like \'%{mydata['vAny']}%\' OR {prefixWithWithoutBranch+"Fax"} like \'{mydata['vAny']}%\' OR {prefixWithWithoutBranch+"Fax"} like \'%{mydata['vAny']}\' )  ")
    for f in filters:
        if mydata["branch"]:
            if f['name']!="Balance":
                
                fullyName="ld."+f['name']
            else:
                fullyName=f['name']
        else:
            fullyName="lh."+f['name']
        if f["value"] != "":
            if f["type"] == "Start":
                
                baseQuary = baseQuary + str(f" AND {fullyName} like \'{f['value']}%\' ")
            elif f["type"] == "Contains":
                baseQuary = baseQuary + str(f" AND ({fullyName} like \'%{f['value']}%\' OR {fullyName} like \'{f['value']}%\' OR {fullyName} like \'%{f['value']}\')")
            elif f["type"] == "Not Equal":
                baseQuary = baseQuary + str(f" AND {fullyName} != \'{f['value']}\' ")
            elif f["type"] == ">":
                baseQuary = baseQuary + str(f" AND {fullyName} > \'{f['value']}\' ")
            elif f["type"] == "<":
                baseQuary = baseQuary + str(f" AND {fullyName} < \'{f['value']}\' ")
            elif f["type"] == "=":
                baseQuary = baseQuary + str(f" AND {fullyName} = \'{f['value']}\' ")
            elif f["type"] == ">=":
                baseQuary = baseQuary + str(f" AND {fullyName} >= \'{f['value']}\' ")
            elif f["type"] == "<=":
                baseQuary = baseQuary + str(f" AND {fullyName} <= \'{f['value']}\' ")
           
    
    if limit != "All":
        if  mydata["branch"]:
             
            baseQuary = baseQuary + str(f"GROUP BY ld.AccNo,ld.Dep limit {limit};")
        else:
           
            baseQuary = baseQuary + str(f"GROUP BY lh.AccNo limit {limit};")
    else:
        if  mydata["branch"]:
             
            baseQuary = baseQuary + str(f"GROUP BY ld.AccNo,ld.Dep;")
        else:
            baseQuary = baseQuary + str(f"GROUP BY lh.AccNo;")
    
    print(baseQuary)
    cur.execute(baseQuary)
    
    r = list(cur)
    hisab = []
    ind = 0
    if(mydata["branch"]):
        for x in r:

            hisab.append({
                    "key":ind,
                    "AccNo":x[4],
                    "AccName":x[5],
                    "Cur":x[6],
                    "Balance":x[0],
                    "set":x[10],
                    "category":x[11],
                    "Price":x[12],
                    "Contact":x[13],
                    "TaxNo":x[14],
                    "Address":x[16],
                    "tel":x[17],
                    "Mobile":x[18],
                    "AccName2":x[19],
                    "Fax":x[20],
                    "Branch":x[1]
                    
                })
            ind = ind +1
    else:
        for x in r:

            hisab.append({
                    "key":ind,
                    "AccNo":x[0],
                    "AccName":x[1],
                    "Cur":x[2],
                    "Balance":x[17],
                    "set":x[6],
                    "category":x[7],
                    "Price":x[8],
                    "Contact":x[9],
                    "TaxNo":x[10],
                    "Address":x[12],
                    "tel":x[13],
                    "Mobile":x[14],
                    "AccName2":x[15],
                    "Fax":x[16],
                    
                })
            ind = ind +1
    # cur.execute(f"""
    #     select * from hisabbr ;
    #     """)
    
    if mydata["branch"]:
        for x in r:
       # if checkListFilter(x):
            hisabBranches.append({
                "AccNo":x[4],
                "Branch":x[1],
                "Balance":x[0],
                "Cur":x[6],
                "AccName":x[5],
                "tel":x[17],
                "Address":x[16],
                "Fax":x[20],
                "Mobile":x[18],
                "Contact":x[13],
                "set":x[10],
                "category":x[11],
                "Price":x[12],
                "TaxNo":x[14],
                "AccName2":x[19],
                "DB":x[2],
                "CR":x[3],
            })

    
    return{
       "Info":"authorized",
       "hisab":hisab,
       "hisabBranches":hisabBranches,
        
   }
    
    
@app.get("/moh/{uid}/Accounting/ItemsFromAccount/{id}/{limit}",status_code=200)
async def StockStatement(uid:str, id:str, limit:int):
    # print(uid)
    # if checkList(uid) == "unauthorized":
    #     return{"Info":"unauthorized"}

    # elif checkList(uid).split("|")[0] == "authorized":
    #     username = checkList(uid).split("|")[1]
    username=uid
    try:
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            response.status_code = status.HTTP_401_UNAUTHORIZED
            return({"Info":"unauthorized",
                    "msg":{e}}) 
        
    cur = conn.cursor()
    if str(id).lower() == "alldata":
        baseQuary = f"SELECT * FROM `goodstrans`   WHERE `accno` is not null "
    else:
        baseQuary = f"SELECT * FROM `goodstrans`   WHERE `accno` = '{id}' "
    
    baseQuary = baseQuary + f"  ORDER BY `TDate` desc,`Time` desc limit {limit} "
    
    cur.execute(baseQuary)
    distype = []
    disStock = []
    dbr = []
    stock = []
    ind = 0
    dbr.append({"value":"Any","label":"Any"})
    distype.append({"value":"Any","label":"Any"})
    disStock.append({"value":"","label":""})
    for x in cur:
        
        stock.append({     
        "key":ind,                 
        "RefType" :x[0] ,  
        "RefNo" :x[1],    
        "TDate":x[2],   
        "LN" :x[3],
        "ItemNo" :x[4],
        "Branch" :x[5],
        "PQty" :x[6],
        "Qty" :x[7],
        "UPrice" :x[8],
        "UFob" :x[9],
        "PQUnit" :x[10],
        "Disc" :x[11],
        "Weight" :x[12],
        "Notes" :x[13],
        "Tax":x[14],
        "Total" :x[15],
        "AccNo" :x[16],
        "Disc100":x[17],
        "AccName":x[18],
        "ItemName":x[19],
        "Time":x[20],
        })
        disitm = {
                "value":x[4],
                "label":x[19]
            }
        if disitm not in disStock:
            disStock.append(disitm)
            
        ind = ind +1
        dtype = {
        "label":x[0],
        "value":x[0],
        }
        if dtype not in distype:
            distype.append(dtype)
        
        br = {
            "label":x[5],
            "value":x[5],
        }
        if br not in dbr:
            dbr.append(br)
    distype.append({"value":"Sales","label":"Sales"})
    distype.append({"value":"Purchase","label":"Purchase"})
    distype.append({"value":"Order","label":"Order"})
    distype.append({"value":"Transfers","label":"Transfers"})
    distype.append({"value":"Receipt V","label":"Receipt V"})
    distype.append({"value":"Payment V","label":"Payment V"})
    distype.append({"value":"Journal  V","label":"Journal  V"})

    
    return{
    "Info":"authorized",
    "stock":stock,
    "disStock":disStock,
    "disType":distype,
    "dbr":dbr,
    }


@app.post("/moh/Accounting/ItemsFromAccount/Filter/",status_code=200)
async def StockStatementFilter(data:dict):
    print(data)
    # if checkList(data["token"]) == "unauthorized":
    #     return{"Info":"unauthorized"}

    # elif checkList(data["token"]).split("|")[0] == "authorized":
    #     username = checkList(data["token"]).split("|")[1]
    username=data["username"]
    print("hola")
    try:
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            response.status_code = status.HTTP_401_UNAUTHORIZED
            return({"Info":"unauthorized",
                    "msg":{e}}) 
    
    cur = conn.cursor()
    if str(data['id']).lower() == "alldata":
        baseQuary = f"SELECT * FROM `goodstrans`   WHERE `AccNo` is not null "
    else:
        baseQuary = f"SELECT * FROM `goodstrans`   WHERE `AccNo` = '{data['id']}' "
        
    if data["data"]["dfrom"] != "":
        datelst = str(data["data"]["dfrom"]).split("-")
        fdate = datelst[0] +"/"+ datelst[1] +"/"+ datelst[2]
        baseQuary = baseQuary + f" and TDate >= '{fdate}' "

    if data["data"]["dto"] != "":
        datelst = str(data["data"]["dto"]).split("-")
        fdate = datelst[0] +"/"+ datelst[1] +"/"+ datelst[2]
        baseQuary = baseQuary + f" and TDate <= '{fdate}' "
    
    if data["data"]["dtype"] == "Sales":
        baseQuary = baseQuary + f" AND (reftype like 'SA%' OR reftype LIKE 'SR%')  and reftype not like 'SAT%'  "
    
    elif data["data"]["dtype"] == "Purchase":
        baseQuary = baseQuary + f" AND (reftype like 'PI%' OR reftype LIKE 'PR%')  "
        
    elif data["data"]["dtype"] == "Order":
        baseQuary = baseQuary + f" AND reftype like 'OD%'  "
        
    elif data["data"]["dtype"] == "Transfers":
        baseQuary = baseQuary + f" AND reftype like 'SAT%'  "
        
    elif data["data"]["dtype"] == "Receipt V":
        baseQuary = baseQuary + f" AND reftype like 'CR%'  "
        
    elif data["data"]["dtype"] == "Payment V":
        baseQuary = baseQuary + f" AND reftype like 'DB%'  "
        
    elif data["data"]["dtype"] == "Journal  V":
        baseQuary = baseQuary + f" AND reftype like 'JV%'  "
        
    elif data["data"]["dtype"] != "Any":
        baseQuary = baseQuary + f" and reftype = \'{data['data']['dtype']}\' "
    
    if data["data"]["db"] != "Any":
        baseQuary = baseQuary + f" and branch = \'{data['data']['db']}\' "
        
        #item name and number search 
    if data["data"]["itm"] != "":
        print(data["data"]["itm"])
        baseQuary = baseQuary + f" and (itemno = \'{data['data']['itm']}\' or itemname = \'{data['data']['itm']}\') "
    
    if data["limit"]!="All":
        limit=data["limit"]
        baseQuary = baseQuary + f" ORDER BY `TDate` desc,`Time` desc limit {limit} "
    else:
        baseQuary = baseQuary + f" ORDER BY `TDate` desc,`Time` desc"
    
    
    cur.execute(baseQuary)

    stock = []
    ind = 0
    for x in cur:
        
        stock.append({     
        "key":ind,                 
        "RefType" :x[0] ,  
        "RefNo" :x[1],    
        "TDate":x[2],   
        "LN" :x[3],
        "ItemNo" :x[4],
        "Branch" :x[5],
        "PQty" :x[6],
        "Qty" :x[7],
        "UPrice" :x[8],
        "UFob" :x[9],
        "PQUnit" :x[10],
        "Disc" :x[11],
        "Weight" :x[12],
        "Notes" :x[13],
        "Tax":x[14],
        "Total" :x[15],
        "AccNo" :x[16],
        "Disc100":x[17],
        "AccName":x[18],
        "ItemName":x[19],
        "Time":x[20],
        })
        ind = ind +1


    return{
    "Info":"authorized",
    "Statment":stock,
    }



@app.get("/moh/getBranches/{uid}/")
async def getBranches(uid:str):

    username=uid
    try:
        conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            
            return({"Info":"unauthorized",
                    "msg":{e}})
        
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT `BR`,`BRName` FROM `goodsqty` WHERE br is not null and brname is not null order by br asc;")   
    branches = []
    
    for br in cur:
        if br[0] != "":
            branches.append({
                "number":br[0],
                "name":br[1],
            })
        
    return{
        "Info":"authorized",
        "branches":branches,
        }
        



@app.get("/moh/{uid}/Accounting/Statement/{id}/{limit}",status_code=200)
async def AccStatement(uid:str ,id:str,limit:int):
    if limit=="All":
        flag=1
    else:
        flag=0
    # if checkList(uid) == "unauthorized":
    #     return{"Info":"unauthorized"}

    # elif checkList(uid).split("|")[0] == "authorized":
    #     username = checkList(uid).split("|")[1]
    username=uid
    try:
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            response.status_code = status.HTTP_409_CONFLICT
            return({"Info":"unauthorized",
                    "msg":{e}})
    
    cur = conn.cursor()
    if str(id).lower() ==  "alldata" :
        if flag==0:
            cur.execute(f"SELECT * FROM `listdaily` ORDER BY `Date` desc,`Time` desc limit {limit}  ;")
        else:
            cur.execute(f"SELECT * FROM `listdaily` ORDER BY `Date` desc,`Time` desc  ;")
    else:
        if flag==0:
            cur.execute(f"SELECT * FROM `listdaily` WHERE `AccNo` = '{id}' ORDER BY `Date` desc, `Time` desc limit {limit}  ;")
        else:
            cur.execute(f"SELECT * FROM `listdaily` WHERE `AccNo` = '{id}' ORDER BY `Date` desc, `Time` desc  ;")

        
    stat = []
    x= 0
    distype = []
    dbr = []
    tdb = 0
    tcr = 0
    dbr.append({"value":"Any","label":"Any"})
    distype.append({"value":"Any","label":"Any"})
    for vstat in cur:
        try:
            lno = math.trunc(float(vstat[2]))
            if (lno < float(vstat[2])):
                f = float(vstat[2]) 
            else:
                f = lno
        except:
            print("nothing")
      
        stat.append({
            "key":x,
            "RefType":vstat[0],
            "RefNo":vstat[1],
            "LNo":f,
            "AccNo":vstat[3],
            "Date":vstat[4],
            "DB":vstat[5],
            "CR":vstat[6],
            "Dep":vstat[7],
            "job":vstat[9],
            "Bank":vstat[10],
            "CHQ":vstat[11],
            "CHQ2":vstat[12],
            "OppAcc":vstat[13],
            "Notes":vstat[14],
             "Time":vstat[15],
            
        })
        x= x +1
        dtype = {
            "label":vstat[0],
            "value":vstat[0],
        }
        if dtype not in distype:
            distype.append(dtype)
        
        br = {
            "label":vstat[7],
            "value":vstat[7],
        }
        if br not in dbr:
            dbr.append(br)
        tdb = tdb + math.trunc(float(vstat[5]))
        tcr = tcr + math.trunc(float(vstat[6]))
    distype.append({"value":"Sales","label":"Sales"})
    distype.append({"value":"Purchase","label":"Purchase"})
    distype.append({"value":"Order","label":"Order"})
    distype.append({"value":"Transfers","label":"Transfers"})
    distype.append({"value":"Receipt V","label":"Receipt V"})
    distype.append({"value":"Payment V","label":"Payment V"})
    distype.append({"value":"Journal  V","label":"Journal  V"})
        
    return{
        "Info":"authorized",
        "Statment":stat,
        "disType":distype,
        "dbr":dbr,
        "tdb":tdb,
        "tcr":tcr,
        "tb":tdb - tcr,
        
        }
    
    
@app.post("/moh/Accounting/Statement/Filter/",status_code=200)
async def AccStatementFilter(data:dict):
    # if checkList(data["token"]) == "unauthorized":
    #     return{"Info":"unauthorized"}
    print(data)
    print("blaaaaaaaaaaaaaaaaaaaaaaa")
    username = data["username"]
    try:
        conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
    except mariadb.Error as e:       
        print(f"Error connecting to MariaDB Platform: {e}")  
        response.status_code = status.HTTP_409_CONFLICT
        return({"Info":"unauthorized",
                "msg":{e}})
    
    cur = conn.cursor()
    print(str(data['id']).lower())
    if "alldata" == str(data['id']).lower():
        baseQuary = f"SELECT * FROM listdaily WHERE AccNo IS NOT NULL"
    else:    
        baseQuary = f"SELECT * FROM listdaily  WHERE AccNo = {data['id']} "
    
    if data["data"]["dfrom"] != "":
        datelst = str(data["data"]["dfrom"]).split("-")
        fdate = datelst[0] +"/"+ datelst[1] +"/"+ datelst[2]
        baseQuary = baseQuary + f" and date >= '{fdate}' "
    
    if data["data"]["dto"] != "":
        datelst = str(data["data"]["dto"]).split("-")
        fdate = datelst[0] +"/"+ datelst[1] +"/"+ datelst[2]
        baseQuary = baseQuary + f" and date <= '{fdate}' "
   
    
    if data["data"]["dtype"] == "Sales":
        baseQuary = baseQuary + f" AND (reftype like 'SA%' OR reftype LIKE 'SR%')  and reftype not like 'SAT%'  "
        
    elif data["data"]["dtype"] == "Purchase":
        baseQuary = baseQuary + f" AND (reftype like 'PI%' OR reftype LIKE 'PR%')  "
        
    elif data["data"]["dtype"] == "Order":
        baseQuary = baseQuary + f" AND reftype like 'OD%'  "
        
    elif data["data"]["dtype"] == "Transfers":
        baseQuary = baseQuary + f" AND reftype like 'SAT%'  "
        
    elif data["data"]["dtype"] == "Receipt V":
        baseQuary = baseQuary + f" AND reftype like 'CR%'  "
        
    elif data["data"]["dtype"] == "Payment V":
        baseQuary = baseQuary + f" AND reftype like 'DB%'  "
        
    elif data["data"]["dtype"] == "Journal  V":
        baseQuary = baseQuary + f" AND reftype like 'JV%'  "
        
    elif data["data"]["dtype"] != "Any":
        baseQuary = baseQuary + f" and reftype = \'{data['data']['dtype']}\' "
    
    if data["data"]["db"] != "Any":
        baseQuary = baseQuary + f" and dep = \'{data['data']['db']}\' "
    if data["limit"] != "All":
        limit=data["limit"]
        print("honn fetit")
        print(limit)
        baseQuary = baseQuary + f" ORDER BY Date desc,Time desc limit {limit}"
    else:
        print("fetit")
        baseQuary = baseQuary + " ORDER BY Date desc,Time desc "

    print(baseQuary)
    cur.execute(baseQuary)
    stat = []
    x= 0
    tdb = 0
    tcr = 0
    for vstat in cur:

        try:    
            lno = math.trunc(float(vstat[2]))
            if (lno < float(vstat[2])):
                f = float(vstat[2]) 
            else:
                f = lno
        except:
            print("x hon:")
            print(x)
            print(vstat[2])
            print(type(vstat[2]))
            
      
        stat.append({
            "key":x,
            "RefType":vstat[0],
            "RefNo":vstat[1],
            "LNo":f,
            "AccNo":vstat[3],
            "Date":vstat[4],
            "DB":vstat[5],
            "CR":vstat[6],
            "Dep":vstat[7],
            "job":vstat[9],
            "Bank":vstat[10],
            "CHQ":vstat[11],
            "CHQ2":vstat[12],
            "OppAcc":vstat[13],
            "Notes":vstat[14],
            "Time":vstat[15],
            
        })
        x= x +1
        tdb = tdb + float(vstat[5])
        tcr = tcr + float(vstat[6])
        
    return{
        "Info":"authorized",
        "Statment":stat,
        "tdb":tdb,
        "tcr":tcr,
        "tb":tdb - tcr,
        }

        

@app.get("/moh/{uid}/Accounting/Balance/{id}/",status_code=200) #honn
async def AccStatement(uid:str ,id:str):
    # if checkList(uid) == "unauthorized":
    #     return{"Info":"unauthorized"}

    # elif checkList(uid).split("|")[0] == "authorized":
    #     username = checkList(uid).split("|")[1]
    username=uid
    try:
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            response.status_code = status.HTTP_409_CONFLICT
            return({"Info":"unauthorized",
                    "msg":{e}})
        
    cur = conn.cursor()
   
    cur.execute("SELECT AccNo, Dep, SUM(DB - CR) AS balance FROM listdaily WHERE AccNo = %s GROUP BY AccNo, Dep;", (id,))
    stat = []
    x= 0
    for vstat in cur:
        if vstat[2]>0:
            DB=vstat[2]
            CR=None
        else:
             CR=-vstat[2]
             DB=None
        stat.append({
            "key":x,
            "AccNo":vstat[0],
            "Branch":vstat[1],
            "DB":DB,
            "CR":CR,
       
            
        })
        x= x +1
    return{
        "Info":"authorized",
        "Branch":stat,
        
        }

@app.get("/moh/{uid}/Accounting/Double/{type}/{number}/",status_code=200)
async def StockStatement(uid:str, type:str, number:str):
    username = uid
    

    try:
        conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username)
             
    except mariadb.Error as e:       
        print(f"Error connecting to MariaDB Platform: {e}")  
        response.status_code = status.HTTP_401_UNAUTHORIZED
            
        return({"Info":"unauthorized",
                     "msg":{e}})
        
    cur = conn.cursor()

    cur.execute(f"SELECT * FROM `goodstrans` WHERE RefType = '{type}' AND RefNo = {number} ORDER BY LN")

      

        
    double = []
    ind = 0
    for x in cur:
        double.append({     
            "key":ind,                 
            "RefType" :x[0],  
            "RefNo" :x[1],    
            "TDate":x[2],   
            "LN" :x[3],
            "ItemNo" :x[4],
            "Branch" :x[5],
            "PQty" :x[6],
            "Qty" :x[7],
            "UPrice" :x[8],
            "UFob" :x[9],
            "PQUnit" :x[10],
            "Disc" :x[11],
            "Weight" :x[12],
            "Notes" :x[13],
            "Tax":x[14],
            "Total" :x[15],
            "AccNo" :x[16],
            "Disc100":x[17],
            "AccName":x[18],
            "ItemName":x[19],
        })
        ind = ind +1

    return{
        "Info":"authorized",
        "double":double,
    }

@app.get("/moh/{uid}/Accounting/Summery/{id}/",status_code=200)
async def AccountingBranch(uid:str, id:str):
    # if checkList(uid) == "unauthorized":
    #     return{"Info":"unauthorized"}

    # elif checkList(uid).split("|")[0] == "authorized":
    #     username = checkList(uid).split("|")[1]
    username=uid
    try:
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            response.status_code = status.HTTP_401_UNAUTHORIZED
            return({"Info":"unauthorized",
                    "msg":{e}})
    
    cur = conn.cursor()
    if id =="ALLDATA":
         cur.execute(f"""SELECT LEFT(ld.RefType, 2) AS RType,ld.AccNo,lh.Name,ld.Dep AS BR, SUM(DB) AS DB, SUM(CR) AS CR, SUM(DB - CR) AS Balance
FROM 
    listdaily 
    ld LEFT JOIN (SELECT AccNo,AccName AS Name FROM listhisab GROUP BY AccNo) lh ON lh.AccNo = ld.AccNo
GROUP BY
    RType,
    ld.AccNo,
    BR;""")
    else:
        cur.execute(f"""    SELECT LEFT(ld.RefType, 2) AS RType,ld.AccNo,lh.Name,ld.Dep AS BR, SUM(DB) AS DB, SUM(CR) AS CR, SUM(DB - CR) AS Balance
FROM 
    listdaily 
    ld LEFT JOIN (SELECT AccNo,AccName AS Name FROM listhisab GROUP BY AccNo) lh ON lh.AccNo = ld.AccNo
WHERE ld.AccNo = '{id}'
GROUP BY
    RType,
    ld.AccNo,
    BR;""")
    summery = []
    ind = 0
    for x in cur:          
        summery.append({     
        "key":ind,                 
        "ItemNo" :x[0],  
        "Branch" :x[1], 
        "InvType":x[2],
        "Qty":x[3], 
        "Total":x[4], 
        "Tax":x[5], 
        })
        ind = ind +1

    
    return{
    "Info":"authorized",
    "summery":summery
    }








        
@app.get("/moh/{uid}/Stock/{limit}/",status_code=200)
async def Stock(uid:str,limit:int):
    # if checkList(uid) == "unauthorized":
    #     return{"Info":"unauthorized"}

    # elif checkList(uid).split("|")[0] == "authorized":
    #     username = checkList(uid).split("|")[1]
    username=uid
    try:
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            response.status_code = status.HTTP_401_UNAUTHORIZED
            return({"Info":"unauthorized",
                    "msg":{e}})
    
    cur = conn.cursor()
    
    cur.execute(f"SELECT go.*,gt.totalQty,gt.Branch FROM goods go LEFT JOIN (SELECT SUM(Qin - Qout) AS totalQty, ItemNo,Branch FROM goodstrans GROUP BY ItemNo) gt ON go.ItemNo = gt.ItemNo limit {limit};")
    qstock = list(cur)
   
    goods = []
    ind = 0
    cur.execute("SELECT DISTINCT `Branch`,Branch FROM `goodstrans` WHERE Branch is not null order by Branch asc ;")



    branches = []
    branches.append({
        "key":"Any",
        "split":"",
        "val":"",
    })
    for l in cur:
        branches.append({
            "key":l[0],
            "split":" - ",
            "val":l[1],
        })
        print(branches)
    for x in qstock:
      
        goods.append({     
            "key":ind,                 
        "ItemNo" :x[0] ,  
        "ItemName" :x[1],    
        "ItemName2":x[2],   
        "MainNo" :x[3],
        "SetG" :x[4],
        "Category" :x[5],
        "Unit" :x[6],
        "Brand" :x[7],
        "Origin" :x[8],
        "Supplier" :x[9],
        "Sizeg" :x[10],
        "Color" :x[11],
        "Family" :x[12],
        "Groupg" :x[13],
        "Tax":x[14],
        "SPrice1" :x[15],
        "SPrice2" :x[16],
        "SPrice3" :x[17],
        "Disc1" :x[18],
        "Disc2" :x[19],
        "Disc3" :x[20],
        "CostPrice":x[21] ,
        "FobCost" :x[22],
        "AvPrice" :x[23],
        "BPUnit" :x[24],
        "PQty" :x[25],
        "PUnit" :x[26],
        "PQUnit" :x[27],
        "SPUnit" :x[28],
        "Qty":x[30],
        "branch":"",
        
        })
        ind = ind +1




    return{
    "Info":"authorized",
    "stock":goods,
    "branches":branches,
    "branchStock": "",
    }


@app.post("/Stock/Filter/{limit}/")
async def stockFilter(data:dict,limit):
    # if checkList(data["token"]) == "unauthorized":
    #     return{"Info":"unauthorized"}
    # elif checkList(data["token"]).split("|")[0] == "authorized":
    #     username = checkList(data["token"]).split("|")[1]
    username = data["username"]
    try:
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            response.status_code = status.HTTP_409_CONFLICT
            return({"Info":"unauthorized",
                    "msg":{e}})
    cur = conn.cursor()
    
    
    mydata = data["data"]["data"]
    filters = data["data"]["filters"]
    
    baseQuary = ""
    ff = 0
    if not mydata["branch"]:
        baseQuary = f"SELECT go.*,gt.qty,gt.Branch FROM goods go LEFT JOIN (SELECT SUM(Qin - Qout) AS qty, ItemNo,Branch FROM goodstrans GROUP BY ItemNo) gt ON go.ItemNo = gt.ItemNo WHERE go.ItemNo IS NOT NULL "
        #baseQuary = "SELECT * FROM goods WHERE itemno IS NOT NULL "
    elif mydata["branch"]:
        ff = 4
        if mydata["selectedBranch"] == "Any":
            baseQuary = f"SELECT go.*,gt.qty,gt.Branch FROM goods go LEFT JOIN (SELECT SUM(Qin - Qout) AS qty, ItemNo,Branch FROM goodstrans GROUP BY ItemNo,Branch) gt ON go.ItemNo = gt.ItemNo WHERE go.ItemNo IS NOT NULL "
            #baseQuary = "select * from goodsbr WHERE itemno IS NOT NULL "
        else:
            baseQuary = f"SELECT go.*,gt.qty,gt.Branch FROM goods go LEFT JOIN (SELECT SUM(Qin - Qout) AS qty, ItemNo,Branch FROM goodstrans WHERE Branch = {mydata['selectedBranch']} GROUP BY ItemNo,Branch) gt ON go.ItemNo = gt.ItemNo WHERE go.ItemNo IS NOT NULL "
            #baseQuary = f"select * from goodsbr WHERE br = \'{mydata['selectedBranch']}\' "
    fullyName='go.'
    if mydata["vAny"] != "":
        if mydata["sAny"] == "Start":
            baseQuary = baseQuary + str(f"AND ( {fullyName}ItemNo like \'{mydata['vAny']}%\' OR {fullyName}MainNo like \'{mydata['vAny']}%\' OR {fullyName}ItemName like \'{mydata['vAny']}%\' OR {fullyName}ItemName2 like \'{mydata['vAny']}%\' ) ")
        elif mydata["sAny"] == "Contains":
            baseQuary = baseQuary + str(f"AND ( {fullyName}ItemNo like \'%{mydata['vAny']}%\' OR  {fullyName}ItemNo like \'{mydata['vAny']}%\' OR  {fullyName}ItemNo like \'%{mydata['vAny']}\' OR {fullyName}MainNo like \'%{mydata['vAny']}%\'  OR {fullyName}MainNo like \'{mydata['vAny']}%\'  OR {fullyName}MainNo like \'%{mydata['vAny']}\' OR {fullyName}ItemName like \'%{mydata['vAny']}%\'  OR {fullyName}ItemName like \'{mydata['vAny']}%\'  OR {fullyName}ItemName like \'%{mydata['vAny']}\' OR {fullyName}ItemName2 like \'%{mydata['vAny']}%\'  OR {fullyName}ItemName2 like \'%{mydata['vAny']}\'  OR {fullyName}ItemName2 like \'{mydata['vAny']}%\' ) ")
    for f in filters:
        
        if f["value"] != "":
            fullyName='go.'
            if f['name'] == "qty":
                fullyName = 'gt.'
            if f["type"] == "Start":
                baseQuary = baseQuary + str(f" AND {fullyName}{f['name']} like \'{f['value']}%\' ")
            elif f["type"] == "Contains":
                baseQuary = baseQuary + str(f" AND ({fullyName}{f['name']} like \'%{f['value']}%\' OR {fullyName}{f['name']} like \'{f['value']}%\' OR {fullyName}{f['name']} like \'%{f['value']}\') ")
            elif f["type"] == "Not Equal":
                baseQuary = baseQuary + str(f" AND {fullyName}{f['name']} != \'{f['value']}\' ")
            elif f["type"] == ">":
                baseQuary = baseQuary + str(f" AND {fullyName}{f['name']} > \'{f['value']}\' ")
            elif f["type"] == "<":
                baseQuary = baseQuary + str(f" AND {fullyName}{f['name']} < \'{f['value']}\' ")
            elif f["type"] == "=":
                baseQuary = baseQuary + str(f" AND {fullyName}{f['name']} = \'{f['value']}\' ")
            elif f["type"] == ">=":
                baseQuary = baseQuary + str(f" AND {fullyName}{f['name']} >= \'{f['value']}\' ")
            elif f["type"] == "<=":
                baseQuary = baseQuary + str(f" AND {fullyName}{f['name']} <= \'{f['value']}\' ")
    
    
    if limit != "All":
        baseQuary = baseQuary + str(f"limit {limit} ;")
    print(baseQuary)
    cur.execute(baseQuary)
    qstock = list(cur)
    goods = []
    ind = 0
    if not mydata["branch"]:
        for x in qstock:
            goods.append({     
                "key":ind,                 
            "ItemNo" :x[0] ,  
            "ItemName" :x[1],    
            "ItemName2":x[2],   
            "MainNo" :x[3],
            "SetG" :x[4],
            "Category" :x[5],
            "Unit" :x[6],
            "Brand" :x[7],
            "Origin" :x[8],
            "Supplier" :x[9],
            "Sizeg" :x[10],
            "Color" :x[11],
            "Family" :x[12],
            "Groupg" :x[13],
            "Tax":x[14],
            "SPrice1" :x[15],
            "SPrice2" :x[16],
            "SPrice3" :x[17],
            "Disc1" :x[18],
            "Disc2" :x[19],
            "Disc3" :x[20],
            "CostPrice":x[21] ,
            "FobCost" :x[22],
            "AvPrice" :x[23],
            "BPUnit" :x[24],
            "PQty" :x[25],
            "PUnit" :x[26],
            "PQUnit" :x[27],
            "SPUnit" :x[28],
            "Qty":x[30],
            "branch":x[31],
            })
            ind = ind +1
            

   
    bStock = []
    ukey = 0
    if mydata["branch"]:
        for x in qstock :
            bStock.append({
            "key":ukey,
            "BR" :x[31],    
            "BRName":x[2],   
            "Qty" :x[30],
            "ItemNo" :x[0],
            "ItemName" :x[1],
            "ItemName2" :x[2],
            "MainNo" :x[3],
            "SetG" :x[4],
            "Category" :x[5],
            "Unit" :x[6],
            "Brand" :x[7],
            "Origin" :x[8],
            "Supplier" :x[9],
            "Sizeg":x[10],
            "Color" :x[11],
            "Family" :x[12],
            "Groupg" :x[13],
            "Tax" :x[14],
            "SPrice1" :x[15],
            "SPrice2" :x[16],
            "SPrice3":x[17] ,
            "Disc1" :x[18],
            "Disc2" :x[19],
            "Disc3" :x[20],
            "CostPrice" :x[21],
            "FobCost" :x[22],
            "AvPrice" :x[23],
            "BPUnit" :x[24],
            "PQty" :x[25],
            "PUnit" :x[26],
            "PQUnit" :x[27],
            "SPUnit" :x[28],
            })
            ukey = ukey + 1
    cur.execute("SELECT DISTINCT `Branch`,Branch FROM `goodstrans` WHERE Branch is not null order by Branch asc ;")



    branches = []
    branches.append({
        "key":"Any",
        "split":"",
        "val":"",
    })
    for l in cur:
        branches.append({
            "key":l[0],
            "split":" - ",
            "val":l[1],
        })
    return{
        "Info":"authorized",
        "stock":goods,
        "branchStock": bStock,
        "branches" : branches
    }
    

@app.get("/moh/{uid}/Stock/Statement/{id}/{limit}",status_code=200)
async def StockStatement(uid:str, id:str,limit:int):
    # if checkList(uid) == "unauthorized":
    #     return{"Info":"unauthorized"}

    # elif checkList(uid).split("|")[0] == "authorized":
    #     username = checkList(uid).split("|")[1]
    username=uid
    try:
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            response.status_code = status.HTTP_401_UNAUTHORIZED
            return({"Info":"unauthorized",
                    "msg":{e}}) 
    
    cur = conn.cursor()
    if str(id).lower() == "alldata":
        baseQuary = f"SELECT * FROM `goodstrans`   WHERE `ItemNo` is not null "
    else:
        baseQuary = f"SELECT * FROM `goodstrans`   WHERE `ItemNo` = '{id}' "
    
    baseQuary = baseQuary + f"  ORDER BY `TDate` desc,`Time` desc limit {limit} "
    
    cur.execute(baseQuary)
    distype = []
    dbr = []
    stock = []
    ind = 0
    dbr.append({"value":"Any","label":"Any"})
    distype.append({"value":"Any","label":"Any"})
    for x in cur:
        
        stock.append({     
        "key":ind,                 
        "RefType" :x[0] ,  
        "RefNo" :x[1],    
        "TDate":x[2],   
        "LN" :x[3],
        "ItemNo" :x[4],
        "Branch" :x[5],
        "PQty" :x[6],
        "Qty" :x[7],
        "UPrice" :x[8],
        "UFob" :x[9],
        "PQUnit" :x[10],
        "Disc" :x[11],
        "Weight" :x[12],
        "Notes" :x[13],
        "Tax":x[14],
        "Total" :x[15],
        "AccNo" :x[16],
        "Disc100":x[17],
        "AccName":x[18],
        "Time":x[20],
        })
        ind = ind +1
        dtype = {
        "label":x[0],
        "value":x[0],
        }
        if dtype not in distype:
            distype.append(dtype)
        
        br = {
            "label":x[5],
            "value":x[5],
        }
        if br not in dbr:
            dbr.append(br)
    distype.append({"value":"Sales","label":"Sales"})
    distype.append({"value":"Purchase","label":"Purchase"})
    distype.append({"value":"Order","label":"Order"})
    distype.append({"value":"Transfers","label":"Transfers"})
    distype.append({"value":"Receipt V","label":"Receipt V"})
    distype.append({"value":"Payment V","label":"Payment V"})
    distype.append({"value":"Journal  V","label":"Journal  V"})

    return{
    "Info":"authorized",
    "stock":stock,
    "disType":distype,
    "dbr":dbr,
    }

    


@app.post("/moh/Stock/Statement/Filter/",status_code=200)
async def StockStatementFilter(data:dict):
    # if checkList(data["token"]) == "unauthorized":
    #     return{"Info":"unauthorized"}

    # elif checkList(data["token"]).split("|")[0] == "authorized":
    #     username = checkList(data["token"]).split("|")[1]
    username=data["username"]
    try:
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            response.status_code = status.HTTP_401_UNAUTHORIZED
            return({"Info":"unauthorized",
                    "msg":{e}}) 
    
    cur = conn.cursor()
    if str(data['id']).lower() == "alldata":
        baseQuary = f"SELECT * FROM `goodstrans`   WHERE `ItemNo` is not null "
    else:
        baseQuary = f"SELECT * FROM `goodstrans`   WHERE `ItemNo` = '{data['id']}' "
        
    if data["data"]["dfrom"] != "":
        datelst = str(data["data"]["dfrom"]).split("-")
        fdate = datelst[0] +"/"+ datelst[1] +"/"+ datelst[2]
        baseQuary = baseQuary + f" and TDate >= '{fdate}' "

    if data["data"]["dto"] != "":
        datelst = str(data["data"]["dto"]).split("-")
        fdate = datelst[0] +"/"+ datelst[1] +"/"+ datelst[2]
        baseQuary = baseQuary + f" and TDate <= '{fdate}' "
    
    if data["data"]["dtype"] == "Sales":
        baseQuary = baseQuary + f" AND (reftype like 'SA%' OR reftype LIKE 'SR%')  and reftype not like 'SAT%'  "
    
    elif data["data"]["dtype"] == "Purchase":
        baseQuary = baseQuary + f" AND (reftype like 'PI%' OR reftype LIKE 'PR%')  "
        
    elif data["data"]["dtype"] == "Order":
        baseQuary = baseQuary + f" AND reftype like 'OD%'  "
        
    elif data["data"]["dtype"] == "Transfers":
        baseQuary = baseQuary + f" AND reftype like 'SAT%'  "
        
    elif data["data"]["dtype"] == "Receipt V":
        baseQuary = baseQuary + f" AND reftype like 'CR%'  "
        
    elif data["data"]["dtype"] == "Payment V":
        baseQuary = baseQuary + f" AND reftype like 'DB%'  "
        
    elif data["data"]["dtype"] == "Journal  V":
        baseQuary = baseQuary + f" AND reftype like 'JV%'  "
        
    elif data["data"]["dtype"] != "Any":
        baseQuary = baseQuary + f" and reftype = \'{data['data']['dtype']}\' "
    
    if data["data"]["db"] != "Any":
        baseQuary = baseQuary + f" and branch = \'{data['data']['db']}\' "
    if data["limit"]!="All":
        limit=data["limit"]
        baseQuary = baseQuary + f" ORDER BY `TDate` desc,`Time` desc limit {limit} "
    else:
        baseQuary = baseQuary + f" ORDER BY `TDate` desc,`Time` desc"
    cur.execute(baseQuary)

    stock = []
    ind = 0
    for x in cur:
        
        stock.append({     
        "key":ind,                 
        "RefType" :x[0] ,  
        "RefNo" :x[1],    
        "TDate":x[2],   
        "LN" :x[3],
        "ItemNo" :x[4],
        "Branch" :x[5],
        "PQty" :x[6],
        "Qty" :x[7],
        "UPrice" :x[8],
        "UFob" :x[9],
        "PQUnit" :x[10],
        "Disc" :x[11],
        "Weight" :x[12],
        "Notes" :x[13],
        "Tax":x[14],
        "Total" :x[15],
        "AccNo" :x[16],
        "Disc100":x[17],
        "AccName":x[18],
        "Time":x[20],
        })
        ind = ind +1


    return{
    "Info":"authorized",
    "Statment":stock,
    }





@app.get("/moh/{uid}/Stock/Branch/{id}/",status_code=200)
async def StockBranch(uid:str, id:str):
    # if checkList(uid) == "unauthorized":
    #     return{"Info":"unauthorized"}

    # elif checkList(uid).split("|")[0] == "authorized":
    #     username = checkList(uid).split("|")[1]
    username=uid
    try:
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            response.status_code = status.HTTP_401_UNAUTHORIZED
            return({"Info":"unauthorized",
                    "msg":{e}})
    
    cur = conn.cursor()
    
    cur.execute(f"SELECT * FROM `goodsqty` WHERE `ItemNo` = '{id}'; ")
    branch = []
    ind = 0
    for x in cur:          
        branch.append({     
        "key":ind,                 
        "ItemNo" :x[0],  
        "BR" :x[1], 
        "BRName":x[2],
        "Qty":x[3], 
        })
        ind = ind +1
    
    return{
    "Info":"authorized",
    "branch":branch
    }



@app.get("/moh/{uid}/Stock/Summery/{id}/",status_code=200)
async def StockBranch(uid:str, id:str):
    # if checkList(uid) == "unauthorized":
    #     return{"Info":"unauthorized"}

    # elif checkList(uid).split("|")[0] == "authorized":
    #     username = checkList(uid).split("|")[1]
    username=uid
    try:
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            response.status_code = status.HTTP_401_UNAUTHORIZED
            return({"Info":"unauthorized",
                    "msg":{e}})
    
    cur = conn.cursor()
    if id =="ALLDATA":
         cur.execute(f"""SELECT 
		ItemNo,Branch AS BR,
    CASE 
        WHEN LEFT(RefType, 3) = 'SAT' THEN LEFT(RefType, 3)
        ELSE LEFT(RefType, 2)
    END AS RType,
    SUM(QTY) AS Qty,
    SUM(Total) AS Total,
    SUM((TAX/100)*Total) AS Tax
    
   
FROM 
    goodstrans 
GROUP BY 
    RType,
    
    Branch;""")
    else:
        cur.execute(f"""SELECT 
            ItemNo,Branch AS BR,
        CASE 
            WHEN LEFT(RefType, 3) = 'SAT' THEN LEFT(RefType, 3)
            ELSE LEFT(RefType, 2)
        END AS RType,
        SUM(QTY) AS Qty,
        SUM(Total) AS Total,
        SUM((TAX/100)*Total) AS Tax
        
    
    FROM 
        goodstrans 
    WHERE 
        ItemNo = '{id}' 
    GROUP BY 
        RType,
        ItemNo,
        Branch;""")
    summery = []
    ind = 0
    for x in cur:          
        summery.append({     
        "key":ind,                 
        "ItemNo" :x[0],  
        "Branch" :x[1], 
        "InvType":x[2],
        "Qty":x[3], 
        "Total":x[4], 
        "Tax":x[5], 
        })
        ind = ind +1

    
    return{
    "Info":"authorized",
    "summery":summery
    }


@app.get("/moh/{uid}/Stock/Double/{type}/{number}/{limit}",status_code=200)
async def StockStatement(uid:str, type:str, number:str,limit:int):

    username=uid
    try:
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            response.status_code = status.HTTP_401_UNAUTHORIZED
            return({"Info":"unauthorized",
                    "msg":{e}})
    
    cur = conn.cursor()
    
    cur.execute(f"SELECT * FROM `goodstrans` WHERE RefType = '{type}' AND RefNo = '{number}' ORDER BY LN limit {limit}")
    double = []
    ind = 0
    for x in cur:
        
        double.append({     
        "key":ind,                 
        "RefType" :x[0] ,  
        "RefNo" :x[1],    
        "TDate":x[2],   
        "LN" :x[3],
        "ItemNo" :x[4],
        "Branch" :x[5],
        "PQty" :x[6],
        "Qty" :x[7],
        "UPrice" :x[8],
        "UFob" :x[9],
        "PQUnit" :x[10],
        "Disc" :x[11],
        "Weight" :x[12],
        "Notes" :x[13],
        "Tax":x[14],
        "Total" :x[15],
        "AccNo" :x[16],
        "Disc100":x[17],
        "AccName":x[18],
        "ItemName":x[19],
        })
        ind = ind +1

    return{
    "Info":"authorized",
    "double":double,
    }

@app.post("/moh/newInvoice/")
async def newInvoice(data:dict):
    username = data["compname"]
    try:
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            response.status_code = status.HTTP_401_UNAUTHORIZED
            return({"Info":"unauthorized",
                    "msg":{e}})
    
    try:
        cur = conn.cursor()
        
        cdate = datetime.now()
        
        print(cdate)
        ddate = str(cdate.date()).split("-")
        tdate = str(cdate.time()).split(":")
        refnumber = str(ddate[1])+str(ddate[2])+str(tdate[0])+str(tdate[1])+str(tdate[2]).split(".")[0]
        
        cur.execute(f"""
                    INSERT INTO `{username}`.`tempinv` (`type`, `number`, `accno`, `accname`, `vdate`, `vtime`, `items`, `user`) 
                    VALUES ('{data["type"]}', '{refnumber}', '{data["accno"]}', '{data["accname"]}', '{str(cdate.date())}', '{str(cdate.time()).split(".")[0]}', '{data["items"]}', '{data["username"]}');
                    """)
        
        items = str(data["items"]).split("!")
        
        print(data)
        
        idx = 1
        
        for item in items:
            if item == "":
                pass
            else:
                pdate = ddate[0] + "/" + ddate[1] + "/" + ddate[2]
                att = str(item).split(";")
                
                total = float(att[1]) * float(att[2]) - (float(att[1]) * float(att[2]) * float(att[4]) ) / 100
                
                cur.execute(f"""
                            UPDATE {username}.goodsqty SET Qty = Qty - {float(att[1])}  WHERE  `ItemNo`='{att[0]}' AND `BR`='{att[3]}';
                            """)
        
                cur.execute(f"""
                            UPDATE {username}.goodsbr SET Qty1 = Qty1 - {float(att[1])}  WHERE  `ItemNoQ`='{att[0]}' AND `BR`='{att[3]}';
                            """)
                
                cur.execute(f"""
                            INSERT INTO `{username}`.`goodstrans` (`RefType`, `RefNo`, `TDate`, `LN`, `ItemNo`, `Branch`, `PQty`, `Qty`, `UPrice`, `UFob`, `PQUnit`, `Disc`, `Weight`, `Notes`, `Tax`, `Total`, `AccNo`, `Disc100`, `AccName`, `ItemName`) 
                            
                            VALUES ('{data['type']}', '{refnumber}', '{pdate}', '{idx}', '{att[0]}', '{att[3]}', '0', '{float(att[1])}', '{float(att[2])}', '0', '', '0', '0', '', '{float(att[5])}', '{total}', '{data["accno"]}', '{float(att[4])}', '{data["accname"]}', '');

                            """)
            idx = idx + 1
        conn.commit()
        
        return{"Info":"authorized",
            "msg":"successfull"}

    except Exception as e:
        return{"Info":"Failed",
            "msg":f"{str(e)}"}



