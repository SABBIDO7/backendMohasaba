import math
import os
import time
from unicodedata import decimal
from urllib import response
import uuid
from datetime import datetime, timedelta



from dataclasses import Field
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel,Field
from typing import Dict, List,Annotated
import mysql.connector as mariadb

import uvicorn
from fastapi import FastAPI, Form, Query, status,HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware
from fastapi.responses import FileResponse, JSONResponse


# from fastapi import FastAPI, WebSocket
# from starlette.responses import JSONResponse
# from starlette.websockets import WebSocketDisconnect



import time

import json
import calendar
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
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


    
            

        

def change_date_format(date_str):
    # Parse the input date string
    date_obj = datetime.strptime(date_str, '%d/%m/%Y')
    
    # Format the date object to the desired format
    formatted_date = date_obj.strftime('%Y/%m/%d')
    return formatted_date

@app.post("/moh/login/")
async def login(compname:str = Form() ,username:str = Form(), password:str = Form()):
           
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
            if users[0].upper() == compname.upper() and users[1].upper() == username.upper() and users[2] == password:
                if str(users[4]) == "N" or str(users[4]) == "n": 
                    
                    return{"Info":"unauthorized",
                            "msg":"Please Check Your Subscription"
                            }         
                else:  
                    uid = uuid.uuid1()
                    if users[8] == "" or users[8] == None:
                        Sbranch = "1"
                    else:
                        Sbranch = users[8].upper()
                    if users[9] == "" or users[9] == None:
                        Abranch = "1"
                    else:
                        Abranch = users[9].upper()
                    if users[10] == ""  or users[10] == None:
                        SalePrice = 1
                    else:
                        SalePrice = users[10]
                    if users[11] == "" or users[11]==None:
                        DeleteInvoice="Y"
                    else:
                        DeleteInvoice=users[11].upper()
                    if users[12] == "" or users[12]==None:
                        DeleteItem="Y"
                    else:
                        DeleteItem=users[12].upper()
                    if users[13] == "" or users[13]==None:
                        Discount="Y"
                    else:
                        Discount=users[13].upper()
                    if users[14] == "" or users[14]==None:
                        Price="Y"
                    else:
                        Price=users[14].upper()
                    if users[15] == "" or users[15]==None:
                        CallInvoice="Y"
                    else:
                        CallInvoice=users[15].upper()
                    if users[16] == "" or users[16]==None:
                        SalesForm="Y"
                    else:
                        SalesForm=users[16].upper()
                    if users[17] == "" or users[17]==None:
                        SalesReturnForm="Y"
                    else:
                        SalesReturnForm=users[17].upper()
                    if users[18] == "" or users[18]==None:
                        OrderForm="Y"
                    else:
                        OrderForm=users[18].upper()
                    if users[19] == "" or users[19]==None:
                        PurchaseForm="Y"
                    else:
                        PurchaseForm=users[19].upper()
                    if users[20] == "" or users[20]==None:
                        PurchaseReturnForm="Y"
                    else:
                        PurchaseReturnForm=users[20].upper()
                    if users[21] == "" or users[21]==None:
                        BranchTransferForm="Y"
                    else:
                        BranchTransferForm=users[21].upper()
                    if users[22] == "" or users[22]==None:
                        SalesUnderZero="Y"
                    else:
                        SalesUnderZero=users[22].upper()
                    if users[23] == "" or users[23]==None:
                        ChangeBranch="Y"
                    else:
                        ChangeBranch=users[23].upper()
                    if users[24] == "" or users[24]==None:
                        CheckInReport="Y"
                    else:
                        CheckInReport=users[24].upper()
                    if users[25] == "" or users[25]==None:
                        AccountingPage="Y"
                    else:
                        AccountingPage=users[25].upper()
                    if users[26] == "" or users[26]==None:
                        InventoryPage="Y"
                    else:
                        InventoryPage=users[26].upper()
                    if users[27] == "" or users[27]==None:
                        TransactionsPage="Y"
                    else:
                        TransactionsPage=users[27].upper()
                    if users[28] == "" or users[28]==None:
                        CheckInPage="Y"
                    else:
                        CheckInPage=users[28].upper()
                    if users[29]=="" or users[29]==None:
                        CompanySettings="Y"
                    else:
                        CompanySettings=users[29].upper()
                    if users[30]=="" or users[30]==None:
                        UserManagement="Y"
                    else:
                        UserManagement=users[30].upper()
                    if users[31]=="" or users[31]==None:
                        CompanyDashboard="Y"
                    else:
                        CompanyDashboard=users[31].upper()
                    print(OrderForm)

                    return{
                        "Info":"authorized",
                        "compname":users[0].upper(),
                        "name":users[1].upper(),
                        "token":uid,
                        "password":users[2],
                        "Sbranch": Sbranch,
                        "Abranch": Abranch,
                        "SalePrice": SalePrice,
                        "Permissions":{
                            "DeleteInvoice":DeleteInvoice,
                            "DeleteItem":DeleteItem,
                            "Discount":Discount,
                            "Price":Price,
                            "CallInvoice":CallInvoice,
                            "SalesForm":SalesForm,
                            "SalesReturnForm":SalesReturnForm,
                            "OrderForm":OrderForm,
                            "PurchaseForm":PurchaseForm,
                            "PurchaseReturnForm":PurchaseReturnForm,
                            "BranchTransferForm":BranchTransferForm,
                            "SalesUnderZero":SalesUnderZero,
                            "ChangeBranch":ChangeBranch,
                            "CheckInReport":CheckInReport,
                            "AccountingPage":AccountingPage,
                            "InventoryPage":InventoryPage,
                            "TransactionsPage":TransactionsPage,
                            "CheckInPage":CheckInPage,
                            "CompanySettings":CompanySettings,
                            "UserManagement":UserManagement,
                            "CompanyDashboard":CompanyDashboard
                        }
                    }

    return {"Info":"unauthorized",
            "msg":"Invalid Username or Password",
        }

@app.post("/INVOICE_DATA_SELECT/")

async def getAccounts(data:dict):
    username=data["username"]
    #print(data)
    
    try:
        conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            
            return{"Info":"error",
                    "msg":f"{e}"}
    
    cur = conn.cursor()
    cur2 = conn.cursor()


    items_json = []
    
    flagA=0
    flagI=0
    ItemsByBranchQuery=""
    try:
        if data["option"] == "Accounts":
            # baseQuary ="SELECT lh.*,Balance from listhisab lh"
            # baseQuary = baseQuary +" LEFT JOIN(SELECT SUM(DB - CR) AS Balance,AccNo FROM listdaily GROUP BY AccNo) ld ON lh.AccNo= ld.AccNo WHERE lh.accno not like '%ALLDATA%' "
            baseQuary = AccountSearchquery("(SELECT * FROM listhisab WHERE accno NOT LIKE '%ALLDATA%' ORDER BY AccNo LIMIT 150) lh")
            if data["value"] != "":
                baseQuary=  AccountSearchquery(F""" (SELECT * FROM listhisab WHERE accno NOT LIKE '%ALLDATA%' AND  accno='{data["value"]}' ORDER BY AccNo LIMIT 150) lh """)
                print(baseQuary)
                cur.execute(baseQuary)
                A=0
            
                for row in cur:
                    account_dict = {
                    "AccNo": row[0],
                    "AccName": row[1],
                    "Cur": row[2],
                    "SETA": row[3],
                    "Category": row[4],
                    "Price": row[5],
                    "Contact": row[6],
                    "TaxNo": row[7],
                    "SMan": row[8],
                    "Address": row[9],
                    "Tel": row[10],
                    "Mobile": row[11],
                    "AccName2": row[12],
                    "Fax": row[13],
                    "DeliveryDays": row[14],
                    "Balance": row[15]
                    }
                # Append the dictionary to the results list
                    items_json.append(account_dict)
                    print(items_json)
                    flagA=1
                #print("rrrtttttt")
                #print(A)
                if flagA==0:
                    baseQuary=  AccountSearchquery(f""" (SELECT * FROM listhisab WHERE accno NOT LIKE '%ALLDATA%' AND  accno='{data["value"]}'   OR (accname LIKE '{data["value"]}%' or accname LIKE '%{data["value"]}' or accname LIKE '%{data["value"]}%' or accno LIKE '{data["value"]}%' or tel LIKE '{data["value"]}%' or tel LIKE '%{data["value"]}' or tel LIKE '%{data["value"]}%' or contact LIKE '{data["value"]}%' or contact LIKE '%{data["value"]}' or contact LIKE '%{data["value"]}%' )  or address  LIKE '{data["value"]}%' or address  LIKE '%{data["value"]}' or address  LIKE '%{data["value"]}%' ORDER BY AccNo LIMIT 150) lh """)

            
        
        if data["option"] == "Items":
            branches=[]
            getbranches="SELECT DISTINCT Branch FROM header WHERE Branch IS NOT NULL"
            cur2.execute(getbranches)
            Columns=""
            for row in cur2:
                branches.append(row[0])
            
            for idx, branch in enumerate(branches):
                if idx == len(branches) - 1:
                    Columns += f"SUM(CASE WHEN gt.Branch = '{branch}' THEN gt.AvQty ELSE 0 END) AS Br{branch}"
                else:
                    Columns += f"SUM(CASE WHEN gt.Branch = '{branch}' THEN gt.AvQty ELSE 0 END) AS Br{branch},"

            if data["SATFromBranch"] != "N" and data["SATToBranch"] !="N":
                baseQuary = f"SELECT go.*,COALESCE(gts.Stock,0),{Columns} FROM goods go LEFT JOIN(SELECT SUM(Qin-Qout) as AvQty,ItemNo,Branch FROM goodstrans  WHERE Branch={data['SATFromBranch']} GROUP BY ItemNo,Branch) gt ON go.ItemNo=gt.ItemNo "
                
            else:
                baseQuary = f"SELECT go.*,COALESCE(gts.Stock,0),{Columns} FROM goods go LEFT JOIN(SELECT SUM(Qin-Qout) as AvQty,ItemNo,Branch FROM goodstrans GROUP BY ItemNo,Branch) gt ON go.ItemNo=gt.ItemNo "
            baseQuary= baseQuary + f" LEFT JOIN(SELECT SUM(Qin-Qout) as Stock,ItemNo FROM goodstrans GROUP BY ItemNo) gts ON gts.ItemNo=go.ItemNo "
            #ItemsByBranchQuery=f"SELECT Branch,Sum(Qin-Qout) as BrQty FROM goodstrans WHERE ItemNo='{data["value"]}'"
            if data["value"] =="":
                baseQuary = baseQuary +" WHERE go.itemno not like '%ALLDATA%' "
                if data["groupName"]!="":
                    
                    baseQuary=baseQuary +f" AND {data['groupType']} = '{data['groupName']}' " 
                baseQuary = baseQuary +" GROUP BY go.itemno "
                
            elif data["value"] != "":
                if data["groupName"]!="":
                    baseQuary1=baseQuary +f" WHERE go.itemno not like '%ALLDATA%' and go.itemno='{data['value']}' AND {data['groupType']} = '{data['groupName']}' GROUP BY go.itemno limit 200;" 
                else:
                    baseQuary1=baseQuary + f" WHERE go.itemno not like '%ALLDATA%' and go.itemno='{data['value']}' GROUP BY go.itemno limit 200;"
                print(baseQuary1)
                cur.execute(baseQuary1)
                
            
                for row in cur:
                    branchesStock = {}
                    brIndex = 1
                    for br in branches:
                        branchesStock[f"{br}"] = row[34 + brIndex]
                        brIndex += 1
                    item_dict = {
                    "ItemNo": row[0],
                    "ItemName": row[1],
                    "ItemName2": row[2],
                    "MainNo": row[3],
                    "SetG": row[4],
                    "Category": row[5],
                    "Unit": row[6],
                    "Brand": row[7],
                    "Origin": row[8],
                    "Supplier": row[9],
                    "Sizeg": row[10],
                    "Color": row[11],
                    "Family": row[12],
                    "Groupg": row[13],
                    "Tax": row[14],
                    "SPrice1": row[15],
                    "Sprice2": row[16],
                    "SPrice3": row[17],
                    "Disc1": row[18],
                    "Disc2": row[19],
                    "Disc3": row[20],
                    "CostPrice": row[21],
                    "FobCost": row[22],
                    "AvPrice": row[23],
                    "BPUnit": row[24],
                    "PQty": row[25],
                    "PUnit": row[26],
                    "PQUnit": row[27],
                    "SPUnit": row[28],
                    "Sprice4": row[29],
                    "SPrice5": row[30],
                    "Disc4": row[31],
                    "Disc5": row[32],
                    "PPrice": row[33],
                    "Stock": row[34],
                    "branchesStock":branchesStock
                    
                    }
                    
                # Append the dictionary to the list
                    items_json.append(item_dict)
                
                    flagI=1
                
                if flagI==0:
                    baseQuary = baseQuary + f"""  WHERE (itemname LIKE '{data["value"]}%' or itemname LIKE '%{data["value"]}' or itemname LIKE '%{data["value"]}%' or go.itemno LIKE '{data["value"]}%' or itemname2 LIKE '{data["value"]}%' or itemname2 LIKE '%{data["value"]}' or itemname2 LIKE '%{data["value"]}%')   """
                    if data["groupName"]!="":
                        baseQuary=baseQuary +f" AND {data['groupType']} = '{data['groupName']}' " 
                    baseQuary= baseQuary+ " GROUP BY go.itemno "
        print(flagA)
        print(flagI)
        if flagI == 0 and flagA==0:
            if flagI==0 and data["option"] == "Items":
                baseQuary = baseQuary + " limit 200 "
            print(baseQuary)
            cur.execute(baseQuary)


        #print(baseQuary)
        
        if data["option"] == "Items" and flagI == 0:
            # Iterate over rows fetched from the cursor
            for row in cur:
                # Construct a dictionary for the current row
                branchesStock = {}
                brIndex = 1
                for br in branches:
                    branchesStock[f"{br}"] = row[34 + brIndex]
                    brIndex += 1
                item_dict = {
                    "ItemNo": row[0],
                    "ItemName": row[1],
                    "ItemName2": row[2],
                    "MainNo": row[3],
                    "SetG": row[4],
                    "Category": row[5],
                    "Unit": row[6],
                    "Brand": row[7],
                    "Origin": row[8],
                    "Supplier": row[9],
                    "Sizeg": row[10],
                    "Color": row[11],
                    "Family": row[12],
                    "Groupg": row[13],
                    "Tax": row[14],
                    "SPrice1": row[15],
                    "Sprice2": row[16],
                    "SPrice3": row[17],
                    "Disc1": row[18],
                    "Disc2": row[19],
                    "Disc3": row[20],
                    "CostPrice": row[21],
                    "FobCost": row[22],
                    "AvPrice": row[23],
                    "BPUnit": row[24],
                    "PQty": row[25],
                    "PUnit": row[26],
                    "PQUnit": row[27],
                    "SPUnit": row[28],
                    "Sprice4": row[29],
                    "SPrice5": row[30],
                    "Disc4": row[31],
                    "Disc5": row[32],
                    "PPrice": row[33],
                    "Stock":row[34],
                    "branchesStock":branchesStock

                }

                # Append the dictionary to the list
                items_json.append(item_dict)
        elif data["option"] == "Accounts" and flagA == 0:
                # Iterate over the rows fetched from the database
            for row in cur:
                # Construct a dictionary for the current row
                account_dict = {
                    "AccNo": row[0],
                    "AccName": row[1],
                    "Cur": row[2],
                    "SETA": row[3],
                    "Category": row[4],
                    "Price": row[5],
                    "Contact": row[6],
                    "TaxNo": row[7],
                    "SMan": row[8],
                    "Address": row[9],
                    "Tel": row[10],
                    "Mobile": row[11],
                    "AccName2": row[12],
                    "Fax": row[13],
                    "DeliveryDays": row[14],
                    "Balance": row[15]
                }
                # Append the dictionary to the results list
                items_json.append(account_dict)
        
            

        # Convert the list of dictionaries to JSON
        
                
        r = list(items_json)
        
        return{
            "Info":"authorized",
            "opp":r
        }
    except Exception as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            
            return{"Info":"error",
                    "msg":f"{e}"} 
def AccountSearchquery(var:str):
    baseQuary =f"""SELECT lh.*, COALESCE(ld.Balance, 0) AS Balance FROM 
 {var}
    CROSS JOIN (
    SELECT 
        CASE 
            WHEN mainCur = 1 THEN Cur1 
            WHEN mainCur = 2 THEN Cur2 
        END AS CompanyCurrency,  
        Rate, 
        mainCur 
    FROM 
        header
    LIMIT 1
) hd
LEFT JOIN (
    SELECT 
        ld.AccNo, 
        SUM(
            CASE 
                WHEN lh.Cur = hd.CompanyCurrency OR ld.RefType NOT LIKE '%_AP' THEN ld.DB - ld.CR
                WHEN hd.mainCur = '1' AND lh.Cur != hd.CompanyCurrency THEN (ld.DB - ld.CR) * hd.Rate
                WHEN hd.mainCur = '2' AND lh.Cur != hd.CompanyCurrency THEN (ld.DB - ld.CR) / hd.Rate
                ELSE 0
            END
        ) AS Balance 
    FROM 
        listdaily ld
    JOIN {var} ON ld.AccNo = lh.AccNo 
    CROSS JOIN (
        SELECT 
            CASE 
                WHEN mainCur = 1 THEN Cur1 
                WHEN mainCur = 2 THEN Cur2 
            END AS CompanyCurrency,  
            Rate, 
            mainCur 
        FROM 
            header
        LIMIT 1
    ) hd
    GROUP BY 
        ld.AccNo
) ld ON lh.AccNo = ld.AccNo;
"""
    return baseQuary
@app.get("/moh/{uid}/Accounting/{limit}/",status_code=200)
async def Accounting(uid:str,limit:int):

    username=uid
    
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
    


    cur.execute(f"""WITH 
account_data AS (
    SELECT * 
    FROM listhisab 
    ORDER BY AccNo 
                LIMIT {limit}
),
header_data AS (
    SELECT
        CASE
            WHEN mainCur = 1 THEN Cur1
            WHEN mainCur = 2 THEN Cur2
        END AS CompanyCurrency,
        Rate,
        mainCur
    FROM header
    LIMIT 1
),
balance_calc AS (
    SELECT
        ld.AccNo,
        SUM(
            CASE
                WHEN lh.Cur = hd.CompanyCurrency OR ld.RefType NOT LIKE '%_AP' THEN ld.DB - ld.CR
                WHEN hd.mainCur = '1' AND lh.Cur != hd.CompanyCurrency THEN (ld.DB - ld.CR) * hd.Rate
                WHEN hd.mainCur = '2' AND lh.Cur != hd.CompanyCurrency THEN (ld.DB - ld.CR) / hd.Rate
                ELSE 0
            END
        ) AS Balance
    FROM listdaily ld
    JOIN account_data lh ON lh.AccNo = ld.AccNo
    CROSS JOIN header_data hd
    GROUP BY ld.AccNo
    HAVING Balance>0.01 OR Balance <-0.01
)
SELECT 
    lh.*,
    COALESCE(ld.Balance, 0) AS Balance
FROM account_data lh
CROSS JOIN header_data hd
LEFT JOIN balance_calc ld ON lh.AccNo = ld.AccNo
WHERE ld.Balance IS NOT NULL;""") #honn
    hisab = []
    ind = 0
    for x in cur:

        hisab.append({
            "key":ind,
            "AccNo":x[0],
            "AccName":x[1],
            "Cur":x[2],
            "DeliveryDays":x[14],
            "Balance":x[15],
            "set":x[3],
            "category":x[4],
            "Price":x[5],
            "Contact":x[6],
            "TaxNo":x[7],
            "Address":x[9],
            "tel":x[10],
            "Mobile":x[11],
            "AccName2":x[12],
            "Fax":x[13],
        })
        ind = ind +1


    cur.execute("SELECT DISTINCT `Branch`,`BranchName` FROM `header` WHERE Branch is not null order by Branch asc;")   
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
   
    return{
    "Info":"authorized",
    "hisab":hisab,
    "hisabBranches":hisabBranches,
    "branches":branches,
    }
        

@app.post("/accounting/filter/{limit}/")
async def accFilter(data:dict,limit):

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
        
        prefixWithWithoutBranch="lh."
        if mydata["selectedBranch"] == "Any":
            #baseQuary = "SELECT * FROM hisabbr WHERE AccNo IS NOT NULL "
            baseQuary="""SELECT SUM(ld.DB - ld.CR) AS Balance, ld.Dep,SUM(ld.DB),SUM(ld.CR),lh.* FROM listdaily ld LEFT JOIN( SELECT * FROM listhisab) lh ON ld.AccNo = lh.AccNo WHERE ld.AccNo IS NOT NULL """
        elif mydata["selectedBranch"] == "":
            #baseQuary = "SELECT * FROM hisabbr WHERE AccNo IS NOT NULL "
            
            baseQuary="""SELECT SUM(ld.DB - ld.CR) AS Balance, ld.Dep,SUM(ld.DB),SUM(ld.CR),lh.* FROM listdaily ld LEFT JOIN( SELECT * FROM listhisab) lh ON ld.AccNo = lh.AccNo WHERE ld.AccNo IS NOT NULL AND ld.Dep IS NULL """
        else:
            #baseQuary = f"SELECT * FROM hisabbr WHERE Branch = \'{mydata['selectedBranch']}\' "
            baseQuary=f"""SELECT SUM(ld.DB - ld.CR) AS Balance, ld.Dep,SUM(ld.DB),SUM(ld.CR),lh.* FROM listdaily ld LEFT JOIN( SELECT * FROM listhisab) lh ON ld.AccNo = lh.AccNo WHERE ld.AccNo IS NOT NULL AND ld.Dep={mydata['selectedBranch']} """
        
        
        
    if mydata["vAny"] != "":
        AccNo=prefixWithWithoutBranch+"AccNo"
        AccName=prefixWithWithoutBranch +"AccName"
        Contact = prefixWithWithoutBranch+"Contact"
        Address= prefixWithWithoutBranch +"Address"
        AccName2=prefixWithWithoutBranch+"AccName2"
        tel = prefixWithWithoutBranch +"tel"
        Fax = prefixWithWithoutBranch+"Fax"

        if mydata["sAny"] == "Start":
            baseQuary = baseQuary + str(f" AND ( {AccNo} like \'{mydata['vAny']}%\' OR {AccName} like \'{mydata['vAny']}%\' OR {Contact} like \'{mydata['vAny']}%\' OR {Address} like \'{mydata['vAny']}%\' OR {tel} like \'{mydata['vAny']}%\' OR {AccName2} like \'{mydata['vAny']}%\' OR {Fax} like \'{mydata['vAny']}%\')  ")
        elif mydata["sAny"] == "Contains":
            baseQuary = baseQuary + str(f" AND ( {AccNo} like \'%{mydata['vAny']}%\' OR  {AccNo} like \'{mydata['vAny']}%\' OR  {AccNo} like \'%{mydata['vAny']}\' OR {AccName} like \'%{mydata['vAny']}%\'  OR {AccName} like \'{mydata['vAny']}%\'  OR {AccName} like \'%{mydata['vAny']}\' OR {Contact} like \'%{mydata['vAny']}%\'  OR {Contact} like \'{mydata['vAny']}%\'  OR {Contact} like \'%{mydata['vAny']}\' OR {Address}  like \'%{mydata['vAny']}%\'  OR {Address}  like \'{mydata['vAny']}%\'  OR {Address}  like \'%{mydata['vAny']}\' OR {tel} like \'%{mydata['vAny']}%\'  OR {tel} like \'{mydata['vAny']}%\'  OR {tel} like \'%{mydata['vAny']}\' OR {AccName2} like \'%{mydata['vAny']}%\'  OR {AccName2} like \'{mydata['vAny']}%\'  OR {AccName2} like \'%{mydata['vAny']}\' OR {Fax} like \'%{mydata['vAny']}%\' OR {Fax} like \'{mydata['vAny']}%\' OR {Fax} like \'%{mydata['vAny']}\' )  ")
    flag=0
    
    for f in filters:
        
        if mydata["branch"]:
            
            if f['name']!="Balance":
                
                fullyName="lh."+f['name']
               
            elif f['name']=="Balance":
                if f["value"] !="":
                    flag=1 #then i want to stop this iteration and enter in for loop and continue the next iteration
                    sign= f["type"]
                    value = f['value']
                    break
        else:
            if f['name'] == "Balance":
                 fullyName=f['name']
            else:
                fullyName="lh."+f['name']
        if f["value"] != "":
            if f["type"] == "Start":     
                baseQuary = baseQuary + str(f" AND {fullyName} like \'{f['value']}%\' ")
            elif f["type"] == "Contains":
                baseQuary = baseQuary + str(f" AND ({fullyName} like \'%{f['value']}%\' OR {fullyName} like \'{f['value']}%\' OR {fullyName} like \'%{f['value']}\')")
            elif f["type"] == "Not Equal":
                if f['name'] == "Balance" and f['value'] == '0':
                    
                    baseQuary = baseQuary + str(f" AND ({fullyName} >= '0.01' OR {fullyName} <= '-0.01') ")
                else:   
                    
                    baseQuary = baseQuary + str(f" AND {fullyName} != \'{f['value']}\' ")
            elif f["type"] == ">":
                if f['name'] == "Balance" and f['value'] == '0':
                    
                    baseQuary = baseQuary + str(f" AND {fullyName} >= '0.01' ")
                else:
                    baseQuary = baseQuary + str(f" AND {fullyName} > \'{f['value']}\' ")
            elif f["type"] == "<":
                baseQuary = baseQuary + str(f" AND {fullyName} < \'{f['value']}\' ")
            elif f["type"] == "=":
                baseQuary = baseQuary + str(f" AND {fullyName} = \'{f['value']}\' ")
            elif f["type"] == ">=":
                baseQuary = baseQuary + str(f" AND {fullyName} >= \'{f['value']}\' ")
            elif f["type"] == "<=":
                baseQuary = baseQuary + str(f" AND {fullyName} <= \'{f['value']}\' ")
   
    #zabit hon having
    
    if  mydata["branch"]:
             
        baseQuary = baseQuary + str(f"GROUP BY ld.AccNo,ld.Dep ")
    else:
           
        baseQuary = baseQuary + str(f"GROUP BY lh.AccNo ")

    if flag ==1:
        if sign == "Not Equal":
            if value == '0':
                
                baseQuary = baseQuary + str(f" HAVING Balance >= '0.01' OR Balance <= '-0.01' ")
            else:
                baseQuary = baseQuary + str(f" HAVING Balance != \'{value}\' ")
        elif sign == ">":
                if f['value']=='0':
                    
                    baseQuary = baseQuary + str(f" HAVING Balance >= '0.01' ") 
                else:
                    baseQuary = baseQuary + str(f" HAVING Balance > \'{value}\' ")
        elif sign == "<":
                baseQuary = baseQuary + str(f" HAVING Balance < \'{value}\' ")
        elif sign == "=":
                baseQuary = baseQuary + str(f" HAVING Balance = \'{value}\' ")
        elif sign == ">=":
                baseQuary = baseQuary + str(f" HAVING Balance >= \'{value}\' ")
        elif sign == "<=":
                baseQuary = baseQuary + str(f" HAVING Balance <= \'{value}\' ")    
    if limit != "All":     
        baseQuary = baseQuary + str(f"limit {limit};")


    
    cur.execute(baseQuary)
    
    r = list(cur)
    hisab = []
    ind = 0
    print(baseQuary)
    if(mydata["branch"]):
        for x in r:

            hisab.append({
                    "key":ind,
                    "AccNo":x[4],
                    "AccName":x[5],
                    "Cur":x[6],
                    "Balance":x[0],
                    "set":x[7],
                    "category":x[8],
                    "Price":x[9],
                    "Contact":x[10],
                    "TaxNo":x[11],
                    "Address":x[13],
                    "tel":x[14],
                    "Mobile":x[15],
                    "AccName2":x[16],
                    "Fax":x[17],
                    "DeliveryDays":x[18],
                    "Branch":x[1],
                    "DB":x[2],
                    "CR":x[3],
                    
                })
            ind = ind +1
    else:
        for x in r:

            hisab.append({
                    "key":ind,
                    "AccNo":x[0],
                    "AccName":x[1],
                    "Cur":x[2],
                    "DeliveryDays":x[14],
                    "Balance":x[15],
                    "set":x[3],
                    "category":x[4],
                    "Price":x[5],
                    "Contact":x[6],
                    "TaxNo":x[7],
                    "Address":x[9],
                    "tel":x[10],
                    "Mobile":x[11],
                    "AccName2":x[12],
                    "Fax":x[13],
                    
                })
            ind = ind +1

    
  
    return{
       "Info":"authorized",
       "hisab":hisab,
       "hisabBranches":hisabBranches,
       "branchSelection":mydata["branch"]
        
   }
    
    
@app.get("/moh/{uid}/Accounting/ItemsFromAccount/{id}/{limit}",status_code=200)
async def StockStatement(uid:str, id:str, limit:int):

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
    cur.execute("SELECT DISTINCT `Branch`,`BranchName` FROM `header` WHERE Branch is not null order by Branch asc;")   
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
            print("exception")
      
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
        tdb = tdb + vstat[5]
        
        tcr = tcr + vstat[6]
    distype.append({"value":"Sales","label":"Sales"})
    distype.append({"value":"Purchase","label":"Purchase"})
    distype.append({"value":"Order","label":"Order"})
    distype.append({"value":"Transfers","label":"Transfers"})
    distype.append({"value":"Receipt V","label":"Receipt V"})
    distype.append({"value":"Payment V","label":"Payment V"})
    distype.append({"value":"Journal  V","label":"Journal  V"})
    print(tcr)
    print(tdb)
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
        
        
        baseQuary = baseQuary + f" ORDER BY Date desc,Time desc limit {limit}"
    else:
        
        baseQuary = baseQuary + " ORDER BY Date desc,Time desc "

    
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
    cur.execute(accountStatament_calcBalance(id))
    #cur.execute("SELECT AccNo, Dep, SUM(DB - CR) AS balance FROM listdaily WHERE AccNo = %s GROUP BY AccNo, Dep;", (id,))
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
    print(stat)
    return{
        "Info":"authorized",
        "Branch":stat,
        
        }
def accountStatament_calcBalance(acc:str):
    calcBalance=f"""WITH 
header_data AS (
    SELECT
        CASE
            WHEN mainCur = 1 THEN Cur1
            WHEN mainCur = 2 THEN Cur2
        END AS CompanyCurrency,
        Rate,
        mainCur
    FROM header
    LIMIT 1
),
cur AS (
    SELECT DISTINCT AccNo, Cur
    FROM listhisab 
    WHERE AccNo = '{acc}'
),
account_data AS (
    SELECT AccNo, Dep 
    FROM listdaily 
    WHERE AccNo = '{acc}'
    GROUP BY AccNo, Dep
),
balance_calc AS (
    SELECT
        ld.AccNo,
        ld.Dep,
        SUM(
            CASE
                WHEN c.Cur = hd.CompanyCurrency OR ld.RefType NOT LIKE '%_AP' THEN ld.DB - ld.CR
                WHEN hd.mainCur = '1' AND c.Cur != hd.CompanyCurrency THEN (ld.DB - ld.CR) * hd.Rate
                WHEN hd.mainCur = '2' AND c.Cur != hd.CompanyCurrency THEN (ld.DB - ld.CR) / hd.Rate
                ELSE 0
            END
        ) AS Balance
    FROM listdaily ld
    JOIN cur c ON c.AccNo = ld.AccNo
    CROSS JOIN header_data hd
    WHERE ld.AccNo = '{acc}'
    GROUP BY ld.AccNo, ld.Dep
)
SELECT 
    ad.AccNo,
    ad.Dep,
    COALESCE(bc.Balance, 0) AS Balance
FROM account_data ad
LEFT JOIN balance_calc bc ON ad.AccNo = bc.AccNo AND ad.Dep = bc.Dep
CROSS JOIN header_data hd;"""
    print(calcBalance)
    return calcBalance
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

    basequery = f"SELECT * FROM `goodstrans` WHERE RefType = '{type}' AND RefNo = {number} ORDER BY LN"
    
    cur.execute(basequery)

        
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

@app.get("/moh/{uid}/Accounting/Summery/{id}/{branch}/{branchSearch}",status_code=200)
async def AccountingBranch(uid:str, id:str, branch:str, branchSearch:bool):

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
         basedquery = f"""SELECT AccNo, Dep AS BR,LEFT(RefType, 2) AS InvType, SUM(DB) AS DB, SUM(CR) AS CR, SUM(DB - CR) AS Balance
FROM 
    listdaily 
GROUP BY
    InvType,
    BR """
    else:
        basedquery = f"""SELECT ld.AccNo,lh.Name ,ld.Dep AS BR,LEFT(ld.RefType, 2) AS InvType, SUM(DB) AS DB, SUM(CR) AS CR, SUM(DB - CR) AS Balance
FROM 
    listdaily 
    ld LEFT JOIN (SELECT AccNo,AccName AS Name FROM listhisab GROUP BY AccNo) lh ON lh.AccNo = ld.AccNo
WHERE ld.AccNo = '{id}'
GROUP BY
    InvType,
    ld.AccNo,
    BR """
    summery = []
    ind = 0
    if branchSearch == True and branch!='Any':
        basedquery = basedquery + f"""HAVING BR = {branch};"""
    cur.execute(basedquery)
    if id=="ALLDATA":
        for x in cur:          
            summery.append({     
        "key":ind,                 
        "AccNo" :x[0],  
        "BR":x[1],
        "InvType":x[2], 
        "DB":x[3], 
        "CR":x[4], 
        "Balance":x[5],
            })
            ind = ind +1
    else:
        for x in cur:          
            summery.append({     
        "key":ind,                 
        "AccNo" :x[0],  
        "Name" :x[1], 
        "BR":x[2],
        "InvType":x[3], 
        "DB":x[4], 
        "CR":x[5], 
        "Balance":x[6],
            })
            ind = ind +1

    
    return{
    "Info":"authorized",
    "summery":summery
    }

@app.get("/moh/{uid}/Accounting/InitialSummery/{id}/{branch}/{branchSearch}",status_code=200)
async def AccountingBranch(uid:str, id:str,branch:str,branchSearch:bool):

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
        basequery = f"""SELECT AccNo, Dep AS BR,RefType AS InvType, SUM(DB) AS DB, SUM(CR) AS CR, SUM(DB - CR) AS Balance
FROM 
    listdaily GROUP BY InvType,BR """
    else:
        basequery = f"""SELECT ld.AccNo,lh.Name ,ld.Dep AS BR,ld.RefType AS InvType, SUM(DB) AS DB, SUM(CR) AS CR, SUM(DB - CR) AS Balance
FROM 
    listdaily 
    ld LEFT JOIN (SELECT AccNo,AccName AS Name FROM listhisab GROUP BY AccNo) lh ON lh.AccNo = ld.AccNo
WHERE ld.AccNo = '{id}' GROUP BY InvType,ld.AccNo,BR """
    if branch!="Any" and branchSearch==True:
        basequery = basequery+ f"""HAVING BR = {branch};"""
    
    cur.execute(basequery)


    summery = []

    ind = 0
    if id=="ALLDATA":
        for x in cur:          
            summery.append({     
        "key":ind,                 
        "AccNo" :x[0],  
        "BR":x[1],
        "InvType":x[2], 
        "DB":x[3], 
        "CR":x[4], 
        "Balance":x[5],
            })
            ind = ind +1
    else:
        for x in cur:          
            summery.append({     
        "key":ind,                 
        "AccNo" :x[0],  
        "Name" :x[1], 
        "BR":x[2],
        "InvType":x[3], 
        "DB":x[4], 
        "CR":x[5], 
        "Balance":x[6],
            })
            ind = ind +1

    
    return{
    "Info":"authorized",
    "summery":summery
    }







        
@app.get("/moh/{uid}/Stock/{limit}/",status_code=200)
async def Stock(uid:str,limit:int):

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
    
    cur.execute(f"SELECT go.*,gt.totalQty,gt.Branch FROM goods go LEFT JOIN (SELECT SUM(Qin - Qout) AS totalQty, ItemNo,Branch FROM goodstrans GROUP BY ItemNo) gt ON go.ItemNo = gt.ItemNo WHERE (totalQty >= 0.01 OR totalQty <= -0.01) limit {limit};")
    qstock = list(cur)
   
    goods = []
    ind = 0
    cur.execute("SELECT DISTINCT `Branch`,`BranchName` FROM `header` WHERE Branch is not null order by Branch asc ;")



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
        "SPrice4" :x[29],
        "SPrice5" :x[30],
        "Disc1" :x[31],
        "Disc2" :x[32],
        "Qty":x[34],
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
                if f['name'] == "qty" and f['value']=='0':
                    
                    baseQuary = baseQuary + str(f" AND ({fullyName}{f['name']} >= '0.01' OR {fullyName}{f['name']} <= '-0.01') ") 
                else:
                    baseQuary = baseQuary + str(f" AND {fullyName}{f['name']} != \'{f['value']}\' ")
            elif f["type"] == ">":
                if f['name'] == "qty" and f['value']=='0':
                    
                    baseQuary = baseQuary + str(f" AND {fullyName}{f['name']} >= '0.01' ") 
                else:
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
            "SPrice4" :x[29],
            "SPrice5" :x[30],
            "Disc1" :x[31],
            "Disc2" :x[32],
            "Qty":x[34],
            "branch":x[35],
            })
            ind = ind +1
            

   
    bStock = []
    ukey = 0
    if mydata["branch"]:
        for x in qstock :
            bStock.append({
            "key":ukey,
            "BR" :x[35],    
            "BRName":x[2],   
            "Qty" :x[34],
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
        
    cur.execute("SELECT DISTINCT `Branch`,`BranchName` FROM `header` WHERE Branch is not null order by Branch asc ;")



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
async def StockStatement(uid:str, id:str,limit):

    username=uid
    try:
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        
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

    username=uid
    try:
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            response.status_code = status.HTTP_401_UNAUTHORIZED
            return({"Info":"unauthorized",
                    "msg":{e}})
    
    cur = conn.cursor()
    
    cur.execute(f"SELECT ItemNo, Branch,ItemName, SUM(Qin-Qout) AS QTY FROM goodstrans WHERE ItemNo= '{id}' GROUP BY ItemNo, Branch")
    branch = []
    ind = 0
    for x in cur:          
        branch.append({
        "key":ind,                 
        "ItemNo" :x[0],  
        "BR" :x[1], 
        "ItemName":x[2],
        "Qty":x[3], 
        })
        ind = ind +1
    
    return{
    "Info":"authorized",
    "branch":branch
    }



@app.get("/moh/{uid}/Stock/Summery/{id}/{branch}/{branchSearch}",status_code=200)
async def StockBranch(uid:str, id:str,branch:str,branchSearch:bool):

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
        basequery = f"""SELECT 
		ItemNo,Branch AS BR,
    CASE 
        WHEN LEFT(RefType, 3) = 'SAT' THEN LEFT(RefType, 3)
        ELSE LEFT(RefType, 2)
    END AS RType,
    SUM(Qin+Qout+Qod) AS Qty,
    SUM(Total) AS Total,
    SUM((TAX/100)*Total) AS Tax
    
   
FROM 
    goodstrans 
GROUP BY 
    RType,
    
    Branch """
    else:
        basequery = f"""SELECT 
            ItemNo,Branch AS BR,
        CASE 
            WHEN LEFT(RefType, 3) = 'SAT' THEN LEFT(RefType, 3)
            ELSE LEFT(RefType, 2)
        END AS RType,
        SUM(Qin+Qout+Qod) AS Qty,
        SUM(Total) AS Total,
        SUM((TAX/100)*Total) AS Tax
        
    
    FROM 
        goodstrans 
    WHERE 
        ItemNo = '{id}' 
    GROUP BY 
        RType,
        ItemNo,
        Branch """
    if branch!="Any" and branchSearch==True:
        basequery = basequery + f"""HAVING Branch = {branch};""" 
    cur.execute(basequery)
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
async def StockStatement(uid:str, type:str, number:str,limit):

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
    if limit =="All":
        query = f"SELECT * FROM `goodstrans` WHERE RefType = '{type}' AND RefNo = '{number}' ORDER BY LN"
    else:
        query = f"SELECT * FROM `goodstrans` WHERE RefType = '{type}' AND RefNo = '{number}' ORDER BY LN limit {limit}"
    cur.execute(query)
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
    compname = data["compname"]
    

    try:
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = compname) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            response.status_code = status.HTTP_401_UNAUTHORIZED
            return({"Info":"unauthorized",
                    "msg":{e}})
    
    try:
        cur =conn.cursor()
        if data["type"] =="SAT_AP":
            Branch=data["SATFromBranch"]
            TBranch=data["SATToBranch"]
        else:
            Branch=data["Abranch"]
            TBranch=''
        print(data["accRefNo"],"tata33")
        print(data)
        if data["accRefNo"]:
            print('adimmmmm')
            cur.execute(f"SELECT UserP FROM invnum WHERE RefNo={data['accRefNo']}")
            result = cur.fetchone()
            if result:
                userP=result[0]
                if userP!=data["username"]:
                    #Reserved
                    return{
                "Info":"Failed",
                
                "msg":"The Invoice Has Been AlReady Taken By The Supervisor"
                } 
            RemovedJson=data["RemovedItems"]
            for fullitem in RemovedJson:
                item=fullitem["item"]
                
                basequery=f"""INSERT INTO `deletehistory` (`User1`, `RefType`, `RefNo`, `LN`, `ItemNo`, `ItemName`, `Qty`, `PQty`, `PUnit`, `UPrice`, `Disc`, `Tax`, `TaxTotal`, `Total`, `Note`, `Branch`, `DateDeleted`, `TimeDeleted`,`PPrice`,`PType`,`PQUnit`,`TotalPieces`,`SPUnit`,`BPUnit`,`DeleteType`) VALUES ('{fullitem["username"]}', '{fullitem["type"]}','{fullitem["RefNo"]}','{item["lno"]}', '{item["no"]}', '{item["name"]}','{item["qty"]}', '{item["PQty"]}', '{item["PUnit"]}', '{item["uprice"]}', '{item["discount"]}', '{item["tax"]}', '{item["TaxTotal"]}','{item["Total"]}','{item["Note"]}', '{item["branch"]}', '{fullitem["DateDeleted"]}', '{fullitem["TimeDeleted"]}','{item["PPrice"]}','{item["PType"]}','{item["PQUnit"]}','{item["TotalPieces"]}','{item["SPUnit"]}','{item["BPUnit"]}','{fullitem["DeleteType"]}'); """
                cur.execute(basequery)
            cur.execute(f"DELETE  FROM invnum WHERE RefNo='{data['accRefNo']}'")
            
            cur.execute(f"DELETE  FROM inv WHERE RefNo='{data['accRefNo']}'")
            
            cur.execute(f"Delete FROM listdaily WHERE RefNo='{data['accRefNo']}' AND RefType='{data['type']}' ")
            
            cur.execute(f"DELETE FROM goodstrans WHERE RefNo='{data['accRefNo']}' AND RefType='{data['type']}' ")
            conn.commit()
        
            basequery = f"""INSERT INTO `invnum` (`User1`, `RefType`,`RefNo`, `AccNo`,`AccName`, `Branch`, `TBranch`, `DateI`, `TimeI`, `DateP`, `TimeP`, `UserP`,`Cur`,`Rate`,`long`,`lat`,`Note`,`DateValue`) VALUES ('{data["username"]}', '{data["type"]}','{data["accRefNo"]}', '{data["accno"]}', '{data["accname"]}', '{Branch}', '{TBranch}', '{data["accDate"]}', '{data["accTime"]}', '','','','{data["Cur"]}','{data["Rate"]}','{data["long"]}','{data["lat"]}','','{data["deliveryDays"]}'); """
        else:
            basequery = f"""INSERT INTO `invnum` (`User1`, `RefType`, `AccNo`,`AccName`, `Branch`, `TBranch`, `DateI`, `TimeI`, `DateP`, `TimeP`, `UserP`,`Cur`,`Rate`,`long`,`lat`,`Note`,`DateValue`) VALUES ('{data["username"]}', '{data["type"]}', '{data["accno"]}', '{data["accname"]}', '{Branch}', '{TBranch}', '{data["accDate"]}', '{data["accTime"]}', '','','','{data["Cur"]}','{data["Rate"]}','{data["long"]}','{data["lat"]}','','{data["deliveryDays"]}'); """

        
        cur.execute(basequery)

        #print(data)
        
        ref_no = cur.lastrowid

        print("RefNo:", ref_no)
        listdailyNote=f"INVOICE RefType:{data['type']} {ref_no} APP"
        if data["type"] !="OD_AP": #IF NOT OD INSERT IN LISTDAILY
            if data["type"]!="DB_AP" and data["type"]!="CR_AP" and data["type"]!="SAT_AP":
                if data["invoiceTotal"]>0:
                    DB=data["invoiceTotal"]
                    CR=0
                else:
                    DB=0
                    CR=(-1) * data["invoiceTotal"]
            
                print("accDateh",data["accDate"])
                accDate=change_date_format(data["accDate"])
                basequery3 = f"""INSERT INTO `listdaily` (`RefType`,`RefNo`,`LNo`, `AccNo`, `Dep`, `Date`, `Time`,`DB`,`CR`,`VDate`,`Job`,`Bank`,`CHQ`,`CHQ2`,`OppAcc`,`Notes`) VALUES ('{data["type"]}','{ref_no}','1.00', '{data["accno"]}', '{Branch}', '{accDate}', '{data["accTime"]}',{DB},{CR},'{accDate}','','','','','{data["accno"]}','{listdailyNote}'); """
                cur.execute(basequery3)
                print("failer 3 pass")

        for item in data["items"]:
            # if item["PPrice"]=="" or item["PPrice"]==None:
            #         item["PPrice"] = "P"
            
            
            basequery = f"""INSERT INTO `inv` (`User1`, `RefType`, `RefNo`, `LN`, `ItemNo`, `ItemName`, `Qty`, `PQty`, `PUnit`, `UPrice`, `Disc`, `Tax`, `TaxTotal`, `Total`, `Note`, `Branch`, `DateT`, `TimeT`,`PPrice`,`PType`,`PQUnit`,`TotalPieces`,`SPUnit`,`BPUnit`) VALUES ('{data["username"]}', '{data["type"]}','{ref_no}','{item["lno"]}', '{item["no"]}', '{item["name"]}','{item["qty"]}', '{item["PQty"]}', '{item["PUnit"]}', '{item["uprice"]}', '{item["discount"]}', '{item["tax"]}', '{item["TaxTotal"]}','{item["Total"]}','{item["Note"]}', '{item["branch"]}', '{item["DateT"]}', '{item["TimeT"]}','{item["PPrice"]}','{item["PType"]}','{item["PQUnit"]}','{item["TotalPieces"]}','{item["SPUnit"]}','{item["BPUnit"]}'); """
            
            cur.execute(basequery)
            if data["type"]!="DB_AP" and data["type"]!="CR_AP":
                if data["type"]=="SA_AP" or data["type"]=="PR_AP" or data["type"]=="SAT_AP":
                    Qin=0
                    Qout=item["TotalPieces"]
                    Qod=0
                elif data["type"]=="PI_AP" or data["type"]=="SR_AP":
                    Qin=item["TotalPieces"]
                    Qout=0
                    Qod=0
                elif data["type"]=="OD_AP":
                    Qin=0
                    Qout=0
                    Qod=item["TotalPieces"]
                print(item["DateT"])
                dateT= change_date_format(item["DateT"])
                basequery2 = f"""INSERT INTO `goodstrans` (`RefType`, `RefNo`, `LN`, `ItemNo`, `ItemName`, `Qty`, `PQty`, `PUnit`, `UPrice`, `Disc`, `Tax`,`Total`, `Notes`, `Branch`, `TDate`, `Time`,`PQUnit`,`UFob`,`Weight`,`AccNo`,`Disc100`,`AccName`,`Qin`,`Qout`,`Qod`) VALUES ('{data["type"]}','{ref_no}','{item["lno"]}', '{item["no"]}', '{item["name"]}','{item["TotalPieces"]}', '{item["PQty"]}', '{item["PUnit"]}', '{item["uprice"]}', '{item["discount"]}', '{item["tax"]}','{item["Total"]}','{item["Note"]}', '{item["branch"]}', '{dateT}', '{item["TimeT"]}','{item["PQUnit"]}',0,0,'{data["accno"]}',0,'{data["accname"]}','{Qin}','{Qout}','{Qod}'); """
                if data["type"]=="SAT_AP":
                    Qin=item["TotalPieces"]
                    Qout=0
                    Qod=0
                    basequeryQin = f"""INSERT INTO `goodstrans` (`RefType`, `RefNo`, `LN`, `ItemNo`, `ItemName`, `Qty`, `PQty`, `PUnit`, `UPrice`, `Disc`, `Tax`,`Total`, `Notes`, `Branch`, `TDate`, `Time`,`PQUnit`,`UFob`,`Weight`,`AccNo`,`Disc100`,`AccName`,`Qin`,`Qout`,`Qod`) VALUES ('{data["type"]}','{ref_no}','{item["lno"]}', '{item["no"]}', '{item["name"]}','{item["TotalPieces"]}', '{item["PQty"]}', '{item["PUnit"]}', '{item["uprice"]}', '{item["discount"]}', '{item["tax"]}','{item["Total"]}','{item["Note"]}', '{TBranch}', '{dateT}', '{item["TimeT"]}','{item["PQUnit"]}',0,0,'{data["accno"]}',0,'{data["accname"]}','{Qin}','{Qout}','{Qod}'); """
                
            else:
                accDate=change_date_format(item["DateT"])
                if data["type"] == "DB_AP":
                    DB=item["Total"]
                    CR=0
                if data["type"] == "CR_AP":
                    DB=0
                    CR=item["Total"]
                basequery2 = f"""INSERT INTO `listdaily` (`RefType`,`RefNo`,`LNo`, `AccNo`, `Dep`, `Date`, `Time`,`DB`,`CR`,`VDate`,`Job`,`Bank`,`CHQ`,`CHQ2`,`OppAcc`,`Notes`) VALUES ('{data["type"]}','{ref_no}','{item["lno"]}', '{data["accno"]}', '{Branch}', '{accDate}', '{item["TimeT"]}',{DB},{CR},'{accDate}','','','','','{data["accno"]}','{listdailyNote}'); """
            cur.execute(basequery2)
            if data["type"]=="SAT_AP":
                 cur.execute(basequeryQin)

           
            print("succss")
            conn.commit()


        return{"Info":"authorized",
            "msg":"successfull",
            "ref_no":ref_no}
        

    except Exception as e:
        print("failer")
        return{"Info":"Failed",
            "msg":f"{str(e)}"}

@app.get("/moh/getInvoiceHistory/{username}/{user}/")
async def getInvoiceHistory(username:str,user:str):
    try:
        conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
    #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
    except mariadb.Error as e:       
        print(f"Error connecting to MariaDB Platform: {e}")  
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return({"Info":"unauthorized",
                "msg":{e}})
    cur = conn.cursor()
    baseQuery = f"""SELECT * FROM invnum WHERE (UserP='' or UserP='{user}') AND DateP='' AND TimeP='' AND User1='{user}' AND RefType!='CHK_AP' """
    cur.execute(baseQuery)
    invoices = []
    for invoice in cur:
        inv={
              'user':invoice[0],
              'RefType': invoice[1],
              'RefNo': invoice[2],
              'AccNo':invoice[3],
              'AccName':invoice[4],
              'Branch': invoice[5],
              'DateI': invoice[7],
              'DateValue':invoice[17]
            }
        invoices.append(inv)
    #print(invoices)
    return{
         "Info":"authorized",
        "Invoices": invoices
        } 

@app.get("/moh/getInvoiceDetails/{username}/{user}/{InvoiceId}/{salePricePrefix}",status_code=200)
async def getInvoiceDetails(username:str,user:str,InvoiceId:str,salePricePrefix:str):
    try:
        conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
    except mariadb.Error as e:       
        print(f"Error connecting to MariaDB Platform: {e}")  
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return({"Info":"unauthorized",
                "msg":{e}})
    cur = conn.cursor()
    cur.execute(f"SELECT UserP FROM invnum WHERE RefNo={InvoiceId}")
    result = cur.fetchone()
    globalBalance=0
    if result:
        userP=result[0]
        if (userP!='' and userP!=None) and userP!=user:
            #Reserved
            return{
         "Info":"unauthorized",
        "Invoices": "",
        "InvProfile":"",
        "message":"The Invoice Is Already taken Or Reserved"
        } 
             
    else:
        #Not Found
        return{
         "Info":"unauthorized",
        "Invoices": "",
        "InvProfile":"",
        "message":"Invoice Not Found"
        } 
         
    
    cur.execute(f"UPDATE invnum SET UserP='{user}' WHERE RefNo='{InvoiceId}' ")
    conn.commit()
    SalePrice = f'Sprice{salePricePrefix}'
    
    branches=[]
    getbranches="SELECT DISTINCT Branch FROM header WHERE Branch IS NOT NULL"
    cur.execute(getbranches)
    Columns=""
    for row in cur:
        branches.append(row[0])
    
    for idx, branch in enumerate(branches):
        if idx == len(branches) - 1:
            Columns += f"""
            SUM(CASE WHEN gt.Branch = '{branch}' THEN gt.AvQty ELSE 0 END) AS Br{branch}
"""
        else:
            Columns += f"""SUM(CASE WHEN gt.Branch = '{branch}' THEN gt.AvQty ELSE 0 END) AS Br{branch},
            """
    #print(InvoiceId)
    baseQueryBalance = calc_balance_invoiceDetails(InvoiceId)
    cur.execute(baseQueryBalance)
    for balance in cur:
        globalBalance=balance[0]
    baseQuery = f"""SELECT i.*,iv.*,g.{SalePrice},ld.Balance,lh.Address,lh.Cur,lh.Mobile,COALESCE(gts.Stock,0) FROM inv i 
    LEFT JOIN(SELECT * FROM invnum) iv ON i.RefNo = iv.RefNo
    LEFT JOIN (SELECT {SalePrice},ItemNo FROM goods) g ON i.ItemNo = g.ItemNo
    LEFT JOIN (SELECT SUM(DB-CR) as Balance,AccNo FROM listdaily GROUP BY AccNo) ld ON iv.AccNo = ld.AccNo
    LEFT JOIN (SELECT Address,Cur,AccNo,Mobile FROM listhisab) lh ON iv.AccNo = lh.AccNo
    LEFT JOIN (SELECT ItemNo,SUM(Qin-Qout) as Stock FROM goodstrans GROUP BY ItemNo) gts ON gts.ItemNo=i.ItemNo
    WHERE i.User1='{user}' AND i.RefNo={InvoiceId}"""
    print(baseQuery)
    cur.execute(baseQuery)
    invoices = []
    InvProfile=[]
    flag=0
    for invoice in cur: 
        # branchesStock = {}
        # brIndex = 1
        # for br in branches:
        #     branchesStock[f"Br{br}"] = invoice[41 + brIndex]
        #     brIndex += 1
        inv={
                "lno":invoice[3],
                "no":invoice[4],
                "name":invoice[5],
                "branch": invoice[6],
                "qty": invoice[7],
                "uprice": invoice[10],
                "discount": invoice[11],
                "tax": invoice[12],
                "TaxTotal": invoice[13],
                "Total": invoice[14],
                "Note": invoice[15],
                "qty": invoice[7],
                "PQty":invoice[8],
                "PUnit":invoice[9],
                "DateT": invoice[16],
                "TimeT":invoice[17],
                "PPrice":invoice[18],
                "PType": invoice[19],
                "PQUnit":invoice[20],
                "TotalPieces":invoice[21],
                "SPUnit": invoice[22],
                "BPUnit":invoice[23],
                "InitialPrice":invoice[42],
                "TotalStockQty":invoice[47]
               
            }
        invoices.append(inv)
        if flag==0:
            accInv={
                # 'user':invoice[18],
                'RefType': invoice[25],
                'RefNo': invoice[26],
                'id':invoice[27],
                'Branch': invoice[29],
                "TBranch":invoice[30],
                'name': invoice[28],
                "date": invoice[31],
                "time": invoice[32],
                "CurNum": invoice[36],
                "Rate": invoice[37],
                "long": invoice[38],
                "lat" :invoice[39],
                "Note": invoice[40],
                "deliveryDays":invoice[41],

                "balance":globalBalance, #invoice[43],
                "address":invoice[44],
                "cur":invoice[45],
                "mobile":invoice[46]
                # "DateP": invoice[26],
                # "TimeP": invoice[27],         
                # "UserP": invoice[28]
                }
            InvProfile.append(accInv)
        flag = flag+1   
    print(InvProfile)
        
    invoices = sorted(invoices, key=lambda x: x["lno"])
    
    return{
         "Info":"authorized",
        "Invoices": invoices,
        "InvProfile":InvProfile,
        "message":""
        } 
def calc_balance_invoiceDetails(RefNo:int):
    calcBalance=f"""WITH inv AS (
    SELECT AccNo 
    FROM invnum 
    WHERE RefNo = {RefNo}
),
header_data AS (
    SELECT
        CASE
            WHEN mainCur = 1 THEN Cur1
            WHEN mainCur = 2 THEN Cur2
        END AS CompanyCurrency,
        Rate,
        mainCur
    FROM header
    LIMIT 1
),
account_data AS (
    SELECT * 
    FROM listhisab 
    WHERE accno = (SELECT AccNo FROM inv)
    ORDER BY AccNo 
    LIMIT 150
),
balance_calc AS (
    SELECT
        ld.AccNo,
        SUM(
            CASE
                WHEN lh.Cur = hd.CompanyCurrency OR ld.RefType NOT LIKE '%_AP' THEN ld.DB - ld.CR
                WHEN hd.mainCur = '1' AND lh.Cur != hd.CompanyCurrency THEN (ld.DB - ld.CR) * hd.Rate
                WHEN hd.mainCur = '2' AND lh.Cur != hd.CompanyCurrency THEN (ld.DB - ld.CR) / hd.Rate
                ELSE 0
            END
        ) AS Balance
    FROM listdaily ld
    JOIN account_data lh ON lh.AccNo = ld.AccNo
    CROSS JOIN header_data hd
    GROUP BY ld.AccNo
)
SELECT 
    COALESCE(ld.Balance, 0) AS Balance,
    (SELECT AccNo FROM inv) AS inv
FROM account_data lh
CROSS JOIN header_data hd
LEFT JOIN balance_calc ld ON lh.AccNo = ld.AccNo;"""
    return calcBalance
    
# @app.post("/moh/RemoveItemFromInvoiceHistory/")
# async def removeItemFromHistory(data:dict):

#     try:
#             print(data)
#             compname=data["compname"]
#             conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = compname) 
#         #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
#     except mariadb.Error as e:       
#             print(f"Error connecting to MariaDB Platform: {e}")  
#             response.status_code = status.HTTP_401_UNAUTHORIZED
#             return({"Info":"unauthorized",
#                     "msg":{e}})
    
#     try:
#         cur =conn.cursor()
#         #cur.execute(f"SELECT * FROM inv WHERE RefNo='{data["RefNo"]}' AND ")
#         item=data["item"]
#         basequery=f"""INSERT INTO `deletehistory` (`User1`, `RefType`, `RefNo`, `LN`, `ItemNo`, `ItemName`, `Qty`, `PQty`, `PUnit`, `UPrice`, `Disc`, `Tax`, `TaxTotal`, `Total`, `Note`, `Branch`, `DateDeleted`, `TimeDeleted`,`PPrice`,`PType`,`PQUnit`,`TotalPieces`,`SPUnit`,`BPUnit`,`DeleteType`) VALUES ('{data["username"]}', '{data["type"]}','{data["RefNo"]}','{item["lno"]}', '{item["no"]}', '{item["name"]}','{item["qty"]}', '{item["PQty"]}', '{item["PUnit"]}', '{item["uprice"]}', '{item["discount"]}', '{item["tax"]}', '{item["TaxTotal"]}','{item["Total"]}','{item["Note"]}', '{item["branch"]}', '{data["DateDeleted"]}', '{data["TimeDeleted"]}','{item["PPrice"]}','{item["PType"]}','{item["PQUnit"]}','{item["TotalPieces"]}','{item["SPUnit"]}','{item["BPUnit"]}','{data["DeleteType"]}'); """
#         print(basequery)
#         cur.execute(basequery)
#         print("inserted")
#         cur.execute(f"DELETE  FROM inv WHERE  User1='{data["username"]}' AND RefType='{data["type"]}' AND RefNo='{data["RefNo"]}' AND ItemNo='{item["no"]}' AND LN='{item["lno"]}';")
#         cur.execute(f"DELETE  FROM goodstrans WHERE RefType='{data["type"]}' AND RefNo='{data["RefNo"]}' AND ItemNo='{item["no"]}' AND LN='{item["lno"]}';")
#         if data["invoiceTotal"]>0:
#             DB=data["invoiceTotal"]
#             CR=0
#         else:
#             DB=0
#             CR=(-1) * data["invoiceTotal"]
#         cur.execute(f"UPDATE listdaily SET DB={DB},CR={CR} WHERE RefType='{data["type"]}' AND RefNo='{data["RefNo"]}'")
#         conn.commit()




#         return({"Info":"authorized",
#                     "msg":"Success"})
    


    # except Exception as e:
    #     print("failer")
    #     print(str(e))
    #     return{"Info":"Failed",
    #     "msg":f"{str(e)}"}

@app.post("/moh/DeleteInvoice/")
async def deleteInvoice(data:dict):

    try:
            
            compname=data["compname"]
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = compname) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            response.status_code = status.HTTP_401_UNAUTHORIZED
            return({"Info":"unauthorized",
                    "msg":{e}})
    
    try:
        cur =conn.cursor()
        items=data["item"]
        RemovedItems=data["RemovedItems"]
        
        for item in items:
            basequery=f"""INSERT INTO `deletehistory` (`User1`, `RefType`, `RefNo`, `LN`, `ItemNo`, `ItemName`, `Qty`, `PQty`, `PUnit`, `UPrice`, `Disc`, `Tax`, `TaxTotal`, `Total`, `Note`, `Branch`, `DateDeleted`, `TimeDeleted`,`PPrice`,`PType`,`PQUnit`,`TotalPieces`,`SPUnit`,`BPUnit`,`DeleteType`) VALUES ('{data["username"]}', '{data["type"]}','{data["RefNo"]}','{item["lno"]}', '{item["no"]}', '{item["name"]}','{item["qty"]}', '{item["PQty"]}', '{item["PUnit"]}', '{item["uprice"]}', '{item["discount"]}', '{item["tax"]}', '{item["TaxTotal"]}','{item["Total"]}','{item["Note"]}', '{item["branch"]}', '{data["DateDeleted"]}', '{data["TimeDeleted"]}','{item["PPrice"]}','{item["PType"]}','{item["PQUnit"]}','{item["TotalPieces"]}','{item["SPUnit"]}','{item["BPUnit"]}','{data["DeleteType"]}'); """
            
            
            cur.execute(basequery)
            
        if RemovedItems:
            for item in RemovedItems:
                basequery=f"""INSERT INTO `deletehistory` (`User1`, `RefType`, `RefNo`, `LN`, `ItemNo`, `ItemName`, `Qty`, `PQty`, `PUnit`, `UPrice`, `Disc`, `Tax`, `TaxTotal`, `Total`, `Note`, `Branch`, `DateDeleted`, `TimeDeleted`,`PPrice`,`PType`,`PQUnit`,`TotalPieces`,`SPUnit`,`BPUnit`,`DeleteType`) VALUES ('{data["username"]}', '{data["type"]}','{data["RefNo"]}','{item["item"]["lno"]}', '{item["item"]["no"]}', '{item["item"]["name"]}','{item["item"]["qty"]}', '{item["item"]["PQty"]}', '{item["item"]["PUnit"]}', '{item["item"]["uprice"]}', '{item["item"]["discount"]}', '{item["item"]["tax"]}', '{item["item"]["TaxTotal"]}','{item["item"]["Total"]}','{item["item"]["Note"]}', '{item["item"]["branch"]}', '{data["DateDeleted"]}', '{data["TimeDeleted"]}','{item["item"]["PPrice"]}','{item["item"]["PType"]}','{item["item"]["PQUnit"]}','{item["item"]["TotalPieces"]}','{item["item"]["SPUnit"]}','{item["item"]["BPUnit"]}','{data["DeleteType"]}'); """
                cur.execute(basequery)
        cur.execute(f"DELETE  FROM inv WHERE User1='{data['username']}' AND RefType='{data['type']}' AND RefNo='{data['RefNo']}' ;")
        
        cur.execute(f"DELETE  FROM invnum WHERE User1='{data['username']}' AND RefType='{data['type']}' AND RefNo='{data['RefNo']}' AND AccNo='{data['client']['id']}';")

        cur.execute(f"Delete FROM listdaily WHERE RefNo='{data['RefNo']}' AND RefType='{data['type']}' ")
            
        cur.execute(f"DELETE FROM goodstrans WHERE RefNo='{data['RefNo']}' AND RefType='{data['type']}' ")
        conn.commit()
        return({"Info":"authorized",
                    "msg":"Success"})
    except Exception as e:
        print("failer")
        print(str(e))
        return{"Info":"Failed",
        "msg":f"{str(e)}"}







@app.get("/moh/getCompanyInfo/{compname}")
async def getCompanyInfo(compname:str):

    try:
            
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = compname) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            response.status_code = status.HTTP_401_UNAUTHORIZED
            return({"Info":"unauthorized",
                    "msg":{e}})
    
    try:
        cur =conn.cursor()
        cur.execute("SELECT * FROM header limit 1;")
       
        for info in cur:
            if info[22]==None or info[22]=="":
                BackOffice="Y"
            else:
                BackOffice= info[22].upper()


            return {"Info":"authorized",
                    "CompanyInfo":{
                    "CompName":info[2],
                    "CompAdd":info[3],
                    "CompTell":info[4],
                    "CompEmail":info[5],
                    "CompTax": info[6],
                    "mainCur":info[7],
                    "Rate":info[8],
                    "Cur1":info[9],
                    "Cur2":info[10],
                    "CASH":info[11],
                    "VISA1":info[12],
                    "VISA2":info[13],
                    "VISA3":info[14],
                    "VISA4":info[15],
                    "VISA5":info[16],
                    "VISA6":info[17],
                    "GroupType":info[18],
                    "PrintFormat":info[19],
                    "CompanyCode": info[20],
                    "Notify":info[21],
                    "BackOffice":BackOffice
                    
                }
             }
        
    except Exception as e:
        print("failer")
        print(str(e))
        return{"Info":"Failed",
        "msg":f"{str(e)}"}


@app.get("/moh/{compname}/ItemStockDetails/Double/{itemNo}/",status_code=200)
async def StockStatement(compname:str, itemNo:str):

    username=compname
    try:
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
    except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            response.status_code = status.HTTP_401_UNAUTHORIZED
            return({"Info":"unauthorized",
                    "msg":{e}})
    
    cur = conn.cursor()

    query = f"""SELECT gt.Branch,SUM(Qin-Qout), h.BranchName FROM `goodstrans` gt
    LEFT JOIN (SELECT Branch,BranchName FROM header) h ON h.Branch=gt.Branch
      WHERE gt.ItemNo = '{itemNo}' GROUP BY gt.Branch"""
    cur.execute(query)
    stockDetails = []
    ind = 0
    for x in cur:
        
        stockDetails.append({     
        "key":ind,                 
        "Branch" :x[0] ,  
        "Qty" :x[1],
        "BranchName":x[2]
        })
        ind = ind +1
    
    return{
    "Info":"authorized",
    "stockDetails":stockDetails,
    "ItemNo":itemNo
    }
@app.post("/moh/ReleaseInvoice/{username}/{user}/{InvoiceId}/")
async def releaseInvoice(InvoiceId:str,user:str,username:str):
    try:
        conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
    except mariadb.Error as e:       
        print(f"Error connecting to MariaDB Platform: {e}")  
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return({"Info":"unauthorized",
                    "msg":{e}})
    
    cur = conn.cursor()
    cur.execute(f"UPDATE invnum SET UserP='' WHERE RefNo='{InvoiceId}' ")
    conn.commit()
    return{
    "Info":"authorized",
    "message":"Success"  }

@app.post("/moh/Invoice_Group_Select")
async def Invoice_Group_Select(data:dict):
    username=data["username"]
    GroupType=data["value"]
    try:
        conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
    except mariadb.Error as e:       
        print(f"Error connecting to MariaDB Platform: {e}")  
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return({"Info":"unauthorized",
                    "msg":{e}})
    
    cur = conn.cursor()
    groupTypes=[]
    BaseQuery = f"SELECT DISTINCT {GroupType} FROM goods WHERE {GroupType} IS NOT NULL AND {GroupType}!=''  "
    print(BaseQuery)
    cur.execute(BaseQuery)

    for group in cur:
         groupTypes.append(
              {
                  "GroupName":group[0] 
              }
         )
    print(groupTypes)
    return{
    "Info":"authorized",
    "message":"Success",
     "groupTypes":groupTypes }

@app.post("/moh/savePhoneNumber/{username}/{phoneNumber}/{AccountId}/")
async def savePhoneNumber(AccountId:str,phoneNumber:str,username:str):
    try:
        conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
    
    
        cur = conn.cursor()
        cur.execute(f"UPDATE listhisab SET Mobile='{phoneNumber}' WHERE AccNo='{AccountId}' ")
        conn.commit()
        return{
        "Info":"authorized",
        "message":"Success"  }
    except Exception as e:       
        print(f"Error : {e}")  
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return({"Info":"Failed",
                    "msg":{e}})

@app.post("/moh/CheckIn/")
async def CheckIn(data:dict):
    try:
        username = data["compname"]
        conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 

        accname=""
    
        cur = conn.cursor()
        if data["method"]!="Note":

            if data["BackOffice"] =="Y":
                try:
                    parts = data['accno'].split("__")
                    if len(parts) > 1:
                        data['accno']= parts[0]
                   
                        print(accname)
                    else:
                        raise ValueError("Invalid format for accno")
                except KeyError:
                    return({"Info":"Failed",
                    "message":"Wrong Barcode Format Data"})
                basequery1=f"SELECT AccNo,AccName FROM listhisab WHERE AccNo='{data['accno']}'"
                print(basequery1)
                cur.execute(basequery1)
                # for row in cur:
                #      accname=row[1]
                rows = cur.fetchall()

                if rows:  # Check if any rows are returned
                    for row in rows:
                        accname = row[1]
                else:
                    # Handle case when no rows are returned
                    return({"Info":"authorized","flag":0,
                            "message":"No Account Found","Account":data["accno"]})
            else:
                try:
                    accname=data["accno"]
                    parts = accname.split("__")
                    if len(parts) > 1:
                        accname = parts[1]
                        data['accno']='xxxxxxxx'
                        data['Note']=parts[0]
                        print(accname)
                    else:
                        raise ValueError("Invalid format for accno")
                except KeyError:
                    return({"Info":"Failed",
                    "message":"Wrong Barcode Format Data"})
        else:
            accname=data["Note"]
        basequery = f"""INSERT INTO `invnum` (`User1`, `RefType`, `AccNo`,`AccName`, `Branch`, `TBranch`, `DateI`, `TimeI`, `DateP`, `TimeP`, `UserP`,`Cur`,`Rate`,`long`,`lat`,`Note`,`DateValue`) VALUES ('{data["username"]}', '{data["type"]}','{data["accno"]}', '{accname}', '', '', '{data["accDate"]}', '{data["accTime"]}', '','','','',0,'{data["long"]}','{data["lat"]}','{data["Note"]}',''); """
        print(basequery)
        cur.execute(basequery)
        conn.commit()
        return{
        "Info":"authorized",
        "message":"Success",
        "flag":1,
          "Account": accname }
    except Exception as e:       
        print(f"Error : {e}")  
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return({"Info":"Failed",
                    "message":{e}})


@app.get("/moh/getUsers/{compname}/")
async def getUsers(compname:str):
    try:
        username = compname
        userResult=[]
        conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
        cur = conn.cursor()
        query = f"SELECT DISTINCT User1 FROM invnum  "
        cur.execute(query)
        for result in cur:
            userResult.append(result[0])
        print(userResult)
       
        return {"status":"success","result":userResult}
    except Exception as e:       
        print(f"Error : {e}")  
        return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content={"Info": "Failed", "message": str(e)})
 
@app.post("/moh/CheckInDashboard/")
async def  CheckInDashboard(data:dict):
    try:
        username = data["compname"]
        searchResult=[]
        print(data)
        conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
        cur = conn.cursor()
        if data['type']=="All":
            query = f"SELECT User1,RefNo,AccName,DateI,TimeI,`long`,lat,Note,AccNo,DateValue  FROM invnum WHERE RefType IS NOT NULL "
        else:
            query = f"SELECT User1,RefNo,AccName,DateI,TimeI,`long`,lat,Note,AccNo,DateValue  FROM invnum WHERE RefType='{data['type']}' "
        if data["search"]!='':
            query=query + f""" AND (User1 LIKE '%{data["search"]}' OR User1 LIKE '{data["search"]}%' OR User1 LIKE '%{data["search"]}%' OR AccName LIKE '%{data["search"]}' OR AccName LIKE '{data["search"]}%' OR AccName LIKE '%{data["search"]}%' OR Note LIKE '%{data["search"]}' OR Note LIKE '{data["search"]}%' OR Note LIKE '%{data["search"]}%' OR RefNo LIKE '{data["search"]}%' OR RefNo LIKE '%{data["search"]}' OR RefNo LIKE '%{data["search"]}%' OR AccNo LIKE '{data["search"]}%' OR AccNo LIKE '%{data["search"]}' OR AccNo LIKE '%{data["search"]}%') """
        if data['fromDate']!=None:
            #from_date = datetime.strptime(data["fromDate"], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%d/%m/%Y")
            print()
            query = query + f" AND STR_TO_DATE(DateI, '%d/%m/%Y') >=  STR_TO_DATE('{data['fromDate']}', '%d/%m/%Y') "
        if data['toDate']!=None:
            #to_date = datetime.strptime(data["toDate"], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%d/%m/%Y")
            query = query + f" AND STR_TO_DATE(DateI, '%d/%m/%Y') <= STR_TO_DATE('{data['toDate']}', '%d/%m/%Y') "
        
        if data['toTime']!=None:
            # to_time = datetime.strptime(data['toTime'], "T%H:%M:%S")
            # to_time_formatted = to_time.strftime("%H:%M:%S")
            query = query + f" AND TimeI <= '{data['toTime']}' "
        if data['fromTime']!=None:
            # from_time = datetime.strptime(data['fromTime'], "T%H:%M:%S")
            # from_time_formatted = from_time.strftime("%H:%M:%S")
            query = query + f" AND TimeI >= '{data['fromTime']}' "

        if data['user']!='':
            query = query + f" AND (User1 LIKE '%{data['user']}' OR User1 LIKE '{data['user']}%' OR User1 LIKE '%{data['user']}%' ) "
        if data['limit']!="All":
            query = query + f"""ORDER BY STR_TO_DATE(DateI, '%d/%m/%Y') DESC,  STR_TO_DATE(SUBSTRING(TimeI, 2), '%H:%i:%s') DESC 
 LIMIT {data["limit"]};"""
        else:
            query = query + f"""ORDER BY STR_TO_DATE(DateI, '%d/%m/%Y') DESC,   STR_TO_DATE(SUBSTRING(TimeI, 2), '%H:%i:%s') DESC ;"""
        print(query)

        cur.execute(query)
        for result in cur:
            result= {
                "User":result[0],
                "RefNo":result[1],
                "AccName": result[2],
                "DateI": result[3],
                "TimeI":result[4],
                "LocationUrl":f"{result[6]},{result[5]}",
                "Notes": result[7],
                "AccNo":result[8]

            },
            searchResult.append(result)
        return({"Info":"success","result":searchResult})
    except Exception as e:       
        print(f"Error : {e}")  
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return({"Info":"Failed",
                    "message":{e}})    


@app.get("/moh/AcccesManagement/{compname}/")
async def  AcccesManagement(compname:str):
    try:
        username =compname.upper()
        result=[]
        columnNamesRes=[]
        branches=[]
        try:
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = "python") 

            conn2 = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
            #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
        
        except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            
            return({"status":"Error",
                    "message":{e}})
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
        cur = conn.cursor()
        cur2=conn2.cursor()
        query = f"SELECT * FROM users WHERE compcode='{username}'"
        print(query)
        cur.execute(query)
        
        cur2.execute("SELECT DISTINCT `Branch` FROM `header` WHERE Branch is not null order by Branch asc;")   
        
        branches = []
        salePrices=['1','2','3','4','5']

        for br in cur2:
            if br[0] != "":
                branches.append(br[0])
        print(branches)
        column_names = [desc[0] for desc in cur.description]
        for i in range(8,len(column_names)):
            if not column_names[i].startswith("checkPrice"):
                columnNamesRes.append(column_names[i])
        print(columnNamesRes)
        for userRes in cur:
            user = {
                "name": userRes[1],
                "permissions": [
                    {column_names[i]: userRes[i] for i in range(8, len(column_names))  if  not +column_names[i].startswith("checkPrice")            
}
                ]
            }
            # Flatten permissions dictionary and convert to the required format
            user["permissions"] = [
                {"name": perm_name, "access": "Y" if perm_access is None or perm_access == '' else perm_access}
                for perm_name, perm_access in user["permissions"][0].items()
            ]
            result.append(user)
        
       
        conn.close()
        conn2.close()
        return ({"status": "success",
                 "users":result,"permissionsName":columnNamesRes,"branches":branches,"salePrices":salePrices})

            
    except Exception as e:
        return ({"status": "Error",
                 "message":{e}})
    

@app.post("/moh/UpdateUsersPermissions/")
async def updateUsersPermsissions(data:dict):

    try:
        compname = data['compname'].upper()
        try:
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = "python") 
            cur=conn.cursor()
        except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            
            return({"status":"Error",
                    "message":{e}})
            
        usersPermissions =data['users']
        usersToUpdate = data['changedUsers']
        
        for user in usersToUpdate:
            user_data = next((u for u in usersPermissions if u['name'] == user), None)
            if user_data:
                updates = []
                for perm in user_data['permissions']:
                    column_name = perm['name']
                    new_value = perm['access']
                    updates.append(f"`{column_name}` = '{new_value}'")
                
                update_query = f"UPDATE Users SET {', '.join(updates)} WHERE username = '{user_data['name']}' AND  compcode='{compname}'"
                print(update_query)
                cur.execute(update_query)
        
        conn.commit()
        return ({"status": "success"})


    except Exception as e:       
         
            print(e)
            return({"status":"Error",
                    "message":{e}})
        

@app.get("/moh/getCompanySettings/{compname}/")
async def getCompanySettings(compname:str):
    try:
        username = compname
        headerResult={}
        conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
        cur = conn.cursor()
        query = f"SELECT GroupType,PrintFormat,CompanyCode,holidays FROM header LIMIT 1"
        cur.execute(query)
        for result in cur:
            headerResult = {
                "GroupType":result[0],
                "PrintFormat": result[1],
                "CompanyCode":result[2],
                "Holidays" : result[3] if result[3] not in [None, "", "null"] else []
                
            }
        print(headerResult)
       
        return {"status":"success","result":headerResult}
    except Exception as e:       
        print(f"Error : {e}")  
        return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content={"Info": "Failed", "message": str(e)})


@app.post("/moh/UpdateCompanySettings/")
async def updateUsersPermsissions(data:dict):

    try:
        try:
            username =data['compname']
            conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
            cur=conn.cursor()
        except mariadb.Error as e:       
            print(f"Error connecting to MariaDB Platform: {e}")  
            
            return({"status":"Error",
                    "message":{e}})

        if data['groupType']!=None:

            update_query = f"""UPDATE header SET GroupType='{data['groupType']}',PrintFormat='{data['printFormat']}',CompanyCode='{data['companyCode']}',Holidays='{data['holidays']}'  LIMIT 1
"""     
        else:
            update_query = f"""UPDATE header SET PrintFormat='{data['printFormat']}',CompanyCode='{data['companyCode']}',Holidays='{data['holidays']}'  LIMIT 1
"""
        cur.execute(update_query)
        
        conn.commit()
        return ({"status": "success"})


    except Exception as e:       
         
            print(e)
            return({"status":"Error",
                    "message":{e}})


@app.get("/moh/getPieChartData/{compname}/{option}/{type}/")
async def getPieChartData(compname:str,option:int,type:str):
    try:
        username = compname
        ResultCur1=[]
        ResultCur2=[]
        Curencies=[]
        res=[]
        index = 0  # Initialize index counter

        conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
        cur = conn.cursor()
        # queryCur= "SELECT Distinct Color FROM goods;"
        # cur.execute(queryCur)
        # for resCur in cur:

        #     if resCur[0]!='':
        #         Curencies.append(resCur[0])
        ref_types = {
            1: {"PI": "PI_AP", "PR": "PR_AP", "SA": "SA_AP", "SR": "SR_AP", "OD": "OD_AP"},
            2: {"PI": "PI", "PR": "PR", "SA": "SA", "SR": "SR", "OD": "OD"},
            3: {"PI": "PI%", "PR": "PR%", "SA": "SA%", "SR": "SR%", "OD": "OD%"}
        }

        
        selected_types = ref_types[option]

        # Create a list of LIKE conditions
        like_conditions = [f"gt.RefType LIKE '{value}'" for value in selected_types.values()]

        # Join the conditions with OR
        like_clause = " OR ".join(like_conditions)
        query = f"""SELECT gt.RefType,SUM(Total),g.Color AS Currency,COUNT(gt.RefNo) AS InvoicesNb FROM goodstrans gt
                 JOIN goods g ON gt.ItemNo = g.ItemNo
                WHERE {like_clause} 
                  GROUP BY gt.RefType """
        print(query)
        cur.execute(query)
        for result in cur:
            color=""
            if result[0]=="SR_AP":
                color="red"
            elif result[0]=="PR_AP":
                color="orange"
            elif result[0]=="SA_AP":
                color="blue"
            elif result[0]=="PI_AP":
                color="yellow"
            elif result[0]=="OD_AP":
                color="green"
            # if result[2]==Curencies[0]:
            #     resCur1 = {
            #         "id":index,
            #         "value":result[1],
            #         "label": f"series {result[0]}",
            #     "Cur":result[2],
            #     "color":color,
            #                     "invoices":result[3]

            #     }
            #     ResultCur1.append(resCur1)
            # else:
            resCur2 = {
                "id":index,
            "value":result[1],
                "label": f"series {result[0]}",
            "Cur":"",
                            # "color":color,
            "invoices":result[3]

            }
            ResultCur2.append(resCur2)
            index += 1  # Increment index for each result item

            
        #res.append(ResultCur1)
        res.append(ResultCur2)
        return {"status":"success","result":res[0]}
    except Exception as e:       
        print(f"Error : {e}")  
        return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content={"Info": "Failed", "message": str(e)})
@app.get("/moh/getBarChartData/{compname}/{option}/{type}/")
async def getBarChartData(compname:str,option:int,type:str):
    try:
        username = compname
  
       
        index = 0  # Initialize index counter

        conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
        cur = conn.cursor()

        query = f"""SELECT 
    gt.RefType, 
    MONTH(gt.TDate) as Month, 
    g.Color, 
    SUM(gt.Total) as Total
FROM 
    goodstrans gt
JOIN 
    goods g ON gt.ItemNo = g.ItemNo
WHERE 
    gt.RefType IN ('PI_AP', 'PR_AP', 'SA_AP', 'SR_AP') AND gt.TDate IS NOT NULL AND gt.TDate!=''
GROUP BY 
    gt.RefType, MONTH(gt.TDate) ORDER BY MONTH(gt.TDate); """
        cur.execute(query)
        results= cur.fetchall()
        data = {}
        for ref_type,month,cur,  total in results:
            month_name = calendar.month_name[month][:3]  # Get month name abbreviation
            if month_name not in data:
                data[month_name] = {"month": month_name} 
            data[month_name][ref_type] = round(total,2)

        dataset = list(data.values())

        return {"status": "success", "result": dataset}
    except Exception as e:
        print(f"Error : {e}")
        return JSONResponse(status_code=HTTPException(status_code=401, detail=str(e)))

@app.get("/moh/getProfitData/{compname}/{year}/{option}/{type}/")
async def getProfitData(compname:str,year:str,option:int,type:str):
    try:
        username = compname
  
       

        conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
        cur = conn.cursor()

        query = f"""SELECT
    ROUND(SUM(CASE 
        WHEN RefType IN ('SA_AP', 'PR_AP') THEN Total 
        WHEN RefType IN ('PI_AP', 'SR_AP') THEN -Total 
        ELSE 0 
    END) ,0)AS Profit
FROM 
    goodstrans
WHERE 
    RefType IN ('SA_AP', 'SR_AP', 'PI_AP', 'PR_AP')
    AND YEAR(TDate) = {year};
 """
        cur.execute(query)
        result= cur.fetchone()

        return {"status": "success", "result": result[0]}
    except Exception as e:
        print(f"Error : {e}")
        return JSONResponse(status_code=HTTPException(status_code=401, detail=str(e)))

@app.get("/moh/getLineChartDataProfit/{compname}/{year}/{option}/{type}/")
async def getLineChartDataProfit(compname:str,year:str,option:int,type:str):
    try:
        username = compname
  
       
        data=[]
        dataMonths=[]
        conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
        cur = conn.cursor()

        query = f"""SELECT MONTH(TDate),
    ROUND(SUM(CASE 
        WHEN RefType IN ('SA_AP', 'PR_AP') THEN Total 
        WHEN RefType IN ('PI_AP', 'SR_AP') THEN -Total 
        ELSE 0 
    END) ,0)AS Profit
FROM 
    goodstrans
WHERE 
    RefType IN ('SA_AP', 'SR_AP', 'PI_AP', 'PR_AP')
    AND YEAR(TDate) = '{year}'
    GROUP BY MONTH(TDate); """
        cur.execute(query)
        results= cur.fetchall()
 
        for month,profit in results:
            data.append(profit)
            month_name = calendar.month_name[month][:3]  # Get month name abbreviation
            dataMonths.append(month_name)
        result=[]
        result.append(dataMonths)
        result.append(data)
        
        return {"status": "success", "result": result}
    except Exception as e:
        print(f"Error : {e}")
        return JSONResponse(status_code=HTTPException(status_code=401, detail=str(e)))

@app.get("/moh/getTopSellersByAmount/{compname}/{year}/{option}/{type}/")
async def getTopSellersByAmount(compname:str,year:str,option:int,type:str):
    try:
        username = compname
  
       
        result=[]
        dataMonths=[]
        conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
        cur = conn.cursor()

        query = f"""SELECT itemName, ROUND(SUM(Total),0) AS TotalAmount FROM goodstrans
WHERE RefType IN ('SA_AP','OD_AP')
AND YEAR(TDate) = {year}
GROUP BY itemNo
ORDER BY TotalAmount
DESC LIMIT 5  """
        cur.execute(query)
        results= cur.fetchall()
 
 
        for res in results:
            result.append({"ItemName":res[0],"TotalAmount":res[1]})
        
        return {"status": "success", "result": result}
    except Exception as e:
        print(f"Error : {e}")
        return JSONResponse(status_code=HTTPException(status_code=401, detail=str(e)))
    

@app.get("/moh/getTopSellersByQuantity/{compname}/{year}/{option}/{type}/")
async def getTopSellersByQuantity(compname:str,year:str,option:int,type:str):
    try:

        username = compname
  
       
        result=[]
        conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost,port=9988,database = username) 
        #conn = mariadb.connect(user="ots", password="", host="127.0.0.1",port=3306,database = username) 
        cur = conn.cursor()

        query = f"""SELECT itemName, ROUND(SUM(Qty),0) AS TotalQty FROM goodstrans
WHERE RefType IN ('SA_AP','OD_AP')
AND YEAR(TDate) = {year}
GROUP BY itemNo
ORDER BY TotalQty
DESC LIMIT 5  """
        cur.execute(query)
        results= cur.fetchall()
 
        for res in results:
            print('ooo',res[0])
            result.append({"ItemName":res[0],"TotalQty":res[1]})
        print(result)
        
        return {"status": "success", "result": result}
    except Exception as e:
        print(f"Error : {e}")
        return JSONResponse(status_code=HTTPException(status_code=401, detail=str(e)))
# @app.get("/inv")
# async def fetch_inv():
    # pool = await asyncpg.create_pool(database="donate", user="root", password="root", host="3307")
    # async with pool.acquire() as conn:
    #     query = "SELECT * FROM online"
    #     rows = await conn.fetch(query)
    #     return rows
# async def listen_to_notifications(websocket: WebSocket):
#     await websocket.accept()
#     try:
#         while True:
#             data = await websocket.receive_text()
#             print("Notification received:", data)
#             # Fetch updated data from inv table and send it to the client
#             # You can use the asyncpg library to interact with the PostgreSQL database
#             # Example:
#             # async with pool.acquire() as conn:
#             #     query = "SELECT * FROM inv"
#             #     rows = await conn.fetch(query)
#             #     await websocket.send_json(rows)
#             print("n")
#     except WebSocketDisconnect:
#         pass

# @app.websocket("/ws")
# async def websocket_endpoint(websocket: WebSocket):
#     await listen_to_notifications(websocket)

# @app.post("/notify")
# async def notify():
#     print("Received notification about operation:")
#     # Handle the notification as needed
#     return {"message": "Notification received"}
app.mount("/locales", StaticFiles(directory="D:/PARADOXProjects/mohasaba2/public/locales"), name="locales")

@app.get("/moh/recommendation/{compname}/{period_months}/{years_back}/", response_model=List[Dict[str, float]])
def recommendation(compname: str, period_months: int, years_back: int):
    try:
        transactions = get_data_from_db(compname, period_months, years_back)
        recommendations = prepare_data(transactions, period_months, years_back)
        return recommendations
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

def prepare_data(transactions, period_months, years_back):
    df = pd.DataFrame(transactions, columns=['ItemNo', 'date', 'quantity'])
    df['date'] = pd.to_datetime(df['date'], format='%Y/%m/%d')

    recommendations = []

    for ItemNo, group in df.groupby('ItemNo'):
        total_quantity = group['quantity'].sum()
        average_quantity = total_quantity / years_back
        recommended_quantity = round(average_quantity)

        recommendations.append({
            'item_number': ItemNo,
            'recommended_quantity': recommended_quantity
        })

    return recommendations

def get_data_from_db(username, period_months, years_back):
    conn = mariadb.connect(user="ots", password="Hkms0ft", host=dbHost, port=9988, database=username)
    cursor = conn.cursor()

    now = datetime.now()
    end_date = now.replace(day=1) + timedelta(days=32)
    end_date = end_date.replace(day=1) - timedelta(days=1)
    start_date = end_date - timedelta(days=period_months * 30)

    query = """
 SELECT ItemNo, TDate, SUM(Qout) as Qout
FROM goodstrans
WHERE TDate BETWEEN %s AND %s
GROUP BY ItemNo, TDate
    """

    all_rows = []
    for year in range(years_back):
        year_start = start_date - timedelta(days=365 * year)
        year_end = end_date - timedelta(days=365 * year)
         # Print the query with parameters
        print(f"Executing query: {query}")
        print(f"Parameters: {year_start.strftime('%Y/%m/%d')}, {year_end.strftime('%Y/%m/%d')}")
        cursor.execute(query, (year_start.strftime('%Y/%m/%d'), year_end.strftime('%Y/%m/%d')))
        rows = cursor.fetchall()
        all_rows.extend(rows)

    conn.close()
    return all_rows