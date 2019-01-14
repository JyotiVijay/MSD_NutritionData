#spark-submit --num-executors 10  --master yarn-client --executor-memory 8g --driver-memory 4g Nutrition_Data.py
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext,HiveContext
import os
import sys
import datetime
import pandas as pd
from time import gmtime, strftime
import subprocess
import glob
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
def main():
    conf = SparkConf().setMaster('yarn-client')
    conf = conf.setAppName('Nutrition_Data')
    sc = SparkContext(conf=conf)
    sqlContext = HiveContext(sc)
    utc_datetime=datetime.datetime.utcnow()
    formatted_string=utc_datetime.strftime("%Y-%m-%d-%H-%M")
    current_t=strftime("%Y-%m-%d.%H:%M:%S", gmtime())
    #textfile='/CTRLFW/ICDD/data/prd/edm/hadoop/icddapp/learnings/evolving_matrix/matrix_output_{0}.html'.format(current_t)
    #subprocess.call(["touch",textfile])
    def avg_data_value_age():  
        dbname='NutritionData'
        avg_data_value_age_df=sqlContext.sql("select question , yearstart , case when age_in_months is null or trim(age_in_months) = '' then '0-12' else age_in_months end as age_in_months_case, avg(data_value) from {0}.Nutrition_Physical_Activity group by question , yearstart , age_in_months ".format(dbname)).coalesce(1)
        avg_data_value_age_df.insertInto('{0}.case_1'.format(dbname))
        current_t=strftime("%Y-%m-%d.%H:%M:%S", gmtime())
        textfile='/CTRLFW/ICDD/data/prd/edm/hadoop/icddapp/learnings/ingestion_stack/input/NutritionData_case1_{0}.html'.format(current_t)
        #subprocess.call(["touch",textfile])
        filew = open(textfile, 'wb')
        header_t01 = """
        <!DOCTYPE html>
        <html><head><style>
        table, th, td {border: 2px solid black;border-collapse: collapse;}th, td { padding: 2px;    text-align: left;}
        table#t01 {width: 75%; background-color: #f1f1c1;}
        table#t02 {width: 75%; background-color: #eee;}
        </style></head>
        
        <body>
        <p>Average of each Questions "Data_Value" by year for all age groups:</p>
        
        <table id="t01">
          <caption><b>CASE_1</b></caption>
          <tr><th>question</th><th>yearstart</th><th>age_in_months</th><th>avg_data_value</th></tr>
        """
        filew.write(header_t01)
        report_case1_df = sqlContext.sql("select * from NutritionData.case_1").collect()
        
        for row in report_case1_df:
            question        = row[0]
            yearstart       = row[1]
            age_in_months   = row[2]
            avg_data_value  = row[3]
            print question , yearstart , age_in_months, avg_data_value
            report_record = "<tr><td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td></tr>".format(question, yearstart, age_in_months, avg_data_value)
            filew.write(report_record)      

        trailer='''
        </table>
        </body>
        </html>'''
        filew.write(trailer)
        filew.close()
        msg = MIMEMultipart()
        me='sitbwcapp'
        to = ["IcddDev.Team@scbdev.com"]
        file = open(textfile, 'rb')
        html = file.read()
        bodyhtml = MIMEText(html, 'html')
        msg.attach(bodyhtml)
        msg['Subject'] = 'Recon Status'
        msg['From'] = me
        msg['To'] = ",".join(to)
        s = smtplib.SMTP('localhost')
        s.sendmail(me, to, msg.as_string())
        s.quit()            
        
    def avg_data_value_gen():   
        dbname='NutritionData'
        avg_data_value_gender_df=sqlContext.sql("select question , yearstart ,avg(data_value) from {0}.Nutrition_Physical_Activity where trim(upper(gender)) = 'FEMALE' group by question , yearstart".format(dbname)).coalesce(1)
        avg_data_value_gender_df.insertInto('{0}.case_2'.format(dbname))
        current_t=strftime("%Y-%m-%d.%H:%M:%S", gmtime())
        textfile_2='/CTRLFW/ICDD/data/prd/edm/hadoop/icddapp/learnings/ingestion_stack/input/NutritionData_case2_{0}.html'.format(current_t)
        #subprocess.call(["touch",textfile_2])
        filew = open(textfile_2, 'wb')
        header_t01 = """
        <!DOCTYPE html>
        <html><head><style>
        table, th, td {border: 2px solid black;border-collapse: collapse;}th, td { padding: 2px;    text-align: left;}
        table#t01 {width: 75%; background-color: #f1f1c1;}
        table#t02 {width: 75%; background-color: #eee;}
        </style></head>
        
        <body>
        <p>Average of each Questions "Data_Value" by year for Females:</p>
        
        <table id="t01">
          <caption><b>CASE_2</b></caption>
          <tr><th>question</th><th>yearstart</th><th>avg_data_value</th></tr>
        """
        filew.write(header_t01)
        report_case2_df = sqlContext.sql("select * from NutritionData.case_2").collect()
        
        for row in report_case2_df:
            question        = row[0]
            yearstart       = row[1]
            avg_data_value  = row[2]
            print question , yearstart , avg_data_value
            report_record = "<tr><td>{0}</td><td>{1}</td><td>{2}</td></tr>".format(question, yearstart, avg_data_value)
            filew.write(report_record)      
        trailer='''
        </table>
        </body>
        </html>'''
        filew.write(trailer)
        filew.close()
        msg = MIMEMultipart()
        me='sitbwcapp'
        to = ["IcddDev.Team@scbdev.com"]
        file = open(textfile_2, 'rb')
        html = file.read()
        bodyhtml = MIMEText(html, 'html')
        msg.attach(bodyhtml)
        msg['Subject'] = 'Recon Status'
        msg['From'] = me
        msg['To'] = ",".join(to)
        s = smtplib.SMTP('localhost')
        s.sendmail(me, to, msg.as_string())
        s.quit()            
        
    print "call function for case_1"
    avg_data_value_age()
    
    print "call function for case_1"
    avg_data_value_gen()
    
if __name__ == '__main__' :
   main()

