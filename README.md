***README.md***

######################### Problem statements##################################

To demonstrate skillset of ETL, Hive and Spark. Please develop automation pipeline to ingest and calculate data in BigData Platform. 

1. Setup Hadoop Stack (Ex. Use Scripts like - Ansible, Bash etc to setup Hadoop Stack or Download Hortonworks Sandbox).
2. Develop scripts/programs to download and ingest data from <https://chronicdata.cdc.gov/views/735e-byxc/rows.csv?accessType=DOWNLOAD> to HDFS and Hive. 
3. Develop scripts/programs to calculate the following using Spark, and store the results into separate Hive tables:
   - Average of each Question’s "Data_Value" by year for all age groups
   - Average of each Question’s "Data_Value" by year for female only
4. Bonus (Optional) - Develop data visualization to show the data. (Ex. SpringBoot, React, JavaScript, HTML - D3.js)
5. Create a ***README.md*** file to include the execution steps to setup environment and run the solution.
6. Check-in the solution in GitHub.


##################### Solution designed ###################################

1. Infra/Tools used

Platform : Hortonworks Cluster
Spark Version:  1.6.3
Spark-Sql: 1.6.3
Spark-core: 1.6.3
Python version: 2.6.9 

2. Assumptions Considered:

1) Downloaded the data file as is to windows machine and pushed to unix local server using winscp .
3) I have considered the age(in months) as string column in the data feed.

Steps::
1) Login to unix sever using Putty and create the following directory to copy the data and use case related jar :
   
   mkdir -p <home>/MSDNutrition/scripts [example: mkdir -p /home/MSD/MSDNutrition/scripts]
   mkdir -p <home>/MSDNutrition/data  [example: mkdir -p /home/MSD/MSDNutrition/data]  
	
2) Download data and move to unix local system using WinSCP or we can also wget command to download the data directly (if no security enable in unix server) 
    On Unix server:
	cd  /home/MSD/MSDNutrition/data
    wget https://chronicdata.cdc.gov/views/735e-byxc/rows.csv?accessType=DOWNLOAD 
	      OR
    copy the data using winscp from windows machine under <home>/MSDNutrition/data	  

3) Create below hive tables from Hive terminal

    A) Input will be stored here-
    create database NutritionData;
    create external table NutritionData.Nutrition_Physical_Activity
    (
    YearStart   string,
    YearEnd   string,
    LocationAbbr string,
    LocationDesc   string,
    Datasource   string,
    Class   string,
    Topic   string,
    Question   string,
    Data_Value_Unit   string,
    Data_Value_Type   string,
    Data_Value   string,
    Data_Value_Alt   string,
    Data_Value_Footnote_Symbol   string,
    Data_Value_Footnote   string,
    Low_Confidence_Limit   string,
    High_Confidence_Limit    string,
    Sample_Size   string,
    Total   string,
    Age_in_months   string,
    Gender   string,
    Race_or_Ethnicity   string,
    GeoLocation   string,
    ClassID   string,
    TopicID   string,
    QuestionID   string,
    DataValueTypeID   string,
    LocationID   string,
    StratificationCategory1   string,
    Stratification1   string,
    StratificationCategoryId1   string,
    StratificationID1 string )
    row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    stored as textfile
    location '/prd/edm/hadoop/icddapp/hdata/NutritionData/Nutrition_Physical_Activity/'
    tblproperties ("skip.header.line.count"="1");
    
    
    B) create below table where output will be saved
    
    create table NutritionData.case_1 (
    Question   string,
    yearstart   string,
    age_in_months   string,
    avg_data_value   string)
    
    C) create table NutritionData.case_2 (
    Question   string,
    yearstart   string,
    avg_data_value   string)			   
	
  
	
4) Run script using below command-
   spark-submit --num-executors 10  --master yarn-client --executor-memory 8g --driver-memory 4g Nutrition_Data.py

output : 
1. hive table NutritionData.case_1 (Average of each Question’s "Data_Value" by year for all age groups)
2. hive table NutritionData.case_2 (Average of each Question’s "Data_Value" by year for female only)
3. DashBord is created to visualise data and an auto generated email will be sent with report details for both the cases.


     