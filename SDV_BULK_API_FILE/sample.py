# # import pandas as pd
# #
# # sdv_bulk = pd.read_excel(r'C:\Users\vamsikkrishna\PycharmProjects\pythonProject1\BULK_CONFIG_FILE\Engagement_training_config_file.xlsx',sheet_name='SDV_BULK')
# #
# # col_list = set(sdv_bulk['Template_field_name'].tolist())
# #
# # gen_data = pd.read_excel(r'C:\Users\vamsikkrishna\PycharmProjects\pythonProject1\sample_synthetic\concat_dataframe_final.xlsx')
# #
# # gen_col_list = set(gen_data.columns.tolist())
# #
# # print(col_list.symmetric_difference(gen_col_list))
# import pandas as pd
# import pyodbc
# import numpy as np
# import datetime
#
# # Connect to the Access database
# # conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\Users\vamsikkrishna\Documents\Database1.accdb;'
# #
# # conn_string = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\Users\jseinfeld\Desktop\Databasetest1.accdb;'
# status = 'Draft'
# now = datetime.datetime.now()
# now =now.isoformat()
# conn_string = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\Users\vamsikkrishna\Documents\Database1.accdb;'
# conn = pyodbc.connect(conn_string)
#
#         # Create a cursor to execute SQL queries
#
# json_ui = {
#
# "ProcessAreaId": "ENG",
#
# "DataSetName": "TEST_ENG",
#
# "TargetSys": "SDD070",
#
# "CountryKey": "US",
#
# "NumOfRecords": 3,
#
#     "DatasetUID" : 'SYN12345',
#
#
# "CreatedBy" :"T_WIP_USER"
#
# }
#
# cursor = conn.cursor()
# table_name = "DataSet"
# VALUES =({json_ui['ProcessAreaId']}, {json_ui['TargetSys']}, {json_ui['DatasetUID']}, {json_ui['DataSetName']},
#                {now}, {json_ui['CreatedBy']}, {status})
#
# VALUES =(json_ui['ProcessAreaId'], json_ui['TargetSys'], json_ui['DatasetUID'], json_ui['DataSetName'],
#                now, json_ui['CreatedBy'], status)
# insert_query = f"""INSERT INTO {table_name} ([Process_Area], [Target_system], [DataSet_GUID], [DataSet_Name], [Created_On], [Created_By], [Status]) VALUES (?, ?, ?, ?, ?, ?, ?)"""
# cursor.execute(insert_query, VALUES)
# conn.commit()
# print("Records inserted successfully!")
# # Close the connection and cursor
# cursor.close()
# conn.close()
#
#
#
#
# import pyodbc
#
# # Connect to the Access database
# conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\Users\vamsikkrishna\Documents\Database1.accdb;'
# conn = pyodbc.connect(conn_str)
#
# # Create a cursor to execute SQL queries
# cursor = conn.cursor()
#
# # Update a column value in a row
# table_name = "DB_Status"
# select_query = f"SELECT * FROM {table_name}"
#
# # Execute the SELECT query
# cursor.execute(select_query)
# rows = cursor.fetchall()
#
# status_dictionary = dict()
#
# for row in rows:
#     status_dictionary[int(row[0])] = row[1]
#
# #conn.commit()
# print(status_dictionary)
# print("Record updated successfully!")
#
# # Close the connection and cursor
# cursor.close()
# conn.close()
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
# # conn_string = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\Users\vamsikkrishna\Documents\Database1.accdb;'
# # conn = pyodbc.connect(conn_string)
# #
# # # Create a cursor to execute SQL queries
# # cursor = conn.cursor()
# #
# # concat_dataframe  = pd.read_excel(r'C:\Users\vamsikkrishna\PycharmProjects\pythonProject1\sample_synthetic\concat_dataframe_final.xlsx')
# # concat_dataframe= concat_dataframe.replace(np.nan,'')
# #
# # concat_dataframe['DataSet_GUID'] = 'SYN12345'
# # concat_dataframe['DataSet_Name'] = 'Test1234'
# #
# # column_names = concat_dataframe.columns.tolist()
# #
# # print(column_names)
# #
# #
# # num_rows = concat_dataframe.shape[0]
# # # User input for column names and values
# # # column_names = input("Enter column names (comma-separated): ").split(",")
# # # values = input("Enter corresponding values (comma-separated): ").split(",")
# #
# # # Build the insert query dynamically
# # table_name = "DataSet_Log"
# # #column_placeholders = ", ".join(["?"] * len(column_names))
# # column_placeholders = ", ".join(["?"] * 1)
# # #columns = ", ".join(column_names)
# #
# # columns = '[How Many Days Will The Deloitte Australia People Spend Overseas / Foreign Jurisdiction]'
# # #insert_query = f"""INSERT INTO {table_name} ({columns}) VALUES ({column_placeholders});"""
# #
# # insert_query = f"""INSERT INTO {table_name} ({columns}) VALUES ({column_placeholders})"""
# #
# # print(insert_query)
# # rows_as_lists = concat_dataframe.values.tolist()
# #
# # # Printing the result
# # for row in rows_as_lists:
# #     print(row)
# #
# # # User input for values
# # # values = []
# # # for _ in range(num_rows):
# # #     row_values = input("Enter values for this row (comma-separated): ").split(",")
# # #     values.append(row_values)
# #
# # # Insert multiple rows
# # #cursor.executemany(insert_query, rows_as_lists)
# # values = ['sdd070']
# # cursor.execute(insert_query, values)
# # conn.commit()
# # print("Records inserted successfully!")
# # # Close the connection and cursor
# # cursor.close()
# # conn.close()
# #
import datetime

import pandas as pd
import pyodbc
#
# # Connect to the Access database
# conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\Users\vamsikkrishna\Documents\Database1.accdb;'
# conn = pyodbc.connect(conn_str)
#
# table_name = "MF_Entity_Mapping"
# # SQL query to select data from the table
# select_query = f"SELECT * FROM {table_name}"
#
# # Retrieve data using pandas
# df = pd.read_sql_query(select_query, conn)
#
# # Close the connection
# conn.close()
#
# # Print the DataFrame
# print(df)
temp_df = pd.DataFrame([])



header = {

"ProcessAreaId": "ENG",

"DataSetName": "TEST_ENG",

"TargetSys": "SDD070",

"CountryKey": "US",

"NumOfRecords": 55,

"CreatedBy" :"T_WIP_USER"

}



header['DatasetUID'] = 'SYN'+'12345'


body_list = [

{

"FieldName": "PBUKR",

"FieldDesc": "Entity",

"FieldValue": [1100,1104]

},
{

"FieldName": "DR",

"FieldDesc": "DefaultRecords",

"FieldValue": 0

},
{

"FieldName": "PLFEZ",

"FieldDesc": "Start Date",

"FieldValue": "2023-06-09"

},
{

"FieldName": "PLSEZ",

"FieldDesc": "End Date",

"FieldValue": "2026-06-08"

},
{

"FieldName": "PST1",

"FieldDesc": "POST1",

"FieldValue": ""

},

{

"FieldName": "MANDT",

"FieldDesc": "MANDT",

"FieldValue": "30"

},
{

"FieldName": "ZZ_CLIENT",

"FieldDesc": "Client",

"FieldValue": [1100016,1100017]

},
{

"FieldName": "Email",

"FieldDesc": "Requester E-Mail",

"FieldValue": "vamsi.nilla@email.com"

},
{

"FieldName": "PRART",

"FieldDesc": "Engagement Type",

"FieldValue": 1

},
{

"FieldName": "Z3",

"FieldDesc": "Eng Partner",

"FieldValue": 3126673

},
{

"FieldName": "Z4",

"FieldDesc": "Eng Manager",

"FieldValue": 3154886

},
{

"FieldName": "L2_levels",

"FieldDesc": "NoofL2_levels",

"FieldValue": 1

},
{

"FieldName": "M",

"FieldDesc": "Material (L2)",

"FieldValue": "6110-1107"

},
{

"FieldName": "MO",

"FieldDesc": "Market Offering (L2)",

"FieldValue": "AE17063724A0956016"

}
]


print(body_list)



input_data_dictionary = dict()
for i in header.keys():
    input_data_dictionary[i] = header[i]
    #print(i)

for j in body_list:
    field_value = j['FieldValue']
    if field_value == '':
        field_value = 0
    input_data_dictionary[j['FieldDesc']] = field_value

print(input_data_dictionary, "input_data_dictionary")


















# temp= pd.DataFrame.from_records(body_list)
#
# current_datetime = datetime.datetime.now()
#
# # Extract only the date and format it as 'mm-dd-yyyy'
# formatted_date = current_datetime.strftime('%m-%d-%Y')
#
# # Print the formatted date
# print(formatted_date)
#
# temp['DatasetUID'] = header['DatasetUID']
# temp['TargetSys'] = header['TargetSys']
# temp['DataSetName'] = header['DataSetName']
# temp['ChangedOn'] = formatted_date
# print(pd.DataFrame.from_records(body_list))
# print(temp.to_json(orient ='records'))
#
# #temp.loc[temp['FieldValue'].apply(lambda x: isinstance(x, list) and len(x) > 0), 'FieldValue'] = temp.loc[temp['FieldValue'].apply(lambda x: isinstance(x, list) and len(x) > 0), 'FieldValue'].apply(lambda x: ','.join(str(str(x))))
# temp.loc[temp['FieldValue'].apply(lambda x: isinstance(x, list) and all(isinstance(item, int) for item in x)), 'FieldValue'] = temp.loc[temp['FieldValue'].apply(lambda x: isinstance(x, list) and all(isinstance(item, int) for item in x)), 'FieldValue'].apply(lambda x: ','.join(map(str, x)))

#temp.loc[temp['']]

#
# import pyodbc
# import pandas as pd
#
# # Connect to the Access database
# conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=path_to_your_database.accdb;'
# conn = pyodbc.connect(conn_str)
#
# # Sample DataFrame
# df = pd.DataFrame({
#     'Name': ['John', 'Jane', 'Mike'],
#     'Age': [30, 28, 32],
#     'Salary': [50000.0, 60000.0, 70000.0]
# })
#
# # Prepare the SQL INSERT query
# table_name = "YourTableName"
#
# # Define the SQL data types for each column
# data_types = {
#     'Name': 'TEXT',
#     'Age': 'INTEGER',
#     'Salary': 'DOUBLE'
# }
#
# # Generate the parameter placeholders
# param_placeholders = ','.join(['?'] * len(df.columns))
#
# # Construct the SQL INSERT statement
# insert_query = f"INSERT INTO {table_name} ({','.join(df.columns)}) VALUES ({param_placeholders})"
#
# # Create a cursor to execute SQL queries
# cursor = conn.cursor()
#
# # Insert DataFrame records into the Access database
# for index, row in df.iterrows():
#     values = [row[column] for column in df.columns]
#     cursor.execute(insert_query, values)
#
# # Commit the changes to the database
# conn.commit()
#
# # Close the connection and cursor
# cursor.close()
# conn.close()





#
# conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\Users\vamsikkrishna\Documents\Database1.accdb;'
# conn = pyodbc.connect(conn_str)
# crsr = conn.cursor()
#
# table_name = "DataSet_Input"
#
# col_values = ", ".join(temp.columns.tolist())
# col_placeholder = ', '.join(['? ']*len(temp.columns.tolist()))
#
# insert_query =f"INSERT INTO [{table_name}] ({col_values}) VALUES ({col_placeholder})"
#
# #print(temp[7])
#
# # for index, row in temp.iterrows():
# #     values = [row[column] for column in temp.columns]
# #     print(values)
# #     crsr.execute(insert_query, values)
#
# crsr.executemany(
#             f"INSERT INTO [{table_name}] ({col_values}) VALUES ({col_placeholder})",
#             temp.itertuples(index=False))
# conn.commit()
# crsr.close()
# conn.close()



#
# conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\Users\vamsikkrishna\Documents\Database1.accdb;'
# conn = pyodbc.connect(conn_str)
# #crsr = conn.cursor()
# DB_Status_dictionary = dict({1:'Draft',2:'generated_draft'})
# print(DB_Status_dictionary)
# table_name = "DataSet"
# DatasetUID = 'TEST_ENGSYN690372'
# condition_column = 'DataSet_GUID'
# status_id= ''
# select_query = f"SELECT Process_Area,DataSet_Name,DataSet_GUID, Status from {table_name} WHERE {condition_column} = '{DatasetUID}'"
#
# df = pd.read_sql_query(select_query, conn)
# conn.close()
# print(df)
# status_value = df['Status'].squeeze().strip()
# print(status_value)
# for key, value in DB_Status_dictionary.items():
#     #print(key,value)
#     print(f"{value}and {status_value}")
#     if value.strip() == status_value.strip():
#         status_id = key
# #key = list(filter(lambda x: DB_Status_dictionary[x] == df['Status'].values[0], DB_Status_dictionary))[0]
# df['statusID'] = status_id
# print(df)
#
# df.rename({"Process_Area":'processAreaId','DataSet_Name':'dataSetName','DataSet_GUID':'dataSetUID','Status':'statusDesc'},inplace=True)
#
# response = df.to_json(orient='records')
# response_new = response[1:-1]
# #print("outputSet :" ,response_new)





