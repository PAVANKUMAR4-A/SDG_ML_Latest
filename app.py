import json
import requests
import pyodbc
import pandas as pd
from flask import Flask, request,render_template,send_file,Request,jsonify
from SDV_BULK_API_FILE.SDV_BULK_UPDATED_file_API import Bulk_Driver
from SDV_BULK_API_FILE.SDV_BULK_GET_Data_Display import Generated_data_display
from SDV_BULK_API_FILE.SDV_BULK_DB_DataSet_file import DB_Updates
from urllib.parse import urlencode
from datetime import datetime

app = Flask(__name__,template_folder='templates')

try:
    @app.route("/API/1.0/dataGenRequestSet", methods=['POST'])
    def data_gen_request_set():
        data = request.get_json()
        header_info = data["HeaderInfo"]
        input_set = data["InputSet"]
        Action = request.args.get('action')
        header_info['Action'] = Action
        print("action header info",header_info)
        #print(input_set)
        if Action.lower() =='Generate'.lower():
            bulk_object = Bulk_Driver()
            responselist = bulk_object.bulk_driver_method(header_info, input_set)
            dat_frame = responselist[0]
            input_dict = responselist[1]

            display_object = Generated_data_display()
            response_display = display_object.Screen_Data_display(dat_frame, input_dict)

            response_display.to_excel(
                r'D:\Users\pavankumar4\PycharmProjects\SDG_ML_Vamsi_1.3\sample_synthetic\output_file.xlsx', index=False)
            # print("second response")
            print("retun printed_display_data", response_display)
            rows = response_display.to_json(orient='records')
            return rows
        elif Action.lower() =='Draft'.lower():
            bulk_object = Bulk_Driver()
            responselist = bulk_object.bulk_driver_method(header_info, input_set)

            response = {'outputSet': responselist}
            #print('app',responselist)
            return response


    @app.route('/API/1.0/getGeoList', methods=['GET'])
    def get_GeoList():

        Database_object = DB_Updates()
        df = Database_object.Retrieve_dropdown()
        temp1_df = df['Field_values'].str.split(',', expand=True)
        temp2_df = temp1_df[0].str.split('@', expand=True)
        temp2_df.rename(columns={0: 'countryKey', 1: 'countryName'}, inplace=True)
        temp2_df = temp2_df.drop_duplicates().reset_index(drop=True)
        temp2_df_list = temp2_df.to_dict('records')
        response = {'geographySet': temp2_df_list}
        return response


    @app.route('/API/1.0/getEntityList', methods=['GET'])
    def get_EntityList():
        Database_object = DB_Updates()
        df = Database_object.Retrieve_dropdown()

        countryKey = request.args.get('countryKey')

        df = df[df['Field_values'].str.contains(countryKey)]

        temp1_df = df['Field_values'].str.split(',', expand=True)
        temp2_df = temp1_df[1].str.split('@', expand=True)
        temp2_df.rename(columns={0: 'entity', 1: 'entityName'}, inplace=True)
        temp2_df = temp2_df.drop_duplicates().reset_index(drop=True)
        temp2_df['countryKey'] = countryKey
        temp2_df_list = temp2_df.to_dict('records')
        response = {'entitySet': temp2_df_list}
        return response


    @app.route('/API/1.0/getClientList', methods=['GET'])
    def Get_ClientList():
        Database_object = DB_Updates()
        df = Database_object.Retrieve_dropdown()

        countryKey = request.args.get('countryKey')

        entity = request.args.get('entity')
        df = df[df['Field_values'].str.contains(countryKey)]
        df = df[df['Field_values'].str.contains(entity)]

        temp1_df = df['Field_values'].str.split(',', expand=True)
        temp2_df = temp1_df[2].str.split('@', expand=True)
        temp2_df.rename(columns={0: 'countryKey', 1: 'clientNum'}, inplace=True)
        temp2_df = temp2_df.drop_duplicates().reset_index(drop=True)
        temp2_df['countryKey'] = countryKey
        temp2_df_list = temp2_df.to_dict('records')
        response = {'clientSet': temp2_df_list}
        return response


    @app.route('/API/1.0/getEngmtTypeList', methods=['GET'])
    def Get_Engagement_typeList():
        Database_object = DB_Updates()
        df = Database_object.Retrieve_dropdown()
        countryKey = request.args.get('countryKey')
        print(countryKey)
        entity = request.args.get('entity')
        print(type(entity), entity)
        if '-' in entity:
            entity_list = entity.split('-')
            entity = "|".join(entity_list)

        df = df[df['Field_values'].str.contains(countryKey)]

        df = df[df['Field_values'].str.contains(str(entity))]

        temp1_df = df['Field_values'].str.split(',', expand=True)

        temp2_df = temp1_df[3].str.split('@', expand=True)

        temp2_df.rename(columns={0: 'engTypeIdNum', 1: 'projectType', 2: 'engTypeId', 3: 'engTypeName', 4: 'BEZEI'},
                        inplace=True)

        temp2_df = temp2_df.drop_duplicates().reset_index(drop=True)
        temp2_df_list = temp2_df.to_dict('records')

        response = {'engTypeSet': temp2_df_list}

        return response


    @app.route('/API/1.0/getPartnerNumbers', methods=['GET'])
    def Get_PartnerNum_List():
        Database_object = DB_Updates()
        df = Database_object.Retrieve_dropdown()
        partner_dict_map = {'Z3': 5, 'Z4': 4, 'Z7': 6, 'Z6': 7}
        countryKey = request.args.get('countryKey')
        print(countryKey)
        entity = request.args.get('entity')
        print(type(entity), entity)
        if '-' in entity:
            entity_list = entity.split('-')
            entity = "|".join(entity_list)
        partnerType = request.args.get('partnerType')
        print(type(partnerType), partnerType)
        df = df[df['Field_values'].str.contains(countryKey)]
        print("country", df)
        df = df[df['Field_values'].str.contains(str(entity))]
        print("entity", df)
        df = df[df['Field_values'].str.contains(str(partnerType))]
        print("partnerType", df)
        temp1_df = df['Field_values'].str.split(',', expand=True)
        print('temp1', temp1_df)
        temp2_df = temp1_df[partner_dict_map[partnerType]].str.split('@', expand=True)
        print('temp2before', temp2_df)
        temp2_df.rename(columns={0: 'partnerTypeId', 1: 'partnerTypeIdDes', 2: 'partnerNum', 3: 'partnerName'},
                        inplace=True)
        print('temp2after', temp2_df)
        temp2_df = temp2_df.drop_duplicates().reset_index(drop=True)
        temp2_df_list = temp2_df.to_dict('records')
        print(temp2_df_list)
        response = {'partnerNumSet': temp2_df_list}
        print("response", response)
        return response


    @app.route('/API/1.0/getPartnerTypeList', methods=['GET'])
    def Get_PartnerType_List():
        Database_object = DB_Updates()
        df = Database_object.Retrieve_dropdown()

        temp1_df = df['Field_values'].str.split(',', expand=True)

        total_columns = len(temp1_df.columns)

        df = pd.DataFrame()
        for i in range(total_columns):
            if i >= 4:
                data = temp1_df[i].str.split('@', expand=True)

                df = pd.concat([df, data], axis=0)

        df.rename(columns={0: 'partnerTypeId', 1: 'partnerTypeName', 2: 'ZZ_CLIENT', 3: 'partnerName'}, inplace=True)
        df.drop(['ZZ_CLIENT', 'partnerName'], axis=1, inplace=True)

        # Remove duplicate entries from the DataFrame
        df_unique = df.drop_duplicates().reset_index(drop=True)

        output_dict = df_unique.to_dict('records')

        final_dict = {"partnerTypeSet": output_dict}

        return final_dict


    @app.route("/API/1.0/getUserInputQuesList", methods=['GET'])
    def get_User_Input_QuesList():
        conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=D:\Users\pavankumar4\Documents\Database1.accdb;'

        connection = pyodbc.connect(conn_str)
        cursor = connection.cursor()

        process_area = request.args.get('ProcessArea')


        # Retrieve data from the Access database
        cursor.execute("SELECT * FROM UserInputQuestions2",)
        rows = cursor.fetchall()
        connection.close()

        # Format data as JSON
        user_input_ques_list = []

        params = {
            'CountryKey': 'str',
            'entity': 'int'

        }
        for row in rows:

            if row.APIRequest is not None:
                user_input_ques_list.append({
                "fieldName": row.FieldName,
                "quesType": row.Question_Type,
                "sequence": int(row.Sequence),
                # "Dependent sequence": row.DependentSeq,
                "question": row.Question,
                #"dataType": row.Datatype,
                "value": row.FieldValue,
                "apiRequest":f"http://127.0.0.1:5000/API/1.0/{row.APIRequest}?{urlencode(params)}",

                "type": row.inputType

                })
            else:
                continue

        return jsonify({"userInputQues": user_input_ques_list})


    @app.route("/API/1.0/getProcLastDataSetsList", methods=['GET'])
    def Get_ProcLastDataSets_List():

        process_area = request.args.get('processArea')


        from_Date = datetime.strptime(request.args.get('dateFrom'),'%m%d%Y').date()
        to_Date = datetime.strptime(request.args.get('dateTo'),'%m%d%Y').date()


        conn_string = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=D:\Users\pavankumar4\Documents\Database1.accdb;'
        conn = pyodbc.connect(conn_string)

        # cursor = conn.cursor()
        sql_query = "SELECT *, Created_On FROM Dataset"

        # Execute the SQL query and fetch the results
        df = pd.read_sql_query(sql_query, conn)

        conn.close()

        # Extract date from 'Created_On' column and create a new column with formatted date
        df['Formatted_Date'] = pd.to_datetime(df['Created_On']).dt.strftime('%m%d%Y')


        filtered_df = df[(df['Formatted_Date'] >= from_Date.strftime('%m%d%Y')) &
                         (df['Formatted_Date'] <= to_Date.strftime('%m%d%Y'))]

        filtered_df.rename(columns={"DataSet_GUID":"id"}, inplace=True)
        filtered_df.drop(['DataSet_Table', 'Changed_By','Changed_On','Formatted_Date','Target_system'], axis=1, inplace=True)


        output_dict = filtered_df.to_dict('records')


        final_dict = {"lastDataSets": output_dict}

        return final_dict


    @app.route("/API/1.0/getTargetSys", methods=['GET'])
    def Get_TargetSys():
        conn_string = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=D:\Users\pavankumar4\Documents\Database1.accdb;'
        conn = pyodbc.connect(conn_string)

        cursor = conn.cursor()

        cursor.execute('SELECT * FROM Target_Sys')

        rows = cursor.fetchall()
        conn.close()
        target_sys = [{'key': row.SDGHost, 'value': row.Targetsys} for row in rows]


        json_data = {
            'targetSys': target_sys
        }

        return (json_data)



    @app.route('/API/1.0/download_excel')
    def download_excel():
        excel_file_path = r'sample_synthetic/output_file.xlsx'
        return send_file(excel_file_path, as_attachment=True)


    @app.route('/API/1.0/display_data')
    def display_data():
        df = pd.read_excel(r'C:\Users\vamsikkrishna\PycharmProjects\pythonProject1\sample_synthetic\output_file.xlsx')
        rows = df.to_dict('records')

        # Get the column names
        columns = df.columns.tolist()

        return render_template(r'table.html', columns=columns, rows=rows)


    @app.route('/API/1.0/')
    def index():
        return render_template(r'index.html')

except Exception as e:
    print('Flask API call', e)


if __name__ == '__main__':
    app.run(host='0.0.0.0',port=5000, debug=True)