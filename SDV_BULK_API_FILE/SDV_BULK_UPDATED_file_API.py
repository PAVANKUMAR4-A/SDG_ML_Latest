from SDV_BULK_API_FILE import BULK_Pickle_file_config_file
from SDV_BULK_API_FILE.SDV_BULK_Autogen_Class_file import Autogen_variable_data_gen
from SDV_BULK_API_FILE.SDV_Master_data_gen_updated import Master_data_gen
from SDV_BULK_API_FILE.SDV_BULK_Default_data_gen import Default_data_gen
from SDV_BULK_API_FILE.SDV_BULK_Backend_data_gen import Backend_data_gen
from SDV_BULK_API_FILE.SDV_BULK_Text_data_gen import Text_data_gen
from SDV_BULK_API_FILE.SDV_BULK_Autogen_Class_Rule_dep_file import Autogen_variable_rule_dep_data_gen
from SDV_BULK_API_FILE.SDV_BULK_GET_Data_Display import Generated_data_display
from SDV_BULK_API_FILE.SDV_BULK_DB_DataSet_file import DB_Updates
import pandas as pd
import numpy as np
import datetime
import json
import random
import datetime as datetime
import time

class Bulk_Driver():

    def bulk_driver_method(self,header,body_list):

        print("entered bulk driver method")

        print(header,"header")
        print(body_list,"body")
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

        DB_Updates_object = DB_Updates()

        input_data_dictionary['DatasetUID'] = DB_Updates_object.Generate_DatasetUID(input_data_dictionary)
        header['DatasetUID'] = input_data_dictionary['DatasetUID']
        DB_Updates_object.Insert_Dataset(input_data_dictionary)
        DB_Status_dictionary = DB_Updates_object.Get_DB_Status_values()
        print('data',input_data_dictionary)
        json_ui = json.dumps(input_data_dictionary)
        Input_Draft_Data_return_value = DB_Updates_object.Input_Draft_Data(header,body_list)





        if input_data_dictionary['Action'].lower()=='Draft'.lower():
            print("Status is Draft")
            status = DB_Status_dictionary[1]
            DB_Updates_object.Update_DataSet(input_data_dictionary, status)
            draft_value_response = DB_Updates_object.Draft_Values_response(header,DB_Status_dictionary)
            print("bulk",draft_value_response)
            return draft_value_response




        elif ((int(input_data_dictionary['NumOfRecords']) != int(0)) and(input_data_dictionary['Action'].lower()=='Generate'.lower())):

            start = time.time()
            concat_dataframe = pd.DataFrame([])

            try:
                pickle_config = BULK_Pickle_file_config_file.Pickle_file_configuration()
                config_file = pickle_config.call_configuration_table(input_data_dictionary['ProcessAreaId'])

                print(config_file,"config file")

                for key, value in config_file.items():
                    if key.lower() == 'Historic_data_locations'.lower():
                        Historic_data_locations = value.loc[:, :]
                        print(Historic_data_locations,"historic data")
                    elif key.lower() == 'SDV_BULK'.lower():
                        sdv_metadata = value.loc[:, :]
                        # print(sdv_metadata)
                    elif key.lower() == 'Bulk_Data_dependency'.lower():
                        Data_dependency_df = value.loc[:, :]
                        # print(Data_dependency_df)
                    elif key.lower() == 'BULK_Pickle_file_locations'.lower():
                        Pickle_file_locations = value.loc[:, :]
                        # print(Pickle_file_locations)

                Display_data = sdv_metadata[sdv_metadata['Data_Display'] == 'Y']['Template_field_name']
                Display_data_columns = Display_data.tolist()
                print(Display_data_columns)

                status = DB_Status_dictionary[2]
                DB_Updates_object.Update_DataSet(input_data_dictionary,status)

                sdv_metadata_Rule_indicator_list = sdv_metadata['Rule_indicator'].unique().tolist()
                #sdv_metadata_Rule_indicator_list.sort()
                print(sdv_metadata_Rule_indicator_list,"rule list")
                RL_dataframe = pd.DataFrame([])
                sdv_metadata_Rule_indicator_list = ['RL', 'MD', 'DV', 'BD', 'TXT', 'RLDP']  # ,'MD','DV','BD','TXT
                for rule in sdv_metadata_Rule_indicator_list:
                    print(rule,"rule text")
                    if rule.lower() == 'MD'.lower():
                        # print("Masterdata_values")
                        ENG_MD_df = sdv_metadata.loc[sdv_metadata['Rule_indicator'] == 'MD',]
                        function_name = ENG_MD_df['Function'].unique().tolist()[0]
                        parameter_value = ENG_MD_df['Parameter'].unique().tolist()[0]
                        Pikcle_file_description = ENG_MD_df['Pikcle_file_description'].unique().tolist()[0]
                        Data_dependency = Data_dependency_df
                        # print(function_name)
                        class_instance = Master_data_gen()
                        method_value = getattr(class_instance, function_name)
                        args = (ENG_MD_df, parameter_value, json_ui, concat_dataframe, Historic_data_locations,
                                Pickle_file_locations, Data_dependency, Pikcle_file_description)
                        result_dataframe = method_value(*args)
                        concat_dataframe = pd.concat([concat_dataframe, result_dataframe], axis=1)
                        # print(concat_dataframe)

                    elif rule.lower() == 'DV'.lower():
                        # print("Defaultdata_values")
                        ENG_DV_df = sdv_metadata.loc[sdv_metadata['Rule_indicator'] == 'DV',]
                        # print(ENG_DV_df[['Field_name','Function']])
                        # function_name = 'Defaultdata_transformations'#ENG_DV_df['Function'].unique().tolist()
                        function_name = ENG_DV_df['Function'].unique().tolist()[0]
                        # print('function_name',function_name)
                        parameter_value = ENG_DV_df['Parameter'].unique().tolist()[0]
                        # print(parameter_value)
                        Pikcle_file_description = ENG_DV_df['Pikcle_file_description'].unique().tolist()[0]
                        Data_dependency = Data_dependency_df
                        # print(function_name)
                        class_instance = Default_data_gen()
                        method_value = getattr(class_instance, function_name)
                        args = (ENG_DV_df, parameter_value, json_ui, concat_dataframe, Historic_data_locations,
                                Pickle_file_locations, Data_dependency, Pikcle_file_description)
                        result_dataframe = method_value(*args)
                        concat_dataframe = pd.concat([concat_dataframe, result_dataframe], axis=1)
                        # print(concat_dataframe)

                    elif rule.lower() == 'BD'.lower():
                        # print("Backend_deriveddata_values")
                        ENG_BD_df = sdv_metadata.loc[sdv_metadata['Rule_indicator'] == 'BD',]
                        # print(ENG_DV_df[['Field_name','Function']])
                        # function_name = 'Defaultdata_transformations'#ENG_DV_df['Function'].unique().tolist()
                        function_name = ENG_BD_df['Function'].unique().tolist()[0]
                        # print('function_name',function_name)
                        parameter_value = ENG_BD_df['Parameter'].unique().tolist()[0]
                        # print(parameter_value)
                        Pikcle_file_description = ENG_BD_df['Pikcle_file_description'].unique().tolist()[0]
                        Data_dependency = Data_dependency_df
                        # print(function_name)
                        class_instance = Backend_data_gen()
                        method_value = getattr(class_instance, function_name)
                        args = (ENG_BD_df, parameter_value, json_ui, concat_dataframe, Historic_data_locations,
                                Pickle_file_locations, Data_dependency, Pikcle_file_description)
                        result_dataframe = method_value(*args)
                        concat_dataframe = pd.concat([concat_dataframe, result_dataframe], axis=1)
                        # print(concat_dataframe)
                    elif rule.lower() == 'TXT'.lower():
                        # print("TXT_data_values")
                        ENG_TXT_df = sdv_metadata.loc[sdv_metadata['Rule_indicator'] == 'TXT',]
                        # print(ENG_DV_df[['Field_name','Function']])
                        # function_name = 'Defaultdata_transformations'#ENG_DV_df['Function'].unique().tolist()
                        function_name = ENG_TXT_df['Function'].unique().tolist()[0]
                        # print('function_name',function_name)
                        parameter_value = ENG_TXT_df['Parameter'].unique().tolist()[0]
                        # print("parameter_valuebefore",parameter_value)
                        Pikcle_file_description = ENG_TXT_df['Pikcle_file_description'].unique().tolist()[0]
                        Data_dependency = Data_dependency_df
                        # print(function_name)
                        class_instance = Text_data_gen()
                        method_value = getattr(class_instance, function_name)
                        args = (ENG_TXT_df, parameter_value, json_ui, concat_dataframe, Historic_data_locations,
                                Pickle_file_locations, Data_dependency, Pikcle_file_description)
                        result_dataframe = method_value(*args)
                        concat_dataframe = pd.concat([concat_dataframe, result_dataframe], axis=1)
                        # print(concat_dataframe)




                    elif rule.lower() == 'RL'.lower():
                        ENG_RL_df = sdv_metadata.loc[sdv_metadata['Rule_indicator'] == 'RL',]
                        # print(ENG_RL_df)

                        for index, row in ENG_RL_df.iterrows():
                            class_instance = Autogen_variable_data_gen()
                            function_name = row['Function']
                            parameter_value = row['Parameter']
                            Data_dependency = row['Data_dependency']

                            column_name = row['Template_field_name']
                            # print("column_name reached",column_name)
                            # print(function_name)
                            method_value = getattr(class_instance, function_name)
                            args = (column_name, parameter_value, Data_dependency, json_ui, concat_dataframe)
                            result_dataframe = method_value(*args)
                            concat_dataframe = pd.concat([concat_dataframe, result_dataframe], axis=1)

                            # print(concat_dataframe)


                    elif rule.lower() == 'RLDP'.lower():
                        ENG_RLDP_df = sdv_metadata.loc[sdv_metadata['Rule_indicator'] == 'RLDP',]
                        # print(ENG_RL_df)
                        function_name = ENG_RLDP_df['Function'].unique().tolist()[0]
                        parameter_value = ENG_RLDP_df['Parameter'].unique().tolist()[0]
                        # Pikcle_file_description = ENG_MD_df['Pikcle_file_description'].unique().tolist()[0]
                        Data_dependency = Data_dependency_df
                        # print(function_name)
                        class_instance = Autogen_variable_rule_dep_data_gen()
                        method_value = getattr(class_instance, function_name)
                        args = (
                            ENG_RLDP_df, parameter_value, json_ui, concat_dataframe, Historic_data_locations,
                            Data_dependency_df)
                        result_dataframe = method_value(*args)
                        concat_dataframe = result_dataframe
                        #print(result_dataframe)


                concat_dataframe.reset_index(drop=True, inplace=True)


                end = time.time()
                print("The time of execution of above program is :",
                      (end - start) * 10 ** 3, "ms")
                concat_dataframe.to_excel(
                    r'D:\Users\pavankumar4\PycharmProjects\SDG_ML_Vamsi_1.3\sample_synthetic\concat_dataframe_final.xlsx',index=False)

                DB_Updates_object.Update_DataSet_Log(input_data_dictionary,concat_dataframe)

                status = DB_Status_dictionary[3]
                DB_Updates_object.Update_DataSet(input_data_dictionary,status)



            except Exception as e:
                print(e)
                status = DB_Status_dictionary[4]
                DB_Updates_object.Update_DataSet(input_data_dictionary, status)
            print("returning statement bulk")


        return[concat_dataframe,input_data_dictionary]
