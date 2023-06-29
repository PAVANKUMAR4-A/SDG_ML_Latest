import  pandas as pd
import numpy as np

Bulk_Master_data = pd.read_excel(r'C:\Users\vamsikkrishna\PycharmProjects\pythonProject1\DB_FILES\Dropdown_parnr.xlsx')
print(Bulk_Master_data)

k= ['LAND1','LANDX','VBUKR','BUTXT','ZZ_CLIENT','ENG_ID','DESCRPTION','CONTRACT_TYPE','BEZEI']

extracted_columns =Bulk_Master_data[k]
Bulk_Master_data['PARVW-VTEXT'] = Bulk_Master_data['PARVW'].astype(str)+'-'+ Bulk_Master_data['VTEXT'].astype(str)
parnr_columns = ['POSID','PARVW-VTEXT','PARNR']
parnr_df = Bulk_Master_data[parnr_columns]
del_columns = ['PARVW','PARNR','PARVW-VTEXT','VTEXT']
Bulk_Master_data.drop(del_columns,inplace=True,axis=1)
unique_posid = parnr_df['POSID'].unique().tolist()
pivot_table = pd.DataFrame([])
for i in unique_posid:
    df_temp= parnr_df.loc[parnr_df['POSID']==i,]
    table = pd.pivot_table(df_temp,values='PARNR',index=['POSID'],columns= ['PARVW-VTEXT'],aggfunc='first').reset_index()
    pivot_table = pd.concat([pivot_table,table],axis=0)

parvw_columns =pivot_table.columns.tolist()
Bulk_Master_data_new = Bulk_Master_data.merge(pivot_table,how = 'inner',left_on ='POSID',right_on = 'POSID' ).reset_index()
k.extend(parvw_columns)

Bulk_Master_data_new.to_excel(r'C:\Users\vamsikkrishna\PycharmProjects\pythonProject1\DB_FILES\Bulk_Master_data_new.xlsx')



col_values  = ['geography','Entity','Client','Engagement_type','Z4@Engagement Manager','Z3@Engagement Partner','Z7@Billing Manager','Z6@Billing Partner']

# df_all_filtered['ColumnA'] = df_all_filtered[df_all_filtered.columns[:]].apply(
#     lambda x: ','.join(x.dropna().astype(str)),
#     axis=1
# )

