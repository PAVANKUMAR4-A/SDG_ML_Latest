import pandas as pd

historic_df = pd.read_excel(r'C:\Users\vamsikkrishna\PycharmProjects\pythonProject1\HISTORIC_DATA\Bulk_API_Master_data.xlsx')
Prefix_values = historic_df['SORTL']
Prefix_values.drop_duplicates(inplace=True)