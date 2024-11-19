from fastapi import APIRouter, HTTPException, Query
import os
import json 
from typing import List
import pandas as pd
import duckdb
from app.handler.budget_expense import save_json

router = APIRouter()

@router.get('/budget_expense')
def get_compare_data(उपशीर्षक: List[str]= Query(...), cities: List[str] = Query(None), months:List[str] = Query(None)):

    var={}

    combined_data=[]

    if not cities:
        cities=['lekbeshi','birgunj','janakpur','tulsipur','shuddhodhan']

    if not months:
        months=['baisakh','jestha','asar','shrawan','bhadra','asoj','kartik','mangsir','poush','magh','falgun','chaitra']

    for city in cities:
        for month in months:
            # Construct the path to the JSON file
            directory_path=f"./data/{city}/budget_expense/{month}"
            file_path = f"./data/{city}/budget_expense/{month}/data.json"
            
            # Ensure the directory exists
            if not os.path.exists(directory_path):
                # print('error')
                continue

            # # Check if the file exists
            if not os.path.exists(file_path):
                save_json(directory_path=directory_path,city=city,month=month)

            with open(f'./data/{city}/budget_expense/{month}/data.json','r') as f:
                json_data=json.load(f)

            # print(json_data)
            # Create a DataFrame for the city and month
            df_key = f"{city}_{month}"
            var[df_key] = pd.DataFrame(json_data['data'])

            # Register the DataFrame in DuckDB
            duckdb.register(df_key, var[df_key])

            combined_data.append(\
                    duckdb.query(f'''
                    select
                        *,
                        '{city}' as city,
                        '{month}' as month
                    from {df_key}
                ''').to_df()\
            )

    # Concatenate all query results into a single DataFrame
    final_df = pd.concat(combined_data, ignore_index=True)
    final_df['बजेट उपशीर्षक संकेत'] = final_df['बजेट उपशीर्षक संकेत'].fillna('')
    final_df['बजेट उपशीर्षक नाम'] = final_df['बजेट उपशीर्षक नाम'].fillna('')
    final_df['खर्च पूंजीगत'] = final_df['खर्च पूंजीगत'].fillna(0)  # If numeric, fill with 0

    if उपशीर्षक[0]=='total':
        # जम्मा
        # print(final_df[final_df['क्र.सं.']=='जम्मा'])
        return final_df[final_df['क्र.सं.']=='जम्मा'].to_dict(orient="records")
        # pass
    elif उपशीर्षक[0]=='all':
        return final_df.to_dict(orient="records")
        # return final_df[final_df['क्र.सं.']!='जम्मा'].to_dict(orient="records")
    else:
        # उपशीर्षक.append('')
        return final_df[final_df['बजेट उपशीर्षक नाम'].isin(उपशीर्षक)].to_dict(orient="records")
    
# @router.get("/budget_expense/{city}")
# def get_budget_expense_data(city:str, month: Optional[str] = None):
#     directory_root="./data/"+city+"/budget_expense/"
#     if month is not None:
#         directory_path=directory_root+month
#         json_path = directory_path+"/data.json"

#         if os.path.exists(json_path):
#             print("JSON File Already Exists! No need further processing")
#             # Open the JSON file and load its content
#             with open(json_path, "r", encoding="utf-8") as json_file:
#                 data = json.load(json_file)
            
#             return {
#                 "status":200,
#                 "idms":city,
#                 "month":month,
#                 "topic": "budget_expense",
#                 "data": data
#             }
        
#         else:
#             print("No json file found, have to process entire excel file")
#             # Check if the file exists
#             if not os.path.exists(directory_path+'/data.xlsx'):
#                 raise HTTPException(status_code=404, detail="Excel file not found")
            
#             try:
#                 excel_path=directory_path+'/data.xlsx'
#                 json_data=budgetexpense_to_json(city=city, month=month, excel_path=excel_path)

#                 return {
#                     "status": 200,
#                     "month":month,
#                     "idms":city,
#                     "topic": "budget_expense",
#                     "data": json_data
#                 }

#             except Exception as e:
#                 raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
        
#     else:
#         # Return data for all months
#         all_data = []
#         try:
#             # List all subdirectories in the root directory (representing months)
#             for month_folder in os.listdir(directory_root):
#                 directory_path = os.path.join(directory_root, month_folder)
#                 json_path = os.path.join(directory_path, "data.json")
                
#                 if os.path.exists(json_path):
#                     # Load JSON data for each month
#                     with open(json_path, "r", encoding="utf-8") as json_file:
#                         month_data = json.load(json_file)
#                     all_data.append({
#                         "month": month_folder,
#                         "data": month_data
#                     })
#                 else:
#                     # If no JSON exists, attempt to process the Excel file
#                     excel_path = os.path.join(directory_path, "data.xlsx")
#                     if os.path.exists(excel_path):
#                         json_data = budgetexpense_to_json(city=city, month=month_folder, excel_path=excel_path)
#                         all_data.append({
#                             "month": month_folder,
#                             "data": json_data
#                         })
#                     else:
#                         print(f"No data found for {month_folder}")
            
#             return {
#                 "status": 200,
#                 "idms":city,
#                 "topic": "budget_expense",
#                 "data": all_data
#             }
        
#         except Exception as e:
#             raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")

# @router.get("/budget_expense/compare")
# def get_data():
#     """
#     Endpoint to get budget expense data for all cities and months.
#     """
#     directory_root = "./data/"  # Root directory where cities are stored
#     all_cities_data = []
    
#     try:
#         # Iterate over all city directories in the root directory
#         for city_folder in os.listdir(directory_root):
#             city_path = os.path.join(directory_root, city_folder, "budget_expense")
            
#             if os.path.isdir(city_path):  # Ensure it's a directory
#                 city_data = {"idms": city_folder, "months": []}
                
#                 # Iterate over all month directories in the city's budget_expense folder
#                 for month_folder in os.listdir(city_path):
#                     month_path = os.path.join(city_path, month_folder)
#                     json_path = os.path.join(month_path, "data.json")
                    
#                     if os.path.exists(json_path):
#                         # Load JSON data for each month
#                         with open(json_path, "r", encoding="utf-8") as json_file:
#                             month_data = json.load(json_file)
#                         city_data["months"].append({
#                             "month": month_folder,
#                             "data": month_data
#                         })
#                     else:
#                         # If no JSON exists, attempt to process the Excel file
#                         excel_path = os.path.join(month_path, "data.xlsx")
#                         if os.path.exists(excel_path):
#                             json_data = budgetexpense_to_json(city=city_folder, month=month_folder, excel_path=excel_path)
#                             city_data["months"].append({
#                                 "month": month_folder,
#                                 "data": json_data
#                             })
#                         else:
#                             print(f"No data found for {month_folder} in {city_folder}")
                
#                 all_cities_data.append(city_data)
        
#         return {
#             "status": 200,
#             "topic": "budget_expense",
#             "data": all_cities_data
#         }

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")
        