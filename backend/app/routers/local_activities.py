import glob
from typing import List
from fastapi import APIRouter, HTTPException, Query
import os
import json 
import duckdb
from app.handler.local_activities import save_json
import pandas as pd

router = APIRouter()

@router.get('/local_activities')
def get_compare_data(वर्ग :List[str]= Query(...), कार्यक्रम: List[str]= Query(None), cities: List[str] = Query(None), years:List[str] = Query(None)):

    var={}

    combined_data=[]

    if not cities:
        cities=['lekbeshi','birgunj','janakpur','tulsipur','shuddhodhan']

    if not years:
        years=['2080-81','2081-82','2082-83','2083-84']

    for city in cities:
        for year in years:
            # Construct the path to the JSON file
            directory_path=f"./data/{city}/local_activities/{year}"
            file_path = f"./data/{city}/local_activities/{year}/data.json"
            
            # Ensure the directory exists
            if not os.path.exists(directory_path):
                # print('error')
                continue

            # # Check if the file exists
            if not os.path.exists(file_path):
                save_json(directory_path=directory_path,city=city,year=year)

            with open(f'./data/{city}/local_activities/{year}/data.json','r') as f:
                json_data=json.load(f)

            # print(json_data)
            # Create a DataFrame for the city and year
            df_key = f"{city}_{year.replace('-','_')}"
            var[df_key] = pd.DataFrame(json_data['data'])

            # Register the DataFrame in DuckDB
            duckdb.register(df_key, var[df_key])

            combined_data.append(\
                    duckdb.query(f'''
                    select
                        *,
                        '{city}' as city,
                        '{year}' as year
                    from {df_key}
                ''').to_df()\
            )

    # Concatenate all query results into a single DataFrame
    final_df = pd.concat(combined_data, ignore_index=True)
    final_df[' मुख्य कार्यक्रम/मुख्य क्रियाकलाप'] = final_df[' मुख्य कार्यक्रम/मुख्य क्रियाकलाप'].fillna('')
    # final_df['बजेट उपशीर्षक नाम'] = final_df['बजेट उपशीर्षक नाम'].fillna('')
    # final_df['खर्च पूंजीगत'] = final_df['खर्च पूंजीगत'].fillna(0)  # If numeric, fill with 0
    if वर्ग[0]=='total':
        return final_df[final_df['क्र.सं.']=='कुल जम्मा'].to_dict(orient="records")
    elif वर्ग[0]=='all':
        # final_df.to_csv('./local_activites.csv',index=False)
        return final_df.to_dict(orient="records")
        # return final_df[final_df['क्र.सं.']!='जम्मा'].to_dict(orient="records")
    else:
        # उपशीर्षक.append('')
        return final_df[final_df['वर्ग'].isin(वर्ग)].to_dict(orient="records")

# @router.get("/local_activities")
# def get_all_local_activities_data():
#     """
#     Endpoint to get quadrimester expense data for all cities and all years.
#     """
#     directory_root = "./data/"  # Root directory where cities are stored
#     all_cities_data = []
#     try:
#         # Iterate over all city directories in the root directory
#         for city_folder in os.listdir(directory_root):
#             city_path = os.path.join(directory_root, city_folder, "local_activities")
            
#             if os.path.isdir(city_path):  # Ensure it's a directory
#                 city_data = {"idms": city_folder, "years": []}
                
#                 # Iterate over all year directories in the city's local_activities folder
#                 for year_folder in os.listdir(city_path):
#                     year_path = os.path.join(city_path, year_folder)
#                     json_path = os.path.join(year_path, "data.json")
                    
#                     if os.path.exists(json_path):
#                         # Load JSON data for each year
#                         with open(json_path, "r", encoding="utf-8") as json_file:
#                             year_data = json.load(json_file)
#                         city_data["years"].append({
#                             "year": year_folder,
#                             "data": year_data
#                         })
#                     else:
#                         # If no JSON exists, attempt to process the Excel file
#                         xlsx_files = glob.glob(os.path.join(year_path, "*.xlsx"))
#                         if xlsx_files:
#                             excel_path = xlsx_files[0]
#                             json_data = local_activities_to_json(city=city_folder, year=year_folder, excel_path=excel_path)
#                             if json_data:
#                                 city_data["years"].append({
#                                     "year": year_folder,
#                                     "data": json_data
#                                 })
#                             else:
#                                 print(f"Failed to process Excel data for {year_folder} in {city_folder}")
#                         else:
#                             print(f"No data found for {year_folder} in {city_folder}")
                
#                 all_cities_data.append(city_data)
        
#         return {
#             "status": 200,
#             "topic": "local_activities",
#             "data": all_cities_data
#         }

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")

# @router.get("/local_activities/{city}")
# def get_local_activities_data(city: str, year: Optional[str] = None):
#     directory_root = f"./data/{city}/local_activities/"
#     if year:
#         directory_path = f"{directory_root}{year}"
#         json_path = f"{directory_path}/data.json"

#         if os.path.exists(json_path):
#             print("JSON File Already Exists! No need for further processing.")
#             with open(json_path, "r", encoding="utf-8") as json_file:
#                 data = json.load(json_file)
#             return {"status": 200, "idms": city, "year": year, "topic": "local_activities", "data": data}
#         else:
#             print("No JSON file found, processing the Excel file.")
#             xlsx_files = glob.glob(os.path.join(directory_path, "*.xlsx"))
#             if xlsx_files:
#                 excel_path = xlsx_files[0]
#                 try:
#                     json_data = local_activities_to_json(city=city, year=year, excel_path=excel_path)
#                     return {"status": 200, "idms": city, "year": year, "topic": "local_activities", "data": json_data}
#                 except Exception as e:
#                     raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
#             else:
#                 raise HTTPException(status_code=404, detail="Excel file not found")
#     else:
#         # Process data for all years
#         all_data = []
#         if not os.path.exists(directory_root):
#             raise HTTPException(status_code=404, detail="City directory not found")
#         try:
#             year_folders = [folder for folder in os.listdir(directory_root) if os.path.isdir(os.path.join(directory_root, folder))]
#             for year_folder in year_folders:
#                 json_path = os.path.join(directory_root, year_folder, "data.json")
#                 if os.path.exists(json_path):
#                     with open(json_path, "r", encoding="utf-8") as json_file:
#                         year_data = json.load(json_file)
#                     all_data.append({"year": year_folder, "data": year_data})
#                 else:
#                     print(f"No JSON data found for {year_folder}, attempting to process Excel file.")
#                     xlsx_files = glob.glob(os.path.join(directory_root, year_folder, "*.xlsx"))
#                     if xlsx_files:
#                         excel_path = xlsx_files[0]
#                         try:
#                             json_data = local_activities_to_json(city=city, year=year_folder, excel_path=excel_path)
#                             all_data.append({"year": year_folder, "data": json_data})
#                         except Exception as e:
#                             print(f"Failed to process Excel data for {year_folder}: {str(e)}")
#                     else:
#                         print(f"No Excel data found for {year_folder}")
#             return {"status": 200, "idms": city, "topic": "local_activities", "data": all_data}
#         except Exception as e:
#             raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")


# @router.get("/local_activities/{city}")
# def get_local_activities_data(city: str, year: Optional[str] = None):
#     directory_root = f"./data/{city}/local_activities/"
#     if year:
#         directory_path = f"{directory_root}{year}"
#         json_path = f"{directory_path}/data.json"

#         if os.path.exists(json_path):
#             print("JSON File Already Exists! No need for further processing.")
#             with open(json_path, "r", encoding="utf-8") as json_file:
#                 data = json.load(json_file)
#             return {"status": 200, "idms": city, "year": year, "topic": "local_activities", "data": data}
#         else:
#             print("No JSON file found, processing the Excel file.")
#             xlsx_files = glob.glob(os.path.join(directory_path, "*.xlsx"))
#             if not xlsx_files:
#                 raise HTTPException(status_code=404, detail="Excel file not found")
#             excel_path = xlsx_files[0]
#             try:
#                 json_data = local_activities_to_json(city=city, year=year, excel_path=excel_path)
#                 return {"status": 200, "idms": city, "year": year, "topic": "local_activities", "data": json.loads(json_data)}
#             except Exception as e:
#                 raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
#     else:
#         # Process data for all years
#         all_data = []
#         if not os.path.exists(directory_root):
#             raise HTTPException(status_code=404, detail="City directory not found")
#         try:
#             year_folders = [folder for folder in os.listdir(directory_root) if os.path.isdir(os.path.join(directory_root, folder))]
#             for year_folder in year_folders:
#                 json_path = os.path.join(directory_root, year_folder, "data.json")
#                 if os.path.exists(json_path):
#                     with open(json_path, "r", encoding="utf-8") as json_file:
#                         year_data = json.load(json_file)
#                     all_data.append({"year": year_folder, "data": year_data})
#                 else:
#                     print(f"No data found for {year_folder}")
#             return {"status": 200, "idms": city, "topic": "local_activities", "data": all_data}
#         except Exception as e:
#             raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")


# @router.get("/local_activities/{city}")
# def get_local_activities_data(city:str, year: Optional[str] = None):
#     directory_root="./data/"+city+"/local_activities/"
#     if year is not None:
#         directory_path=directory_root+year
#         json_path = directory_path+"/data.json"

#         if os.path.exists(json_path):
#             print("JSON File Already Exists! No need further processing")
#             # Open the JSON file and load its content
#             with open(json_path, "r", encoding="utf-8") as json_file:
#                 data = json.load(json_file)
            
#             return {
#                 "status":200,
#                 "idms":city,
#                 "year":year,
#                 "topic":"local_activities",
#                 "data": data
#             }
        
#         else:
#             print("No json file found, have to process entire excel file")

#             # Check if the file exists
#             # Find all .xlsx files in the given directory
#             xlsx_files = glob.glob(os.path.join(directory_root+year, "*.xlsx"))
#             # print(xlsx_files)
#             if xlsx_files:
#                 excel_path=xlsx_files[0]
#             else:
#                 raise HTTPException(status_code=404, detail="Excel file not found")
            
#             try:
#                 json_data=local_activities_to_json(city=city, year=year, excel_path=excel_path)

#                 return {
#                     "status": 200,
#                     "year":year,
#                     "idms":city,
#                     "topic":"local_activities",
#                     "data": json_data
#                 }

#             except Exception as e:
#                 raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
        
#     else:
#         # Return data for all year
#         all_data = []
#         try:
#             # List all subdirectories in the root directory (representing years)
#             for year_folder in os.listdir(directory_root):
#                 # print(year_folder)
#                 directory_path = directory_root+year_folder
#                 # print(directory_path)
#                 json_path = os.path.join(directory_path, "data.json")
                
#                 if os.path.exists(json_path):
#                     # Load JSON data for each year
#                     with open(json_path, "r", encoding="utf-8") as json_file:
#                         year_data = json.load(json_file)
#                     all_data.append({
#                         "year": year_folder,
#                         "data": year_data
#                     })
#                 else:
#                     # If no JSON exists, attempt to process the Excel file
#                     # excel_path = os.path.join(directory_path, "data.xlsx")
#                     file_path = directory_root+year

#                     # Find all .xlsx files in the given directory
#                     xlsx_files = glob.glob(os.path.join(file_path, "*.xlsx"))
#                     excel_path=xlsx_files[0]

#                     if os.path.exists(excel_path):
#                         json_data = local_activities_to_json(city=city, year=year, excel_path=excel_path)
#                         all_data.append({
#                             "year": year_folder,
#                             "data": json.loads(json_data)
#                         })
#                     else:
#                         print(f"No data found for {year_folder}")
            
#             return {
#                 "status": 200,
#                 "idms":city,
#                 "topic":"local_activities",
#                 "data": all_data
#             }
        
#         except Exception as e:
#             raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")



