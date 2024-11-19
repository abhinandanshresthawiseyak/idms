import glob
import os
from typing import Optional
from fastapi import HTTPException
import pandas as pd
import time
import json
from app.utils import get_metadata, nepali_to_english_number

def quadrimesterexpense_to_json(city, year, excel_path):
    # Read the Excel file
    df = pd.read_excel(excel_path, engine='openpyxl',skiprows=5)

    df.columns = ['क्र.सं.', 
              'खर्च शीर्षक संकेत',
                'शीर्षक', 
                'प्रथम चौमासिक बजेट', 'प्रथम चौमासिक खर्च', 'दोश्रो चौमासिक	बजेट',
       'दोश्रो चौमासिक खर्च', 'तेस्रो चौमासिक	बजेट', 'तेस्रो चौमासिक खर्च', 'बजेट जम्मा', 'खर्च जम्मा', 'जम्मा खर्च(%)', 'मौज्दात जम्मा']
    
    for col in df.columns[3:]:
        # Fill NaN values with 0 using .loc
        df.loc[:, col].fillna(0)
        
        # Convert Nepali number strings to English numbers using .loc
        df.loc[:, col] = df[col].apply(lambda x: nepali_to_english_number(x) if isinstance(x, str) else x)
        
        # Remove commas and convert to float using .loc
        # df.loc[:, col] = df[col].str.replace(',', '').astype(float)
        # Remove commas and convert to float using .loc
        df.loc[:, col] = df[col].str.replace(',', '').str.replace("(", '').str.replace(")", '').astype(float)

    metadata = get_metadata(excel_path, 3)
    df['शीर्षक']=df['शीर्षक'].fillna('')
    df = df.where(pd.notna(df), None)
    result = {
        "metadata": metadata,
        "data": df.to_dict(orient="records")
    }

    # Save JSON with metadata
    file_path = f"./data/{city}/quadrimester_expense/{year}/data.json"
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)
    
    # Return JSON data as a string
    # return json.dumps(result, ensure_ascii=False)
    return result

def save_json(directory_path, city, year):
    xlsx_files = glob.glob(os.path.join(directory_path, "*.xlsx"))
    if xlsx_files:
        excel_path = xlsx_files[0]
        try:
            json_data = quadrimesterexpense_to_json(city=city, year=year, excel_path=excel_path)
            print("Saved\n",json_data)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
        
def get_quadrimester_expense_data(city: str, year: Optional[str] = None, month=None):
    directory_root = f"./data/{city}/quadrimester_expense/"
    
    if year:
        directory_path = f"{directory_root}{year}"
        json_path = os.path.join(directory_path, "data.json")

        if os.path.exists(json_path):
            print("JSON File Already Exists! No need for further processing.")
            with open(json_path, "r", encoding="utf-8") as json_file:
                data = json.load(json_file)
            return {
                "idms": city,
                "year": year,
                "topic": "quadrimester_expense",
                "data": data
            }

        else:
            print("No JSON file found, have to process entire Excel file.")
            xlsx_files = glob.glob(os.path.join(directory_path, "*.xlsx"))
            if xlsx_files:
                excel_path = xlsx_files[0]
                try:
                    json_data = quadrimesterexpense_to_json(city=city, year=year, excel_path=excel_path)
                    return {
                        "year": year,
                        "idms": city,
                        "topic": "quadrimester_expense",
                        "data": json_data  # Ensure json_data is parsed correctly
                    }
                except Exception as e:
                    return {
                        "idms": city,
                        "year": year,
                        "topic": "quadrimester_expense",
                        "data": []
                    }
                    raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
            else:
                return {
                        "idms": city,
                        "year": year,
                        "topic": "quadrimester_expense",
                        "data": []
                }
                raise HTTPException(status_code=404, detail="Excel file not found")

    else:
        # Return data for all years
        all_data = []
        if not os.path.exists(directory_root):
            return {
                "idms": city,
                "topic": "quadrimester_expense",
                "data": []
            }
            raise HTTPException(status_code=404, detail="No quadrimester expense directory found for the specified city.")

        year_folders = [folder for folder in os.listdir(directory_root) if os.path.isdir(os.path.join(directory_root, folder))]
        for year_folder in year_folders:
            json_path = os.path.join(directory_root, year_folder, "data.json")
            if os.path.exists(json_path):
                with open(json_path, "r", encoding="utf-8") as json_file:
                    year_data = json.load(json_file)
                all_data.append({
                    "year": year_folder,
                    "data": year_data
                })
            else:
                print(f"No JSON data found for {year_folder}, attempting to process Excel file.")
                xlsx_files = glob.glob(os.path.join(directory_root, year_folder, "*.xlsx"))
                if xlsx_files:
                    excel_path = xlsx_files[0]
                    try:
                        json_data = quadrimesterexpense_to_json(city=city, year=year_folder, excel_path=excel_path)
                        all_data.append({
                            "year": year_folder,
                            "data": json_data
                        })
                    except Exception as e:
                        return {"idms": city, "year": year, "topic": "quadrimester_expense", "data": [],"message":"No excel found"}  # No Excel file found
                        print(f"Failed to process Excel data for {year_folder}: {str(e)}")
                else:
                    return {"idms": city, "year": year, "topic": "quadrimester_expense", "data": [],"message":"No excel found"}  # No Excel file found
                    print(f"No Excel data found for {year_folder}")

        return {
            "idms": city,
            "topic": "quadrimester_expense",
            "data": all_data
        }



# def get_all_quadrimester_expense_data():
#     """
#     Endpoint to get quadrimester expense data for all cities and all years.
#     """
#     directory_root = "./data/"  # Root directory where cities are stored
#     all_cities_data = []
    
#     try:
#         # Iterate over all city directories in the root directory
#         for city_folder in os.listdir(directory_root):
#             city_path = os.path.join(directory_root, city_folder, "quadrimester_expense")
            
#             if os.path.isdir(city_path):  # Ensure it's a directory
#                 city_data = {"idms": city_folder, "years": []}
                
#                 # Iterate over all year directories in the city's quadrimester_expense folder
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
#                             json_data = quadrimesterexpense_to_json(city=city_folder, year=year_folder, excel_path=excel_path)
#                             city_data["years"].append({
#                                 "year": year_folder,
#                                 "data": json.loads(json_data)
#                             })
#                         else:
#                             print(f"No data found for {year_folder} in {city_folder}")
                
#                 all_cities_data.append(city_data)
        
#         return {
#             "status": 200,
#             "topic": "quadrimester_expense",
#             "data": all_cities_data
#         }

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")
