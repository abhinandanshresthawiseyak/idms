import glob
import os
from typing import Optional
from fastapi import HTTPException
import pandas as pd
import time
import json
from app.utils import get_metadata, nepali_to_english_number, remove_prefix

def budgetexpense_to_json(city, month, excel_path):
    # Read the Excel file
    df = pd.read_excel(excel_path, engine='openpyxl',skiprows=5)

    df.columns = ['क्र.सं.', 
            'बजेट उपशीर्षक संकेत',
            'बजेट उपशीर्षक नाम', 
            'बजेट चालु', 
            'बजेट पूंजीगत', 
            'बजेट जम्मा', 
            'खर्च चालु',
            'खर्च पूंजीगत', 
            'खर्च जम्मा', 
            'खर्च (%)', 
            'मौज्दात चालु', 
            'मौज्दात पूंजीगत', 
            'मौज्दात जम्मा']
    
    for col in df.columns[3:]:
        # Fill NaN values with 0 using .loc
        df.loc[:, col].fillna(0)
        
        # Convert Nepali number strings to English numbers using .loc
        df.loc[:, col] = df[col].apply(lambda x: nepali_to_english_number(x) if isinstance(x, str) else x)
        
        # Remove commas and convert to float using .loc
        df.loc[:, col] = df[col].str.replace(',', '').astype(float)

    df = df.where(pd.notna(df), None)
    metadata = get_metadata(excel_path, 3)
    
    result = {
        "metadata": metadata,
        "data": remove_prefix(df.to_dict(orient="records"))
    }
    
    # Save JSON with metadata
    file_path = f"./data/{city}/budget_expense/{month}/data.json"
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)
    
    # Return JSON data as a string
    # return json.dumps(result, ensure_ascii=False)
    return result

def save_json(directory_path, city, month):
    xlsx_files = glob.glob(os.path.join(directory_path, "*.xlsx"))
    if xlsx_files:
        excel_path = xlsx_files[0]
        try:
            json_data = budgetexpense_to_json(city=city, month=month, excel_path=excel_path)
            print("Saved\n",json_data)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

def get_budget_expense_data(city:str, year=None, month: Optional[str] = None):
    directory_root="./data/"+city+"/budget_expense/"
    if month is not None:
        directory_path=directory_root+month
        json_path = directory_path+"/data.json"

        if os.path.exists(json_path):
            print("JSON File Already Exists! No need further processing")
            # Open the JSON file and load its content
            with open(json_path, "r", encoding="utf-8") as json_file:
                data = json.load(json_file)
            
            return {
                "idms":city,
                "month":month,
                "topic": "budget_expense",
                "data": data
            }
        
        else:
            print("No json file found, have to process entire excel file")
            # Check if the file exists
            if not os.path.exists(directory_path+'/data.xlsx'):
                raise HTTPException(status_code=404, detail="Excel file not found")
            
            try:
                excel_path=directory_path+'/data.xlsx'
                json_data=budgetexpense_to_json(city=city, month=month, excel_path=excel_path)

                return {
                    "month":month,
                    "idms":city,
                    "topic": "budget_expense",
                    "data": json_data
                }

            except Exception as e:
                return {
                    "idms": city,
                    "topic": "budget_expense",
                    "data": []  # Return empty data in case of a general error
                }
                raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
        
    else:
        # Return data for all months
        all_data = []
        try:
            # List all subdirectories in the root directory (representing months)
            for month_folder in os.listdir(directory_root):
                directory_path = os.path.join(directory_root, month_folder)
                json_path = os.path.join(directory_path, "data.json")
                
                if os.path.exists(json_path):
                    # Load JSON data for each month
                    with open(json_path, "r", encoding="utf-8") as json_file:
                        month_data = json.load(json_file)
                    all_data.append({
                        "month": month_folder,
                        "data": month_data
                    })
                else:
                    # If no JSON exists, attempt to process the Excel file
                    excel_path = os.path.join(directory_path, "data.xlsx")
                    if os.path.exists(excel_path):
                        json_data = budgetexpense_to_json(city=city, month=month_folder, excel_path=excel_path)
                        all_data.append({
                            "month": month_folder,
                            "data": json_data
                        })
                    else:
                        print(f"No data found for {month_folder}")
            
            return {
                "idms":city,
                "topic": "budget_expense",
                "data": all_data
            }
        
        except Exception as e:
            return {
                "idms": city,
                "topic": "budget_expense",
                "data": []  # Return empty data in case of a general error
            }
            raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")