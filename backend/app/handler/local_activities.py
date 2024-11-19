import glob
import os
from typing import Optional
from fastapi import HTTPException
import pandas as pd
import time
import json
from app.utils import get_metadata, nepali_to_english_number

def save_json(directory_path, city, year):
    xlsx_files = glob.glob(os.path.join(directory_path, "*.xlsx"))
    if xlsx_files:
        excel_path = xlsx_files[0]
        try:
            json_data = local_activities_to_json(city=city, year=year, excel_path=excel_path)
            print("Saved\n",json_data)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

def local_activities_to_json(city, year, excel_path):
    # Read the Excel file
    df = pd.read_excel(excel_path, engine='openpyxl',skiprows=4)

    # Assigning values to 'topic' column based on NaN values in other columns
    for i, row in df.iterrows():
        if pd.isna(row['विनियोजन']) and pd.isna(row['खर्च']) and pd.isna(row['खर्च (%)']) and pd.isna(row['मौज्दात']):
            df.at[i, 'वर्ग'] = row['क्र.सं.']
    
    df['वर्ग'] = df['वर्ग'].ffill()
    result_df = df.dropna(subset=df.columns[1:6], how='all')
    for col in result_df.columns[2:6]:
        # Fill NaN values with 0 using .loc
        result_df.loc[:, col].fillna(0)
        
        # Convert Nepali number strings to English numbers using .loc
        result_df.loc[:, col] = result_df[col].apply(lambda x: nepali_to_english_number(x) if isinstance(x, str) else x)
        
        # Remove commas and convert to float using .loc
        result_df.loc[:, col] = result_df[col].str.replace(',', '').astype(float)

    metadata = get_metadata(excel_path, 3)
    # Replace NaN values with None
    result_df = result_df.where(pd.notna(df), None)
    result = {
        "metadata": metadata,
        "data": result_df.to_dict(orient="records")
    }

    # Save JSON with metadata
    file_path = f"./data/{city}/local_activities/{year}/data.json"
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)
    
    # Return JSON data as a string
    return result
    # return result, ensure_ascii=False)

def get_local_activities_data(city: str, year: Optional[str] = None, month=None):
    directory_root = f"./data/{city}/local_activities/"
    if year:
        directory_path = f"{directory_root}{year}"
        json_path = f"{directory_path}/data.json"

        if os.path.exists(json_path):
            print("JSON File Already Exists! No need for further processing.")
            with open(json_path, "r", encoding="utf-8") as json_file:
                data = json.load(json_file)
            return {"idms": city, "year": year, "topic": "local_activities", "data": data}
        else:
            print("No JSON file found, processing the Excel file.")
            xlsx_files = glob.glob(os.path.join(directory_path, "*.xlsx"))
            if xlsx_files:
                excel_path = xlsx_files[0]
                try:
                    json_data = local_activities_to_json(city=city, year=year, excel_path=excel_path)
                    # print(json_data)
                    return {"idms": city, "year": year, "topic": "local_activities", "data": json_data}
                except Exception as e:
                    # raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
                    return {"idms": city, "year": year, "topic": "local_activities", "data": []}  # No Excel file found
            else:
                # raise HTTPException(status_code=404, detail="Excel file not found")
                return {"idms": city, "year": year, "topic": "local_activities", "data": []}  # No Excel file found
    else:
        # Process data for all years
        all_data = []
        if not os.path.exists(directory_root):
            # raise HTTPException(status_code=404, detail="City directory not found")
            return {"idms": city, "year": year, "topic": "local_activities", "data": [],"message":'No Excel Found'}  # No Excel file found
        try:
            year_folders = [folder for folder in os.listdir(directory_root) if os.path.isdir(os.path.join(directory_root, folder))]
            for year_folder in year_folders:
                json_path = os.path.join(directory_root, year_folder, "data.json")
                if os.path.exists(json_path):
                    with open(json_path, "r", encoding="utf-8") as json_file:
                        month_data = json.load(json_file)
                    all_data.append({"year": year_folder, "data": month_data})
                else:
                    print(f"No JSON data found for {year_folder}, attempting to process Excel file.")
                    xlsx_files = glob.glob(os.path.join(directory_root, year_folder, "*.xlsx"))
                    if xlsx_files:
                        excel_path = xlsx_files[0]
                        try:
                            json_data = local_activities_to_json(city=city, year=year_folder, excel_path=excel_path)
                            all_data.append({"year": year_folder, "data": json_data})
                        except Exception as e:
                            # print(f"Failed to process Excel data for {year_folder}: {str(e)}")
                            return {"idms": city, "year": year, "topic": "local_activities", "data": [],"message":"No excel found"}  # No Excel file found
                    else:
                        print(f"No Excel data found for {year_folder}")
                        return {"idms": city, "year": year, "topic": "local_activities", "data": []}  # No Excel file found
            return {"idms": city, "topic": "local_activities", "data": all_data}
        except Exception as e:
            # raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")
            return {"idms": city, "year": year, "topic": "local_activities", "data": []}  # No Excel file found
