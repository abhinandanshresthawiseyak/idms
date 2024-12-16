import logging
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
import os
import json 
from typing import List, Optional
from app.handler import quadrimester_expense, budget_expense, local_activities
from fastapi.responses import JSONResponse
import subprocess
from datetime import datetime
import pytz
import time
from fastapi.security import HTTPBasic, HTTPBasicCredentials

router = APIRouter(prefix="/api")
security = HTTPBasic()

# Define the switch-case logic using a dictionary
topic_handler = {
    "quadrimester_expense": quadrimester_expense.get_quadrimester_expense_data,
    "budget_expense": budget_expense.get_budget_expense_data,
    "local_activities": local_activities.get_local_activities_data,
    # "test": test.get_local_activities_data
}

# Basic Authentication dependency
def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = credentials.username == "idms"
    correct_password = credentials.password == "admin"

    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=401,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    
    return credentials.username

@router.post("/trigger_crawl")
def trigger_crawl(user: str = Depends(verify_credentials)):

    script_dir = "/app/backend/crawler"
    log_file_path = "/app/backend/crawler/logs/"+datetime.now(pytz.timezone('Asia/Kathmandu')).strftime("%Y-%m-%d %H:%M:%S")+".log"  # Define the path to the log file
    try:
        # Change to the script directory
        if not os.path.exists(script_dir):
            raise FileNotFoundError(f"Directory not found: {script_dir}")
        os.chdir(script_dir)

        # Define the command to run the crawler
        command = ["python", "crawler.py"]
        
        # Open the log file for writing
        log_file = open(log_file_path, "a")  # Append mode to keep previous logs

        # Run the command as a background process
        process = subprocess.Popen(command, stdout=log_file, stderr=log_file)
        
        return {'message': f'OK, Triggered, you can find logs at {log_file_path}'}

    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@router.post("/dataset")
async def upload_dataset(city:str, category:str, year:str=Query(None), month:str=Query(None), file: UploadFile = File(...), user: str = Depends(verify_credentials)):

    cities = ['lekbeshi','birgunj','janakpur','shuddhodhan','tulsipur'] 
    months=['baisakh','jestha','asar','shrawan','bhadra','asoj','kartik','mangsir','poush','magh','falgun','chaitra',None] 

    if month and year:
        raise HTTPException(status_code=400, detail="month and year combination is currently unavailable")

    if city not in cities:
        raise HTTPException(status_code=400, detail="check the city spelling, city not found")        

    if month not in months:
        raise HTTPException(status_code=400, detail="check spelling of month")

    if year and '/' in year:
        raise HTTPException(status_code=400, detail="message':'use - instead of /")

    # Construct the path to the JSON file
    directory_path=f"/app/backend/data/{city}/{category}/{year}" if year else f"./data/{city}/{category}/{month}"

    original_filename = file.filename # Get the original filename from the uploaded file
    file_path = f"{directory_path}/{original_filename}"
    
    # Ensure the directory exists
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

    # Save the uploaded file
    try:
        # Check if the file already exists
        if os.path.exists(file_path):
            return {"message": "File Already Exists"}

        # Save the file to the constructed path
        with open(file_path, "wb") as f:
            content = await file.read()  # Read the file content
            f.write(content)  # Write the content to the file

        return {"message": "File uploaded successfully", "file_name": original_filename}

    except Exception as e:
        return {"message": f"An error occurred: {str(e)}"}
        
@router.delete("/dataset")
async def delete_dataset(file_name: str, city: str, category: str, year: str=Query(None), month: str = Query(None), user: str = Depends(verify_credentials)):
    
    cities = ['lekbeshi', 'birgunj', 'janakpur', 'shuddhodhan', 'tulsipur']
    months = ['baisakh', 'jestha', 'asar', 'shrawan', 'bhadra', 'asoj', 'kartik', 'mangsir', 'poush', 'magh', 'falgun', 'chaitra', None]

    if month and year:
        raise HTTPException(status_code=400, detail="year and month combination unavailable")

    # Validate city
    if city not in cities:
        raise HTTPException(status_code=400, detail="City not found. Check the city spelling.")
    
    # Validate month
    if month not in months:
        raise HTTPException(status_code=400, detail="Invalid month spelling.")
    
    if year and '/' in year:
        raise HTTPException(status_code=400, detail="use - instead of /")

    # Check the path to the file
    directory_path = f"/app/backend/data/{city}/{category}/{year}" if year else f"/app/backend/data/{city}/{category}/{month}"
    file_path = f"{directory_path}/{file_name}"
    
    # Check if the directory exists
    if not os.path.exists(directory_path):
        raise HTTPException(status_code=404, detail="Directory does not exist.")
    
    # Check if the file exists
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found.")
    
    # Delete the file
    try:
        os.remove(file_path)
        # Check if the directory is empty
        if not os.listdir(directory_path):
            os.rmdir(directory_path)  # Remove the directory if it's empty
        return {"message": f"File {file_name} deleted successfully alongside {directory_path} folder"}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    
@router.get('/dataset')
def get_all_data(user: str = Depends(verify_credentials)):
    """
    Function to automatically explore the directory structure,
    returning all cities, categories, years, months, and data files with timestamps.
    """

    # all_data={}
    all_data = []
    valid_month = ['baisakh', 'jestha', 'asar', 'shrawan', 'bhadra', 'asoj', 'kartik', 'mangsir', 'poush', 'magh', 'falgun', 'chaitra']
    BASE_DIR = "/app/backend/data"
    
    # Walk through the base directory (cities level)
    for city in os.listdir(BASE_DIR): # ['janakpur', 'tulsipur', 'shuddhodhan', 'birgunj', 'lekbeshi']
        city_path = os.path.join(BASE_DIR, city)  # './data/janakpur'

        if not os.path.isdir(city_path): # check if it is directory or not; if not continue directly to another iteration
            continue  # Skip if not a directory

        # For each city, check for categories
        # all_data[city] = {}

        for category in os.listdir(city_path):
            category_path = os.path.join(city_path, category)

            if not os.path.isdir(category_path):
                continue  # Skip if not a directory

            # all_data[city][category] = {}

            # Now, scan for years or months
            for year_or_month in os.listdir(category_path):
                year_or_month_path = os.path.join(category_path, year_or_month)

                if not os.path.isdir(year_or_month_path):
                    continue  # Skip if not a directory

                # Initialize the dictionary for the year_or_month if not already present
                # if year_or_month not in all_data[city][category]:
                #     all_data[city][category][year_or_month] = []

                # Now look for .json and .xlsx files
                for file_name in os.listdir(year_or_month_path):
                    file_path = os.path.join(year_or_month_path, file_name)

                    # Get file timestamps (created and modified)
                    file_stat = os.stat(file_path)
                    created_timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(file_stat.st_ctime))
                    modified_timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(file_stat.st_mtime))

                    # Check if file is a .json or .xlsx
                    file_info = {
                        "city":city,
                        "category":category,
                        "year": year_or_month if '-' in year_or_month else None,
                        "month": year_or_month if year_or_month in valid_month else None,
                        "file_name": file_name,
                        "type": "json" if file_name.endswith(".json") else "xlsx",
                        "created": created_timestamp,
                        "modified": modified_timestamp
                    }

                    # Add to the list for the year/month
                    # all_data[city][category][year_or_month].append(file_info)
                    all_data.append(file_info)

    return all_data

# @router.get("/api")
# def read_root(
#     topics: List[str] = Query(...),  # ... means this is a required field
#     cities: Optional[List[str]] = Query(None),
#     years: Optional[List[str]] = Query(None),  # Optional parameter, defaults to None
#     months: Optional[List[str]] = Query(None)
# ):
#     response=[]

#     # Handle the case where year or month is not provided
#     if not cities:
#         cities=['lekbeshi','birgunj','janakpur','tulsipur','shuddhodhan']
#     if not years:
#         years = [None]  # Treat missing year as a list with a single `None` element
#     if not months:
#         months = [None]  # Treat missing month as a list with a single `None` element

#     # Iterate over each topic, city, year, and month
#     for topic in topics:
#         for city in cities:
#             for year in years:
#                 for month in months:
#                     # Use a switch-case like structure with the dictionary
#                     if topic in topic_handler:
#                         # Call the appropriate processor function for the topic
#                         result = topic_handler[topic](city, year, month)
#                         response.append(result)
#                     else:
#                         # Handle unknown topics
#                         response.append(f"No processor available for topic {topic}")

#     return response


