from fastapi import APIRouter, HTTPException, Query
import os
import json 
from typing import List, Optional
from app.handler import quadrimester_expense, budget_expense, local_activities
from fastapi.responses import JSONResponse
import subprocess
from datetime import datetime
import pytz

router = APIRouter()

# Define the switch-case logic using a dictionary
topic_handler = {
    "quadrimester_expense": quadrimester_expense.get_quadrimester_expense_data,
    "budget_expense": budget_expense.get_budget_expense_data,
    "local_activities": local_activities.get_local_activities_data,
    # "test": test.get_local_activities_data
}

@router.post("/api/trigger_crawl")
def trigger_crawl():
    script_dir = "/app/crawler"
    log_file_path = "/app/crawler/logs/"+datetime.now(pytz.timezone('Asia/Kathmandu')).strftime("%Y-%m-%d %H:%M:%S")+".log"  # Define the path to the log file
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


