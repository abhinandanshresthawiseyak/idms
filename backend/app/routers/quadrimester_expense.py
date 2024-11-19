from fastapi import APIRouter, HTTPException, Query
import os
import json 
from typing import List, Optional
from app.handler.quadrimester_expense import save_json
from fastapi.responses import JSONResponse
import pandas as pd
import json, duckdb
from app.utils import get_json

router = APIRouter()

@router.get('/quadrimester_expense/qe')
def get_qe_data(शीर्षक: List[str]=Query(...), cities: List[str]=Query(None), years: List[str]=Query(None), quarter: List[str]=Query(None)):

    var={} # dictionary to hold mapping of city_year and corresponding dataframe --> city_year: dataframe
    combined_data=[] # List to hold [df1,df2,df3] for concatenation (similar to sql union) based on cities and years passed in the endpoint
    quarter_response=[] # Response pattern if quarter is not None

    if not cities:
        cities=['lekbeshi','birgunj','janakpur','tulsipur','shuddhodhan']

    if not years:
        years=['2080-81','2081-82','2082-83','2083-84']

    if quarter:
        keys=[]

        quarter_keys = {
            "first": ["क्र.सं.", "शीर्षक", "city", "year","प्रथम चौमासिक बजेट", "प्रथम चौमासिक खर्च"],
            "second": ["क्र.सं.", "शीर्षक", "city", "year","दोश्रो चौमासिक\tबजेट", "दोश्रो चौमासिक खर्च"],
            "third": ["क्र.सं.", "शीर्षक", "city", "year","तेस्रो चौमासिक\tबजेट", "तेस्रो चौमासिक खर्च"],
            "total":["क्र.सं.", "शीर्षक", "city", "year","बजेट जम्मा","खर्च जम्मा","मौज्दात जम्मा","जम्मा खर्च(%)"]
        }

        # Extract data for specified quarter or total
        for i in range(0, len(quarter)):
            keys.extend([quarter_keys.get(quarter[i], [None])]) # Default to list with None if not found

        # quarter_keys = {
        #     "first": ["क्र.सं.", "शीर्षक","प्रथम चौमासिक बजेट", "प्रथम चौमासिक खर्च","city","year"],
        #     "second": ["क्र.सं.", "शीर्षक","दोश्रो चौमासिक\tबजेट", "दोश्रो चौमासिक खर्च","city","year"],
        #     "third": ["क्र.सं.", "शीर्षक","तेस्रो चौमासिक\tबजेट", "तेस्रो चौमासिक खर्च","city","year"],
        #     "total":["क्र.सं.", "शीर्षक", "बजेट जम्मा","खर्च जम्मा","मौज्दात जम्मा","जम्मा खर्च(%)","city","year"]
        # }

        # # Extract data for specified quarter or total
        # key = quarter_keys.get(quarter[0], [None])  # Default to list with None if not found


    for city in cities:
        for year in years:
            # Construct the path to the JSON file
            directory_path=f"data/{city}/quadrimester_expense/{year}"
            file_path = f"data/{city}/quadrimester_expense/{year}/data.json"
            
            # Ensure the directory exists
            if not os.path.exists(directory_path):
                continue

            # Check if the file exists
            if not os.path.exists(file_path):
                save_json(directory_path=directory_path,city=city,year=year)
    
            with open(f'./data/{city}/quadrimester_expense/{year}/data.json','r') as f:
                json_data=json.load(f)

            # print(json_data)
            # Create a DataFrame for the city and month
            # print(year.replace('-','_'))
            df_key = f"{city}_{year.replace('-','_')}"
            # print(df_key)
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
            
    final_df = pd.concat(combined_data, ignore_index=True)
    
    if शीर्षक[0]=='total':
        if quarter:
            '''
            [[{'क्र.सं.': '१',
                'शीर्षक': 'पारिश्रमिक कर्मचारी',
                'city': 'lekbeshi',
                'year': '2080-81',
                'प्रथम चौमासिक बजेट': 59297560.0,
                'प्रथम चौमासिक खर्च': 68972219.07},
                {'क्र.सं.': '१',
                'शीर्षक': 'पारिश्रमिक कर्मचारी',
                'city': 'lekbeshi',
                'year': '2081-82',
                'प्रथम चौमासिक बजेट': 58538250.0,
                'प्रथम चौमासिक खर्च': 62513343.36}],
                [{'क्र.सं.': '१',
                'शीर्षक': 'पारिश्रमिक कर्मचारी',
                'city': 'lekbeshi',
                'year': '2080-81',
                'दोश्रो चौमासिक\tबजेट': 58804520.0,
                'दोश्रो चौमासिक खर्च': 55412646.23},
                {'क्र.सं.': '१',
                'शीर्षक': 'पारिश्रमिक कर्मचारी',
                'city': 'lekbeshi',
                'year': '2081-82',
                'दोश्रो चौमासिक\tबजेट': 58538250.0,
                'दोश्रो चौमासिक खर्च': 0.0}]] 
                since, quarter first and second will have different column names, we cannot use union (pandas concat) dataframes of two different quarters; so rather we can modify the response by converting each dataframe to dictionary and then extending into a single response dictionary for all quarters
            '''
            for q in [final_df[final_df['क्र.सं.']=='कुल जम्मा'][key].to_dict(orient="records") for key in keys]:
                quarter_response.extend(q)
            return quarter_response
            # return final_df[final_df['क्र.सं.']=='कुल जम्मा'][keys[0]].to_dict(orient="records")
        else:
            return final_df[final_df['क्र.सं.']=='कुल जम्मा'].to_dict(orient="records")

    elif शीर्षक[0]=='all':
        if quarter:
            for q in [final_df[key].to_dict(orient="records") for key in keys]:
                quarter_response.extend(q)
            return quarter_response
            # return final_df[keys[0]].to_dict(orient="records")
        else:
            return final_df.to_dict(orient="records")

    else:
        # शीर्षक.append('')
        if quarter:
            for q in [final_df[final_df['शीर्षक'].isin(शीर्षक)][key].to_dict(orient="records") for key in keys]:
                quarter_response.extend(q)
            return quarter_response
            # return final_df[final_df['शीर्षक'].isin(शीर्षक)][keys[0]].to_dict(orient="records")
        else:
            return final_df[final_df['शीर्षक'].isin(शीर्षक)].to_dict(orient="records")


# @router.get("/quadrimester_expense")
# def get_chart_data(city: str,
#     year: str,
#     quarter: str):

#     # Construct the path to the JSON file
#     directory_path=f"data/{city}/quadrimester_expense/{year}"
#     file_path = f"data/{city}/quadrimester_expense/{year}/data.json"
    
#     # Check if the file exists
#     if not os.path.exists(file_path):
#         save_json(directory_path=directory_path,city=city,year=year)
    
#     # Load the JSON data from the file
#     try:
#         with open(file_path, 'r') as file:
#             json_data  = json.load(file)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error reading the file: {str(e)}")
    
#     # if chart_type=="bar" and title == 'budget-expense':
#     # List to store results based on quarter
#     # results = []
#     quarter_keys = {
#         "first": ["प्रथम चौमासिक बजेट", "प्रथम चौमासिक खर्च"],
#         "second": ["दोश्रो चौमासिक\tबजेट", "दोश्रो चौमासिक खर्च"],
#         "third": ["तेस्रो चौमासिक\tबजेट", "तेस्रो चौमासिक खर्च"],
#         "total":["बजेट जम्मा","खर्च जम्मा","मौज्दात जम्मा","जम्मा खर्च(%)"]
#     }
#     # Extract data for specified quarter or total
#     keys = quarter_keys.get(quarter, [None])  # Default to list with None if not found

#     # Prepare results based on available keys
#     results = []
#     if keys[0]:  # Checks if first key is not None
#         for item in json_data["data"]:
#             result = {"क्र.सं.": item.get("क्र.सं."), "शीर्षक": item.get("शीर्षक")}
#             for key in keys:
#                 result[key] = item.get(key, 0)  # Default to 0 if key not found
#             results.append(result)
    
#     # Return filtered data
#     return {
#         "city": city,
#         "year": year,
#         "quarter": quarter,
#         "data": results
#     }

# @router.get("/quadrimester_expense/compare_years")
# def get_comparision_data(शीर्षक: List[str]=Query(...)):

#     cities=['lekbeshi','birgunj','janakpur','tulsipur','shuddhodhan']
#     years=['2080-81','2081-82']

#     var_df={}

#     for city in cities:
#         for year in years:
#             # Construct the path to the JSON file
#             directory_path=f"data/{city}/quadrimester_expense/{year}"
#             file_path = f"data/{city}/quadrimester_expense/{year}/data.json"
            
#             # Ensure the directory exists
#             if not os.path.exists(directory_path):
#                 continue

#             # Check if the file exists
#             if not os.path.exists(file_path):
#                 save_json(directory_path=directory_path,city=city,year=year)
    
#             # Load the JSON data from the file
#             try:
#                 var_df[f"qd_{year.replace('-','_')}"]=pd.DataFrame(get_json(file_path)['data'])
#             except Exception as e:
#                 raise HTTPException(status_code=500, detail=f"Error reading the file: {str(e)}")
#     # Connecting to DuckDB
#     con = duckdb.connect()

#     # Registering the dataframes
#     con.register('qd_2080_81', var_df['qd_2080_81'])
#     con.register('qd_2081_82', var_df['qd_2081_82'])

#     if शीर्षक[0]=='total':
#         # SQL query using registered dataframes
#         try:
#             result = con.execute(f"""
#                 SELECT *, '2080-81' as year FROM qd_2080_81
#                 UNION ALL
#                 SELECT *, '2081-82' as year FROM qd_2081_82
#             """).df()
#             result=result[result['क्र.सं.']=='कुल जम्मा'].to_dict(orient="records")
#         finally:
#             # Unregister the dataframes and close connection
#             con.unregister('qd_2080_81')
#             con.unregister('qd_2081_82')
#             con.close()

#         # Print or return the result
#         # return result
#     elif शीर्षक[0]=='all':
#         # SQL query using registered dataframes
#         try:
#             result = con.execute(f"""
#                 SELECT *, '2080-81' as year FROM qd_2080_81
#                 UNION ALL
#                 SELECT *, '2081-82' as year FROM qd_2081_82
#             """).df()
#             result=result[result['क्र.सं.']!='कुल जम्मा'].to_dict(orient="records")
#         finally:
#             # Unregister the dataframes and close connection
#             con.unregister('qd_2080_81')
#             con.unregister('qd_2081_82')
#             con.close()

#         # Print or return the result
#         # return result
#     else:
#          # SQL query using registered dataframes
#         try:
#             result = con.execute(f"""
#                 SELECT *, '2080-81' as year FROM qd_2080_81
#                 UNION ALL
#                 SELECT *, '2081-82' as year FROM qd_2081_82
#             """).df()
#             result=result[result['शीर्षक'].isin(शीर्षक)].to_dict(orient="records")
#         finally:
#             # Unregister the dataframes and close connection
#             con.unregister('qd_2080_81')
#             con.unregister('qd_2081_82')
#             con.close()

#         # Print or return the result
#     return result
    


