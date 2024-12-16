from fastapi import APIRouter, HTTPException, Query
import os
import json 
from typing import List, Optional
from app.handler.quadrimester_expense import save_json
from fastapi.responses import JSONResponse
import pandas as pd
import json, duckdb
from app.utils import get_json

router = APIRouter(prefix="/api")

column_translation_map = {
    "क्र.सं.": "Serial No.",
    "खर्च शीर्षक संकेत": "Expense Code",
    "शीर्षक": "Title",
    "प्रथम चौमासिक बजेट": "First Quarter Budget",
    "प्रथम चौमासिक खर्च": "First Quarter Expenditure",
    "दोश्रो चौमासिक बजेट": "Second Quarter Budget",
    "दोश्रो चौमासिक खर्च": "Second Quarter Expenditure",
    "तेस्रो चौमासिक बजेट": "Third Quarter Budget",
    "तेस्रो चौमासिक खर्च": "Third Quarter Expenditure",
    "बजेट जम्मा": "Total Budget",
    "खर्च जम्मा": "Total Expenditure",
    "जम्मा खर्च(%)": "Total Expenditure (%)",
    "मौज्दात जम्मा": "Remaining Total"
}

title_nepali_to_english = {
    "पारिश्रमिक कर्मचारी": "Employee Remuneration",
    "पोशाक": "Clothing Allowance",
    "औषधीउपचार खर्च": "Medical Expenses",
    "महंगी भत्ता": "Dearness Allowance",
    "फिल्ड भत्ता": "Field Allowance",
    "कर्मचारीको बैठक भत्ता": "Employee Meeting Allowance",
    "कर्मचारी प्रोत्साहन तथा पुरस्कार": "Employee Incentive and Awards",
    "अन्य भत्ता": "Other Allowances",
    "पदाधिकारी बैठक भत्ता": "Officer Meeting Allowance",
    "पदाधिकारीअन्य सुबिधा": "Officer Other Benefits",
    "पदाधिकारी अन्य भत्ता": "Officer Other Allowances",
    "कर्मचारीको योगदानमा आधारित निवृतभरण तथा उपदान कोष खर्च": "Employee Contribution Based Retirement and Grant Fund Expense",
    "कर्मचारीको योगदानमा आधारित बीमा कोष खर्च": "Employee Contribution Based Insurance Fund Expense",
    "कर्मचारी कल्याण कोष": "Employee Welfare Fund",
    "अन्य सामाजिक सुरक्षा खर्च": "Other Social Security Expenses",
    "पानी तथा बिजुली": "Water and Electricity",
    "संचार महसुल": "Communication Charges",
    "इन्धन (पदाधिकारी)": "Fuel (Officers)",
    "इन्धन (कार्यालय प्रयोजन)": "Fuel (Office Use)",
    "सवारी साधन मर्मत खर्च": "Vehicle Repair Expenses",
    "बिमा तथा नवीकरण खर्च": "Insurance and Renewal Expenses",
    "मेशिनरी तथा औजार मर्मत सम्भार तथा सञ्चालन खर्च": "Machinery and Tools Maintenance and Operation Expenses",
    "निर्मित सार्वजनिक सम्पत्तिको मर्मत सम्भार खर्च": "Public Property Maintenance and Care Expenses",
    "मसलन्द तथा कार्यालय सामाग्री": "Office Furniture and Supplies",
    "पुस्तक तथा सामग्री खर्च": "Books and Materials Expense",
    "इन्धन - अन्य प्रयोजन": "Fuel - Other Purposes",
    "पत्रपत्रिका, छपाई तथा सूचना प्रकाशन खर्च": "Newspapers, Printing and Information Publication Expenses",
    "अन्य कार्यालय संचालन खर्च": "Other Office Operating Expenses",
    "सेवा र परामर्श खर्च": "Service and Consultation Expenses",
    "सूचना प्रणाली तथा सफ्टवेयर संचालन खर्च": "Information System and Software Operation Expenses",
    "करार सेवा शुल्क": "Contract Service Fees",
    "कर्मचारी तालिम खर्च": "Employee Training Expenses",
    "सीप विकास तथा जनचेतना तालिम तथा गोष्ठी सम्बन्धी खर्च": "Skill Development and Awareness Training and Seminar Expenses",
    "कार्यक्रम खर्च": "Program Expenses",
    "विविध कार्यक्रम खर्च": "Miscellaneous Program Expenses",
    "अनुगमन, मूल्यांकन खर्च": "Monitoring and Evaluation Expenses",
    "भ्रमण खर्च": "Travel Expenses",
    "विशिष्ट व्यक्ति तथा प्रतिनिधि मण्डलको भ्रमण खर्च": "Travel Expenses for Distinguished Individuals and Delegations",
    "अन्य भ्रमण खर्च": "Other Travel Expenses",
    "विविध खर्च": "Miscellaneous Expenses",
    "सभा सञ्चालन खर्च": "Meeting Conducting Expenses",
    "शैक्षिक संस्थाहरूलाई सहायता": "Aid to Educational Institutions",
    "धार्मिक तथा सांस्कृतिक संस्था सहायता": "Aid to Religious and Cultural Institutions",
    "अन्य सस्था सहायता": "Aid to Other Institutions",
    "छात्रवृत्ति": "Scholarships",
    "उद्दार, राहत तथा पुनर्स्थापना खर्च": "Rescue, Relief, and Rehabilitation Expenses",
    "औषधीखरिद खर्च": "Medicine Purchase Expenses",
    "अन्य सामाजिक सहायता": "Other Social Assistance",
    "मृत कर्मचारीको सुविधा तथा सहायता": "Facilities and Assistance for Deceased Employees",
    "घरभाडा": "House Rent",
    "सवारी साधन तथा मेशिनरी औजार भाडा": "Vehicle and Machinery Tool Rent",
    "अन्य भाडा": "Other Rent",
    "राजस्व फिर्ता": "Revenue Return",
    "अन्य फिर्ता": "Other Returns",
    "भैपरी आउने चालु खर्च": "Incidental Current Expenses",
    "गैर आवासीय भवन निर्माण/खरिद": "Non-residential Building Construction/Purchase",
    "निर्मित भवनको संरचनात्मक सुधार खर्च": "Structural Improvement Expenses of Constructed Buildings",
    "सवारी साधन": "Vehicles",
    "मेशिनरी तथा औजार": "Machinery and Tools",
    "फर्निचर तथा फिक्चर्स": "Furniture and Fixtures",
    "पशुधन तथा बागवानी विकास खर्च": "Livestock and Horticulture Development Expenses",
    "सडक तथा पूल निर्माण": "Road and Bridge Construction",
    "तटबन्ध तथा बाँधनिर्माण": "Embankment and Dam Construction",
    "सिंचाई संरचना निर्माण": "Irrigation Structure Construction",
    "खानेपानी संरचना निर्माण": "Drinking Water Structure Construction",
    "सरसफाई संरचना निर्माण": "Sanitation Structure Construction",
    "अन्य सार्वजनिक निर्माण": "Other Public Construction",
    "निर्मित भवनको संरचनात्मक सुधार खर्च": "Structural Improvement Expenses for Constructed Buildings",
    "पूँजीगत सुधार खर्च सार्वजनिक निर्माण": "Capital Improvement Expenses for Public Construction",
    "जग्गाप्राप्ति खर्च": "Land Acquisition Expenses",
    "भैपरी आउने पूँजीगत": "Incidental Capital Expenses"
}

@router.get('/quadrimester_expense')
def get_qe_data(शीर्षक: List[str]=Query(...), cities: List[str]=Query(None), years: List[str]=Query(None), quarter: List[str]=Query(None), nepali:bool=Query(True)):

    var={} # dictionary to hold mapping of city_year and corresponding dataframe --> city_year: dataframe
    combined_data=[] # List to hold [df1,df2,df3] for concatenation (similar to sql union) based on cities and years passed in the endpoint
    quarter_response=[] # Response pattern if quarter is not None

    fiscal_year = ['2080-81','2081-82','2082-83','2083-84','2084-85','2085-86','2086-87','2087-88','2088-89','2089-90'] # Just for sorting fiscal year in ascending order in returning back response 

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
            directory_path=f"/app/backend/data/{city}/quadrimester_expense/{year}"
            file_path = f"/app/backend/data/{city}/quadrimester_expense/{year}/data.json"
            
            # Ensure the directory exists
            if not os.path.exists(directory_path):
                continue

            # Check if the file exists
            if not os.path.exists(file_path):
                save_json(directory_path=directory_path,city=city,year=year)
    
            with open(f'/app/backend/data/{city}/quadrimester_expense/{year}/data.json','r') as f:
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
    
    if not combined_data:
        return {"message": "No data available for the specified filters."}

    final_df = pd.concat(combined_data, ignore_index=True)
    

    if not nepali:
        final_df = final_df.rename(columns=column_translation_map)
        final_df['Title'] = final_df['Title'].map(title_nepali_to_english).fillna(final_df['Title'])

    # if शीर्षक[0]=='total':
    #     if quarter:
    #         '''
    #         [[{'क्र.सं.': '१',
    #             'शीर्षक': 'पारिश्रमिक कर्मचारी',
    #             'city': 'lekbeshi',
    #             'year': '2080-81',
    #             'प्रथम चौमासिक बजेट': 59297560.0,
    #             'प्रथम चौमासिक खर्च': 68972219.07},
    #             {'क्र.सं.': '१',
    #             'शीर्षक': 'पारिश्रमिक कर्मचारी',
    #             'city': 'lekbeshi',
    #             'year': '2081-82',
    #             'प्रथम चौमासिक बजेट': 58538250.0,
    #             'प्रथम चौमासिक खर्च': 62513343.36}],
    #             [{'क्र.सं.': '१',
    #             'शीर्षक': 'पारिश्रमिक कर्मचारी',
    #             'city': 'lekbeshi',
    #             'year': '2080-81',
    #             'दोश्रो चौमासिक\tबजेट': 58804520.0,
    #             'दोश्रो चौमासिक खर्च': 55412646.23},
    #             {'क्र.सं.': '१',
    #             'शीर्षक': 'पारिश्रमिक कर्मचारी',
    #             'city': 'lekbeshi',
    #             'year': '2081-82',
    #             'दोश्रो चौमासिक\tबजेट': 58538250.0,
    #             'दोश्रो चौमासिक खर्च': 0.0}]] 
    #             since, quarter first and second will have different column names, we cannot use union (pandas concat) dataframes of two different quarters; so rather we can modify the response by converting each dataframe to dictionary and then extending into a single response dictionary for all quarters
    #         '''
    #         for q in [final_df[final_df['क्र.सं.']=='कुल जम्मा'][key].to_dict(orient="records") for key in keys]:
    #             quarter_response.extend(q)
    #         return quarter_response
    #         # return final_df[final_df['क्र.सं.']=='कुल जम्मा'][keys[0]].to_dict(orient="records")
    #     else:
    #         return final_df[final_df['क्र.सं.']=='कुल जम्मा'].to_dict(orient="records")

    # elif शीर्षक[0]=='all':
    #     if quarter:
    #         for q in [final_df[key].to_dict(orient="records") for key in keys]:
    #             quarter_response.extend(q)
    #         return quarter_response
    #         # return final_df[keys[0]].to_dict(orient="records")
    #     else:
    #         return final_df.to_dict(orient="records")

    # else:
    #     # शीर्षक.append('')
    #     if quarter:
    #         for q in [final_df[final_df['शीर्षक'].isin(शीर्षक)][key].to_dict(orient="records") for key in keys]:
    #             quarter_response.extend(q)
    #         return quarter_response
    #         # return final_df[final_df['शीर्षक'].isin(शीर्षक)][keys[0]].to_dict(orient="records")
    #     else:
    #         return final_df[final_df['शीर्षक'].isin(शीर्षक)].to_dict(orient="records")

    if शीर्षक[0] == 'total':
        if quarter:
            quarter_response = []
            for keys in [quarter_keys.get(q, []) for q in quarter if q in quarter_keys]:
                # Filter the DataFrame for each quarter and extend the response
                quarter_response.extend(
                    final_df[final_df['Serial No.' if not nepali else 'क्र.सं.'] == 'कुल जम्मा'][keys].to_dict(orient="records")
                )
            data= quarter_response
        else:
            data= final_df[final_df['Serial No.' if not nepali else 'क्र.सं.'] == 'कुल जम्मा'].to_dict(orient="records")

    elif शीर्षक[0] == 'all':
        if quarter:
            quarter_response = []
            for keys in [quarter_keys.get(q, []) for q in quarter if q in quarter_keys]:
                # Filter the DataFrame for each quarter and extend the response
                quarter_response.extend(final_df[keys].to_dict(orient="records"))
            data= quarter_response
        else:
            data= final_df.to_dict(orient="records")

    else:
        field_name = 'Title' if not nepali else 'शीर्षक'
        if quarter:
            quarter_response = []
            for keys in [quarter_keys.get(q, []) for q in quarter if q in quarter_keys]:
                # Filter the DataFrame for specific subheadings and each quarter
                quarter_response.extend(
                    final_df[final_df[field_name].isin(शीर्षक)][keys].to_dict(orient="records")
                )
            data= quarter_response
        else:
            data= final_df[final_df[field_name].isin(शीर्षक)].to_dict(orient="records")

    return {
        "filterOptions":{
            # "years": os.listdir(f"./data/{cities[0]}/quadrimester_expense")
            "year": sorted(
                os.listdir(f"/app/backend/data/{cities[0]}/quadrimester_expense"),
                key=lambda year: fiscal_year.index(year) if year in fiscal_year else None
            )
        },
        "data":data
    }

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
    


