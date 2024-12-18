import pandas as pd
import time
import json

class TokenBucket:
    '''
    Class to implement Token bucket for API Call Limiting to prevent DDos and Dos Attacks. 
    Reference: https://www.linkedin.com/pulse/api-defense-rate-limiting-using-fastapi-token-buckets-talamantes-vbatc/
    '''
    def __init__(self, capacity, refill_rate, block_duration=60):
        self.capacity = capacity  # Maximum number of tokens in the bucket
        self.refill_rate = refill_rate  # Rate at which tokens are added per second
        self.tokens = capacity  # Start with the bucket full
        self.last_refill = time.time()  # Record the time when the bucket was last refilled
        self.blocked_until = 0  # Tracks when the bucket is unblocked (set to 0 initially)
        self.block_duration = block_duration  # Block duration (in seconds), default is 60 seconds

    def add_tokens(self):
        """ Add tokens to the bucket based on the time elapsed since the last refill """
        now = time.time()
        if self.tokens < self.capacity:
            # Calculate the number of tokens to add
            tokens_to_add = (now - self.last_refill) * self.refill_rate
            # Update the token count, ensuring it doesn't exceed the capacity
            self.tokens = min(self.capacity, self.tokens + tokens_to_add)
            self.last_refill = now  # Update the last refill time

    def take_token(self):
        """ Attempt to take a token from the bucket """
        now = time.time()

        # Check if the bucket is still blocked
        if now < self.blocked_until:
            return False  # The user is still blocked

        self.add_tokens()  # Refill tokens based on elapsed time

        if self.tokens >= 1:
            self.tokens -= 1  # Deduct a token for the API call
            return True  # Allow the request to proceed

        # If no tokens are available, block for 1 minute
        self.blocked_until = now + self.block_duration
        return False  # Block the request

def get_json(path):
    # Assuming the JSON is stored in a file named 'data.json'
    with open(path, 'r') as file:
        data_dict = json.load(file)
    
    return data_dict

def nepali_to_english_number(nepali_str):
    nepali_num_map = str.maketrans('०१२३४५६७८९', '0123456789')
    return nepali_str.translate(nepali_num_map)

def get_metadata(file_path, row):
    # Load the Excel file, setting the third row as the header (index 2)
    meta_df = pd.read_excel(file_path, header=row,nrows=0, engine="openpyxl")
    return meta_df.columns[0]

# Function to replace all instances of "लेकवेशी " with ""
def remove_prefix(data):
    for item in data:
        for key, value in item.items():
            if isinstance(value, str) and "लेकवेशी " in value:
                item[key] = value.replace("लेकवेशी ", "")
    return data

# def budgetexpense_to_json(city, month, excel_path):
#     # Read the Excel file
#     df = pd.read_excel(excel_path, engine='openpyxl',skiprows=5)

#     df.columns = ['क्र.सं.', 
#             'बजेट उपशीर्षक संकेत',
#             'बजेट उपशीर्षक नाम', 
#             'बजेट चालु', 
#             'बजेट पूंजीगत', 
#             'बजेट जम्मा', 
#             'खर्च चालु',
#             'खर्च पूंजीगत', 
#             'खर्च जम्मा', 
#             'खर्च (%)', 
#             'मौज्दात चालु', 
#             'मौज्दात पूंजीगत', 
#             'मौज्दात जम्मा']
    
#     for col in df.columns[3:]:
#         # Fill NaN values with 0 using .loc
#         df.loc[:, col].fillna(0)
        
#         # Convert Nepali number strings to English numbers using .loc
#         df.loc[:, col] = df[col].apply(lambda x: nepali_to_english_number(x) if isinstance(x, str) else x)
        
#         # Remove commas and convert to float using .loc
#         df.loc[:, col] = df[col].str.replace(',', '')

#     metadata = get_metadata(excel_path, 3)
#     result = {
#         "metadata": metadata,
#         "data": df.to_json(orient="records",force_ascii=False)
#     }
    
#     # Save JSON with metadata
#     file_path = f"./data/{city}/budget_expense/{month}/data.json"
#     with open(file_path, 'w', encoding='utf-8') as f:
#         json.dump(result, f, ensure_ascii=False, indent=4)
    
#     # Return JSON data as a string
#     return json.dumps(result, ensure_ascii=False)
#     # df.to_json(f"./data/{city}/budget_expense/{month}/data.json",orient="records", force_ascii=False)
#     # json_data = df.to_json(orient="records", force_ascii=False)
    
#     # return json_data


# def quadrimesterexpense_to_json(city, year, excel_path):
#     # Read the Excel file
#     df = pd.read_excel(excel_path, engine='openpyxl',skiprows=5)

#     df.columns = ['क्र.सं.', 
#               'खर्च शीर्षक संकेत',
#                 'शीर्षक', 
#                 'प्रथम चौमासिक बजेट', 'प्रथम चौमासिक खर्च', 'दोश्रो चौमासिक	बजेट',
#        'दोश्रो चौमासिक खर्च', 'तेस्रो चौमासिक	बजेट', 'तेस्रो चौमासिक खर्च', 'बजेट जम्मा', 'खर्च जम्मा', 'जम्मा खर्च(%)', 'मौज्दात जम्मा']
    
#     for col in df.columns[3:]:
#         # Fill NaN values with 0 using .loc
#         df.loc[:, col].fillna(0)
        
#         # Convert Nepali number strings to English numbers using .loc
#         df.loc[:, col] = df[col].apply(lambda x: nepali_to_english_number(x) if isinstance(x, str) else x)
        
#         # Remove commas and convert to float using .loc
#         df.loc[:, col] = df[col].str.replace(',', '')

#     metadata = get_metadata(excel_path, 3)
#     result = {
#         "metadata": metadata,
#         "data": df.to_json(orient="records",force_ascii=False)
#     }

#     # Save JSON with metadata
#     file_path = f"./data/{city}/quadrimester_expense/{year}/data.json"
#     with open(file_path, 'w', encoding='utf-8') as f:
#         json.dump(result, f, ensure_ascii=False, indent=4)
    
#     # Return JSON data as a string
#     return json.dumps(result, ensure_ascii=False)
#     # df.to_json(f"./data/{city}/quadrimester_expense/{year}/data.json",orient="records", force_ascii=False)
#     # json_data = df.to_json(orient="records", force_ascii=False)
    
#     # return json_data


# def local_activities_to_json(city, year, excel_path):
#     # Read the Excel file
#     df = pd.read_excel(excel_path, engine='openpyxl',skiprows=4)

#     # Assigning values to 'topic' column based on NaN values in other columns
#     for i, row in df.iterrows():
#         if pd.isna(row['विनियोजन']) and pd.isna(row['खर्च']) and pd.isna(row['खर्च (%)']) and pd.isna(row['मौज्दात']):
#             df.at[i, 'वर्ग'] = row['क्र.सं.']
    
#     df['वर्ग'] = df['वर्ग'].ffill()
#     result_df = df.dropna(subset=df.columns[1:6], how='all')
#     for col in result_df.columns[2:6]:
#         # Fill NaN values with 0 using .loc
#         result_df.loc[:, col].fillna(0)
        
#         # Convert Nepali number strings to English numbers using .loc
#         result_df.loc[:, col] = result_df[col].apply(lambda x: nepali_to_english_number(x) if isinstance(x, str) else x)
        
#         # Remove commas and convert to float using .loc
#         result_df.loc[:, col] = result_df[col].str.replace(',', '')
    
#     metadata = get_metadata(excel_path, 3)
#     result = {
#         "metadata": metadata,
#         "data": result_df.to_json(orient="records", force_ascii=False)
#     }

#     # Save JSON with metadata
#     file_path = f"./data/{city}/local_activities/{year}/data.json"
#     with open(file_path, 'w', encoding='utf-8') as f:
#         json.dump(result, f, ensure_ascii=False, indent=4)
    
#     # Return JSON data as a string
#     return json.dumps(result, ensure_ascii=False)
    # result_df.to_json(f"./data/{city}/local_activities/{year}/data.json",orient="records", force_ascii=False)
    # json_data = result_df.to_json(orient="records", force_ascii=False)
    
    # return json_data

