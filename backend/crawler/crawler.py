from pathlib import Path
import uuid
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys

import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import os, time
from Levenshtein import jaro

from utils import is_file_modified, nepali_to_english_number

# Set up Chrome options for headless mode (if you want it headless)
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")  # Required in Docker to prevent crashes
chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
chrome_options.add_argument("--remote-debugging-port=9222")  # Needed to start Chrome in headless mode
chrome_options.add_argument("--disable-gpu")  # Optional: Disable GPU if not needed

driver = webdriver.Chrome(options=chrome_options) # Initialize WebDriver with options

# Function to check if the target title is uploaded to IDMS Portal since last run
def check_title_and_modified_date(url):
    '''
    Function to check url: https://data.<IDMS municipaltiy>.gov.np/datasets?sort=metadata_modified+desc 
    Checks:
        1. Last Modified: <Date>
        2. If Last Modified > Defined Condition and Title is in [List of Titles to Scrape] Then:
            Returns url_lists to follow the link where we can find csv, excel files in the url_lists
    '''
    try:
        # Request the page content
        response = requests.get(url+'/datasets?formats=XLSX&sort=title_string+desc')
        response.raise_for_status()  # Check if the request was successful
        soup = BeautifulSoup(response.text, "html.parser")

        # Initialize a list to store datasets
        url_list=[]

        # # Example: Find all dataset entries (modify selectors based on actual structure)
        dataset_items = soup.find_all("div", class_="datasetList_datasetCard__Wg_Mx")  # Adjust based on actual HTML structure
        for item in dataset_items:
            link = item.find("a")

            a_href = link.get("href") # We got href
            a_text = link.get_text(strip=True) # We got title of href

            # Find Modified Date
            for p in item.find_all("p"):
                if "Last Modified" in p.text:
                    text=p.text
            # Use regex to extract the date part only
            match = re.search(r'\b[A-Za-z]{3} \d{2} \d{4}\b', text)
            date_str = match.group()  # This will print only the date part
            # Convert date string to datetime object
            modified_date = datetime.strptime(date_str, "%b %d %Y")

            url_list.append([url+a_href, a_text, modified_date])

        return url_list
            # break
            # if a_text=='२०७८ को जनगणना अनुसार तुलसीपुर  उप–महानगरपालिकाको विस्तृत जनसांख्यिक विवरण' and modified_date<datetime.now():
            #     return (a_href, a_text, modified_date) 

    except requests.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None

# Function to get csv data from the url if there has been files uploaded to the url found by check_title_and_modified_date function
def get_csv_links(a_href, a_text, modified_date):

    csv_links=[]

    # Navigate to the page
    driver.get(a_href)  # Replace with the actual URL

    # Find the button containing the span with the text "API" and click it
    try:
        api_buttons = driver.find_elements(By.XPATH, "//button[.//span[text()='API']]")

        for api_button in api_buttons:
            api_button.click()
            print("Clicked the API button successfully.")

            # Wait for the modal or API link to appear
            WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.CLASS_NAME, "api_apiEndpoint__wY3N_"))
            )
            
            # Locate the <a> tag within the modal and get the href attribute
            api_link = driver.find_element(By.XPATH, "//div[@class='api_apiEndpoint__wY3N_']//a")
            link_url = api_link.get_attribute("href")
            print("API Link URL:", link_url)
            csv_links.append(link_url)
            # Locate and click the close button to close the modal
            close_button = driver.find_element(By.XPATH, "//button[@aria-label='Close']")
            close_button.click()
            print("Closed the modal.")

            # Add a brief pause if needed
            WebDriverWait(driver, 1)
        
        return csv_links
    except Exception as e:
        print("Error clicking the API button:", e)

def get_excel_links(a_href, a_text, modified_date):

    '''
    Function the depends upon check_title_and_modified_date() function 
    Finds:
        1. Finds Preview button for Excel files only (Excludes CSVs, PDFs) and it's title 
        2. Returns Dictionary with mapping
            {
                'name of file': 'link to the file',
                'स्वास्थ्य कर्मचारी विवरण (२०८०-०८१)': 'https://dms.lekbeshimun.gov.np/dataset/e4a37dfb-f978-46b4-b402-9cdacc08e972/resource/cd7a5513-ee59-48cc-8655-a07315b366e3/download/-copy-2.xlsx', 
                'स्वास्थ्य कर्मचारी विवरण (२०७८-०७९)': 'https://dms.lekbeshimun.gov.np/dataset/e4a37dfb-f978-46b4-b402-9cdacc08e972/resource/8e988fbe-5e0e-4529-ae5e-9ec2a30034ae/download/-33.xlsx'
            }
    '''

    excel_title_links={}
    driver.get(a_href)  # Replace with the actual URL

    try:
        h4 = driver.find_elements(
            By.XPATH,
            "//div[contains(@class, 'Resource_files__tBhWR')]" +
            "[div[contains(@class, 'ant-col ant-col-xs-4 ant-col-lg-2') and .//*[name()='svg' and @fill='purple']]]" +
            "//h4"
        )

        preview_excel_buttons = driver.find_elements(
            By.XPATH,
            "//div[contains(@class, 'Resource_files__tBhWR')]" +
            "[div[contains(@class, 'ant-col ant-col-xs-4 ant-col-lg-2') and .//*[name()='svg' and @fill='purple']]]" +
            "//button[div[contains(@class, 'Resource_btndiv__uJ7Uo')]//span[text()='Preview']]"
        )

        print("h4==previewbutton",len(h4)==len(preview_excel_buttons))

        # print(h4,h4[0].text)
        # print(preview_excel_buttons)
        for i, preview_excel_button in enumerate(preview_excel_buttons):
            driver.maximize_window()
            try:
                # print(preview_excel_button)
                preview_excel_button.click()
                # print("Clicked the API button successfully.")
                
                # Wait for the modal or API link to appear
                WebDriverWait(driver, 30).until(
                    EC.visibility_of_element_located((By.CLASS_NAME, "xlsx_container__5SzzP"))
                )
                # time.sleep(1)
                # Locate the iframe and retrieve the src attribute
                iframe = driver.find_element(By.XPATH, "//div[@class='xlsx_container__5SzzP']//iframe[@title='PDF Preview']")
                iframe_src = iframe.get_attribute("src")
                # print(f"Iframe src link: {iframe_src} and title: {h4[i].text}")
                excel_title_links[h4[i].get_attribute("innerText")]=iframe_src.split("src=")[-1]

                # driver.save_screenshot(f"debug_screenshot{i}{str(uuid.uuid4())}.png")
                # Wait for either of the buttons to be present
                close_button = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//button[contains(@class, 'preview_close_btn__2sBmv') or contains(@class, 'xlsx_close_btn__MKD_c') or contains(@class,'preview_modal__footer__button__Mo1ZZ')]"))
                )

                # # Resize the button with JavaScript
                # driver.execute_script("""
                #     arguments[0].style.width = '100px';
                #     arguments[0].style.height = '100px';
                #     arguments[0].style.fontSize = '100px';  // Optional: Make the text larger if needed
                # """, close_button)

                # Fallback JavaScript click
                driver.execute_script("arguments[0].click();", close_button)
                WebDriverWait(driver, 1)
            
            except Exception as e:
                driver.save_screenshot(f"debug_screenshot{str(uuid.uuid4())}.png")
                print(e)
                # continue
        return excel_title_links
    except Exception as e:
        print(e)
        
def save_file_to_location(excel_title_links, city):
    '''
        Saves File to the IDMS-Backend/data/{city}/{topic}/FOLDER_NAME <20XX-XY>/filename.xlsx
        Every Topic should have different FOLDER_NAME: FILENAME Mapping based on which the crawler will save the file to the desired location.

    '''

    topic_filename_mappings = {
        'local_activities': ['गत आ.व. ०८०।०८१ को मुख्या क्रियाकलाप अनुसार', 'चालु  आ.व. ०८१।०८२ को मुख्या क्रियाकलाप अनुसार'],
        'quadrimester_expense': ['चालु आ.व. ०८१।०८२ को चौमासिक खर्च विवरण', 'गत आ.व. ०८०।०८१ को चौमासिक खर्च विवरण'],
        'health_employee_details': ['स्वास्थ्य कर्मचारी विवरण (२०७८-०७९)'],
        '': []
    }

    base_path = f"/app/data/{city}"

    for key, url in excel_title_links.items():
        topic = None
        folder_name = None

        for category, titles in topic_filename_mappings.items():
            for title in titles:
                if jaro(key, title) > 0.9:
                    topic = category
                    if topic =='health_employee_details': # convert (२०७८-०७९) to 2078-79 string which will be folder location
                        filename_portion = nepali_to_english_number(key.split(' ')[3]).replace('(', '').replace(')', '')
                        parts = filename_portion.split('-')
                        folder_name = f"{parts[0]}-{parts[1][1:]}"  # Add '2' and clean
                        folder_name = folder_name.replace(".xlsx", "")
                        break

                    elif topic in ['quadrimester_expense', 'local_activities']: # convert ०८१।०८२ to 2081-82 string which will be folder location
                        filename_portion = nepali_to_english_number(key.split(' ')[2]).replace('।','-')
                        parts = filename_portion.split('-')
                        folder_name = f"2{parts[0]}-{parts[1][1:]}"  # Add '2' and clean
                        break

                    # filename_portion = nepali_to_english_number(key.split(' ')[3]).replace('(', '').replace(')', '')
                    # parts = filename_portion.split('-')
                    # folder_name = f"{parts[0]}-{parts[1][1:]}"  # Add '2' and clean
                    # folder_name = folder_name.replace(".xlsx", "")
                    # break

            if topic:
                break

        # Default handling if no match
        if not topic or not folder_name:
            print(f"Could not determine topic or folder name for {key}. Skipping.")
            continue

        folder_path = f"{base_path}/{topic}/{folder_name}"
        file_path = f"{folder_path}/{key}.xlsx"

        # Create folder structure 
        Path(folder_path).mkdir(parents=True, exist_ok=True)

        # Check if the file already exists
        if os.path.exists(file_path):
            print(f"File '{file_path}' already exists.")
            if is_file_modified(filename=f"{key}.xlsx", file_path=file_path, url=url):
                command = f"wget -O '{file_path}' '{url}'"
                os.system(command)
                print(f"File '{file_path}' updated.")
        else:
            command = f"wget -O '{file_path}' '{url}'"
            os.system(command)
            print(f"Downloaded and saved: '{file_path}' from {url}.")

if __name__=='__main__':
    # define urls
    urls={
        'https://data.lekbeshimun.gov.np':'lekbeshi',
        'https://data.tulsipurmun.gov.np':'tulsipur',
        'https://data.birgunjmun.gov.np':'birgunj'
    }

    for url, city in urls.items():
        url_list = check_title_and_modified_date(url)

        for url in url_list:
            a_href, a_text, modified_date=url
            print(a_href, a_text, modified_date)
            excel_title_links=get_excel_links(a_href, a_text, modified_date)
            print(excel_title_links)
            save_file_to_location(excel_title_links, city)
            
            # csv_links=get_csv_links(a_href, a_text, modified_date)
            # print(csv_links)
            time.sleep(1)
        time.sleep(1)

    # Close the WebDriver
    driver.quit()