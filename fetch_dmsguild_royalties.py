
"""
DMs Guild Royalty Report Fetcher

This module automates the process of fetching and processing royalty reports from DMs Guild
(dmsguild.com). It handles website authentication, data extraction, local storage, and
Google Sheets synchronization.

The script performs the following main functions:
1. Securely manages DMs Guild login credentials
2. Automates browser interaction to fetch royalty reports
3. Processes and cleans sales data
4. Saves reports locally as CSV files
5. Syncs data to Google Sheets with proper formatting

Key Features:
    - Secure credential storage with basic encryption
    - Automated web navigation and data extraction
    - Local CSV backup of reports
    - Google Sheets integration with formatting
    - Duplicate prevention in Google Sheets
    - Error handling with screenshots for debugging

Requirements:
    - Python 3.x
    - Chrome browser
    - Google Cloud project with Sheets API enabled
    - Service account credentials for Google Sheets API

Required Packages:
    - selenium: Web automation
    - pandas: Data processing
    - beautifulsoup4: HTML parsing
    - google-api-python-client: Google Sheets integration
    - python-dateutil: Date handling
    - numpy: Numerical operations

Usage:
    python fetch_dmsguild_royalties.py

Configuration:
    - credentials.txt: Stores encrypted DMs Guild login details
    - Google Sheets service account JSON file
    - SPREADSHEET_ID: ID of target Google Sheet
"""
import os
import time
import base64
import calendar
from datetime import datetime, date
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def get_sheet_service(credentials_path):
    """Initialize and return Google Sheets service."""
    creds = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    return build('sheets', 'v4', credentials=creds)

def get_existing_sheet_data(service, spreadsheet_id):
    """Retrieve existing data from Google Sheet."""
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='Sheet1!A:I'
    ).execute()
    return result.get('values', [])

def prepare_update_data(update_df, existing_data):
    """Prepare data for sheet update."""
    if not existing_data:
        headers = update_df.columns.tolist()
        values = [[clean_value_for_sheets(value) for value in row]
                 for row in update_df.values.tolist()]
        values.insert(0, headers)
        return values, 'Sheet1!A1'

    values = [[clean_value_for_sheets(value) for value in row]
              for row in update_df.values.tolist()]
    start_row = len(existing_data) + 1
    return values, f'Sheet1!A{start_row}'

def check_for_duplicate_entry(existing_data, new_month, new_year):
    """Check if data for given month/year already exists."""
    if not existing_data:
        return False

    headers = existing_data[0]
    try:
        month_idx = headers.index('Month')
        year_idx = headers.index('Year')
    except ValueError as exc:
        raise ValueError("Could not find Month or Year columns in existing sheet") from exc

    for row in existing_data[1:]:
        if (len(row) > max(month_idx, year_idx) and
            row[month_idx] == new_month and
            str(row[year_idx]) == str(new_year)):
            return True
    return False

def apply_currency_formatting(service, spreadsheet_id, df_to_format):
    """Apply currency formatting to Net and Royalties columns."""
    try:
        net_idx = df_to_format.columns.get_loc('Net')
        royalties_idx = df_to_format.columns.get_loc('Royalties')

        requests = []
        for column_idx in [net_idx, royalties_idx]:
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': 0,
                        'startColumnIndex': column_idx,
                        'endColumnIndex': column_idx + 1,
                        'startRowIndex': 1
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'numberFormat': {
                                'type': 'CURRENCY',
                                'pattern': '"$"#,##0.00'
                            }
                        }
                    },
                    'fields': 'userEnteredFormat.numberFormat'
                }
            })

        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={'requests': requests}
        ).execute()
    except (KeyError, IndexError) as exc:
        print(f"Warning: Could not apply currency formatting: {exc}")

def update_google_sheet(update_df, spreadsheet_id, credentials_path):
    """Update Google Sheet with new data, avoiding duplicates and applying formatting."""
    try:
        service = get_sheet_service(credentials_path)
        new_month = update_df['Month'].iloc[0]
        new_year = update_df['Year'].iloc[0]

        existing_data = get_existing_sheet_data(service, spreadsheet_id)

        if check_for_duplicate_entry(existing_data, new_month, new_year):
            print(f"Data for {new_month} {new_year} already exists in sheet. Skipping update.")
            return False

        values, range_name = prepare_update_data(update_df, existing_data)

        # pylint: disable=no-member
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='RAW',
            body={'values': values}
        ).execute()

        apply_currency_formatting(service, spreadsheet_id, update_df)
        return True

    except HttpError as error:
        print(f"An error occurred: {error}")
        print(f"Spreadsheet ID: {spreadsheet_id}")
        raise
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise

def clean_value_for_checks(value):
    """Clean and convert values to be compatible with Google Sheets."""
    if pd.isna(value):
        return ""
    if isinstance(value, (int, float)):
        return value if not pd.isna(value) else ""
    return str(value).strip()


def get_last_month_dates():
    """Get the start and end dates for the last month."""
    today = date.today()
    first_of_this_month = today.replace(day=1)
    last_month_end = first_of_this_month - relativedelta(days=1)
    last_month_start = last_month_end.replace(day=1)

    return last_month_start.strftime('%Y-%m-%d'), last_month_end.strftime('%Y-%m-%d')

def encrypt(text, key):
    """Encrypt the text using XOR with the key and base64 encoding."""
    return base64.urlsafe_b64encode(''.join(chr(ord(c) ^ ord(k)) for c, k in zip(
        text, key * len(text))).encode()).decode()

def decrypt(text, key):
    """Decrypt the text using XOR with the key and base64 decoding."""
    return ''.join(chr(ord(c) ^ ord(k)) for c, k in zip(
        base64.urlsafe_b64decode(text).decode(), key * len(text))
    )

def read_credentials(file_path, key):
    """Read and decrypt the credentials from a file."""
    with open(file_path, 'r', encoding='utf-8') as file:
        existing_username = file.readline().strip()
        encrypted_password = file.readline().strip()

    existing_password = decrypt(encrypted_password, key)
    return existing_username, existing_password

def write_credentials(file_path, username_to_write, password_to_write, key):
    """Encrypt and write the credentials to a file."""
    encrypted_password = encrypt(password_to_write, key)
    with open(file_path, 'w', encoding='utf=8') as file:
        file.write(f"{username_to_write}\n{encrypted_password}")

def process_sales_table(html_content, month, year):
    """Process the HTML content of the sales table into a DataFrame."""
    soup = BeautifulSoup(html_content, 'html.parser')
    rows = soup.find_all('tr')
    print(f"\nFound {len(rows)} rows in the table")

    if len(rows) < 2:
        print("Not enough rows found in table")
        return pd.DataFrame()

    data_rows = []
    for row in rows[1:]:
        cols = row.find_all('td')
        if len(cols) != 7:
            print(f"Skipping row with {len(cols)} columns (probably total row)")
            continue

        try:
            row_data = {
                'Publisher': cols[0].text.strip(),
                'Title': cols[1].text.strip(),
                'SKU': cols[2].text.strip(),
                'Units_Sold': int(cols[3].text.strip() or '0'),
                'Net': float(cols[4].text.strip().replace('$', '').replace(',', '') or '0'),
                'Royalty_Rate': float(cols[5].text.strip().replace('%', '') or '0'),
                'Royalties': float(cols[6].text.strip().replace('$', '').replace(',', '') or '0')
            }
            data_rows.append(row_data)
        except (IndexError, ValueError, AttributeError) as exc:
            print(f"Error processing row: {exc}")
            print(row.prettify())
            continue

    if not data_rows:
        print("No valid data rows found")
        return pd.DataFrame()

    sales_df = pd.DataFrame(data_rows)
    sales_df['Month'] = calendar.month_name[month]
    sales_df['Year'] = year

    columns_order = ['Month', 'Year', 'Publisher', 'Title', 'SKU', 'Units_Sold',
                     'Net', 'Royalty_Rate', 'Royalties']
    return sales_df[columns_order]

def fetch_dmsguild_royalties(dmsguild_username, dmsguild_password):
    """Main function to fetch royalty reports."""
    start_date, end_date = get_last_month_dates()
    # Set up Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")

    # Set up the webdriver with Chrome options
    driver = webdriver.Chrome(options=chrome_options)

    try:
        driver = login_to_dmsguild(driver, dmsguild_username, dmsguild_password)
        driver = navigate_to_royalty_page(driver)
        driver = set_date_range(driver, start_date, end_date)
        table_html = extract_table_data(driver)

        # Process the data
        report_date = datetime.strptime(start_date, '%Y-%m-%d')
        sales_df = process_sales_table(table_html, report_date.month, report_date.year)

        return sales_df
    except (WebDriverException, TimeoutException) as e:
        handle_error("Browser automation error: " + str(e))
        return pd.DataFrame()  # Return empty DataFrame on error
    except ValueError as e:
        handle_error("Data processing error: " + str(e))
        return pd.DataFrame()  # Return empty DataFrame on error
    except ConnectionError as e:
        handle_error("Network error: " + str(e))
        return pd.DataFrame()  # Return empty DataFrame on error
    finally:
        driver.quit()

def login_to_dmsguild(driver, dmsguild_username, dmsguild_password):
    """Handle the login process."""
    driver.get("https://www.dmsguild.com/login.php")

    login_link = driver.find_element(By.CSS_SELECTOR, "a.login_window")
    driver.execute_script("arguments[0].click();", login_link)

    username_field = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.ID, "login_email_address"))
    )
    password_field = driver.find_element(By.ID, "login_password")

    username_field.send_keys(dmsguild_username)
    password_field.send_keys(dmsguild_password)

    login_button = driver.find_element(By.ID, "loginbutton")
    login_button.click()

    return driver

def navigate_to_royalty_page(driver):
    """Navigate to the royalty report page."""
    account_link = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "a.nav-bar-link[href='https://www.dmsguild.com/account.php']"))
    )
    account_link.click()

    royalty_link = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "a[href='https://www.dmsguild.com/royalty_report.php']"))
    )
    royalty_link.click()

    return driver

def set_date_range(driver, start_date, end_date):
    """Set the date range for the report."""
    time.sleep(3)

    start_date_field = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.NAME, "startdate"))
    )
    end_date_field = driver.find_element(By.NAME, "enddate")

    start_date_field.clear()
    end_date_field.clear()

    driver.execute_script(f"arguments[0].value = '{start_date}';", start_date_field)
    driver.execute_script(f"arguments[0].value = '{end_date}';", end_date_field)

    driver.execute_script("arguments[0].dispatchEvent(new Event('change'));", start_date_field)
    driver.execute_script("arguments[0].dispatchEvent(new Event('change'));", end_date_field)

    submit_button = driver.find_element(By.NAME, "submit_report")
    submit_button.click()

    return driver

def extract_table_data(driver):
    """Extract the table HTML from the page."""
    table = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, "table[cellpadding='5'][cellspacing='0'][border='1']"))
    )
    time.sleep(2)

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, "standardText"))
    )

    return table.get_attribute('outerHTML')

def handle_error(error):
    """Handle errors with screenshot and re-raising."""
    print(f"An error occurred: {str(error)}")
    print("Taking error screenshot...")
    raise RuntimeError(str(error))

def get_report_filepath(output_dir="reports"):
    """Generate the expected filepath for the current month's report."""
    abs_output_dir = os.path.abspath(output_dir)

    if not os.path.exists(abs_output_dir):
        os.makedirs(abs_output_dir)

    timestamp = datetime.now().strftime("%Y%m")
    filename = f"dmsguild_report_{timestamp}.csv"
    return os.path.join(abs_output_dir, filename)

def save_to_local_file(df_to_save, output_dir="reports"):
    """Save DataFrame to a local CSV file in the specified directory."""
    #filepath = get_report_filepath(output_dir)
    #print(f"filepath: {filepath}")
 
    # Save to CSV
    df_to_save.to_csv(output_dir, index=False)
    return output_dir

def load_existing_report(filepath):
    """Load an existing report if it exists."""
    try:
        if os.path.exists(filepath):
            print(f"Found existing report at: {filepath}")
            return pd.read_csv(filepath)
        return None
    except (FileNotFoundError, pd.errors.EmptyDataError, pd.errors.ParserError) as exc:
        print(f"Error loading existing report: {exc}")
        return None

def clean_value_for_sheets(value):
    """Clean and convert values to be compatible with Google Sheets."""
    if pd.isna(value):
        return ""
    if isinstance(value, (int, float)):
        return value if not pd.isna(value) else ""
    return str(value).strip()

# Optional: Add a function to verify data before sending
def verify_data_for_sheets(df_to_verify):
    """Verify that DataFrame contains valid data for Google Sheets."""
    issues = []

    # Check for NaN values
    nan_counts = df_to_verify.isna().sum()
    if nan_counts.any():
        issues.extend([f"Column '{col}' has {count} NaN values"
                      for col, count in nan_counts.items() if count > 0])

    # Check data types
    for col in df_to_verify.columns:
        if df_to_verify[col].dtype not in [np.int64, np.float64, object]:
            issues.append(f"Column '{col}' has unusual dtype: {df_to_verify[col].dtype}")

    # Print verification results
    if issues:
        print("\nData verification found issues:")
        for issue in issues:
            print(f"- {issue}")
    else:
        print("\nData verification passed")

    return len(issues) == 0

if __name__ == "__main__":
    CREDENTIALS_FILE = "credentials.txt"
    # Store key in environment variable, not code
    # Command to create key in powershell: $env:DMSGUILD_ENCRYPTION_KEY = "my_key_here"
    encryption_key = os.getenv('DMSGUILD_ENCRYPTION_KEY')
    if not encryption_key:
        raise ValueError("DMSGUILD_ENCRYPTION_KEY environment variable not set")

    #store the credentials in the environment variables
    # $env:GOOGLE_SHEETS_CREDENTIALS = "my_key_name_here.json"
    # $env:GOOGLE_SHEETS_SPREADSHEET_ID = "my_spreadsheet_id_here"
    GOOGLE_SHEETS_CREDENTIALS = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
    SPREADSHEET_ID = os.getenv('GOOGLE_SHEETS_SPREADSHEET_ID')

    if not GOOGLE_SHEETS_CREDENTIALS or not SPREADSHEET_ID:
        raise ValueError("Missing required environment variables")

    # Check for existing report first
    report_filepath = get_report_filepath()
    df = load_existing_report(report_filepath)

    if df is not None:
        print("Using existing report for this month")
    else:
        print("No existing report found for this month. Fetching new data...")
        if os.path.exists(CREDENTIALS_FILE):
            username, password = read_credentials(CREDENTIALS_FILE, encryption_key)
            df = fetch_dmsguild_royalties(username, password)

            if df is not None:
                # Save to local file
                save_to_local_file(df, report_filepath)
                print(f"Data saved locally to: {report_filepath}")
        else:
            print("Credentials file not found. Let's create one.")
            username = input("Enter your DMs Guild username: ")
            password = input("Enter your DMs Guild password: ")
            write_credentials(CREDENTIALS_FILE, username, password, encryption_key)
            print(f"Credentials saved to {CREDENTIALS_FILE}")
            df = fetch_dmsguild_royalties(username, password)
            if df is not None:
                save_to_local_file(df, report_filepath)

    # Update Google Sheet if we have data
    if df is not None:
        try:
            # Verify data first
            verify_data_for_sheets(df)

            # Update Google Sheet
            update_google_sheet(df, SPREADSHEET_ID, GOOGLE_SHEETS_CREDENTIALS)
            print("Successfully updated Google Sheet")
        except (ValueError, HttpError, IOError) as exc:
            print(f"Error updating Google Sheet: {exc}")
