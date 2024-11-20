import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
import pandas as pd
import time
import base64
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup
import calendar
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import numpy as np

def save_to_local_file(df, output_dir="reports"):
    """Save DataFrame to a local CSV file in the specified directory."""
    # Create reports directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m")
    filename = f"dmsguild_report_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)
    
    # Save to CSV
    df.to_csv(filepath, index=False)
    print(f"Report saved to: {filepath}")
    return filepath

def update_google_sheet(df, spreadsheet_id, credentials_path):
    """Append DataFrame content to Sheet1 of the Google Sheet, avoiding duplicates and applying formatting."""
    try:
        # Load credentials from service account file
        creds = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        
        # Build the Sheets API service
        service = build('sheets', 'v4', credentials=creds)
        
        # Get the month and year we're trying to add
        new_month = df['Month'].iloc[0]
        new_year = df['Year'].iloc[0]
        print(f"\nChecking for existing data for {new_month} {new_year}...")
        
        # First, check if Sheet1 has any data and get headers
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range='Sheet1!A:I'
        ).execute()
        
        if 'values' not in result or not result['values']:
            print("Sheet is empty, adding headers and data...")
            headers = df.columns.tolist()
            values = [[clean_value_for_sheets(value) for value in row] for row in df.values.tolist()]
            values.insert(0, headers)
            range_name = 'Sheet1!A1'
            start_row = 1
        else:
            existing_data = result['values']
            headers = existing_data[0]
            
            # Find Month and Year column indices
            try:
                month_idx = headers.index('Month')
                year_idx = headers.index('Year')
            except ValueError:
                raise ValueError("Could not find Month or Year columns in existing sheet")
            
            # Check for existing entries with same month/year
            duplicate_found = False
            for row in existing_data[1:]:
                if (len(row) > max(month_idx, year_idx) and 
                    row[month_idx] == new_month and 
                    str(row[year_idx]) == str(new_year)):
                    duplicate_found = True
                    break
            
            if duplicate_found:
                print(f"Data for {new_month} {new_year} already exists in sheet. Skipping update.")
                return False
            
            print(f"No existing data found for {new_month} {new_year}. Proceeding with update...")
            values = [[clean_value_for_sheets(value) for value in row] for row in df.values.tolist()]
            start_row = len(existing_data) + 1
            range_name = f'Sheet1!A{start_row}'
        
        # Update the sheet with the new data
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='RAW',
            body={'values': values}
        ).execute()
        
        print(f"Updated Google Sheet: {result.get('updatedCells')} cells updated at {range_name}")
        
        # Apply currency formatting to Net and Royalties columns
        try:
            # Find the indices for Net and Royalties columns
            net_idx = df.columns.get_loc('Net')
            royalties_idx = df.columns.get_loc('Royalties')
            
            # Convert column indices to A1 notation
            net_column = chr(65 + net_idx)  # 65 is ASCII for 'A'
            royalties_column = chr(65 + royalties_idx)
            
            # Create formatting request
            requests = []
            for column, column_letter in [(net_idx, net_column), (royalties_idx, royalties_column)]:
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': 0,  # Sheet1 is usually ID 0
                            'startColumnIndex': column,
                            'endColumnIndex': column + 1,
                            'startRowIndex': 1  # Skip header row
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
            
            # Apply the formatting
            body = {'requests': requests}
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()
            
            print(f"Applied currency formatting to columns {net_column} and {royalties_column}")
            
        except Exception as e:
            print(f"Warning: Could not apply currency formatting: {str(e)}")
        
        return True
        
    except HttpError as error:
        print(f"An error occurred: {error}")
        print("\nDebug: Error details:")
        print(f"Spreadsheet ID: {spreadsheet_id}")
        raise
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise
            
def clean_value_for_checks(value):
    """Clean and convert values to be compatible with Google Sheets."""
    if pd.isna(value):
        return ""
    elif isinstance(value, (int, float)):
        return value if not pd.isna(value) else ""
    else:
        return str(value).strip()

def get_last_month_dates():
    today = date.today()
    first_of_this_month = today.replace(day=1)
    last_month_end = first_of_this_month - relativedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    
    return last_month_start.strftime('%Y-%m-%d'), last_month_end.strftime('%Y-%m-%d')

def encrypt(text, key):
    return base64.urlsafe_b64encode(''.join(chr(ord(c) ^ ord(k)) for c, k in zip(text, key * len(text))).encode()).decode()

def decrypt(text, key):
    return ''.join(chr(ord(c) ^ ord(k)) for c, k in zip(base64.urlsafe_b64decode(text).decode(), key * len(text)))

def read_credentials(file_path, key):
    with open(file_path, 'r') as file:
        username = file.readline().strip()
        encrypted_password = file.readline().strip()
    
    password = decrypt(encrypted_password, key)
    return username, password

def write_credentials(file_path, username, password, key):
    encrypted_password = encrypt(password, key)
    with open(file_path, 'w') as file:
        file.write(f"{username}\n{encrypted_password}")

def process_sales_table(html_content, month, year):
    # Parse the HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    
    print("\nDebugging table content:")
    print(html_content)
    
    # Get all rows
    rows = soup.find_all('tr')
    print(f"\nFound {len(rows)} rows in the table")
    
    if len(rows) < 2:  # Need at least header and one data row
        print("Not enough rows found in table")
        return None
    
    # Print first few rows for debugging
    for i, row in enumerate(rows[:3]):
        print(f"\nRow {i} contains {len(row.find_all('td'))} columns:")
        print(row.prettify())
    
    # Skip header row, process until the total row
    data_rows = []
    for row in rows[1:]:  # Skip header
        cols = row.find_all('td')
        if len(cols) != 7:  # If not a standard data row
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
        except Exception as e:
            print(f"Error processing row: {e}")
            print("Row content:")
            print(row.prettify())
            continue
    
    if not data_rows:
        print("No valid data rows found")
        return None
        
    # Create DataFrame
    df = pd.DataFrame(data_rows)
    
    # Add Month and Year columns
    df['Month'] = calendar.month_name[month]
    df['Year'] = year
    
    # Reorder columns to put Month and Year first
    columns_order = ['Month', 'Year', 'Publisher', 'Title', 'SKU', 'Units_Sold', 
                     'Net', 'Royalty_Rate', 'Royalties']
    df = df[columns_order]
    
    return df

def fetch_dmsguild_royalties(username, password):
    # Calculate date range for last month
    start_date, end_date = get_last_month_dates()
    print(f"Fetching royalty report for date range: {start_date} to {end_date}")
    
    # Set up Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    
    # Set up the webdriver with Chrome options
    driver = webdriver.Chrome(options=chrome_options)
    try:
        # Navigate to the login page
        driver.get("https://www.dmsguild.com/login.php")

        # click the login link to get to the login modal for existing users
        login_link = driver.find_element(By.CSS_SELECTOR, "a.login_window")
        driver.execute_script("arguments[0].click();", login_link)
        
        username_field = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "login_email_address"))
        )
        print("Found email address field")
    
        password_field = driver.find_element(By.ID, "login_password")
        print("Found password field")

        # Fill in the username and password
        username_field.send_keys(username)
        password_field.send_keys(password)

        # Find and click the login button
        login_button = driver.find_element(By.ID, "loginbutton")
        login_button.click()

        # Wait for login to complete and find the Account link
        account_link = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "a.nav-bar-link[href='https://www.dmsguild.com/account.php']"))
        )
        print("Found Account link, clicking...")
        account_link.click()

        # Wait for the account page to load and find the Royalty Report link
        print("Looking for Royalty Report link...")
        royalty_link = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "a[href='https://www.dmsguild.com/royalty_report.php']"))
        )
        print("Found Royalty Report link, clicking...")
        royalty_link.click()

        # Wait for page load
        time.sleep(3)
        print("Setting date range...")

        # Find and fill in the date fields using explicit waits
        start_date_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "startdate"))
        )
        end_date_field = driver.find_element(By.NAME, "enddate")

        print("Found date fields, clearing existing values...")
        # Clear existing values
        start_date_field.clear()
        end_date_field.clear()

        print("Entering new dates...")
        # Enter new dates using JavaScript
        driver.execute_script(f"arguments[0].value = '{start_date}';", start_date_field)
        driver.execute_script(f"arguments[0].value = '{end_date}';", end_date_field)

        # Trigger change events
        driver.execute_script("arguments[0].dispatchEvent(new Event('change'));", start_date_field)
        driver.execute_script("arguments[0].dispatchEvent(new Event('change'));", end_date_field)

        print("Looking for submit button...")
        # Find and click the submit button
        submit_button = driver.find_element(By.NAME, "submit_report")
        print("Found submit button, clicking...")
        submit_button.click()

        print("Report submitted!")

        # Wait for results table to appear and be populated
        print("Waiting for results table to load...")
        
        # First wait for any existing table to be cleared/removed (in case there was one)
        time.sleep(2)
        
        # Then wait for the new table with results
        table = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table[cellpadding='5'][cellspacing='0'][border='1']"))
        )
        
        # Additional wait to ensure table is fully populated
        time.sleep(2)
        
        # Also wait for at least one data row to appear
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "standardText"))
        )

        print("Table loaded, extracting data...")
        
        # Get the HTML content
        table_html = table.get_attribute('outerHTML')
        
        # Save the HTML content for debugging
        with open("table_debug.html", "w", encoding="utf-8") as f:
            f.write(table_html)
        print("Saved table HTML to table_debug.html")
        
        # Get month and year from the date range
        report_date = datetime.strptime(start_date, '%Y-%m-%d')
        month = report_date.month
        year = report_date.year

        # Process the table into a DataFrame
        df = process_sales_table(table_html, month, year)
        
        if df is not None:
            print("\nExtracted data into DataFrame:")
            print(df)
        else:
            print("Failed to create DataFrame from table data")
        
        input("Press Enter to continue...")
        return df

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        print("Taking error screenshot...")
        driver.save_screenshot("error_screenshot.png")
        raise
    finally:
        driver.quit()

def get_report_filepath(output_dir="reports"):
    """Generate the expected filepath for the current month's report."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    timestamp = datetime.now().strftime("%Y%m")
    filename = f"dmsguild_report_{timestamp}.csv"
    return os.path.join(output_dir, filename)

def load_existing_report(filepath):
    """Load an existing report if it exists."""
    try:
        if os.path.exists(filepath):
            print(f"Found existing report at: {filepath}")
            return pd.read_csv(filepath)
        return None
    except Exception as e:
        print(f"Error loading existing report: {str(e)}")
        return None

def save_to_local_file(df, filepath):
    """Save DataFrame to a local CSV file."""
    df.to_csv(filepath, index=False)
    print(f"Report saved to: {filepath}")
    return filepath

def clean_value_for_sheets(value):
    """Clean and convert values to be compatible with Google Sheets."""
    if pd.isna(value):
        return ""
    elif isinstance(value, (int, float)):
        return value if not pd.isna(value) else ""
    else:
        return str(value).strip()

# Optional: Add a function to verify data before sending
def verify_data_for_sheets(df):
    """Verify that DataFrame contains valid data for Google Sheets."""
    issues = []
    
    # Check for NaN values
    nan_counts = df.isna().sum()
    if nan_counts.any():
        issues.extend([f"Column '{col}' has {count} NaN values" 
                      for col, count in nan_counts.items() if count > 0])
    
    # Check data types
    for col in df.columns:
        if df[col].dtype not in [np.int64, np.float64, object]:
            issues.append(f"Column '{col}' has unusual dtype: {df[col].dtype}")
    
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
    
    # Google Sheets configuration
    GOOGLE_SHEETS_CREDENTIALS = "arctic-sign-398401-5f09044d1b14.json"
    SPREADSHEET_ID = "1mtdp0DCDFWEVJPlb44MuNdqJnZCV9IDhPbFnDyj1G1Q"
    
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
        except Exception as e:
            print(f"Error updating Google Sheet: {str(e)}")