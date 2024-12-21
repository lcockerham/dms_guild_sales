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
from fetch_dmsguild_royalties import handle_error

def load_existing_data(filename='dmsguild_products.csv'):
    """Load existing product data if available."""
    if os.path.exists(filename):
        try:
            df = pd.read_csv(filename)
            print(f"Loaded {len(df)} existing product records")
            return df
        except Exception as e:
            print(f"Error loading existing data: {e}")
    return pd.DataFrame(columns=['name', 'metal', 'date_added', 'url', 'rating', 'edition', 'authors', 'artists', 'pages', 'price', 'ratings_count'])

def fetch_product_data():
    print("Fetching product data...")
    """Main function to fetch royalty reports."""
     # Load existing data
    output_file = 'dmsguild_products.csv'
    df = load_existing_data(output_file)
    # Set up Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--headless")  # For newer versions

    # Set up the webdriver with Chrome options
    driver = webdriver.Chrome(options=chrome_options)

    try:
        driver = navigate_to_classics_page(driver)
        
        while True:
        # Process products on current page
            navigate_products(driver, df, output_file)
            
            # Try to find next page link
            try:
                next_link = driver.find_element(By.XPATH, "//a[contains(text(),'[Next >>]')]")
                next_link.click()
                time.sleep(5)  # Wait for new page to load
            except:
                print("No more pages to process")
                break
        #table_html = extract_table_data(driver)

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

def navigate_to_classics_page(driver):
    """Navigate to the product page."""
    driver.get("https://www.dmsguild.com/browse.php?filters=45471_0_0_0_0_0_0_0&src=fid45471&page=27")
    # time.sleep(1)
    # dnd_classics_link = WebDriverWait(driver, 10).until(
    #     EC.element_to_be_clickable(
    #         (By.PARTIAL_LINK_TEXT, "Classics")
    #     )
    # )
    # dnd_classics_link.click()
    time.sleep(1)
    return driver


def navigate_products(driver, existing_df, output_file='dmsguild_products.csv'):
    """Navigate through each product link, scrape info, then return to list."""
    
    # Find all product links
    product_links = WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located(
            (By.CLASS_NAME, "product_listing_link")
        )
    )

    # Store unique URLs
    product_urls = sorted(list(set(link.get_attribute('href') for link in product_links)))
    
    # Filter out already processed URLs
    processed_urls = set(existing_df['url']) if not existing_df.empty else set()
    new_urls = [url for url in product_urls if url not in processed_urls]
    
    print(f"Found {len(new_urls)} new products to process")

    # Process each product
    for i, url in enumerate(new_urls):
        try:
            print(f"\nProcessing product {i+1} of {len(new_urls)}: {url}")
            
            # Navigate to product page
            driver.get(url)
            
            # Get product info
            product_info = get_product_info(driver)
            product_info['url'] = url
            
            # Add to existing DataFrame
            new_row = pd.DataFrame([product_info])
            existing_df = pd.concat([existing_df, new_row], ignore_index=True)
            
            # Save after each product
            existing_df.to_csv(output_file, index=False)
            print(f"Saved progress to {output_file}")
            
        except Exception as e:
            print(f"Error processing product at {url}: {str(e)}")
            continue

    return existing_df

def get_product_info(driver):
    print("Getting product info...")
    """Scrape specific product information."""
    product_data = {}
    time.sleep(3) #throttling how quickly we scrape
    
    try:
        # Get product name
        name_element = driver.find_element(By.XPATH, "//span[@itemprop='name']")
        product_data['name'] = name_element.text
    except:
        product_data['name'] = None
        print("Could not find product name")
    
    try:
        # Get metal status (if exists)
        metal_element = driver.find_element(By.XPATH, "//img[contains(@alt, 'seller')]")
        product_data['metal'] = metal_element.get_attribute('alt')
    except:
        product_data['metal'] = None
        print("Could not find metal status")
    
    try:
        # Get date added
        date_element = driver.find_element(By.XPATH, "//div[contains(@class, 'widget-information-item-content') and contains(text(), 'added to our catalog')]")
        date_text = date_element.text
        # Extract date using string manipulation
        date_str = date_text.split('added to our catalog on ')[1].replace('.', '')
        # Convert to datetime object
        date_obj = datetime.strptime(date_str, '%B %d, %Y')
        product_data['date_added'] = date_obj
    except:
        product_data['date_added'] = None
        print("Could not find date added")
    
    try:
        # Get product rating
        rating_element = driver.find_element(By.ID, "product-rate-score-value")
        product_data['rating'] = float(rating_element.get_attribute('value'))
        print(f"Found rating: {product_data['rating']}")
    except Exception as e:
        product_data['rating'] = None
        print(f"Could not find product rating: {str(e)}")

    try:
        # Get edition. We are just going to grab the first one listed for simplicity.
        edition_element = driver.find_element(By.XPATH, "//div[@class='widget-information-item-title'][contains(text(), 'Rules Edition')]/following-sibling::div//ul[@class='rules-system-list']/li/a")
        product_data['edition'] = edition_element.text
        print(f"Found edition: {product_data['edition']}")
    except Exception as e:
        product_data['edition'] = None
        print(f"Could not find product edition: {str(e)}")

    try:
        # Get authors and artists
        author_elements = driver.find_elements(By.XPATH, "//div[@class='widget-information-item-title'][contains(text(), 'Author')]/following-sibling::div//a")
        artist_elements = driver.find_elements(By.XPATH, "//div[@class='widget-information-item-title'][contains(text(), 'Artist')]/following-sibling::div//a")
        
        product_data['authors'] = [author.text for author in author_elements]
        product_data['artists'] = [artist.text for artist in artist_elements]
        
        print(f"Found authors: {product_data['authors']}")
        print(f"Found artists: {product_data['artists']}")
    except Exception as e:
        product_data['authors'] = None
        product_data['artists'] = None
        print(f"Could not find authors/artists: {str(e)}")

    try:
        # Get page count
        pages_element = driver.find_element(By.XPATH, "//div[@class='widget-information-item-title'][contains(text(), 'Pages')]/following-sibling::div[@class='widget-information-item-content']")
        product_data['pages'] = int(pages_element.text.strip())
        print(f"Found pages: {product_data['pages']}")
    except Exception as e:
        product_data['pages'] = None
        print(f"Could not find page count: {str(e)}")

    try:
        # Get price
        price_element = driver.find_element(By.ID, "product-price-strike")
        # Remove $ and convert to float
        price_str = price_element.text.replace('$', '').strip()
        product_data['price'] = float(price_str)
        print(f"Found price: ${product_data['price']}")
    except Exception as e:
        product_data['price'] = None
        print(f"Could not find price: {str(e)}")

    try:
        # Get ratings count from meta tag
        ratings_element = driver.find_element(By.XPATH, "//meta[@itemprop='reviewCount']")
        product_data['ratings_count'] = int(ratings_element.get_attribute('content'))
        print(f"Found ratings count: {product_data['ratings_count']}")
    except Exception as e:
        product_data['ratings_count'] = None
        print(f"Could not find ratings count: {str(e)}")

    return product_data

def test_product_scraping(url):
    """Test product info scraping on a single URL."""
    print(f"Testing product scraping on: {url}")
    
    # Set up Chrome driver
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        # Navigate to URL
        driver.get(url)
        time.sleep(3)  # Wait for page to load
        
        # Get product info
        product_info = get_product_info(driver)
        
        # Print results
        print("\nScraped Data:")
        print("-" * 50)
        for key, value in product_info.items():
            print(f"{key}: {value}")
            
    except Exception as e:
        print(f"Error during testing: {str(e)}")
    finally:
        driver.quit()

if __name__ == "__main__":
    fetch_product_data()
    #test_url = "https://www.dmsguild.com/product/121306/DD-Rules-Compendium-4e?filters=45471_0_0_0_0_0_0_0"  # Replace with your test URL
    #test_product_scraping(test_url)
