from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import urlparse
import time
import re
import random
import csv
from datetime import datetime
import os
import math

# Constants
MAX_MERCHANTS = 1000
RESULTS_PER_PAGE = 20
CHUNK_SIZE = 10
DELAY_BETWEEN_PAGES = (2, 5)
DELAY_BETWEEN_BUSINESSES = (1, 3)
DELAY_BETWEEN_CHUNKS = (15, 30)
RETRY_ATTEMPTS = 3
MAX_SCROLLS = 50

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
]

PROXIES = []

def get_short_url(long_url):
    parsed = urlparse(long_url)
    if "/place/" in parsed.path:
        return f"https://www.google.com{parsed.path}"
    return long_url

def extract_phone_number(driver):
    try:
        phone_element = driver.find_element(By.XPATH, '//button[@data-tooltip="Copy phone number"]')
        return phone_element.get_attribute('aria-label').split(': ')[1]
    except:
        pass
    try:
        phone_button = driver.find_element(By.XPATH, '//button[contains(@aria-label, "Phone:")]')
        return phone_button.get_attribute("aria-label").split(": ")[1]
    except:
        pass
    try:
        contact_section = driver.find_element(By.XPATH, '//div[contains(text(), "Phone")]/following-sibling::div')
        return contact_section.text
    except:
        pass
    try:
        contact_elements = driver.find_elements(By.XPATH, '//div[contains(@class, "Io6YTe")]')
        phone_pattern = re.compile(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}')
        for el in contact_elements:
            text = el.text.strip()
            if phone_pattern.search(text) and not any(w in text.lower() for w in ['street', 'ave', 'road', 'st']):
                return text
    except:
        pass
    return "N/A"

def get_random_user_agent():
    return random.choice(USER_AGENTS)

def get_random_proxy():
    if PROXIES:
        return random.choice(PROXIES)
    return None

def initialize_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument(f'user-agent={get_random_user_agent()}')
    proxy = get_random_proxy()
    if proxy:
        options.add_argument(f'--proxy-server={proxy}')
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def random_delay(min_seconds, max_seconds):
    time.sleep(random.uniform(min_seconds, max_seconds))

def save_chunk_to_csv(results, filename, write_header=False):
    if not results:
        return
    mode = 'w' if write_header else 'a'
    keys = results[0].keys()
    with open(filename, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        if write_header:
            writer.writeheader()
        writer.writerows(results)

def collect_business_links(query, num_merchants, progress_callback=None):
    driver = initialize_driver()
    wait = WebDriverWait(driver, 30)
    search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
    driver.get(search_url)
    
    try:
        wait.until(EC.presence_of_element_located((By.XPATH, '//div[@role="feed"]')))
    except TimeoutException:
        driver.quit()
        if progress_callback:
            progress_callback(0, "Failed to load search results")
        return []
    
    business_links, seen_links = [], set()
    scroll_attempts = 0
    last_count = 0
    container = driver.find_element(By.XPATH, '//div[@role="feed"]')
    
    while len(business_links) < num_merchants and scroll_attempts < MAX_SCROLLS:
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", container)
        time.sleep(3)
        
        try:
            result_cards = wait.until(EC.presence_of_all_elements_located(
                (By.XPATH, '//a[contains(@class, "hfpxzc")]')))
        except StaleElementReferenceException:
            result_cards = driver.find_elements(By.XPATH, '//a[contains(@class, "hfpxzc")]')
        
        if len(result_cards) <= last_count:
            scroll_attempts += 1
        last_count = len(result_cards)
        
        for card in result_cards:
            try:
                link = card.get_attribute('href')
                if link and link not in seen_links:
                    business_links.append(link)
                    seen_links.add(link)
                    if progress_callback and len(business_links) % 5 == 0:
                        progress = min(20, int(20 * len(business_links) / num_merchants))
                        progress_callback(progress, f"Collected {len(business_links)}/{num_merchants} links")
                    if len(business_links) >= num_merchants:
                        break
            except StaleElementReferenceException:
                continue
        
        random_delay(*DELAY_BETWEEN_PAGES)
    
    driver.quit()
    return business_links[:num_merchants]

def scrape_business_details(links, progress_callback=None):
    results = []
    driver = None
    total = len(links)
    
    for index, link in enumerate(links):
        if index % CHUNK_SIZE == 0:
            if driver:
                driver.quit()
            driver = initialize_driver()
            wait = WebDriverWait(driver, 25)
        
        for attempt in range(RETRY_ATTEMPTS):
            try:
                driver.get(link)
                time.sleep(random.uniform(1.0, 2.5))
                
                try:
                    name = wait.until(EC.visibility_of_element_located(
                        (By.XPATH, '//h1[contains(@class, "DUwDvf")]'))).text
                except:
                    name = "N/A"
                
                phone = extract_phone_number(driver)
                short_link = get_short_url(driver.current_url)
                
                results.append({
                    "Name": name,
                    "Phone": phone,
                    "Link": short_link
                })
                
                if progress_callback:
                    progress = 20 + int(80 * (index + 1) / total)
                    message = f"Scraped {index + 1}/{total} businesses"
                    progress_callback(progress, message)
                
                break
            except Exception as e:
                if attempt < RETRY_ATTEMPTS - 1:
                    time.sleep(random.uniform(3, 8))
                else:
                    results.append({
                        "Name": "ERROR",
                        "Phone": "ERROR",
                        "Link": link
                    })
                    if progress_callback:
                        progress = 20 + int(80 * (index + 1) / total)
                        message = f"Failed to scrape business {index + 1}/{total}"
                        progress_callback(progress, message)
        
        if index < len(links) - 1:
            random_delay(*DELAY_BETWEEN_BUSINESSES)
    
    if driver:
        driver.quit()
    return results

def run_scraper(query, num_merchants, progress_callback=None):
    # Initialize progress
    if progress_callback:
        progress_callback(0, "Starting scraping process...")
    
    query = query.strip()
    if not query:
        return None, "Invalid query."
    
    try:
        num_merchants = min(max(int(num_merchants), 1), MAX_MERCHANTS)
    except:
        num_merchants = 20
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"output/google_maps_businesses_{timestamp}.csv"
    
    # Update progress
    if progress_callback:
        progress_callback(10, "Collecting business links...")
    
    business_links = collect_business_links(query, num_merchants, progress_callback)
    
    if not business_links:
        if progress_callback:
            progress_callback(100, "No business links found")
        return None, "No business links found."
    
    if progress_callback:
        progress_callback(20, f"Found {len(business_links)} businesses. Starting scraping...")
    
    chunks = [business_links[i:i + CHUNK_SIZE] for i in range(0, len(business_links), CHUNK_SIZE)]
    all_results = []
    total_chunks = len(chunks)
    
    for i, chunk in enumerate(chunks):
        if progress_callback:
            progress = 20 + int(10 * (i / total_chunks))
            message = f"Processing chunk {i+1}/{total_chunks} ({len(chunk)} businesses)"
            progress_callback(progress, message)
        
        chunk_results = scrape_business_details(chunk, progress_callback)
        all_results.extend(chunk_results)
        save_chunk_to_csv(chunk_results, output_filename, write_header=(i == 0))
        
        if i < len(chunks) - 1:
            time.sleep(random.randint(*DELAY_BETWEEN_CHUNKS))
    
    if progress_callback:
        progress_callback(100, f"Scraping completed! Saved {len(all_results)} businesses")
    
    return output_filename, f"Scraped {len(all_results)} businesses!"