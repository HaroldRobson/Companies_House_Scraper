import pandas as pd
import requests
from bs4 import BeautifulSoup
from googlesearch import search as google_search
from urllib.parse import urlparse, urljoin
import time
import re
from collections import deque
import logging
import re

# --- Configuration ---
INPUT_CSV_NAME = 'CH_MLB.csv'
OUTPUT_CSV_NAME = 'CH_MLB_with_domains_emails_v3.csv' # Updated output name
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
REQUEST_TIMEOUT = 10  # seconds
DELAY_BETWEEN_GOOGLE_SEARCHES = 5 # seconds
DELAY_BETWEEN_PAGE_REQUESTS = 1 # seconds
MAX_PAGES_TO_CRAWL_PER_SITE = 2  # <<<< MODIFIED: Check at most two pages
MAX_GOOGLE_RESULTS_TO_CHECK = 5
COMPANIES_HOUSE_DOMAIN = "find-and-update.company-information.service.gov.uk"
MAX_COMPANIES_TO_PROCESS = 150
# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Helper Functions ---

def get_domain_from_google(company_name):
    """
    Searches Google for 'company_name UK' and returns the domain and full URL
    of the first result that is not a Companies House website.
    """
    query = f"{company_name} UK"
    logging.info(f"Googling for: {query}")
    try:
        search_results_iterator = google_search(
            query,
            num_results=MAX_GOOGLE_RESULTS_TO_CHECK,
            lang="en"
        )

        results_checked = 0
        for url in search_results_iterator:
            if results_checked >= MAX_GOOGLE_RESULTS_TO_CHECK:
                break
            results_checked += 1

            parsed_url = urlparse(url)
            domain_netloc = parsed_url.netloc.lower()

            current_domain_standardized = domain_netloc
            if current_domain_standardized.startswith('www.'):
                current_domain_standardized = current_domain_standardized[4:]

            if "compan" in current_domain_standardized or "gazette" in current_domain_standardized or "guide" in current_domain_standardized:
                logging.info(f"Skipping Companies House / company platform link: {url}")
                continue

            domain_to_return = domain_netloc
            if domain_to_return.startswith('www.'):
                domain_to_return = domain_to_return[4:]
            
            logging.info(f"Found potential domain: {domain_to_return} from {url}")
            return domain_to_return, url
        
        logging.warning(f"No suitable non-Companies House website found in the top {results_checked} Google results for {company_name}.")
        return None, None

    except Exception as e:
        logging.error(f"Error during Google search for {company_name}: {e}")
        return None, None
    finally:
        time.sleep(DELAY_BETWEEN_GOOGLE_SEARCHES)


def scrape_site_for_email_context(start_url, base_domain):
    """Crawls a website (max MAX_PAGES_TO_CRAWL_PER_SITE pages) starting from start_url,
       staying within base_domain, and extracts valid email candidates based on specific criteria."""
    if not start_url or not base_domain:
        return []

    urls_to_visit = deque([start_url])
    visited_urls = set()
    email_contexts = set()  # Stores validated email candidates
    pages_crawled = 0

    headers = {'User-Agent': USER_AGENT} # Assuming USER_AGENT is defined globally

    # --- Constants for email extraction ---
    EMAIL_REGEX_PATTERN = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
    BOUNDARY_DELIMITERS = [' ', ':', ',', '.', '(', ')', '[', ']', '<', '>', '"', "'"] # Added more common delimiters
    MAX_EMAIL_CANDIDATE_LENGTH = 30
    # ---

    logging.info(f"Starting crawl for {base_domain}, max {MAX_PAGES_TO_CRAWL_PER_SITE} page(s).") # Assuming MAX_PAGES_TO_CRAWL_PER_SITE is global

    while urls_to_visit and pages_crawled < MAX_PAGES_TO_CRAWL_PER_SITE: # Assuming MAX_PAGES_TO_CRAWL_PER_SITE is global
        current_url = urls_to_visit.popleft()

        if current_url in visited_urls:
            continue

        logging.info(f"Crawling page {pages_crawled + 1}/{MAX_PAGES_TO_CRAWL_PER_SITE}: {current_url}") # Assuming MAX_PAGES_TO_CRAWL_PER_SITE is global
        visited_urls.add(current_url)
        pages_crawled += 1

        try:
            response = requests.get(current_url, headers=headers, timeout=REQUEST_TIMEOUT, allow_redirects=True) # Assuming REQUEST_TIMEOUT is global
            response.raise_for_status()

            final_url_netloc = urlparse(response.url).netloc.lower()
            if final_url_netloc.startswith('www.'):
                final_url_netloc = final_url_netloc[4:]

            if final_url_netloc != base_domain:
                logging.warning(f"Response URL {response.url} (domain: {final_url_netloc}) is off base domain {base_domain}. Skipping content and links from this page.")
                time.sleep(DELAY_BETWEEN_PAGE_REQUESTS) # Assuming DELAY_BETWEEN_PAGE_REQUESTS is global
                continue

            content_type = response.headers.get('Content-Type', '').lower()
            if 'html' not in content_type:
                logging.info(f"Skipping non-HTML content at {current_url} (type: {content_type})")
                time.sleep(DELAY_BETWEEN_PAGE_REQUESTS) # Assuming DELAY_BETWEEN_PAGE_REQUESTS is global
                continue

            soup = BeautifulSoup(response.content, 'html.parser')

            # Find all text nodes that contain an "@" symbol
            texts_with_at_symbol = soup.find_all(string=lambda text: text and "@" in text)

            for text_node in texts_with_at_symbol:
                # Get the full text content of the node, clean whitespace
                full_text_content = ' '.join(text_node.strip().split())
                if not full_text_content:
                    continue

                # Iterate through potential email matches in the text content
                for match in re.finditer(EMAIL_REGEX_PATTERN, full_text_content):
                    email_candidate = match.group(0)
                    start_index = match.start(0)
                    end_index = match.end(0)

                    # 1. Check length limit
                    if len(email_candidate) > MAX_EMAIL_CANDIDATE_LENGTH:
                        continue

                    # 2. Check left boundary
                    is_left_boundary_ok = False
                    if start_index == 0: # Email candidate is at the beginning of the text
                        is_left_boundary_ok = True
                    else:
                        left_char = full_text_content[start_index - 1]
                        if left_char in BOUNDARY_DELIMITERS:
                            is_left_boundary_ok = True
                    
                    if not is_left_boundary_ok:
                        continue

                    # 3. Check right boundary
                    is_right_boundary_ok = False
                    if end_index == len(full_text_content): # Email candidate is at the end of the text
                        is_right_boundary_ok = True
                    else:
                        right_char = full_text_content[end_index]
                        if right_char in BOUNDARY_DELIMITERS:
                            is_right_boundary_ok = True
                    
                    # If all conditions are met, add the validated email candidate
                    if is_right_boundary_ok:
                        email_contexts.add(email_candidate)
                        logging.debug(f"Found potential email: {email_candidate} in {current_url}")


            # Only add new links if we haven't reached the page limit for adding to queue
            if pages_crawled < MAX_PAGES_TO_CRAWL_PER_SITE: # Assuming MAX_PAGES_TO_CRAWL_PER_SITE is global
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    joined_url = urljoin(current_url, href)
                    parsed_joined_url = urlparse(joined_url)

                    if parsed_joined_url.scheme in ['http', 'https']:
                        link_netloc = parsed_joined_url.netloc.lower()
                        if link_netloc.startswith('www.'):
                            link_netloc = link_netloc[4:]
                        
                        if link_netloc == base_domain and joined_url not in visited_urls and joined_url not in urls_to_visit:
                            urls_to_visit.append(joined_url)
            
            time.sleep(DELAY_BETWEEN_PAGE_REQUESTS) # Assuming DELAY_BETWEEN_PAGE_REQUESTS is global

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching {current_url}: {e}")
            time.sleep(DELAY_BETWEEN_PAGE_REQUESTS) # Assuming DELAY_BETWEEN_PAGE_REQUESTS is global
        except Exception as e:
            logging.error(f"Unexpected error processing {current_url}: {e}")
            time.sleep(DELAY_BETWEEN_PAGE_REQUESTS) # Assuming DELAY_BETWEEN_PAGE_REQUESTS is global

    if email_contexts:
        logging.info(f"Found {len(email_contexts)} unique potential email addresses on {base_domain} (from {pages_crawled} page(s) crawled).")
    else:
        logging.info(f"No potential email addresses found matching criteria on {base_domain} (from {pages_crawled} page(s) crawled).")
        
    return list(email_contexts)


# --- Main Script ---
if __name__ == "__main__":
    try:
        df = pd.read_csv(INPUT_CSV_NAME)
    except FileNotFoundError:
        logging.error(f"Input file '{INPUT_CSV_NAME}' not found.")
        print(f"Creating a dummy '{INPUT_CSV_NAME}' for demonstration.")
        dummy_data = {
            'Company Name': ['Acme Corp Ltd', 'Beta Solutions Inc', 'Gamma Innovations', 'NonExistent Company XYZ', 'HM Revenue & Customs'],
            'OtherData': [1,2,3,4,5]
            }
        df = pd.DataFrame(dummy_data)
        df.to_csv(INPUT_CSV_NAME, index=False)
        # exit()

    if 'Company Name' not in df.columns:
        logging.error(f"'Company Name' column not found in '{INPUT_CSV_NAME}'.")
        exit()
    if MAX_COMPANIES_TO_PROCESS and MAX_COMPANIES_TO_PROCESS > 0 and MAX_COMPANIES_TO_PROCESS < len(df):
        logging.info(f"Limiting processing to the first {MAX_COMPANIES_TO_PROCESS} companies.")
        df = df.head(MAX_COMPANIES_TO_PROCESS)
    else:
        logging.info(f"Processing all {len(df)} companies.")
    # <<<< End of new section >>>>

    company_domains = []
    company_email_contexts = []

    # For testing with a subset:
    # df = df.head(3) 
    # logging.info(f"Processing {len(df)} companies (subset for testing).")

    for index, row in df.iterrows():
        company_name = str(row['Company Name']).strip()
        if not company_name:
            logging.warning(f"Skipping row {index+1} due to empty Company Name.")
            company_domains.append("")
            company_email_contexts.append("")
            continue
            
        logging.info(f"\n--- Processing Company: {company_name} ({index + 1}/{len(df)}) ---")

        domain, start_url = get_domain_from_google(company_name)
        
        if domain and start_url:
            company_domains.append(domain)
            contexts = scrape_site_for_email_context(start_url, domain)
            company_email_contexts.append("; ".join(contexts) if contexts else "")
        else:
            company_domains.append("")
            company_email_contexts.append("")
            # logging.warning(f"Could not determine valid domain for {company_name} or no email contexts found.") # This log might be redundant if get_domain_from_google already logged

    df['company_domain'] = company_domains
    df['company_email'] = company_email_contexts

    try:
        df.to_csv(OUTPUT_CSV_NAME, index=False, encoding='utf-8')
        logging.info(f"\nSuccessfully processed all companies. Output saved to '{OUTPUT_CSV_NAME}'")
    except Exception as e:
        logging.error(f"Error saving output CSV: {e}")
