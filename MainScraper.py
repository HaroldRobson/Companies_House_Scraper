import csv
import time
import re
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By # Not explicitly used, but good to have if needed
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# --- Configuration ---
SEARCH_CRITERIA_TAG = "adv_manchester_sic28150_incorp_any_diss_any"
BASE_URL = "https://find-and-update.company-information.service.gov.uk"
# This is your base search URL. Page numbers will be appended to this.
SEARCH_URL_BASE = "https://find-and-update.company-information.service.gov.uk/advanced-search/get-results?companyNameIncludes=&companyNameExcludes=&registeredOfficeAddress=Manchester&incorporationFromDay=&incorporationFromMonth=&incorporationFromYear=&incorporationToDay=&incorporationToMonth=&incorporationToYear=&sicCodes=28150&dissolvedFromDay=&dissolvedFromMonth=&dissolvedFromYear=&dissolvedToDay=&dissolvedToMonth=&dissolvedToYear="
MAX_COMPANIES = 104 # Adjust as needed
OUTPUT_CSV_FILE = f"companies_house_{SEARCH_CRITERIA_TAG.replace(' ','_')}.csv"

# --- Helper Functions ---
def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36")
    # chrome_options.add_argument("--headless")
    try:
        print("Attempting to launch undetected-chromedriver...")
        driver = uc.Chrome(options=chrome_options, use_subprocess=True)
        print("undetected-chromedriver launched successfully.")
    except Exception as e:
        print(f"Error launching undetected-chromedriver: {e}")
        print("Please ensure Google Chrome is installed/updated and 'undetected-chromedriver' is current.")
        print("Try specifying 'version_main' in uc.Chrome() or closing all Chrome instances.")
        raise
    return driver

def extract_email_from_text(text):
    match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', text)
    return match.group(0) if match else "Not found"

# --- Main Scraping Logic ---
def scrape_companies_house():
    driver = setup_driver()
    companies_data = []
    processed_company_links = set()

    print(f"Starting scrape using base advanced search URL: {SEARCH_URL_BASE}")
    print(f"Output will be saved to: {OUTPUT_CSV_FILE}")

    page_number = 1 # Start with page 1

    try:
        while len(companies_data) < MAX_COMPANIES:
            current_url = SEARCH_URL_BASE
            if page_number > 1:
                # Append page number for subsequent pages.
                # SEARCH_URL_BASE already has query params, so we use '&'
                current_url = f"{SEARCH_URL_BASE}&page={page_number}"

            print(f"\nFetching search results (Page {page_number}): {current_url}")
            driver.get(current_url)
            time.sleep(5) # Allow page to load fully

            page_content = driver.page_source
            # For debugging specific pages:
            # with open(f"debug_search_page_{page_number}.html", "w", encoding="utf-8") as f:
            #     f.write(page_content)
            soup = BeautifulSoup(page_content, 'html.parser')

            if "prove you are not a robot" in page_content.lower() or "enter characters" in page_content.lower():
                print(f"CAPTCHA detected on search page {page_number}. Please solve manually in the browser.")
                input("Press Enter in this console after solving CAPTCHA to continue...")
                page_content = driver.page_source # Re-fetch after solving
                soup = BeautifulSoup(page_content, 'html.parser')

            search_result_items = []
            # --- Primary attempt: Table structure ---
            search_results_table = soup.find('table', class_='govuk-table')
            if search_results_table and search_results_table.find('tbody'):
                # print(f"DEBUG: Page {page_number}: Found results table with 'govuk-table' class and 'tbody'.")
                candidate_cells = search_results_table.select('tbody > tr > td.govuk-table__cell')
                for cell in candidate_cells:
                    if cell.select_one('h2.govuk-heading-m > a.govuk-link[href^="/company/"]'):
                        search_result_items.append(cell)
                # print(f"DEBUG: Page {page_number}: Extracted {len(search_result_items)} items from table cells.")
            else:
                # --- Fallback: List structure ---
                # print(f"Warning: Page {page_number}: Primary table structure not found. Falling back to list-based selectors.")
                search_result_items = soup.select('ul#results-list > li')
                # if search_result_items:
                    # print(f"DEBUG: Page {page_number}: Found {len(search_result_items)} items using fallback list selector.")

            if not search_result_items:
                no_results_h1 = soup.find('h1', string=re.compile(r"\s*0\s+companies found", re.IGNORECASE))
                if no_results_h1 and page_number == 1: # "0 companies found" is most relevant on the first query
                    print(f"No companies found for the specified advanced search criteria (0 companies found message on first page).")
                else: # For page_number > 1, or if no "0 companies" message, assume end of results
                    print(f"No search result items extracted on page {page_number}. Assuming end of paginated results.")
                break # Break from the main while loop (no more items on this page or subsequent pages)

            new_companies_found_on_this_page = False
            for item_container in search_result_items: # item_container is a <td> (primary) or <li> (fallback)
                if len(companies_data) >= MAX_COMPANIES:
                    break # Max companies reached

                link_element = item_container.select_one('h2.govuk-heading-m > a.govuk-link[href^="/company/"]')
                if not link_element:
                    continue
                
                company_name = "Not found"
                temp_link_soup_outer = BeautifulSoup(str(link_element), 'html.parser')
                actual_link_tag = temp_link_soup_outer.find('a')
                if actual_link_tag:
                    for hidden_span in actual_link_tag.select('span.govuk-visually-hidden'):
                        hidden_span.decompose()
                    company_name = actual_link_tag.get_text(strip=True)
                else:
                    company_name = link_element.get_text(strip=True).replace("(link opens a new window)", "").strip()

                company_ch_link_relative = link_element.get('href')
                company_ch_link = urljoin(BASE_URL, company_ch_link_relative) # Join with site's BASE_URL

                if company_ch_link in processed_company_links:
                    continue # Skip if already processed

                processed_company_links.add(company_ch_link)
                new_companies_found_on_this_page = True

                print(f"Processing ({len(companies_data) + 1}/{MAX_COMPANIES}): {company_name} ({company_ch_link}) from page {page_number}")

                driver.get(company_ch_link)
                time.sleep(3)
                company_page_content = driver.page_source
                company_soup = BeautifulSoup(company_page_content, 'html.parser')

                if "prove you are not a robot" in company_page_content.lower() or "enter characters" in company_page_content.lower():
                    print(f"CAPTCHA detected on company page: {company_name}. Please solve manually.")
                    input("Press Enter after solving CAPTCHA to continue...")
                    company_page_content = driver.page_source
                    company_soup = BeautifulSoup(company_page_content, 'html.parser')

                location = "Not found"
                location_dt = company_soup.find('dt', string=re.compile(r"Registered office address", re.IGNORECASE))
                if location_dt:
                    location_dd = location_dt.find_next_sibling('dd')
                    if location_dd:
                        location_parts = [part.strip() for part in location_dd.get_text(separator='\n').split('\n') if part.strip()]
                        location = ", ".join(location_parts)

                sic_description_parts = []
                sic_found_method = "None"
                sic_section_label = company_soup.find(['h2', 'h3', 'dt'], string=re.compile(r"Nature of business \(SIC\)", re.IGNORECASE))
                if sic_section_label:
                    ul_element = None; current_element = sic_section_label
                    for _ in range(3):
                        next_sibling = current_element.find_next_sibling()
                        if not next_sibling: break
                        if next_sibling.name == 'ul': ul_element = next_sibling; break
                        if next_sibling.name == 'dd' and next_sibling.find('ul'): ul_element = next_sibling.find('ul'); break
                        if next_sibling.name == 'div' and next_sibling.find('ul'): ul_element = next_sibling.find('ul'); break
                        current_element = next_sibling
                    if not ul_element and sic_section_label.parent: ul_element = sic_section_label.parent.find('ul')
                    if ul_element:
                        span_tags_in_ul = ul_element.select('li > span[id^="sic"], li span[id^="sic"]')
                        if span_tags_in_ul:
                            for span_tag in span_tags_in_ul:
                                text = span_tag.get_text(strip=True)
                                if text and re.match(r"^\d{5}\s*-", text): sic_description_parts.append(text)
                            if sic_description_parts: sic_found_method = "Label->UL/DD->LI->SPAN[id^=sic]"
                        else: 
                            list_items = ul_element.find_all('li', recursive=False)
                            for li_item in list_items:
                                text = li_item.get_text(strip=True)
                                if text and re.match(r"^\d{5}\s*-", text): sic_description_parts.append(text)
                            if sic_description_parts: sic_found_method = "Label->UL/DD->LI Text"
                unique_sics = list(dict.fromkeys([s.strip() for s in sic_description_parts if s.strip()])) if sic_description_parts else []
                sic_description = " | ".join(unique_sics) if unique_sics else "Not found"
                
                email_address = "Not found" # Email scraping is usually more involved

                companies_data.append({
                    "Company Name": company_name, "Companies House Link": company_ch_link,
                    "Location": location, "Email Address": email_address,
                    "SIC Description": sic_description, "SIC Found Method": sic_found_method
                })
                print(f"  Location: {location}")
                print(f"  SIC: {sic_description} (Method: {sic_found_method})")
                print(f"  Email: {email_address}")
                print(f"--- Collected {len(companies_data)}/{MAX_COMPANIES} companies ---")

            # After processing all items on the current page
            if len(companies_data) >= MAX_COMPANIES:
                print(f"Reached MAX_COMPANIES limit of {MAX_COMPANIES}.")
                break # Break from the main while loop

            if not new_companies_found_on_this_page and search_result_items:
                # This means the page had items, but all were duplicates of ones already processed.
                # This isn't an error, just informational. We'll proceed to the next page.
                print(f"Info: No *new* companies found on page {page_number} (all items were already processed).")

            page_number += 1 # Increment to go to the next page
            time.sleep(2) # Small pause before fetching the next page

    except Exception as e:
        print(f"A critical error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'driver' in locals() and driver:
            print("Closing browser...")
            driver.quit()

    if companies_data:
        print(f"\nWriting {len(companies_data)} companies to {OUTPUT_CSV_FILE}...")
        all_keys = set().union(*(d.keys() for d in companies_data))
        preferred_order = ["Company Name", "Companies House Link", "Location", "SIC Description", "SIC Found Method", "Email Address"]
        fieldnames = [k for k in preferred_order if k in all_keys] + sorted([k for k in all_keys if k not in preferred_order])
        
        with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as output_file:
            dict_writer = csv.DictWriter(output_file, fieldnames=fieldnames)
            dict_writer.writeheader()
            dict_writer.writerows(companies_data)
        print(f"Data successfully written to {OUTPUT_CSV_FILE}")
    else:
        print("No data collected or an error occurred before data collection.")

if __name__ == "__main__":
    print("Reminder: Close any existing Google Chrome windows for best results with undetected_chromedriver.")
    # input("Press Enter to start scraping after closing Chrome instances...")
    scrape_companies_house()
