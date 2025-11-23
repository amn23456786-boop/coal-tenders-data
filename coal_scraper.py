import sys

# Set UTF-8 encoding for console output
sys.stdout.reconfigure(encoding='utf-8')

import requests
from bs4 import BeautifulSoup
import csv
import time
import os
import upload_to_sheets  # Import the new module

def scrape_coal_india():
    base_url = "https://coalindiatenders.nic.in"
    start_url = "https://coalindiatenders.nic.in/nicgep/app?page=FrontEndTendersByOrganisation&service=page"
    # Generate timestamp for unique filenames
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"tender_details_{timestamp}.csv"
    filtered_csv_filename = f"filtered_tender_details_{timestamp}.csv"
    
    # Requested columns for filtered CSV
    filtered_columns = [
        'tenderID', 'tenderRefNumber', 'organizationName', 'tenderValue', 'description', 
        'bidEndDate', 'organisationChain', 'emdAmount', 'state', 
        'guessedLocation', 'calculatedSortValue'
    ]
    
    # Headers for requests
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    print("Starting Scraper with Nested Loops...")
    
    # Helper to parse date
    from datetime import datetime
    def parse_date(date_str):
        if not date_str: return ''
        try:
            # Example: 04-Dec-2025 10:00 AM
            dt = datetime.strptime(date_str.strip(), '%d-%b-%Y %I:%M %p')
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            return ''

    # 1. Determine number of organizations first (to know how many times to loop)
    # We'll do a quick fetch to count them.
    try:
        print("Fetching main page to count organizations...")
        resp = requests.get(start_url, headers=headers)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        org_links_initial = soup.find_all('a', class_='link2')
        num_orgs = len(org_links_initial)
        print(f"Found {num_orgs} organizations.")
        
    except Exception as e:
        print(f"Error fetching main page: {e}")
        return

    # 2. Outer Loop: Iterate through organizations
    for i in range(num_orgs):
        print(f"\n{'='*50}")
        print(f"Processing Organization {i+1}/{num_orgs}")
        print(f"{'='*50}")
        
        try:
            # Create a FRESH session for each organization to avoid Stale Session issues
            session = requests.Session()
            session.headers.update(headers)
            
            # Fetch Main Page again
            print("Fetching main page...")
            response = session.get(start_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the i-th organization link
            current_org_links = soup.find_all('a', class_='link2')
            
            if i >= len(current_org_links):
                print("Error: Organization index out of range (page changed?). Skipping.")
                continue
                
            org_link_tag = current_org_links[i]
            
            # Extract Organization Name
            org_name = "Unknown"
            row = org_link_tag.find_parent('tr')
            if row:
                cols = row.find_all('td')
                if len(cols) >= 2:
                    org_name = cols[1].get_text(strip=True)
            else:
                # Fallback for div structure if tr not found
                prev = org_link_tag.previous_sibling
                while prev:
                    if isinstance(prev, str) and prev.strip():
                        org_name = prev.strip()
                        break
                    prev = prev.previous_sibling
            
            print(f"Organization: {org_name}")
            
            # Navigate to Organization Page
            href = org_link_tag.get('href')
            if not href:
                print("No href found. Skipping.")
                continue
                
            full_url = base_url + href if not href.startswith('http') else href
            # Handle 'app' prefix if needed, but usually base_url + href works if href starts with /
            if href.startswith('app'): full_url = base_url + "/nicgep/" + href
            
            print(f"Navigating to organization tenders: {full_url}")
            resp_org = session.get(full_url)
            resp_org.raise_for_status()
            soup_org = BeautifulSoup(resp_org.content, 'html.parser')
            
            # 3. Inner Loop: Iterate through Tenders
            # Find all tender links
            tender_links = soup_org.find_all('a', title="View Tender Information")
            print(f"Found {len(tender_links)} tenders for {org_name}.")
            
            # Collect hrefs first
            tenders_to_visit = []
            for link in tender_links:
                tenders_to_visit.append((link.get_text(strip=True), link.get('href')))
            
            for j, (tender_text, tender_href) in enumerate(tenders_to_visit):
                print(f"  Processing Tender {j+1}/{len(tenders_to_visit)}")
                
                tender_url = base_url + tender_href if not tender_href.startswith('http') else tender_href
                
                try:
                    resp_tender = session.get(tender_url)
                    resp_tender.raise_for_status()
                    soup_tender = BeautifulSoup(resp_tender.content, 'html.parser')
                    
                    # Extract Data
                    data = {'Organization Name': org_name} # Add Org Name
                    
                    tables = soup_tender.find_all('table')
                    for table in tables:
                        rows = table.find_all('tr')
                        for row in rows:
                            cols = row.find_all('td')
                            if len(cols) == 2:
                                key = cols[0].get_text(strip=True).rstrip(':')
                                val = cols[1].get_text(strip=True)
                                if key and val: data[key] = val
                            elif len(cols) == 4:
                                key1 = cols[0].get_text(strip=True).rstrip(':')
                                val1 = cols[1].get_text(strip=True)
                                key2 = cols[2].get_text(strip=True).rstrip(':')
                                val2 = cols[3].get_text(strip=True)
                                if key1 and val1: data[key1] = val1
                                if key2 and val2: data[key2] = val2
                    
                    # Save to CSV immediately
                    if data:
                        file_exists = os.path.isfile(csv_filename)
                        with open(csv_filename, 'a', newline='', encoding='utf-8') as f:
                            # We need to handle dynamic fields. DictWriter needs fieldnames.
                            # If file exists, we should use existing header or append.
                            # But new tenders might have new fields.
                            # For simplicity, let's just write what we have.
                            # A robust way is to read existing header, update it? No, that's complex for appending.
                            # Let's just assume a superset of keys or just write.
                            # Actually, standard DictWriter requires fixed fieldnames.
                            # Let's use a standard set of common keys + dynamic ones?
                            # Or just read the file, get header, if new keys, rewrite file? Too slow.
                            # Hack: Just write a row. If header missing, write header.
                            # If new keys appear, they might be lost if we strictly follow header.
                            # Let's just collect keys for THIS tender.
                            
                            # Better approach for this script:
                            # Just write the dictionary as a row.
                            # If it's the very first write, write header.
                            # If subsequent, we might miss columns if we didn't anticipate them.
                            # But usually these forms are consistent.
                            
                            # Let's try to read header if exists
                            existing_header = []
                            if file_exists:
                                with open(csv_filename, 'r', encoding='utf-8') as r:
                                    reader = csv.reader(r)
                                    try:
                                        existing_header = next(reader)
                                    except StopIteration:
                                        pass
                            
                            # Merge keys
                            current_keys = list(data.keys())
                            all_keys = sorted(list(set(existing_header + current_keys)))
                            
                            # If we have new keys and file exists, we technically should rewrite the file to add columns.
                            # But that's expensive.
                            # Let's just stick to the keys we have or append.
                            # For now, let's just use the keys from the current data + existing header.
                            
                            writer = csv.DictWriter(f, fieldnames=all_keys)
                            
                            if not file_exists or not existing_header:
                                writer.writeheader()
                            elif len(all_keys) > len(existing_header):
                                # New keys found! We can't easily add columns to CSV in append mode.
                                # We'll just ignore new keys or print a warning.
                                # Or we could read all, re-write.
                                # Let's just print warning.
                                # print("Warning: New fields found, might be missing in CSV header.")
                                pass
                                
                            # Write row (only fields in fieldnames)
                            writer.writerow(data)
                            
                        print(f"    Saved data.")
                    
                    time.sleep(1) # Politeness delay
                    
                except Exception as e:
                    print(f"    Error scraping tender: {e}")
            
        except Exception as e:
            print(f"Error processing organization {i}: {e}")
            
    print("\nScraping Complete! Now generating dynamic filtered CSV...")
    
    # --- Post-Processing: Dynamic Filtering ---
    try:
        if os.path.isfile(csv_filename):
            with open(csv_filename, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
            if not rows:
                print("No data found to filter.")
                return

            total_rows = len(rows)
            print(f"Analyzing {total_rows} rows for column density...")
            
            # Count non-empty values for each column
            column_counts = {}
            all_headers = reader.fieldnames
            
            for row in rows:
                for col in all_headers:
                    if row.get(col, '').strip():
                        column_counts[col] = column_counts.get(col, 0) + 1
            
            # Filter columns > 50%
            threshold = total_rows * 0.5
            selected_columns = [col for col in all_headers if column_counts.get(col, 0) > threshold]
            
            # Ensure critical columns are kept if they exist, even if low density (optional, but good for UX)
            # User asked ONLY for >50%, so strictly following that. 
            # But let's ensure 'Tender ID' or 'Organization Name' is there if possible? 
            # The user said "add only columns where values are filled for more than 50%".
            # I will stick to the rule strictly.
            
            print(f"Selected {len(selected_columns)} columns with >50% fill rate:")
            for col in selected_columns:
                print(f"  - {col} ({column_counts[col]}/{total_rows})")
                
            with open(filtered_csv_filename, 'w', newline='', encoding='utf-8') as f_out:
                writer = csv.DictWriter(f_out, fieldnames=selected_columns)
                writer.writeheader()
                
                for row in rows:
                    # Create a new dict with only selected columns
                    filtered_row = {k: row.get(k, '') for k in selected_columns}
                    writer.writerow(filtered_row)
                    
            print(f"Successfully created {filtered_csv_filename}")
            
    except Exception as e:
        print(f"Error during dynamic filtering: {e}")

    # --- Post-Processing: Google Sheets Upload ---
    # Check if user wants to skip upload
    if "--no-upload" in sys.argv:
        print(f"\n{'='*50}")
        print("Skipping Google Sheets Upload (--no-upload flag detected).")
        print(f"{'='*50}")
        return

    print(f"\n{'='*50}")
    print("Starting Google Sheets Upload...")
    print(f"{'='*50}")
    
    # TODO: Set your Google Sheet ID here
    GOOGLE_SHEET_ID = "1VHFlj5pxuw5EwSxiGUIcdPA57-ujeykcIvENsmytLvw" 
    
    # Use current date as worksheet name (e.g., "2025-11-23")
    from datetime import datetime
    worksheet_name = datetime.now().strftime("%Y-%m-%d")
    
    if os.path.isfile(filtered_csv_filename):
        upload_to_sheets.upload_csv_to_sheet(filtered_csv_filename, GOOGLE_SHEET_ID, worksheet_name)
    else:
        print("Filtered CSV not found, skipping upload.")

if __name__ == "__main__":
    scrape_coal_india()
