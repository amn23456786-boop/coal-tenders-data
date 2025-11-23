import gspread
import csv
import os
import sys

def upload_csv_to_sheet(csv_filename, sheet_id, worksheet_name=None, service_account_file='service_account.json'):
    """
    Uploads a CSV file to a Google Sheet.
    """
    # 1. Check for Service Account File
    if not os.path.exists(service_account_file):
        print(f"Error: Service account file '{service_account_file}' not found.")
        print("Please place your Google Cloud service account JSON file in this directory.")
        return False

    try:
        print(f"Authenticating with Google Sheets using {service_account_file}...")
        gc = gspread.service_account(filename=service_account_file)
        
        print(f"Opening Google Sheet with ID: '{sheet_id}'...")
        try:
            sh = gc.open_by_key(sheet_id)
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"Error: Google Sheet with ID '{sheet_id}' not found.")
            print("Please ensure the sheet exists and is shared with the service account email.")
            return False

        # Select or Create Worksheet
        if worksheet_name:
            try:
                worksheet = sh.worksheet(worksheet_name)
                print(f"Found existing worksheet: '{worksheet_name}'")
            except gspread.exceptions.WorksheetNotFound:
                print(f"Worksheet '{worksheet_name}' not found. Creating it...")
                worksheet = sh.add_worksheet(title=worksheet_name, rows="100", cols="20")
        else:
            # Default to first sheet if no name provided
            try:
                worksheet = sh.sheet1
            except:
                worksheet = sh.get_worksheet(0)
            
        print(f"Uploading data to worksheet: '{worksheet.title}'...")
        
        # Read CSV data
        with open(csv_filename, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            csv_data = list(reader)
            
        if not csv_data:
            print("CSV file is empty. Nothing to upload.")
            return True

        # Clear existing content and update
        worksheet.clear()
        worksheet.update(csv_data)
        
        print(f"Successfully uploaded {len(csv_data)} rows to '{sh.title}'.")
        return True

    except Exception as e:
        print(f"An error occurred during Google Sheets upload: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python upload_to_sheets.py <csv_file> <sheet_id> [worksheet_name]")
    else:
        ws_name = sys.argv[3] if len(sys.argv) > 3 else None
        upload_csv_to_sheet(sys.argv[1], sys.argv[2], ws_name)
