import pandas as pd
from playwright.sync_api import sync_playwright
from datetime import datetime, timedelta
import pymysql
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def scrape_disco_load_profile():
    """
    Scrape DISCO Load Profile data from niggrid.org using Playwright.
    Returns a DataFrame with columns: Date, Company, Load_Allocation_MW
    """
    logging.info("Starting DISCO Load Profile scraping...")
    
    try:
        with sync_playwright() as p:
            # Launch browser
            # ---------------------------------------------------------------
            # SSL FIX: ignore_https_errors=True bypasses the expired
            # certificate warning on www.niggrid.org (NET::ERR_CERT_DATE_INVALID)
            # This is the equivalent of clicking "Advanced → Proceed anyway"
            # ---------------------------------------------------------------
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(ignore_https_errors=True)
            page = context.new_page()
            
            # Set a reasonable timeout and user agent
            page.set_default_timeout(120000)
            page.set_extra_http_headers({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            })
            
            # Navigate directly to DISCO Load Profile page
            logging.info("Navigating to DISCO Load Profile page...")
            page.goto('https://niggrid.org/discoloadprofile', timeout=120000)
            page.wait_for_load_state('networkidle')
            
            # Wait a bit for dynamic content to load
            page.wait_for_timeout(5000)
            
            logging.info("Page loaded successfully, extracting data...")
            
            # Extract current timestamp
            current_datetime = datetime.utcnow() + timedelta(hours=1)  # Nigeria time (UTC+1)
            
            # Extract data from the page
            data_rows = []
            
            # Strategy 1: Look for table rows with company and load data
            table_rows = page.locator('tr').all()
            logging.info(f"Found {len(table_rows)} table rows")
            
            for i, row in enumerate(table_rows):
                try:
                    cells = row.locator('td, th').all()
                    if len(cells) >= 2:
                        company_text = cells[0].text_content().strip()
                        load_text = cells[1].text_content().strip()
                        
                        logging.info(f"Row {i}: '{company_text}' | '{load_text}'")
                        
                        # Check if this looks like a data row (contains "Disco" and excludes headers)
                        if (company_text and load_text and 
                            'disco' in company_text.lower() and
                            company_text.lower() not in ['company', 'distribution company'] and
                            load_text.lower() != 'load allocation (mw)'):
                            
                            try:
                                # Clean the load value and convert to float
                                load_clean = (load_text.replace(',', '')
                                                      .replace('MW', '')
                                                      .replace('(', '')
                                                      .replace(')', '')
                                                      .strip())
                                load_value = float(load_clean)
                                
                                data_rows.append({
                                    'Date': current_datetime,
                                    'Company': company_text,
                                    'Load_Allocation_MW': load_value
                                })
                                logging.info(f"Added: {company_text} - {load_value} MW")
                                
                            except ValueError as e:
                                logging.warning(f"Could not parse load value '{load_text}' for {company_text}: {e}")
                
                except Exception as e:
                    logging.warning(f"Error processing row {i}: {e}")
                    continue
            
            # Strategy 2: If no table data found, try alternative selectors
            if not data_rows:
                logging.info("No table data found, trying alternative parsing...")
                
                # Look for specific patterns or divs containing the data
                page_content = page.content()
                logging.info(f"Page content length: {len(page_content)} characters")
                
                # Try to find elements with company names
                disco_elements = page.locator('text=/.*disco.*/i').all()
                logging.info(f"Found {len(disco_elements)} elements containing 'disco'")
                
                for element in disco_elements[:20]:  # Limit to avoid too much noise
                    try:
                        text_content = element.text_content().strip()
                        if text_content and len(text_content) < 100:  # Reasonable length for company names
                            logging.info(f"Disco element: '{text_content}'")
                    except Exception:
                        continue

            # Close context before browser
            context.close()
            browser.close()
            logging.info(f"Scraping completed. Found {len(data_rows)} records.")
            
            if not data_rows:
                logging.warning("No data extracted from the page!")
                return None
            
            return data_rows
            
    except Exception as e:
        logging.error(f"Error during scraping: {e}")
        return None

def load_to_database_delete_insert(data_rows):
    """
    Load DISCO Load Profile data to database using delete+insert pattern.
    For each company, delete existing record for current datetime, then insert fresh record.
    """
    if not data_rows:
        logging.warning("No data to insert into database.")
        return
    
    logging.info("Starting database upload (delete+insert mode)...")
    
    try:
        # Database connection details
        db_connection = pymysql.connect(
            host="31.97.56.29",
            port=3306,
            database="jksutauf_nesidb",
            user="jksutauf_martins",
            password="Pass1234"
        )
        cursor = db_connection.cursor()
        
        # Start transaction
        cursor.execute("BEGIN")
        
        # Get the date/hour for this batch (all records should have same timestamp)
        sample_datetime = data_rows[0]['Date']
        date_str = sample_datetime.strftime('%Y-%m-%d')
        hour_str = sample_datetime.strftime('%H:00')
        
        # SQL statements
        sql_select = """
            SELECT Date, Company, Load_Allocation_MW
            FROM discoloadprofile
            WHERE DATE(Date) = %s AND HOUR(Date) = %s
        """
        
        sql_delete = """
            DELETE FROM discoloadprofile
            WHERE DATE(Date) = %s AND HOUR(Date) = %s AND Company = %s
        """
        
        sql_insert = """
            INSERT INTO discoloadprofile (Date, Company, Load_Allocation_MW)
            VALUES (%s, %s, %s)
        """
        
        # Check and log existing data
        cursor.execute(sql_select, (date_str, sample_datetime.hour))
        existing_rows = cursor.fetchall()
        
        if existing_rows:
            logging.info(f"Found {len(existing_rows)} existing rows for {date_str} {hour_str}:")
            for row in existing_rows:
                logging.info(f"  Existing: {row}")
        else:
            logging.info(f"No existing rows found for {date_str} {hour_str}")
        
        # Process each new record
        logging.info(f"Processing {len(data_rows)} new records:")
        for row_data in data_rows:
            date_val = row_data['Date']
            company_val = row_data['Company']
            load_val = row_data['Load_Allocation_MW']
            
            logging.info(f"  New: ({date_val}, {company_val}, {load_val})")
            
            # Delete existing record for this company/date/hour
            cursor.execute(sql_delete, (
                date_val.strftime('%Y-%m-%d'),
                date_val.hour,
                company_val
            ))
            deleted_count = cursor.rowcount
            if deleted_count > 0:
                logging.info(f"    Deleted {deleted_count} existing record(s)")
            
            # Insert fresh record
            cursor.execute(sql_insert, (date_val, company_val, load_val))
            logging.info(f"    Inserted new record")
        
        # Commit transaction
        db_connection.commit()
        db_connection.close()
        
        logging.info("Delete+Insert transaction completed successfully.")
        
    except Exception as e:
        logging.error(f"Database error: {e}")
        try:
            db_connection.rollback()
            logging.info("Transaction rolled back due to error")
        except:
            pass

def revalidate_previous_hours():
    """
    Revalidate the previous few hours to catch any late updates or missed data.
    This is especially useful for ensuring data consistency.
    """
    logging.info("Starting revalidation of previous hours...")
    
    # Get current Nigeria time
    now_utc = datetime.utcnow()
    now_nigeria = now_utc + timedelta(hours=1)
    
    # Revalidate the last 3 hours
    hours_to_revalidate = range(-3, 1)  # -3, -2, -1, 0 (current hour)
    
    for offset in hours_to_revalidate:
        target_hour = now_nigeria + timedelta(hours=offset)
        target_hour = target_hour.replace(minute=0, second=0, microsecond=0)
        
        logging.info(f"Revalidating hour: {target_hour.strftime('%Y-%m-%d %H:%M')}")
        
        # Scrape current data
        data_rows = scrape_disco_load_profile()
        if data_rows:
            # Update timestamps to match the target hour we're revalidating
            for row in data_rows:
                row['Date'] = target_hour
            
            load_to_database_delete_insert(data_rows)
        else:
            logging.warning(f"No data retrieved for revalidation of {target_hour}")

def main():
    """
    Main function that orchestrates the scraping and database operations.
    Designed to be run every hour via GitHub Actions.
    """
    try:
        logging.info("=== Starting DISCO Load Profile ETL Process ===")
        
        # Get current Nigeria time
        now_utc = datetime.utcnow()
        now_nigeria = now_utc + timedelta(hours=1)
        
        logging.info(f"Current Nigeria time: {now_nigeria.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Strategy: Always scrape current data and store with current timestamp
        # This captures the "live" load allocation at this moment
        
        # 1. Scrape current DISCO load profile
        data_rows = scrape_disco_load_profile()
        
        if data_rows:
            # 2. Load to database
            load_to_database_delete_insert(data_rows)
            
            # 3. If it's early morning (1-3 AM), do additional revalidation
            if now_nigeria.hour in [1, 2, 3]:
                logging.info("Early morning detected, performing additional revalidation...")
                revalidate_previous_hours()
        else:
            logging.error("Failed to scrape DISCO load profile data")
            return False
        
        logging.info("=== DISCO Load Profile ETL Process Completed Successfully ===")
        return True
        
    except Exception as e:
        logging.error(f"Error in main process: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)  # Exit with error code for GitHub Actions
