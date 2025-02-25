import pandas as pd
from playwright.sync_api import sync_playwright
from datetime import datetime, timedelta
import pymysql

def scrape_and_process_data(target_date):
    print(f"Starting data scraping process for {target_date.strftime('%Y-%m-%d %H:%M')}...")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto('https://www.niggrid.org/', timeout=120000)

            # Click on the "Generate Hourly Data" button
            hourly_data_button = page.wait_for_selector('#sideContent_loginVWShortCuts_lnkGencoProfile2')
            hourly_data_button.click()

            # Open the calendar
            calendar_element = page.wait_for_selector('#MainContent_txtReadingDate')
            calendar_element.click()

            # Select the year
            select_year = page.query_selector('//*[@id="ui-datepicker-div"]/div/div/select[2]')
            select_year.select_option(str(target_date.year))

            # Select the month (note: month is zero-indexed in the UI)
            select_month = page.query_selector('//*[@id="ui-datepicker-div"]/div/div/select[1]')
            select_month.select_option(str(target_date.month - 1))

            # Select the day
            day_element = page.wait_for_selector(
                f'//*[@id="ui-datepicker-div"]/table/tbody/tr/td/a[text()="{target_date.day}"]'
            )
            day_element.click()

            # Click the "Generate Readings" button
            generate_button = page.wait_for_selector('#MainContent_btnGetReadings')
            generate_button.click()

            # Wait for the data to load
            page.wait_for_timeout(8000)

            # Extract table headers
            headers = [header.text_content().strip() for header in page.query_selector_all('th')]

            # Extract data rows
            table_rows = page.query_selector_all('tr')
            all_data = []
            for row in table_rows:
                cols = row.query_selector_all('td')
                if cols:
                    row_data = [col.text_content().strip() for col in cols]
                    # Prepend the Date column (YYYY-MM-DD)
                    row_data.insert(0, target_date.strftime('%Y-%m-%d'))
                    all_data.append(row_data)

            print(f"Scraped data for {target_date.strftime('%Y-%m-%d')}")
            browser.close()

        # Convert to DataFrame
        if headers and headers[0] == '':
            headers[0] = 'Index'  # Handle potential empty header column
        columns = ['Date'] + headers
        hourly_data_df = pd.DataFrame(all_data, columns=columns)

        print("Starting data processing...")

        # Validate and clean the DataFrame
        # Replace empty strings with NaN
        hourly_data_df = hourly_data_df.replace(r'^\s*$', pd.NA, regex=True)

        # Drop rows without a valid Genco
        hourly_data_df = hourly_data_df.dropna(subset=['Genco'])

        # Remove rows where Genco == 'zTOTAL'
        hourly_data_df = hourly_data_df[hourly_data_df['Genco'] != 'zTOTAL']

        # Remove unnecessary columns
        hourly_data_df = hourly_data_df.drop(columns=['#', 'TotalGeneration'], errors='ignore')

        # Rename columns (note: do NOT rename '24:00' to '00:00')
        hourly_data_df.rename(columns={
            # '24:00': '00:00',  # Keep this commented out to preserve "24:00"
            'Genco': 'Gencos'
        }, inplace=True)

        # Identify hourly columns for unpivoting
        hour_columns = [col for col in hourly_data_df.columns if ':' in col]
        if not hour_columns:
            raise ValueError("No hourly columns found for unpivoting!")

        # Unpivot the DataFrame to convert hour columns into rows
        unpivoted_df = pd.melt(
            hourly_data_df,
            id_vars=['Date', 'Gencos'],
            value_vars=hour_columns,
            var_name='Hour',
            value_name='EnergyGeneratedMWh'
        )

        # Convert energy to numeric, drop invalid
        unpivoted_df['EnergyGeneratedMWh'] = pd.to_numeric(unpivoted_df['EnergyGeneratedMWh'], errors='coerce')
        unpivoted_df.dropna(subset=['EnergyGeneratedMWh'], inplace=True)

        return unpivoted_df

    except Exception as e:
        print(f"Scraping process failed: {e}")
        return None


def load_to_database_delete_insert(df):
    """
    For each row, delete existing row(s) in the DB for (Date, Hour, Gencos),
    then insert a fresh record. This ensures any changes are reflected,
    without requiring a unique key constraint.
    """
    print("Starting database upload (delete+insert mode)...")
    try:
        # Database connection details
        db_host = "148.251.246.72"
        db_port = 3306
        db_name = "jksutauf_nesidb"
        db_user = "jksutauf_martins"
        db_password = "12345678"

        db_connection = pymysql.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        cursor = db_connection.cursor()

        # We'll do everything in a single transaction
        # so that partial changes don't persist if there's an error.
        cursor.execute("BEGIN")

        # Make sure df columns are in the correct order
        df = df[['Date', 'Hour', 'Gencos', 'EnergyGeneratedMWh']]

        # Prepare SQL statements
        sql_delete = """
            DELETE FROM combined_hourly_energy_generated_mwh
            WHERE Date=%s AND Hour=%s AND Gencos=%s
        """
        sql_insert = """
            INSERT INTO combined_hourly_energy_generated_mwh (Date, Hour, Gencos, EnergyGeneratedMWh)
            VALUES (%s, %s, %s, %s)
        """

        # Convert DataFrame rows to a list of tuples
        data_tuples = df.to_records(index=False).tolist()

        for record in data_tuples:
            date_val, hour_val, genco_val, energy_val = record

            # 1) Delete old row(s)
            cursor.execute(sql_delete, (date_val, hour_val, genco_val))

            # 2) Insert fresh row
            cursor.execute(sql_insert, (date_val, hour_val, genco_val, energy_val))

        db_connection.commit()
        db_connection.close()

        print("Delete+Insert transaction completed successfully.")

    except Exception as e:
        print(f"An error occurred while uploading to the database: {e}")


def main():
    try:
        # 1) Determine 'target_nigeria' as "Now in Nigeria (UTC+1) minus 1 hour".
        now_utc = datetime.utcnow()
        now_nigeria = now_utc + timedelta(hours=1)
        # The "hour to pull" is now_nigeria - 1 hour
        target_nigeria = now_nigeria - timedelta(hours=1)
        target_nigeria = target_nigeria.replace(minute=0, second=0, microsecond=0)
        print("Base target hour (Nigeria time):", target_nigeria.strftime("%Y-%m-%d %H:%M"))

        # 2) Re-check the previous 3 hours plus the new hour
        #    That means offsets of -2, -1, 0, +1 from the base.
        hours_to_revalidate = range(-3, 2)  # [-2, -1, 0, 1]

        for offset in hours_to_revalidate:
            check_hour = target_nigeria + timedelta(hours=offset)
            entire_day_df = scrape_and_process_data(check_hour)
            if entire_day_df is None:
                print(f"Failed scraping day for {check_hour.date()}, skipping offset={offset}...")
                continue

            # Format hour "HH:00", if midnight then "24:00"
            target_hour_str = check_hour.strftime("%H:00")
            if target_hour_str == "00:00":
                target_hour_str = "24:00"

            # Filter for exactly that hour
            one_hour_df = entire_day_df[
                (entire_day_df['Date'] == check_hour.strftime('%Y-%m-%d')) &
                (entire_day_df['Hour'] == target_hour_str)
            ].copy()

            if one_hour_df.empty:
                print(f"No data found for hour={target_hour_str} on {check_hour.strftime('%Y-%m-%d')}.")
                continue

            # Load to DB via delete+insert
            load_to_database_delete_insert(one_hour_df)

        print("ETL process with revalidation completed successfully.")

    except Exception as e:
        print(f"An error occurred in the main process: {e}")

if __name__ == "__main__":
    main()
