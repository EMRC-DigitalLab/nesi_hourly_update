import pandas as pd
from playwright.sync_api import sync_playwright
from datetime import datetime, timedelta
import pymysql

def scrape_and_process_data(target_date):
    print(f"Starting data scraping process for {target_date.strftime('%Y-%m-%d')}...")

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

            # Select the month
            select_month = page.query_selector('//*[@id="ui-datepicker-div"]/div/div/select[1]')
            select_month.select_option(str(target_date.month - 1))

            # Select the day
            day_element = page.wait_for_selector(f'//*[@id="ui-datepicker-div"]/table/tbody/tr/td/a[text()="{target_date.day}"]')
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
                    row_data.insert(0, target_date.strftime('%Y-%m-%d'))  # Add the Date as the first column
                    all_data.append(row_data)

            print(f"Scraped data for {target_date.strftime('%Y-%m-%d')}")
            browser.close()

        # Convert to DataFrame
        if headers[0] == '':
            headers[0] = 'Index'  # Handle potential empty header column
        columns = ['Date'] + headers
        hourly_data_df = pd.DataFrame(all_data, columns=columns)

        print("Starting data processing...")

        # Validate and clean the DataFrame
        hourly_data_df = hourly_data_df.replace(r'^\s*$', pd.NA, regex=True)  # Replace empty strings with NaN
        hourly_data_df = hourly_data_df.dropna(subset=['Genco'])  # Drop rows with missing Genco data

        # Remove unnecessary columns
        hourly_data_df = hourly_data_df.drop(columns=['#', 'TotalGeneration'], errors='ignore')

        # Rename columns
        hourly_data_df.rename(columns={'24:00': '00:00', 'Genco': 'Gencos'}, inplace=True)

        # Validate columns for unpivoting
        hour_columns = [col for col in hourly_data_df.columns if ':' in col]
        if not hour_columns:
            raise ValueError("No hourly columns found for unpivoting!")

        # Unpivot the DataFrame
        unpivoted_df = pd.melt(
            hourly_data_df,
            id_vars=['Date', 'Gencos'],
            value_vars=hour_columns,
            var_name='Hour',
            value_name='EnergyGeneratedMWh'
        )

        # Final cleanup
        unpivoted_df['EnergyGeneratedMWh'] = pd.to_numeric(unpivoted_df['EnergyGeneratedMWh'], errors='coerce')
        unpivoted_df.dropna(subset=['EnergyGeneratedMWh'], inplace=True)

        print("Data processing completed successfully.")
        return unpivoted_df

    except Exception as e:
        print(f"Scraping process failed: {e}")
        return None

def load_to_database(df):
    print("Starting database upload...")
    try:
        # Database connection details
        db_host = "148.251.246.72"
        db_port = 3306
        db_name = "jksutauf_nesidb"
        db_user = "jksutauf_martins"
        db_password = "12345678"

        # Create a MySQL connection
        db_connection = pymysql.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )

        # Create a cursor
        cursor = db_connection.cursor()

        # Prepare the insert statement
        sql = "INSERT INTO combined_hourly_energy_generated_mwh (Date, Hour, Gencos, EnergyGeneratedMWh) VALUES (%s, %s, %s, %s)"

        df = df[['Date', 'Hour', 'Gencos', 'EnergyGeneratedMWh']]

        # Create a list of tuples from the dataframe records
        data_tuples = df.to_records(index=False).tolist()
        print("DATA TUPLES", data_tuples)

        # Execute the SQL command in batches
        batch_size = 1000
        for i in range(0, len(data_tuples), batch_size):
            cursor.executemany(sql, data_tuples[i:i+batch_size])

        # Commit the changes and close the connection
        db_connection.commit()
        db_connection.close()

        print("Data inserted successfully into the database.")

    except Exception as e:
        print(f"An error occurred while uploading to the database: {e}")

def main():
    try:
        yesterday = datetime.now() - timedelta(days=1)
        print("Starting ETL process...")
        final_df = scrape_and_process_data(yesterday)

        if final_df is not None:
            load_to_database(final_df)
            print("ETL process completed successfully.")
        else:
            print("Data scraping failed. Aborting process.")

    except Exception as e:
        print(f"An error occurred in the main process: {e}")

if __name__ == "__main__":
    main()
