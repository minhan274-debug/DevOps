import psycopg2
import os
import csv
from datetime import datetime, timedelta
import zipfile
import boto3
import yaml

def load_database_config():
    try:
        with open('/home/ubuntu/meperia/test/msss/current/config/database.yml', 'r') as file:
            config = yaml.safe_load(file)
        return config['production']  # Get information from the production section
    except FileNotFoundError:
        print("Database configuration file database.yml not found")
        return None

def connect_to_main_database():
    config = load_database_config()
    if config:
        try:
            # Get values from the configuration
            host = config['host']
            port = config['port']
            user = config['username']
            password = config['password']
            database = config['database']

            # Connect to the main database through PgBouncer
            conn = psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database
            )
            return conn
        except psycopg2.Error as e:
            print("Could not connect to the main database:", e)
            return None
    else:
        return None

def get_organizations_info(conn):
    try:
        # Create a cursor object
        cur = conn.cursor()
        
        # Execute a query to get db_name, db_ip from the organizations table
        cur.execute("SELECT db_name, db_ip FROM organizations;")
        
        # Get the query result
        organizations_info = cur.fetchall()
        
        # Close the cursor
        cur.close()
        
        return organizations_info
    except psycopg2.Error as e:
        print("Error querying organizations information:", e)
        return None

def export_to_csv(conn, db_name, table_name, csv_filename, start_date, end_date):
    try:
        # Create a cursor object
        cur = conn.cursor()
        
        # Execute a query to fetch data from the table and write to a CSV file
        cur.execute(f"COPY (SELECT * FROM {table_name} WHERE created_at BETWEEN %s AND %s) TO STDOUT WITH CSV HEADER", (start_date, end_date))
        with open(csv_filename, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow([desc[0] for desc in cur.description])
            csv_writer.writerows(cur)
        
        # Close the cursor
        cur.close()
    except psycopg2.Error as e:
        print(f"Error exporting data from {table_name}:", e)

def zip_csv_file(csv_filename, zip_filename):
    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        zipf.write(csv_filename, os.path.basename(csv_filename))

def upload_to_s3(zip_filename, s3_bucket, s3_key):
    s3 = boto3.client('s3')
    with open(zip_filename, 'rb') as data:
        s3.upload_fileobj(data, s3_bucket, s3_key)

def main():
    # Connect to the main database
    main_conn = connect_to_main_database()
    if main_conn is None:
        return
    print("SUCCESS")
    '''
    # Get db_name, db_ip information from the organizations table
    organizations_info = get_organizations_info(main_conn)
    if organizations_info is None:
        main_conn.close()
        return
    
    # Iterate through each row of organizations_info
    for org_info in organizations_info:
        db_name, _ = org_info
        
        # Table names list
        table_names = ['table_a_name', 'table_b_name', 'table_c_name']  # Modify table names list here
        
        # Create a temporary folder for each database
        temp_folder = f"/vol1/athena/{db_name}/"
        os.makedirs(temp_folder, exist_ok=True)
        
        # Calculate start and end time for the time range (from the 1st day of the current month to the 1st day of the next month)
        current_date = datetime.now().date()
        start_date = current_date.replace(day=1)
        if start_date.month == 12:
            end_date = start_date.replace(year=start_date.year + 1, month=1)
        else:
            end_date = start_date.replace(month=start_date.month + 1)
        
        # Export data from each table and save to the temporary folder
        for table_name in table_names:
            # Create CSV file and zip file names with the current date
            csv_filename = f"{temp_folder}/{table_name}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
            zip_filename = f"{temp_folder}/{table_name}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.zip"
            
            # Export data from the table within the time range and write to CSV file
            export_to_csv(main_conn, db_name, table_name, csv_filename, start_date, end_date)
            
            # Compress CSV file to zip file
            zip_csv_file(csv_filename, zip_filename)
            
            # Upload zip file to S3
            s3_bucket = "athena"
            s3_key = f"{db_name}/{table_name}/{os.path.basename(zip_filename)}"
            upload_to_s3(zip_filename, s3_bucket, s3_key)
            
            # Remove temporary CSV file and zip file
            os.remove(csv_filename)
            os.remove(zip_filename)
    '''
    # Close connection to the main database
    main_conn.close()

if __name__ == "__main__":
    main()




