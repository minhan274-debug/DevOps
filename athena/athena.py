import psycopg2
import os
import csv
from datetime import datetime, timedelta
import zipfile
import boto3
import yaml

def load_database_config():
    try:
        with open('/opt/production/mss/current/config/database.yml', 'r') as file:
            config = yaml.safe_load(file)
        return config['production']  # Lấy thông tin từ phần production
    except FileNotFoundError:
        print("Không tìm thấy tệp cấu hình database.yml")
        return None

def connect_to_main_database():
    config = load_database_config()
    if config:
        try:
            # Lấy giá trị từ cấu hình
            host = config['host']
            port = config['port']
            user = config['username']
            password = config['password']
            database = config['database']

            # Kết nối đến database main thông qua PgBouncer
            conn = psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database
            )
            return conn
        except psycopg2.Error as e:
            print("Không thể kết nối đến database main:", e)
            return None
    else:
        return None

def get_organizations_info(conn):
    try:
        # Tạo một đối tượng cursor
        cur = conn.cursor()
        
        # Thực hiện truy vấn để lấy thông tin db_name, db_ip từ bảng organizations
        cur.execute("SELECT db_name, db_ip FROM organizations;")
        
        # Lấy kết quả của truy vấn
        organizations_info = cur.fetchall()
        
        # Đóng cursor
        cur.close()
        
        return organizations_info
    except psycopg2.Error as e:
        print("Lỗi khi truy vấn thông tin organizations:", e)
        return None

def export_to_csv(conn, db_name, table_name, csv_filename, start_date, end_date):
    try:
        # Tạo một đối tượng cursor
        cur = conn.cursor()
        
        # Thực hiện truy vấn để lấy dữ liệu từ bảng và ghi vào file CSV
        cur.execute(f"COPY (SELECT * FROM {table_name} WHERE created_at BETWEEN %s AND %s) TO STDOUT WITH CSV HEADER", (start_date, end_date))
        with open(csv_filename, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow([desc[0] for desc in cur.description])
            csv_writer.writerows(cur)
        
        # Đóng cursor
        cur.close()
    except psycopg2.Error as e:
        print(f"Lỗi khi export dữ liệu từ bảng {table_name}:", e)

def zip_csv_file(csv_filename, zip_filename):
    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        zipf.write(csv_filename, os.path.basename(csv_filename))

def upload_to_s3(zip_filename, s3_bucket, s3_key):
    s3 = boto3.client('s3')
    with open(zip_filename, 'rb') as data:
        s3.upload_fileobj(data, s3_bucket, s3_key)

def main():
    # Kết nối đến database main
    main_conn = connect_to_main_database()
    if main_conn is None:
        return
    
    # Lấy thông tin db_name, db_ip từ bảng organizations
    organizations_info = get_organizations_info(main_conn)
    if organizations_info is None:
        main_conn.close()
        return
    
    # Duyệt qua từng row của organizations_info
    for org_info in organizations_info:
        db_name, _ = org_info
        
        # Danh sách tên bảng
        table_names = ['table_a_name', 'table_b_name', 'table_c_name']  # Thay đổi danh sách tên bảng tại đây
        
        # Tạo thư mục tạm thời cho mỗi database
        temp_folder = f"/vol1/athena/{db_name}/"
        os.makedirs(temp_folder, exist_ok=True)
        
        # Tính toán thời điểm bắt đầu và kết thúc cho khoảng thời gian (từ ngày 1 của tháng hiện tại đến ngày 1 của tháng tiếp theo)
        current_date = datetime.now().date()
        start_date = current_date.replace(day=1)
        if start_date.month == 12:
            end_date = start_date.replace(year=start_date.year + 1, month=1)
        else:
            end_date = start_date.replace(month=start_date.month + 1)
        
        # Thực hiện export từng bảng và lưu vào thư mục tạm thời
        for table_name in table_names:
            # Tạo tên file CSV và file zip với ngày hiện tại
            csv_filename = f"{temp_folder}/{table_name}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
            zip_filename = f"{temp_folder}/{table_name}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.zip"
            
            # Export dữ liệu từ bảng trong khoảng thời gian và ghi vào file CSV
            export_to_csv(main_conn, db_name, table_name, csv_filename, start_date, end_date)
            
            # Nén file CSV thành file zip
            zip_csv_file(csv_filename, zip_filename)
            
            # Đẩy file zip lên S3
            s3_bucket = "your_s3_bucket_name"
            s3_key = f"athena/{db_name}/{table_name}/{os.path.basename(zip_filename)}"
            upload_to_s3(zip_filename, s3_bucket, s3_key)
            
            # Xóa file CSV và file zip tạm thời
            os.remove(csv_filename)
            os.remove(zip_filename)
    
    # Đóng kết nối tới database main
    main_conn.close()

if __name__ == "__main__":
    main()
