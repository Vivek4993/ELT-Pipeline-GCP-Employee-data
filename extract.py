from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta
import bcrypt
from google.cloud import storage

fake = Faker()

def generate_employee_data(num_rows=1000):
    """Generates dummy employee data."""

    data = []
    for _ in range(num_rows):
        first_name = fake.first_name()
        last_name = fake.last_name()
        middle_name = fake.middle_name()
        gender = random.choice(["Male", "Female", "Other"])
        email = fake.email()
        phone_number = fake.phone_number()
        address = fake.address()
        birthdate = fake.date_of_birth(minimum_age=20, maximum_age=65).strftime('%Y-%m-%d')
        ssn = fake.ssn()
        job_title = fake.job()
        department = random.choice(["Sales", "Marketing", "Engineering", "HR", "Finance", "IT"])
        salary = random.randint(40000, 150000)
        hire_date = (datetime.now() - timedelta(days=random.randint(365, 365*10))).strftime('%Y-%m-%d')
        employee_id = random.randint(10000, 99999)

        password = fake.password(length=12, special_chars=True, digits=True, upper_case=True, lower_case=True)
        hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

        data.append({
            "employee_id": employee_id,
            "first_name": first_name,
            "last_name": last_name,
            "middle_name": middle_name,
            "gender": gender,
            "email": email,
            "phone_number": phone_number,
            "address": address,
            "birthdate": birthdate,
            "ssn": ssn,
            "job_title": job_title,
            "department": department,
            "salary": salary,
            "hire_date": hire_date,
            "hashed_password": hashed_password.decode('utf-8'),
        })

    df = pd.DataFrame(data)
    return df

def generate_and_upload_csv(num_rows=1000, filename="employee_data.csv", bucket_name="your-bucket-name"):
    """Generates employee data, saves to CSV in memory, and uploads to GCS."""

    df = generate_employee_data(num_rows)

    csv_buffer = df.to_csv(index=False, lineterminator='\n', encoding='utf-8')

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)

    blob.upload_from_string(csv_buffer, 'text/csv')

    print(f"Employee data saved to {filename} and uploaded to gs://{bucket_name}/{filename}")


if __name__ == "__main__":
    num_rows = 100000
    filename = "employee_data.csv"
    bucket_name = "your-bucket-name"  # **REPLACE WITH YOUR BUCKET NAME**

    generate_and_upload_csv(num_rows, filename, bucket_name)