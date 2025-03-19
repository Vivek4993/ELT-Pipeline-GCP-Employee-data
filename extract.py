import csv
from faker import Faker
import random
import string
from google.cloud import storage
import os
import pandas as pd
import bcrypt
from datetime import datetime, timedelta
import json

fake = Faker()

avro_schema = {
    "type": "record",
    "name": "Employee",
    "namespace": "com.example.employee",
    "fields": [
        {"name": "employee_id", "type": "string"},  # employee_id as string
        {"name": "first_name", "type": "string"},
        {"name": "last_name", "type": "string"},
        {"name": "gender", "type": "string"},
        {"name": "email", "type": "string"},
        {"name": "phone_number", "type": "string"},
        {"name": "address", "type": "string"},
        {"name": "birthdate", "type": "string"},
        {"name": "ssn", "type": ["string", "null"]},
        {"name": "job_title", "type": "string"},
        {"name": "department", "type": "string"},
        {"name": "salary", "type": "string"},  # salary as string
        {"name": "hire_date", "type": "string"},
        {"name": "hashed_password", "type": "string"}
    ]
}

def generate_employee_data(num_rows=100):
    data = []
    for _ in range(num_rows):
        first_name = fake.first_name()
        last_name = fake.last_name()
        gender = random.choice(["Male", "Female", "Other"])
        email = fake.email()
        phone_number = fake.phone_number()
        address = fake.address().replace('\n', ' ') # Remove newlines from address
        birthdate = fake.date_of_birth(minimum_age=20, maximum_age=65).strftime('%Y-%m-%d')
        ssn = fake.ssn()
        job_title = fake.job().replace(',', ';')  # Replace commas in job_title
        department = random.choice(["Sales", "Marketing", "Engineering", "HR", "Finance", "IT"]).replace(',', ';') # Replace commas from department
        salary = random.randint(40000, 150000)
        hire_date = (datetime.now() - timedelta(days=random.randint(365, 365*10))).strftime('%Y-%m-%d')
        employee_id = random.randint(10000, 99999)
        password = fake.password(length=12, special_chars=True, digits=True, upper_case=True, lower_case=True)
        hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

        data.append({
            "employee_id": str(employee_id), # Convert employee_id to string
            "first_name": first_name,
            "last_name": last_name,
            "gender": gender,
            "email": email,
            "phone_number": phone_number,
            "address": address,
            "birthdate": birthdate,
            "ssn": ssn,
            "job_title": job_title,
            "department": department,
            "salary": str(salary), # Convert salary to string
            "hire_date": hire_date,
            "hashed_password": hashed_password.decode('utf-8'),
        })

    df = pd.DataFrame(data)
    return df


def generate_and_upload_csv(num_rows=100, filename="employee_data.csv", bucket_name="de-employee-data"):
    df = generate_employee_data(num_rows)

    filepath = os.path.join(os.path.dirname(__file__), filename)

    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['employee_id', 'first_name', 'last_name', 'gender', 'email', 'phone_number', 'address', 'birthdate', 'ssn', 'job_title', 'department', 'salary', 'hire_date', 'hashed_password']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)  # Crucial: Quote all fields

        writer.writeheader()
        for _, row in df.iterrows():
            writer.writerow(row.to_dict())  # Write the rows

    print(f"Employee data saved locally to: {filepath}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_filename(filepath)

if __name__ == "__main__":
    num_rows = 100
    filename = "employee_data.csv"
    bucket_name = "de-employee-data"  # **REPLACE WITH YOUR BUCKET NAME**

    generate_and_upload_csv(num_rows, filename, bucket_name)    

    print(f"Employee data uploaded to gs://{bucket_name}/{filename}")


if __name__ == "__main__":
    # ... (same as before)

    with open("employee.json", "w") as f:
        json.dump(avro_schema, f, indent=2)

    print("Avro schema saved to employee.json")