import psycopg2
import csv
import os

# Connection parameters from docker/postgres_docker_compose.yml
DB_HOST = "localhost"
DB_PORT = 5455
DB_NAME = "school_info"
DB_USER = "postgresUser"
DB_PASSWORD = "postgresPW"
SCHEMA = "public"

DATA_DIR = "data/sample_data"

TABLES = {
    "classes_schedules": {
        "columns": [
            ("class_id", "VARCHAR(10) PRIMARY KEY"),
            ("class_name", "VARCHAR(100)"),
            ("subject", "VARCHAR(50)"),
            ("teacher_name", "VARCHAR(100)"),
            ("teacher_id", "VARCHAR(10)"),
            ("teacher_email", "VARCHAR(100)"),
            ("room_number", "VARCHAR(20)"),
            ("building", "VARCHAR(50)"),
            ("class_time", "VARCHAR(50)"),
            ("duration_minutes", "INTEGER"),
            ("max_students", "INTEGER"),
            ("enrolled_students", "INTEGER"),
            ("grade_level", "INTEGER"),
            ("semester", "VARCHAR(20)"),
            ("academic_year", "INTEGER"),
            ("class_description", "TEXT"),
            ("prerequisites", "TEXT"),
            ("textbook_required", "TEXT"),
            ("supply_list", "TEXT")
        ]
    },
    "grades_assignments": {
        "columns": [
            ("assignment_id", "VARCHAR(10) PRIMARY KEY"),
            ("student_id", "INTEGER"),
            ("student_name", "VARCHAR(100)"),
            ("class_id", "VARCHAR(10)"),
            ("class_name", "VARCHAR(100)"),
            ("teacher_name", "VARCHAR(100)"),
            ("assignment_name", "VARCHAR(100)"),
            ("assignment_type", "VARCHAR(50)"),
            ("due_date", "DATE"),
            ("submission_date", "DATE"),
            ("points_possible", "INTEGER"),
            ("points_earned", "INTEGER"),
            ("percentage", "REAL"),
            ("letter_grade", "VARCHAR(5)"),
            ("late_penalty", "VARCHAR(50)"),
            ("comments", "TEXT"),
            ("subject", "VARCHAR(50)"),
            ("grade_level", "INTEGER"),
            ("semester", "VARCHAR(20)"),
            ("academic_year", "INTEGER"),
            ("weight_category", "VARCHAR(50)"),
            ("rubric_score", "REAL")
        ]
    },
    "student_information": {
        "columns": [
            ("student_id", "INTEGER PRIMARY KEY"),
            ("full_name", "VARCHAR(100)"),
            ("first_name", "VARCHAR(50)"),
            ("last_name", "VARCHAR(50)"),
            ("age", "INTEGER"),
            ("grade", "INTEGER"),
            ("class_section", "VARCHAR(10)"),
            ("teacher_name", "VARCHAR(100)"),
            ("teacher_email", "VARCHAR(100)"),
            ("school_name", "VARCHAR(100)"),
            ("school_address", "VARCHAR(200)"),
            ("parent_name", "VARCHAR(100)"),
            ("parent_phone", "VARCHAR(30)"),
            ("parent_email", "VARCHAR(100)"),
            ("enrollment_date", "DATE"),
            ("fees_paid", "INTEGER"),
            ("total_fees", "INTEGER"),
            ("subject_grades", "TEXT"),
            ("attendance_percentage", "REAL"),
            ("notes", "TEXT")
        ]
    },
    "teacher_staff": {
        "columns": [
            ("teacher_id", "VARCHAR(10) PRIMARY KEY"),
            ("full_name", "VARCHAR(100)"),
            ("first_name", "VARCHAR(50)"),
            ("last_name", "VARCHAR(50)"),
            ("email", "VARCHAR(100)"),
            ("phone", "VARCHAR(30)"),
            ("department", "VARCHAR(50)"),
            ("position", "VARCHAR(50)"),
            ("salary", "INTEGER"),
            ("hire_date", "DATE"),
            ("years_experience", "INTEGER"),
            ("education_level", "VARCHAR(100)"),
            ("certifications", "TEXT"),
            ("subjects_taught", "TEXT"),
            ("classes_assigned", "TEXT"),
            ("office_room", "VARCHAR(30)"),
            ("office_hours", "VARCHAR(50)"),
            ("emergency_contact", "VARCHAR(100)"),
            ("emergency_phone", "VARCHAR(30)"),
            ("address", "VARCHAR(200)"),
            ("birth_date", "DATE"),
            ("ssn_last_4", "INTEGER")
        ]
    }
}

def connect():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def create_schema_and_tables(conn):
    with conn.cursor() as cur:
        # Create schema if not exists
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")
        for table, meta in TABLES.items():
            columns_sql = ", ".join([f"{col} {dtype}" for col, dtype in meta["columns"]])
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA}.{table} (
                    {columns_sql}
                );
            """)
    conn.commit()

def parse_value(value, dtype):
    if value == "" or value is None:
        return None
    if "INTEGER" in dtype:
        try:
            return int(value)
        except Exception:
            return None
    if "REAL" in dtype:
        try:
            return float(value)
        except Exception:
            return None
    if "DATE" in dtype:
        # Accepts YYYY-MM-DD or similar
        return value if value else None
    return value

def load_csv_to_table(conn, table_name, csv_path):
    columns = [col for col, _ in TABLES[table_name]["columns"]]
    dtypes = [dtype for _, dtype in TABLES[table_name]["columns"]]
    with open(csv_path, newline='', encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        rows = []
        for row in reader:
            values = [parse_value(row.get(col, None), dtype) for col, dtype in zip(columns, dtypes)]
            rows.append(values)
    with conn.cursor() as cur:
        # Truncate table before loading
        cur.execute(f"TRUNCATE {SCHEMA}.{table_name} RESTART IDENTITY CASCADE;")
        # Insert rows
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO {SCHEMA}.{table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        for row in rows:
            cur.execute(insert_sql, row)
    conn.commit()

def main():
    conn = connect()
    try:
        create_schema_and_tables(conn)
        for table in TABLES:
            csv_path = os.path.join(DATA_DIR, f"{table}.csv")
            if os.path.exists(csv_path):
                print(f"Loading {csv_path} into {SCHEMA}.{table} ...")
                load_csv_to_table(conn, table, csv_path)
                print(f"Loaded {csv_path} successfully.")
            else:
                print(f"CSV file {csv_path} not found, skipping.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
