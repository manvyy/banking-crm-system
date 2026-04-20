import random
import uuid
from faker import Faker
from sqlalchemy import create_engine, text

# Initialize Faker
fake = Faker()

# Connect to the MySQL Docker container exposed on localhost
DB_URI = "mysql+pymysql://root:mypass@crm_mysql:3306/banking_crm"
engine = create_engine(DB_URI)

NUM_CUSTOMERS = 1000
NUM_TRANSACTIONS = 10000

def setup_database():
    with engine.connect() as conn:
        print("1. Creating Database Schema...")
        
        # Drop tables if they exist to start fresh
        conn.execute(text("DROP TABLE IF EXISTS transactions;"))
        conn.execute(text("DROP TABLE IF EXISTS customers;"))
        
        # Create Customers Table
        conn.execute(text("""
            CREATE TABLE customers (
                customer_id INT PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                email VARCHAR(100) UNIQUE,
                phone_number VARCHAR(20),
                account_type VARCHAR(20),
                join_date DATE
            );
        """))
        
        # Create Transactions Table (with Foreign Key linking to Customers)
        conn.execute(text("""
            CREATE TABLE transactions (
                transaction_id VARCHAR(36) PRIMARY KEY,
                customer_id INT,
                transaction_date DATETIME,
                transaction_type VARCHAR(20),
                amount DECIMAL(10, 2),
                status VARCHAR(20),
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            );
        """))
        print("Schema created successfully!")

        print(f"2. Generating and inserting {NUM_CUSTOMERS} customers...")
        customer_ids = []
        for _ in range(NUM_CUSTOMERS):
            c_id = fake.unique.random_number(digits=8, fix_len=True)
            customer_ids.append(c_id)
            conn.execute(text("""
                INSERT INTO customers (customer_id, first_name, last_name, email, phone_number, account_type, join_date)
                VALUES (:c_id, :fname, :lname, :email, :phone, :acc_type, :jdate)
            """), {
                "c_id": c_id, "fname": fake.first_name(), "lname": fake.last_name(),
                "email": fake.unique.email(), "phone": fake.phone_number()[:20],
                "acc_type": random.choice(["Checking", "Savings", "Credit"]),
                "jdate": fake.date_between(start_date='-5y', end_date='today')
            })
            
        print(f"3. Generating and inserting {NUM_TRANSACTIONS} transactions...")
        for _ in range(NUM_TRANSACTIONS):
            conn.execute(text("""
                INSERT INTO transactions (transaction_id, customer_id, transaction_date, transaction_type, amount, status)
                VALUES (:t_id, :c_id, :tdate, :ttype, :amt, :status)
            """), {
                "t_id": str(uuid.uuid4()),
                "c_id": random.choice(customer_ids),
                "tdate": fake.date_time_between(start_date='-1y', end_date='now'),
                "ttype": random.choice(["Deposit", "Withdrawal", "Transfer", "Payment"]),
                "amt": round(random.uniform(5.0, 5000.0), 2),
                "status": random.choices(["Completed", "Pending", "Failed"], weights=[0.9, 0.08, 0.02])[0]
            })

        conn.commit()
        print("✅ Database successfully populated with realistic mock data!")

if __name__ == "__main__":
    setup_database()
