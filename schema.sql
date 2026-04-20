-- Drop tables if they exist to start fresh
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS customers;

-- Create Customers Table
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100) UNIQUE,
    phone_number VARCHAR(20),
    account_type VARCHAR(20),
    join_date DATE
);

-- Create Transactions Table (with Foreign Key linking to Customers)
CREATE TABLE transactions (
    transaction_id VARCHAR(36) PRIMARY KEY,
    customer_id INT,
    transaction_date DATETIME,
    transaction_type VARCHAR(20),
    amount DECIMAL(10, 2),
    status VARCHAR(20),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
