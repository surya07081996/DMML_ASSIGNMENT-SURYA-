
    CREATE TABLE CustomerChurn (
        customer_id INT PRIMARY KEY,
        credit_score FLOAT,
        country VARCHAR(50),
        gender VARCHAR(10),
        age INT,
        tenure INT,
        balance FLOAT,
        products_number INT,
        credit_card INT,
        active_member INT,
        estimated_salary FLOAT,
        churn INT,
        balance_salary_ratio FLOAT,
        credit_age_ratio FLOAT
    );
    