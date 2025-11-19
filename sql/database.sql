-- DB Control
CREATE DATABASE IF NOT EXISTS Control;
USE Control;

CREATE TABLE configs (
    ID INT PRIMARY KEY AUTO_INCREMENT,
    file_path VARCHAR(500) NOT NULL,
    file_status ENUM('NEW', 'PROCESSING', 'DONE', 'ERROR') NOT NULL DEFAULT 'NEW',
    create_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    processed_at DATETIME NULL
);

CREATE TABLE log (
    ID BIGINT PRIMARY KEY AUTO_INCREMENT,
    job_name VARCHAR(100) NOT NULL,
    log_level ENUM('INFO', 'ERROR', 'WARN') NOT NULL,
    message TEXT,
    log_time DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE job_status (
    job_name VARCHAR(100) PRIMARY KEY,
    is_running BOOLEAN NOT NULL DEFAULT FALSE,
    last_run DATETIME
);

CREATE TABLE source_config (
    config_key VARCHAR(100) PRIMARY KEY,
    config_value VARCHAR(500) NOT NULL,
    description TEXT,
);

-- DB Staging
CREATE DATABASE IF NOT EXISTS Staging;
USE Staging;

CREATE TABLE gold_prices (
    ID BIGINT PRIMARY KEY AUTO_INCREMENT,
    config_id INT NOT NULL,  -- Khóa ngoại từ DB Control
    region VARCHAR(50),
    buying VARCHAR(50), 	-- dữ liệu cào về có thể là text, chứa dấu , nên set là varchar
    purchasing VARCHAR(50),
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP
);


-- DB Data Warehouse
CREATE DATABASE IF NOT EXISTS DataWarehouse;
USE DataWarehouse;

CREATE TABLE Dim_Date (
    date_key INT PRIMARY KEY, -- Khóa chính nhân tạo (Surrogate Key)
    full_datetime DATETIME NOT NULL,
    Date DATE NOT NULL,
    Hour INT NOT NULL,
    Minute INT NOT NULL,
    day_of_week VARCHAR(20),
    Month VARCHAR(20),
    Quarter INT,
    Year INT,
    UNIQUE(full_datetime) -- Đảm bảo không trùng lặp thời gian
);

CREATE TABLE Dim_Product (
    product_key INT PRIMARY KEY AUTO_INCREMENT,
    product_type VARCHAR(100) NOT NULL, -- "Vàng miếng SJC 999.9"
    unit VARCHAR(50), -- "Chỉ", "Lượng"
    UNIQUE(product_type)
);



CREATE TABLE Dim_Region (
    region_key INT PRIMARY KEY AUTO_INCREMENT,
    region_name VARCHAR(100) NOT NULL, -- "Hồ Chí Minh"
    UNIQUE(region_name)
);

USE DataWarehouse;
CREATE TABLE Fact_GiaVang (
    date_key INT NOT NULL,
    product_key INT NOT NULL,
    region_key INT NOT NULL,
    buying_price BIGINT, -- giá sau khi làm sạch 
    purchasing_price BIGINT, 
    
    -- Khóa chính tổng hợp
    PRIMARY KEY (date_key, product_key, region_key),
    
    -- Khai báo các khóa ngoại
    FOREIGN KEY (date_key) REFERENCES Dim_Date(date_key),
    FOREIGN KEY (product_key) REFERENCES Dim_Product(product_key),
    FOREIGN KEY (region_key) REFERENCES Dim_Region(region_key)
);


CREATE DATABASE IF NOT EXISTS DataMart;
USE DataMart;

CREATE TABLE Agg_GiaVang_Report (
    report_date DATE NOT NULL,
    product_type VARCHAR(100) NOT NULL,
    region_name VARCHAR(100) NOT NULL,
    
    -- Dữ liệu đã được tính toán
    avg_buying DECIMAL(18, 2),
    avg_purchasing DECIMAL(18, 2),
    last_buying_price BIGINT,
    last_purchasing_price BIGINT,
    min_buying_price BIGINT,
    max_buying_price BIGINT,
    
    -- Khóa chính tổng hợp
    PRIMARY KEY (report_date, product_type, region_name)
);