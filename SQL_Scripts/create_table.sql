-- =================================================================
-- Drop existing database (optional) and create a fresh database
-- =================================================================
--IF EXISTS (SELECT name FROM sys.databases WHERE name = N'ecom_db')
--BEGIN
--    ALTER DATABASE ecom_db SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
--    DROP DATABASE ecom_db;
--END

CREATE DATABASE ecom_db;

USE ecom_db;

-- =================================================================
-- Customers Table
-- =================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.customers') AND type = N'U')
BEGIN
    CREATE TABLE dbo.customers (
        customer_id INT IDENTITY(1,1) PRIMARY KEY,
        first_name NVARCHAR(60) NOT NULL,
        last_name NVARCHAR(60) NOT NULL,
        email NVARCHAR(255) NOT NULL UNIQUE,
        phone NVARCHAR(40) NULL,
        country NVARCHAR(80) NULL,
        state NVARCHAR(80) NULL,
        city NVARCHAR(80) NULL,
        postal_code NVARCHAR(20) NULL,
        street NVARCHAR(255) NULL,
        signup_date DATETIME2(3) NOT NULL,
        last_login DATETIME2(3) NULL,
        segment NVARCHAR(30) NULL,
        lifetime_value DECIMAL(14,2) NOT NULL DEFAULT (0.00)
    );
END

-- =================================================================
-- Products Table
-- =================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.products') AND type = N'U')
BEGIN
    CREATE TABLE dbo.products (
        product_id INT IDENTITY(1,1) PRIMARY KEY,
        sku NVARCHAR(64) NOT NULL UNIQUE,
        product_name NVARCHAR(255) NOT NULL,
        category NVARCHAR(80) NOT NULL,
        brand NVARCHAR(80) NULL,
        description NVARCHAR(MAX) NULL,
        price DECIMAL(10,2) NOT NULL,
        rrp DECIMAL(10,2) NULL,
        created_at DATETIME2(3) NOT NULL,
        popularity INT NOT NULL DEFAULT (1)
    );
END

-- =================================================================
-- Inventory Table
-- =================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.inventory') AND type = N'U')
BEGIN
    CREATE TABLE dbo.inventory (
        inventory_id INT IDENTITY(1,1) PRIMARY KEY,
        product_id INT NOT NULL UNIQUE,
        stock_level INT NOT NULL,
        reorder_threshold INT NOT NULL,
        last_restocked DATETIME2(3) NULL,
        CONSTRAINT FK_inventory_product FOREIGN KEY (product_id) REFERENCES dbo.products(product_id)
    );
END

-- =================================================================
-- Orders Table
-- =================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.orders') AND type = N'U')
BEGIN
    CREATE TABLE dbo.orders (
        order_id INT IDENTITY(1,1) PRIMARY KEY,
        order_uuid NVARCHAR(36) NOT NULL UNIQUE,
        customer_id INT NOT NULL,
        order_date DATETIME2(3) NOT NULL,
        status NVARCHAR(30) NULL,
        payment_method NVARCHAR(40) NULL,
        subtotal DECIMAL(12,2) NOT NULL,
        shipping DECIMAL(10,2) NOT NULL,
        tax DECIMAL(10,2) NOT NULL,
        discount DECIMAL(10,2) NOT NULL,
        total_amount DECIMAL(12,2) NOT NULL,
        placed_via NVARCHAR(64) NULL,
        CONSTRAINT FK_orders_customer FOREIGN KEY (customer_id) REFERENCES dbo.customers(customer_id)
    );
END

-- =================================================================
-- Order Items Table
-- =================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.order_items') AND type = N'U')
BEGIN
    CREATE TABLE dbo.order_items (
        order_item_id INT IDENTITY(1,1) PRIMARY KEY,
        order_id INT NOT NULL,
        product_id INT NOT NULL,
        sku NVARCHAR(64) NULL,
        quantity INT NOT NULL,
        unit_price DECIMAL(10,2) NOT NULL,
        line_total DECIMAL(12,2) NOT NULL,
        CONSTRAINT FK_orderitems_order FOREIGN KEY (order_id) REFERENCES dbo.orders(order_id),
        CONSTRAINT FK_orderitems_product FOREIGN KEY (product_id) REFERENCES dbo.products(product_id)
    );
END

-- =================================================================
-- Clickstream Table
-- =================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.clickstream') AND type = N'U')
BEGIN
    CREATE TABLE dbo.clickstream (
        click_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        customer_id INT NULL,
        session_id NVARCHAR(64) NULL,
        page NVARCHAR(80) NULL,
        page_url NVARCHAR(255) NULL,
        product_id INT NULL,
        [timestamp] DATETIME2(3) NULL,
        device NVARCHAR(40) NULL,
        user_agent NVARCHAR(255) NULL,
        referrer NVARCHAR(255) NULL,
        utm_source NVARCHAR(80) NULL,
        ip_address NVARCHAR(45) NULL,
        CONSTRAINT FK_click_customer FOREIGN KEY (customer_id) REFERENCES dbo.customers(customer_id),
        CONSTRAINT FK_click_product FOREIGN KEY (product_id) REFERENCES dbo.products(product_id)
    );
END

-- =================================================================
-- Indexes
-- =================================================================
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_orders_customer_date' AND object_id = OBJECT_ID('dbo.orders'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_orders_customer_date ON dbo.orders(customer_id, order_date);
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_click_session_time' AND object_id = OBJECT_ID('dbo.clickstream'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_click_session_time ON dbo.clickstream(session_id, [timestamp]);
END
