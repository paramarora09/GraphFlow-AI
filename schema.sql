DROP SCHEMA public CASCADE;
CREATE SCHEMA public;

-- ====================================================================
-- 1. MASTER DATA (Supporting Entities)
-- ====================================================================

CREATE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255),
    search_term VARCHAR(255)
);

CREATE TABLE addresses (
    address_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) REFERENCES customers(customer_id) ON DELETE CASCADE,
    city VARCHAR(100),
    country VARCHAR(100)
);

CREATE TABLE customer_companies (
    customer_id VARCHAR(50) REFERENCES customers(customer_id) ON DELETE CASCADE,
    company_code VARCHAR(50),
    PRIMARY KEY (customer_id, company_code)
);

CREATE TABLE customer_sales_areas (
    customer_id VARCHAR(50) REFERENCES customers(customer_id) ON DELETE CASCADE,
    sales_org VARCHAR(50),
    dist_channel VARCHAR(50),
    PRIMARY KEY (customer_id, sales_org, dist_channel)
);

CREATE TABLE plants (
    plant_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE products (
    product_id VARCHAR(50) PRIMARY KEY,
    category VARCHAR(100),
    unit VARCHAR(50)
);

CREATE TABLE product_descriptions (
    product_id VARCHAR(50) REFERENCES products(product_id) ON DELETE CASCADE,
    description TEXT,
    language VARCHAR(10),
    PRIMARY KEY (product_id, language)
);

CREATE TABLE product_plants (
    product_id VARCHAR(50) REFERENCES products(product_id) ON DELETE CASCADE,
    plant_id VARCHAR(50) REFERENCES plants(plant_id) ON DELETE CASCADE,
    PRIMARY KEY (product_id, plant_id)
);

CREATE TABLE product_storage (
    product_id VARCHAR(50) REFERENCES products(product_id) ON DELETE CASCADE,
    plant_id VARCHAR(50) REFERENCES plants(plant_id) ON DELETE CASCADE,
    storage_location VARCHAR(100),
    PRIMARY KEY (product_id, plant_id, storage_location)
);

-- ====================================================================
-- 2. TRANSACTIONAL HEADERS (Main Graph Nodes)
-- ====================================================================

CREATE TABLE orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) REFERENCES customers(customer_id) ON DELETE RESTRICT,
    status VARCHAR(50),
    order_date TIMESTAMP,
    total_amount DECIMAL(18,2)
);

CREATE TABLE deliveries (
    delivery_id VARCHAR(50) PRIMARY KEY,
    shipping_point VARCHAR(100),
    status VARCHAR(50),
    dispatch_date TIMESTAMP
);

CREATE TABLE invoices (
    invoice_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) REFERENCES customers(customer_id) ON DELETE RESTRICT,
    accounting_document VARCHAR(50),
    total_amount DECIMAL(18,2),
    issue_date TIMESTAMP,
    is_cancelled BOOLEAN
);

CREATE TABLE payments (
    payment_id VARCHAR(50),
    payment_item_id VARCHAR(50),
    amount DECIMAL(18,2),
    customer_id VARCHAR(50) REFERENCES customers(customer_id) ON DELETE RESTRICT,
    clearing_document VARCHAR(50),
    payment_date TIMESTAMP,
    PRIMARY KEY (payment_id, payment_item_id)
);

CREATE TABLE journal_entries (
    journal_entry_id VARCHAR(50),
    journal_item_id VARCHAR(50),
    gl_account VARCHAR(50),
    amount DECIMAL(18,2),
    PRIMARY KEY (journal_entry_id, journal_item_id)
);

-- ====================================================================
-- 3. TRANSACTIONAL ITEMS (Graph Edges / Line Items)
-- ====================================================================

CREATE TABLE order_items (
    order_id VARCHAR(50) REFERENCES orders(order_id) ON DELETE CASCADE,
    order_item_id VARCHAR(50),
    product_id VARCHAR(50) REFERENCES products(product_id) ON DELETE RESTRICT,
    quantity DECIMAL(18,3),
    unit_price DECIMAL(18,2),
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE delivery_items (
    delivery_id VARCHAR(50) REFERENCES deliveries(delivery_id) ON DELETE CASCADE,
    delivery_item_id VARCHAR(50),
    order_id VARCHAR(50), -- Cannot strict FK back to Order due to SAP extraction behavior
    product_id VARCHAR(50) REFERENCES products(product_id) ON DELETE RESTRICT,
    quantity DECIMAL(18,3),
    PRIMARY KEY (delivery_id, delivery_item_id)
);

CREATE TABLE invoice_items (
    invoice_id VARCHAR(50) REFERENCES invoices(invoice_id) ON DELETE CASCADE,
    invoice_item_id VARCHAR(50),
    reference_id VARCHAR(50), -- Captures either Delivery/Order ID
    product_id VARCHAR(50) REFERENCES products(product_id) ON DELETE RESTRICT,
    amount DECIMAL(18,2),
    PRIMARY KEY (invoice_id, invoice_item_id)
);
