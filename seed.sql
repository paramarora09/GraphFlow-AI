-- Insert Addresses
INSERT INTO addresses (full_address, city, state, country, postal_code) VALUES 
('123 Main St', 'New York', 'NY', 'USA', '10001'),
('456 Market St', 'San Francisco', 'CA', 'USA', '94103');

-- Insert Customers
INSERT INTO customers (billing_address_id, name, email, phone) VALUES 
(1, 'Alice Smith', 'alice@example.com', '555-0100'),
(2, 'Bob Jones', 'bob@example.com', '555-0200');

-- Insert Products
INSERT INTO products (name, category, price) VALUES 
('Laptop Pro', 'Electronics', 1200.00),
('Wireless Mouse', 'Electronics', 45.00),
('Desk Chair', 'Furniture', 250.00);

-- Insert Orders (Bob's order will be an anomaly with no invoice)
INSERT INTO orders (customer_id, status, order_date) VALUES 
(1, 'COMPLETED', CURRENT_TIMESTAMP - INTERVAL '5 days'),
(2, 'PROCESSING', CURRENT_TIMESTAMP - INTERVAL '2 days'); 

-- Insert Order Items
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES 
(1, 1, 1, 1200.00),
(1, 2, 1, 45.00),
(2, 3, 2, 250.00);

-- Insert Deliveries
INSERT INTO deliveries (order_id, shipping_address_id, status, tracking_num, dispatch_date, delivery_date) VALUES
(1, 1, 'DELIVERED', 'TRK12345', CURRENT_TIMESTAMP - INTERVAL '4 days', CURRENT_TIMESTAMP - INTERVAL '1 day'),
(2, 2, 'IN_TRANSIT', 'TRK98765', CURRENT_TIMESTAMP - INTERVAL '1 day', NULL);

-- Insert Invoices (Notice we skip Order 2 to simulate a missing billing event)
INSERT INTO invoices (order_id, total_amount, status, issue_date, due_date) VALUES 
(1, 1245.00, 'PAID', CURRENT_TIMESTAMP - INTERVAL '4 days', CURRENT_TIMESTAMP + INTERVAL '26 days'); 

-- Insert Payments
INSERT INTO payments (invoice_id, amount, status, method, payment_date) VALUES
(1, 1245.00, 'COMPLETED', 'CREDIT_CARD', CURRENT_TIMESTAMP - INTERVAL '3 days');
