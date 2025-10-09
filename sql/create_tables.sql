
-- Drop tables in correct order (reverse of creation due to foreign keys)
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

-- Create Customers Dimension Table
CREATE TABLE customers (
    customer_id BIGINT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    "Gender" VARCHAR(20),
    age DECIMAL(5,2),
    customer_since VARCHAR(50),
    phone VARCHAR(30),
    place_name VARCHAR(100),
    county VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    region VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Products Dimension Table
CREATE TABLE products (
    sku VARCHAR(100) PRIMARY KEY,
    category VARCHAR(100),
    unit_price DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Orders Fact Table
CREATE TABLE orders (
    item_id BIGINT PRIMARY KEY,        
    order_id VARCHAR(100) NOT NULL,    
    order_date VARCHAR(20),
    status VARCHAR(50),
    customer_id BIGINT REFERENCES customers(customer_id),
    sku VARCHAR(100) REFERENCES products(sku),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    line_total DECIMAL(12,2),
    discount_amount DECIMAL(10,2),
    total DECIMAL(12,2),
    payment_method VARCHAR(50),
    year INTEGER,
    month VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX idx_orders_order_id ON orders(order_id);
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_sku ON orders(sku);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_customers_email ON customers(email);

-- Create views for common business queries
CREATE VIEW order_summary AS
SELECT 
    o.order_id,
    o.order_date,
    c.first_name || ' ' || c.last_name as customer_name,
    c.email,
    COUNT(o.item_id) as total_items,
    SUM(o.line_total) as subtotal,
    SUM(o.discount_amount) as total_discount,
    SUM(o.total) as order_total
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY o.order_id, o.order_date, c.first_name, c.last_name, c.email;

CREATE VIEW product_performance AS
SELECT 
    p.sku,
    p.category,
    p.unit_price as current_price,
    COUNT(o.item_id) as times_ordered,
    SUM(o.quantity) as total_quantity_sold,
    AVG(o.unit_price) as avg_selling_price,
    SUM(o.line_total) as total_revenue
FROM products p
LEFT JOIN orders o ON p.sku = o.sku
GROUP BY p.sku, p.category, p.unit_price
ORDER BY total_revenue DESC;