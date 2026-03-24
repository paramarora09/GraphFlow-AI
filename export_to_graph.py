import psycopg2
import pandas as pd
from neo4j import GraphDatabase
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Adjust these parameters to match your environment
PG_CONN_STR = "dbname=business_db user=admin password=password host=localhost port=5432"
NEO_URI = "bolt://localhost:7687"
NEO_USER = "neo4j"
NEO_PASS = "password"

def fetch_pg_batch(query):
    """ Fetch data from PostgreSQL natively into a list of dicts via pandas. """
    try:
        conn = psycopg2.connect(PG_CONN_STR)
        df = pd.read_sql_query(query, conn)
        conn.close()
        # Convert pandas timestamps to regular strings to avoid Neo4j serialization crashes
        for col in df.select_dtypes(include=['datetime64[ns]']).columns:
            df[col] = df[col].astype(str).replace('NaT', None)
        return df.to_dict('records')
    except Exception as e:
        logging.error(f"Postgres Error on query '{query[:50]}...': {e}")
        return []

def run_cypher_batch(session, cypher_query, batch, batch_size=2000):
    """ Safely execute UNWIND queries in optimized chunks. """
    if not batch:
        return
    for i in range(0, len(batch), batch_size):
        chunk = batch[i:i+batch_size]
        try:
            session.run(cypher_query, parameters={'batch': chunk})
        except Exception as e:
            logging.error(f"Neo4j Batch Error: {e}")

def export_nodes(session):
    logging.info("--- EXPORTING NODES ---")

    # 1. Master Data Nodes
    logging.info("Exporting Master Data Nodes...")
    customers = fetch_pg_batch("SELECT customer_id, name, search_term FROM customers")
    run_cypher_batch(session, '''
        UNWIND $batch AS row MERGE (c:Customer {id: row.customer_id})
        SET c.name = row.name, c.search_term = row.search_term
    ''', customers)

    addresses = fetch_pg_batch("SELECT address_id, city, country FROM addresses")
    run_cypher_batch(session, '''
        UNWIND $batch AS row MERGE (a:Address {id: row.address_id})
        SET a.city = row.city, a.country = row.country
    ''', addresses)

    companies = fetch_pg_batch("SELECT DISTINCT company_code FROM customer_companies")
    run_cypher_batch(session, '''
        UNWIND $batch AS row MERGE (cc:Company {id: row.company_code})
    ''', companies)

    sales_areas = fetch_pg_batch("SELECT DISTINCT sales_org FROM customer_sales_areas")
    run_cypher_batch(session, '''
        UNWIND $batch AS row MERGE (sa:SalesArea {id: row.sales_org})
    ''', sales_areas)

    plants = fetch_pg_batch("SELECT plant_id, name FROM plants")
    run_cypher_batch(session, '''
        UNWIND $batch AS row MERGE (pl:Plant {id: row.plant_id})
        SET pl.name = row.name
    ''', plants)

    products = fetch_pg_batch("SELECT product_id, category, unit FROM products")
    run_cypher_batch(session, '''
        UNWIND $batch AS row MERGE (p:Product {id: row.product_id})
        SET p.category = row.category, p.unit = row.unit
    ''', products)

    # 2. Transactional Headers Nodes
    logging.info("Exporting Transactional Nodes...")
    orders = fetch_pg_batch("SELECT order_id, status, order_date, total_amount FROM orders")
    run_cypher_batch(session, '''
        UNWIND $batch AS row MERGE (o:Order {id: row.order_id})
        SET o.status = row.status, o.order_date = row.order_date, o.total_amount = row.total_amount
    ''', orders)

    deliveries = fetch_pg_batch("SELECT delivery_id, shipping_point, status, dispatch_date FROM deliveries")
    run_cypher_batch(session, '''
        UNWIND $batch AS row MERGE (d:Delivery {id: row.delivery_id})
        SET d.shipping_point = row.shipping_point, d.status = row.status, d.dispatch_date = row.dispatch_date
    ''', deliveries)

    invoices = fetch_pg_batch("SELECT invoice_id, accounting_document, total_amount, issue_date, is_cancelled FROM invoices")
    run_cypher_batch(session, '''
        UNWIND $batch AS row MERGE (i:Invoice {id: row.invoice_id})
        SET i.accounting_document = row.accounting_document, i.total_amount = row.total_amount, i.issue_date = row.issue_date, i.is_cancelled = row.is_cancelled
    ''', invoices)

    payments = fetch_pg_batch("SELECT payment_id, payment_item_id, amount, clearing_document, payment_date FROM payments")
    run_cypher_batch(session, '''
        UNWIND $batch AS row MERGE (pay:Payment {id: row.payment_id, item_id: row.payment_item_id})
        SET pay.amount = row.amount, pay.clearing_document = row.clearing_document, pay.payment_date = row.payment_date
    ''', payments)

    journal_entries = fetch_pg_batch("SELECT journal_entry_id, journal_item_id, gl_account, amount FROM journal_entries")
    run_cypher_batch(session, '''
        UNWIND $batch AS row MERGE (je:JournalEntry {id: row.journal_entry_id, item_id: row.journal_item_id})
        SET je.gl_account = row.gl_account, je.amount = row.amount
    ''', journal_entries)


def export_edges(session):
    logging.info("--- EXPORTING EDGES ---")

    # 1. Master Data Relationships
    logging.info("Exporting Master Data Relationships...")
    
    # Customer -> Address
    run_cypher_batch(session, '''
        UNWIND $batch AS row
        MATCH (c:Customer {id: row.customer_id})
        MATCH (a:Address {id: row.address_id})
        MERGE (c)-[:LOCATED_AT]->(a)
    ''', fetch_pg_batch("SELECT customer_id, address_id FROM addresses WHERE customer_id IS NOT NULL"))

    # Customer -> Company
    run_cypher_batch(session, '''
        UNWIND $batch AS row
        MATCH (c:Customer {id: row.customer_id})
        MATCH (cc:Company {id: row.company_code})
        MERGE (c)-[:ASSIGNED_TO]->(cc)
    ''', fetch_pg_batch("SELECT customer_id, company_code FROM customer_companies"))

    # Product -> Plant
    run_cypher_batch(session, '''
        UNWIND $batch AS row
        MATCH (p:Product {id: row.product_id})
        MATCH (pl:Plant {id: row.plant_id})
        MERGE (p)-[:MANUFACTURED_AT]->(pl)
    ''', fetch_pg_batch("SELECT product_id, plant_id FROM product_plants"))

    # 2. Transactional Relationships (With edge properties)
    logging.info("Exporting Transactional Relationships (Orders & Deliveries)...")

    # Customer -> Order
    run_cypher_batch(session, '''
        UNWIND $batch AS row
        MATCH (c:Customer {id: row.customer_id})
        MATCH (o:Order {id: row.order_id})
        MERGE (c)-[:PLACED]->(o)
    ''', fetch_pg_batch("SELECT customer_id, order_id FROM orders WHERE customer_id IS NOT NULL"))

    # Order -> Product (from order_items)
    run_cypher_batch(session, '''
        UNWIND $batch AS row
        MATCH (o:Order {id: row.order_id})
        MATCH (p:Product {id: row.product_id})
        MERGE (o)-[rel:CONTAINS {item_id: row.order_item_id}]->(p)
        SET rel.quantity = row.quantity, rel.unit_price = row.unit_price
    ''', fetch_pg_batch("SELECT order_id, order_item_id, product_id, quantity, unit_price FROM order_items WHERE product_id IS NOT NULL"))

    # Delivery -> Product (from delivery_items)
    run_cypher_batch(session, '''
        UNWIND $batch AS row
        MATCH (d:Delivery {id: row.delivery_id})
        MATCH (p:Product {id: row.product_id})
        MERGE (d)-[rel:SHIPPED {item_id: row.delivery_item_id}]->(p)
        SET rel.quantity = row.quantity
    ''', fetch_pg_batch("SELECT delivery_id, delivery_item_id, product_id, quantity FROM delivery_items WHERE product_id IS NOT NULL"))

    # Delivery -> Order (Linked via delivery_items)
    run_cypher_batch(session, '''
        UNWIND $batch AS row
        MATCH (d:Delivery {id: row.delivery_id})
        MATCH (o:Order {id: row.order_id})
        MERGE (d)-[:FULFILLS]->(o)
    ''', fetch_pg_batch("SELECT DISTINCT delivery_id, order_id FROM delivery_items WHERE order_id IS NOT NULL AND order_id != ''"))

    # 3. Financial Flow Edge Mapping 
    logging.info("Exporting Financial Relationships (Invoices & Payments)...")

    # Invoice -> Customer
    run_cypher_batch(session, '''
        UNWIND $batch AS row
        MATCH (i:Invoice {id: row.invoice_id})
        MATCH (c:Customer {id: row.customer_id})
        MERGE (i)-[:BILLED_TO]->(c)
    ''', fetch_pg_batch("SELECT invoice_id, customer_id FROM invoices WHERE customer_id IS NOT NULL"))

    # Invoice -> Product (from invoice_items)
    run_cypher_batch(session, '''
        UNWIND $batch AS row
        MATCH (i:Invoice {id: row.invoice_id})
        MATCH (p:Product {id: row.product_id})
        MERGE (i)-[rel:BILLED_FOR {item_id: row.invoice_item_id}]->(p)
        SET rel.amount = row.amount
    ''', fetch_pg_batch("SELECT invoice_id, invoice_item_id, product_id, amount FROM invoice_items WHERE product_id IS NOT NULL"))

    # Invoice -> Delivery/Order (Linked via invoice_items' reference_id)
    # Since we don't know if reference_id points to an Order or a Delivery, we test against both separately
    references = fetch_pg_batch("SELECT DISTINCT invoice_id, reference_id FROM invoice_items WHERE reference_id IS NOT NULL AND reference_id != ''")
    run_cypher_batch(session, '''
        UNWIND $batch AS row
        MATCH (i:Invoice {id: row.invoice_id})
        MATCH (o:Order {id: row.reference_id})
        MERGE (i)-[:REFERENCES]->(o)
    ''', references)
    
    run_cypher_batch(session, '''
        UNWIND $batch AS row
        MATCH (i:Invoice {id: row.invoice_id})
        MATCH (d:Delivery {id: row.reference_id})
        MERGE (i)-[:REFERENCES]->(d)
    ''', references)

    # Payment -> Customer
    run_cypher_batch(session, '''
        UNWIND $batch AS row
        MATCH (pay:Payment {id: row.payment_id, item_id: row.payment_item_id})
        MATCH (c:Customer {id: row.customer_id})
        MERGE (pay)-[:PAID_BY]->(c)
    ''', fetch_pg_batch("SELECT payment_id, payment_item_id, customer_id FROM payments WHERE customer_id IS NOT NULL"))

    # Payment -> Invoice (The accounting link)
    # Finds an Invoice where the accounting_document generation matches the Payment's clearing_document logic
    run_cypher_batch(session, '''
        UNWIND $batch AS row
        MATCH (pay:Payment {id: row.payment_id, item_id: row.payment_item_id})
        MATCH (i:Invoice {accounting_document: row.clearing_document})
        MERGE (pay)-[:CLEARED]->(i)
    ''', fetch_pg_batch("SELECT payment_id, payment_item_id, clearing_document FROM payments WHERE clearing_document IS NOT NULL AND clearing_document != ''"))

def main():
    try:
        neo_driver = GraphDatabase.driver(NEO_URI, auth=(NEO_USER, NEO_PASS))
        with neo_driver.session() as session:
            logging.info("Successfully connected to Neo4j database.")
            export_nodes(session)
            export_edges(session)
            logging.info("--- GRAPH EXPORT DONE ---")
    except Exception as e:
        logging.critical(f"Graph Connection/Execution failed: {e}")
    finally:
        if 'neo_driver' in locals():
            neo_driver.close()

if __name__ == "__main__":
    main()
