import os
import json
import logging
import psycopg2
from psycopg2.extras import execute_values

# Configure logging to capture missing mandatory fields and general progress
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection parameters (update as needed)
DB_PARAMS = {
    "dbname": "business_db",
    "user": "admin",
    "password": "password",
    "host": "localhost",
    "port": 5432
}

def get_connection():
    return psycopg2.connect(**DB_PARAMS)

def load_jsonl_to_table(conn, folder_path, table_name, pk_field, required_fields, columns_mapping):
    """
    Generic fast-loader using psycopg2 execute_values.
    Gracefully handles empty/null fields using standard dictionary .get() 
    """
    if not os.path.exists(folder_path):
        logging.warning(f"Directory not found, skipping: {folder_path}")
        return

    cursor = conn.cursor()
    files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.jsonl')]
    
    if not files:
        logging.info(f"No JSONL files found in {folder_path}")
        return

    db_cols = list(columns_mapping.values())
    
    # Use ON CONFLICT DO NOTHING assuming the tables use the provided PK as the primary constraint
    conflict_sql = f"ON CONFLICT ({columns_mapping[pk_field]}) DO NOTHING" if pk_field else ""
    query = f"INSERT INTO {table_name} ({', '.join(db_cols)}) VALUES %s {conflict_sql}"
    
    batch = []
    batch_size = 5000  # High batch size for fast execution
    total_inserted = 0
    total_skipped = 0

    for file_path in files:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_number, line in enumerate(f, 1):
                if not line.strip(): continue
                record = json.loads(line)
                
                # Check for mandatory keys
                missing_reqs = [req for req in required_fields if not record.get(req)]
                if missing_reqs:
                    logging.error(f"[{table_name}] Missing mandatory fields {missing_reqs} on line {line_number} in {os.path.basename(file_path)}. Skipping record.")
                    total_skipped += 1
                    continue
                
                # Map JSON keys to PostgreSQL columns gracefully (falling back to None/Null)
                row = tuple(record.get(json_key) for json_key in columns_mapping.keys())
                batch.append(row)
                
                if len(batch) >= batch_size:
                    try:
                        execute_values(cursor, query, batch)
                        conn.commit()
                        total_inserted += len(batch)
                    except Exception as e:
                        logging.error(f"[{table_name}] Batch insert error: {e}")
                        conn.rollback()
                    batch = []

    # Final batch
    if batch:
        try:
            execute_values(cursor, query, batch)
            conn.commit()
            total_inserted += len(batch)
        except Exception as e:
            logging.error(f"[{table_name}] Final batch insert error: {e}")
            conn.rollback()
            
    cursor.close()
    logging.info(f"Finished loading {table_name}: Inserted {total_inserted} records, Skipped {total_skipped} anomalies.")

# =====================================================================
# 1. CORE OPERATIONAL ENTITIES (Headers)
# =====================================================================

def load_sales_order_headers(conn, base_path):
    mapping = {
        "salesOrder": "order_id",
        "soldToParty": "customer_id",
        "overallDeliveryStatus": "status",
        "creationDate": "order_date",
        "totalNetAmount": "total_amount"
    }
    required = ["salesOrder", "soldToParty"]
    load_jsonl_to_table(conn, os.path.join(base_path, 'sales_order_headers'), 'orders', 'salesOrder', required, mapping)

def load_outbound_delivery_headers(conn, base_path):
    mapping = {
        "deliveryDocument": "delivery_id",
        "shippingPoint": "shipping_point",
        "overallGoodsMovementStatus": "status",
        "actualGoodsMovementDate": "dispatch_date"
    }
    required = ["deliveryDocument"]
    load_jsonl_to_table(conn, os.path.join(base_path, 'outbound_delivery_headers'), 'deliveries', 'deliveryDocument', required, mapping)

def load_billing_document_headers(conn, base_path):
    mapping = {
        "billingDocument": "invoice_id",
        "soldToParty": "customer_id",
        "accountingDocument": "accounting_document",
        "totalNetAmount": "total_amount",
        "billingDocumentDate": "issue_date",
        "billingDocumentIsCancelled": "is_cancelled"
    }
    required = ["billingDocument"]
    load_jsonl_to_table(conn, os.path.join(base_path, 'billing_document_headers'), 'invoices', 'billingDocument', required, mapping)

def load_payments_accounts_receivable(conn, base_path):
    mapping = {
        "accountingDocument": "payment_id",
        "accountingDocumentItem": "payment_item_id",
        "amountInTransactionCurrency": "amount",
        "customer": "customer_id",
        "clearingAccountingDocument": "clearing_document",
        "clearingDate": "payment_date"
    }
    required = ["accountingDocument", "amountInTransactionCurrency"]
    load_jsonl_to_table(conn, os.path.join(base_path, 'payments_accounts_receivable'), 'payments', 'accountingDocument', required, mapping)

def load_journal_entry_items(conn, base_path):
    mapping = {
        "accountingDocument": "journal_entry_id",
        "accountingDocumentItem": "journal_item_id",
        "glAccount": "gl_account",
        "amountInTransactionCurrency": "amount"
    }
    required = ["accountingDocument"]
    load_jsonl_to_table(conn, os.path.join(base_path, 'journal_entry_items_accounts_receivable'), 'journal_entries', 'accountingDocument', required, mapping)

# =====================================================================
# 2. EDGE PROPERTY / ITEM ENTITIES
# =====================================================================

def load_sales_order_items(conn, base_path):
    mapping = {
        "salesOrder": "order_id",
        "salesOrderItem": "order_item_id",
        "material": "product_id",
        "requestedQuantity": "quantity",
        "netAmount": "unit_price"
    }
    required = ["salesOrder", "material"]
    load_jsonl_to_table(conn, os.path.join(base_path, 'sales_order_items'), 'order_items', None, required, mapping)

def load_outbound_delivery_items(conn, base_path):
    mapping = {
        "deliveryDocument": "delivery_id",
        "deliveryDocumentItem": "delivery_item_id",
        "referenceSdDocument": "order_id", # VERY IMPORTANT FOR GRAPH LINKAGE
        "actualDeliveryQuantity": "quantity"
    }
    required = ["deliveryDocument"]
    load_jsonl_to_table(conn, os.path.join(base_path, 'outbound_delivery_items'), 'delivery_items', None, required, mapping)

def load_billing_document_items(conn, base_path):
    mapping = {
        "billingDocument": "invoice_id",
        "billingDocumentItem": "invoice_item_id",
        "referenceSdDocument": "reference_id", # VERY IMPORTANT FOR GRAPH LINKAGE
        "material": "product_id",
        "netAmount": "amount"
    }
    required = ["billingDocument", "material"]
    load_jsonl_to_table(conn, os.path.join(base_path, 'billing_document_items'), 'invoice_items', None, required, mapping)

# =====================================================================
# 3. MASTER DATA ENTITIES (Supporting)
# =====================================================================

def load_business_partners(conn, base_path):
    mapping = {
        "businessPartner": "customer_id",
        "businessPartnerName": "name",
        "searchTerm1": "search_term"
    }
    required = ["businessPartner"]
    load_jsonl_to_table(conn, os.path.join(base_path, 'business_partners'), 'customers', 'businessPartner', required, mapping)

def load_business_partner_addresses(conn, base_path):
    mapping = {
        "addressID": "address_id",
        "businessPartner": "customer_id",
        "cityName": "city",
        "country": "country"
    }
    required = ["addressID", "businessPartner"]
    load_jsonl_to_table(conn, os.path.join(base_path, 'business_partner_addresses'), 'addresses', 'addressID', required, mapping)

def load_customer_company_assignments(conn, base_path):
    mapping = {
        "businessPartner": "customer_id",
        "companyCode": "company_code"
    }
    required = ["businessPartner", "companyCode"]
    load_jsonl_to_table(conn, os.path.join(base_path, 'customer_company_assignments'), 'customer_companies', None, required, mapping)

def load_customer_sales_area_assignments(conn, base_path):
    mapping = {
        "businessPartner": "customer_id",
        "salesOrganization": "sales_org",
        "distributionChannel": "dist_channel"
    }
    required = ["businessPartner", "salesOrganization"]
    load_jsonl_to_table(conn, os.path.join(base_path, 'customer_sales_areas'), 'customer_sales_areas', None, required, mapping)

def load_plants(conn, base_path):
    mapping = {
        "plant": "plant_id",
        "plantName": "name"
    }
    required = ["plant"]
    load_jsonl_to_table(conn, os.path.join(base_path, 'plants'), 'plants', 'plant', required, mapping)

def load_products(conn, base_path):
    mapping = {
        "product": "product_id",
        "productType": "category",
        "baseUnit": "unit"
    }
    required = ["product"]
    load_jsonl_to_table(conn, os.path.join(base_path, 'products'), 'products', 'product', required, mapping)

def load_product_descriptions(conn, base_path):
    mapping = {
        "product": "product_id",
        "productDescription": "description",
        "language": "language"
    }
    required = ["product"]
    load_jsonl_to_table(conn, os.path.join(base_path, 'product_descriptions'), 'product_descriptions', None, required, mapping)

def load_product_plants(conn, base_path):
    mapping = {
        "product": "product_id",
        "plant": "plant_id"
    }
    required = ["product", "plant"]
    load_jsonl_to_table(conn, os.path.join(base_path, 'product_plants'), 'product_plants', None, required, mapping)

def load_product_storage_locations(conn, base_path):
    mapping = {
        "product": "product_id",
        "plant": "plant_id",
        "storageLocation": "storage_location"
    }
    required = ["product", "plant", "storageLocation"]
    load_jsonl_to_table(conn, os.path.join(base_path, 'product_storage_locations'), 'product_storage', None, required, mapping)

# =====================================================================
# MAIN EXECUTION PIPELINE
# =====================================================================

def main():
    base_path = os.path.join(os.getcwd(), 'sap-o2c-data')
    
    try:
        conn = get_connection()
        logging.info("Successfully connected to PostgreSQL database.")
        
        # 1. Load Master Data First (to satisfy FK constraints)
        logging.info("--- LOADING MASTER DATA ---")
        load_business_partners(conn, base_path)
        load_business_partner_addresses(conn, base_path)
        load_customer_company_assignments(conn, base_path)
        load_customer_sales_area_assignments(conn, base_path)
        load_plants(conn, base_path)
        load_products(conn, base_path)
        load_product_descriptions(conn, base_path)
        load_product_plants(conn, base_path)
        load_product_storage_locations(conn, base_path)

        # 2. Load Transactional Headers
        logging.info("--- LOADING TRANSACTIONAL HEADERS ---")
        load_sales_order_headers(conn, base_path)
        load_outbound_delivery_headers(conn, base_path)
        load_billing_document_headers(conn, base_path)
        load_payments_accounts_receivable(conn, base_path)
        load_journal_entry_items(conn, base_path)

        # 3. Load Transactional Items (The Edges)
        logging.info("--- LOADING TRANSACTIONAL ITEMS AND EDGES ---")
        load_sales_order_items(conn, base_path)
        load_outbound_delivery_items(conn, base_path)
        load_billing_document_items(conn, base_path)
        
        logging.info("--- ALL DATA LOADED SUCCESSFULLY ---")
        conn.close()
        
    except psycopg2.Error as e:
        logging.critical(f"Database connection or execution failed: {e}")
    except Exception as e:
        logging.critical(f"Critical error running ETL process: {e}")

if __name__ == "__main__":
    main()
