import polars as pl
import psycopg2
from psycopg2.extras import execute_values
import os
import logging
from datetime import datetime
from typing import Dict, List, Tuple
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
BASE_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DATA_DIR = BASE_DIR / "data" / "processed"
SQL_SCHEMA_FILE = BASE_DIR / "sql" / "create_tables.sql"

# Database configuration from .env
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST'),
    'port': int(os.getenv('POSTGRES_PORT')),
    'database': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD')
}

def test_database_connection() -> bool:
    """Test database connection and PostgreSQL version"""
    try:
        logger.info("🔌 Testing database connection...")
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        logger.info(f"✅ Connected to PostgreSQL: {version.split(',')[0]}")
        
        cursor.close()
        connection.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Database connection failed: {str(e)}")
        return False

def create_database_connection() -> psycopg2.extensions.connection:
    """Create and return database connection"""
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        connection.autocommit = True
        logger.info(f"✅ Connected to database: {DB_CONFIG['database']}")
        return connection
    except Exception as e:
        logger.error(f"❌ Failed to connect to database: {str(e)}")
        raise

def execute_sql_file(connection: psycopg2.extensions.connection, sql_file_path: Path):
    """Execute SQL commands from file"""
    logger.info(f"📄 Executing SQL schema: {sql_file_path.name}")
    
    if not sql_file_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file_path}")
    
    cursor = connection.cursor()
    
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as file:
            sql_commands = file.read()
        
        cursor.execute(sql_commands)
        logger.info("✅ Database schema created successfully")
        
    except Exception as e:
        logger.error(f"❌ Error executing SQL file: {str(e)}")
        raise
    finally:
        cursor.close()

def load_csv_to_dataframe(file_path: Path) -> pl.DataFrame:
    """Load CSV file into Polars DataFrame with proper data types"""
    if not file_path.exists():
        raise FileNotFoundError(f"CSV file not found: {file_path}")
    
    try:
        # Try loading with specific dtypes for orders.csv
        if file_path.name == "orders.csv":
            logger.info(f"📊 Loading {file_path.name} with order-specific dtypes...")
            df = pl.read_csv(
                file_path,
                dtypes={
                    'order_id': pl.Utf8,       # Force as string
                    'item_id': pl.Int64,       # Keep as integer
                    'customer_id': pl.Int64,   # Keep as integer
                    'sku': pl.Utf8,           # Force as string
                    'order_date': pl.Utf8,    # Keep as string
                    'status': pl.Utf8,        # Force as string
                    'payment_method': pl.Utf8, # Force as string
                    'month': pl.Utf8,         # Force as string
                },
                null_values=["", "NULL", "null", "None"]
            )
        else:
            # Load other files normally
            df = pl.read_csv(file_path)
            
    except Exception as e:
        logger.warning(f"Dtype loading failed for {file_path.name}: {e}")
        logger.info("Using fallback loading with string inference...")
        
        # Fallback: Load with higher schema inference or treat problematic columns as strings
        df = pl.read_csv(
            file_path,
            infer_schema_length=50000,  # Increase schema inference
            ignore_errors=True,         # Skip problematic rows
            null_values=["", "NULL", "null", "None"]
        )
    
    logger.info(f"📊 Loaded {df.height:,} records from {file_path.name}")
    return df


def prepare_data_for_postgres(df: pl.DataFrame) -> List[Tuple]:
    """Convert DataFrame to list of tuples for PostgreSQL insertion - SIMPLIFIED"""
    
    # Convert to Python list directly - much simpler!
    data_tuples = []
    
    for row in df.iter_rows():
        clean_row = []
        for value in row:
            # Simple null handling
            if value is None:
                clean_row.append(None)
            elif isinstance(value, str) and value.lower() in ['nan', 'null', '']:
                clean_row.append(None)
            else:
                clean_row.append(value)
        data_tuples.append(tuple(clean_row))
    
    logger.info(f"📦 Prepared {len(data_tuples):,} records for PostgreSQL insertion")
    return data_tuples

def bulk_insert_data(connection: psycopg2.extensions.connection, 
                    table_name: str, 
                    data_tuples: List[Tuple], 
                    columns: List[str]):
    """Insert data into PostgreSQL table using batch insertion - WORKING VERSION"""
    
    cursor = connection.cursor()
    
    try:
        # Create INSERT query with proper quoting
        quoted_columns = [f'"{col}"' for col in columns]
        columns_str = ','.join(quoted_columns)
        
        # Create the base INSERT query
        insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES %s"
        
        logger.info(f"💾 Bulk inserting {len(data_tuples):,} records into {table_name}...")
        logger.info(f"🔍 Using {len(columns)} columns")
        
        # Use execute_values with the correct template
        execute_values(
            cursor,
            insert_query,
            data_tuples,
            template=None,
            page_size=1000
        )
        
        connection.commit()
        logger.info(f"✅ Successfully inserted {len(data_tuples):,} records into {table_name}")
        
    except Exception as e:
        connection.rollback()
        logger.error(f"❌ Error inserting data into {table_name}: {str(e)}")
        raise
    finally:
        cursor.close()


def load_customers_table(connection: psycopg2.extensions.connection):
    """Load customers data into PostgreSQL"""
    logger.info("👥 Loading customers table...")
    
    customers_file = PROCESSED_DATA_DIR / "customers.csv"
    df_customers = load_csv_to_dataframe(customers_file)
    
    # Get column names and prepare data
    columns = df_customers.columns
    data_tuples = prepare_data_for_postgres(df_customers)
    
    # Insert data
    bulk_insert_data(connection, "customers", data_tuples, columns)

def load_products_table(connection: psycopg2.extensions.connection):
    """Load products data into PostgreSQL"""
    logger.info("🛍️ Loading products table...")
    
    products_file = PROCESSED_DATA_DIR / "products.csv"
    df_products = load_csv_to_dataframe(products_file)
    
    columns = df_products.columns
    data_tuples = prepare_data_for_postgres(df_products)
    
    bulk_insert_data(connection, "products", data_tuples, columns)

def load_orders_table(connection: psycopg2.extensions.connection):
    """Load orders data into PostgreSQL"""
    logger.info("📦 Loading orders table...")
    
    orders_file = PROCESSED_DATA_DIR / "orders.csv"
    df_orders = load_csv_to_dataframe(orders_file)
    
    columns = df_orders.columns
    data_tuples = prepare_data_for_postgres(df_orders)
    
    bulk_insert_data(connection, "orders", data_tuples, columns)

def validate_data_load(connection: psycopg2.extensions.connection) -> Dict[str, int]:
    """Validate that data was loaded correctly"""
    logger.info("🔍 Validating data load...")
    
    cursor = connection.cursor()
    results = {}
    
    try:
        # Count records in each table
        validation_queries = {
            'customers': 'SELECT COUNT(*) FROM customers',
            'products': 'SELECT COUNT(*) FROM products', 
            'orders': 'SELECT COUNT(*) FROM orders'
        }
        
        for table_name, query in validation_queries.items():
            cursor.execute(query)
            count = cursor.fetchone()[0]
            results[table_name] = count
            logger.info(f"📊 {table_name.capitalize()}: {count:,} records")
        
        # Validate referential integrity
        logger.info("🔗 Checking referential integrity...")
        
        # Check orders -> customers references
        cursor.execute("""
            SELECT COUNT(*) FROM orders o 
            LEFT JOIN customers c ON o.customer_id = c.customer_id 
            WHERE c.customer_id IS NULL
        """)
        orphaned_customers = cursor.fetchone()[0]
        
        # Check orders -> products references  
        cursor.execute("""
            SELECT COUNT(*) FROM orders o 
            LEFT JOIN products p ON o.sku = p.sku 
            WHERE p.sku IS NULL
        """)
        orphaned_products = cursor.fetchone()[0]
        
        if orphaned_customers == 0 and orphaned_products == 0:
            logger.info("✅ Referential integrity: PERFECT")
        else:
            logger.warning(f"⚠️ Found {orphaned_customers} orphaned customer refs, {orphaned_products} orphaned product refs")
        
        # Business validation queries
        logger.info("📈 Running business validation queries...")
        
        # Total revenue
        cursor.execute("SELECT SUM(total) FROM orders")
        total_revenue = cursor.fetchone()[0]
        logger.info(f"💰 Total Revenue: ${total_revenue:,.2f}")
        
        # Average order value
        cursor.execute("""
            SELECT AVG(order_total) FROM (
                SELECT order_id, SUM(total) as order_total 
                FROM orders 
                GROUP BY order_id
            ) subq
        """)
        avg_order_value = cursor.fetchone()[0]
        logger.info(f"📊 Average Order Value: ${avg_order_value:.2f}")
        
        # Top categories
        cursor.execute("""
            SELECT p.category, COUNT(*) as order_count, SUM(o.total) as revenue
            FROM orders o
            JOIN products p ON o.sku = p.sku
            GROUP BY p.category
            ORDER BY revenue DESC
            LIMIT 5
        """)
        top_categories = cursor.fetchall()
        logger.info("🏆 Top 5 Categories by Revenue:")
        for category, count, revenue in top_categories:
            logger.info(f"   {category}: {count:,} orders, ${revenue:,.2f}")
        
        results['orphaned_customers'] = orphaned_customers
        results['orphaned_products'] = orphaned_products
        results['total_revenue'] = float(total_revenue) if total_revenue else 0
        results['avg_order_value'] = float(avg_order_value) if avg_order_value else 0
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Validation error: {str(e)}")
        raise
    finally:
        cursor.close()

def load_to_postgres() -> Dict[str, int]:
    """Main function to load all transformed data into PostgreSQL"""
    
    logger.info("🚀 Starting PostgreSQL data loading process...")
    start_time = datetime.now()
    
    connection = None
    
    try:
        # 1. Test connection first
        if not test_database_connection():
            raise Exception("Database connection test failed")
        
        # 2. Create database connection
        connection = create_database_connection()
        
        # 3. Create tables from SQL file
        execute_sql_file(connection, SQL_SCHEMA_FILE)
        
        # 4. Load data in correct order (dimensions first, then facts)
        logger.info("📊 Loading dimension tables first...")
        load_customers_table(connection)
        load_products_table(connection)
        
        logger.info("📊 Loading fact tables...")
        load_orders_table(connection)
        
        # 5. Validate the load
        validation_results = validate_data_load(connection)
        
        # 6. Final report
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("✅ PostgreSQL loading completed successfully!")
        logger.info(f"⏱️  Total loading time: {duration:.2f} seconds")
        logger.info("🎯 Database ready for analytics and reporting!")
        
        return validation_results
        
    except Exception as e:
        logger.error(f"❌ Loading failed: {str(e)}")
        raise
    
    finally:
        if connection:
            connection.close()
            logger.info("🔌 Database connection closed")

# Main execution
if __name__ == "__main__":
    print("=" * 60)
    print("🏪 RETAIL ETL PIPELINE - POSTGRESQL LOADING")
    print("=" * 60)
    
    # Check environment variables
    if not DB_CONFIG['password']:
        print("❌ Error: Database password not found in .env file")
        print("Please create .env file with DB_PASSWORD=your_password")
        exit(1)
    
    try:
        # Load all data
        results = load_to_postgres()
        
        print("\n📋 LOADING SUMMARY:")
        print("=" * 50)
        print(f"CUSTOMERS: {results['customers']:,} records loaded")
        print(f"PRODUCTS: {results['products']:,} records loaded") 
        print(f"ORDERS: {results['orders']:,} records loaded")
        print(f"TOTAL REVENUE: ${results['total_revenue']:,.2f}")
        print(f"AVG ORDER VALUE: ${results['avg_order_value']:.2f}")
        
        print(f"\n✅ SUCCESS: Complete ETL pipeline finished!")
        print("🎯 Your data is now ready for analytics in PostgreSQL!")
        print("\n📊 Try these sample queries:")
        print("   SELECT * FROM order_summary LIMIT 10;")
        print("   SELECT * FROM product_performance LIMIT 10;")
        
    except Exception as e:
        print(f"\n❌ LOADING FAILED: {str(e)}")
        print("Please check the error logs above for details.")
        print("\nCommon issues:")
        print("- PostgreSQL not running")
        print("- Wrong credentials in .env file")
        print("- Database 'retail_db' doesn't exist")
        print("- Permission issues")
