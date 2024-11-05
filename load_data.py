import sqlite3
import json
import datetime

def create_database(db_name):
    # SQL statements to create tables
    create_users_table = '''
CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    state TEXT,
    created_date DATETIME,
    last_login DATETIME,
    role TEXT,
    active BOOLEAN
    -- Other user-specific fields
);
'''

    create_brands_table = '''
CREATE TABLE IF NOT EXISTS brands (
    brand_id TEXT PRIMARY KEY,
    barcode TEXT,
    brand_code TEXT,
    name TEXT,
    category TEXT,
    category_code TEXT,
    cpg TEXT,
    top_brand BOOLEAN
    -- Other brand-specific fields
);
'''

    create_receipts_table = '''
CREATE TABLE IF NOT EXISTS receipts (
    receipt_id TEXT PRIMARY KEY,
    purchase_date DATETIME,
    create_date DATETIME,
    date_scanned DATETIME,
    finished_date DATETIME,
    modify_date DATETIME,
    points_awarded_date DATETIME,
    total_spent REAL,
    bonus_points_earned INTEGER,
    bonus_points_earned_reason TEXT,
    purchased_item_count INTEGER,
    rewards_receipt_status TEXT
    -- Other receipt-level fields
);
'''

    create_receipt_items_table = '''
CREATE TABLE IF NOT EXISTS receipt_items (
    receipt_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    barcode TEXT,
    receipt_id TEXT NOT NULL,
    brand_id TEXT,
    brand_code TEXT,
    description TEXT,
    quantity_purchased INTEGER,
    item_price REAL,
    final_price REAL,
    needs_fetch_review BOOLEAN,
    partner_item_id TEXT,
    prevent_target_gap_points BOOLEAN,
    user_flagged_barcode TEXT,
    user_flagged_new_item BOOLEAN,
    user_flagged_price REAL,
    user_flagged_quantity INTEGER,
    -- Other item-specific fields
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (barcode) REFERENCES brands(barcode),
    FOREIGN KEY (receipt_id) REFERENCES receipts(receipt_id)
);
'''
    # Connect to the SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect(db_name)
    # Enable foreign key support
    conn.execute('PRAGMA foreign_keys = ON;')
    cursor = conn.cursor()

    # Execute the SQL statements to create tables
    cursor.execute(create_users_table)
    cursor.execute(create_brands_table)
    cursor.execute(create_receipts_table)
    cursor.execute(create_receipt_items_table)

    conn.commit()
    conn.close()
    print(f"Database '{db_name}' created with the required tables.")

def load_users_data(db_name, jsonl_file):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    with open(jsonl_file, 'r', encoding='utf-8') as infile:
        for line in infile:
            user = json.loads(line)
            # Extract fields from the JSON object
            user_id = user['_id']['oid']
            state = user.get('state')
            created_date = user.get('createdDate', {}).get('date')
            last_login = user.get('lastLogin', {}).get('date')
            role = user.get('role')
            active = user.get('active')

            print('User_id', user_id)
            print('state', state)
            print('created_date', created_date)
            print('last_login', last_login)
            print('role', role)
            print('active', active)
            
            # Convert timestamp to datetime if necessary
            if created_date:
                created_date = datetime.datetime.fromtimestamp(created_date / 1000)
            if last_login:
                last_login = datetime.datetime.fromtimestamp(last_login / 1000)

            # Insert into the users table
            cursor.execute('''
                INSERT INTO users (user_id, state, created_date, last_login, role, active)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (user_id, state, created_date, last_login, role, active))
    conn.commit()
    conn.close()
    print(f"Users data loaded into '{db_name}'.")


def load_brands_data(db_name, jsonl_file):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    with open(jsonl_file, 'r', encoding='utf-8') as infile:
        for line_number, line in enumerate(infile, start=1):
            try:
                brand = json.loads(line)
                # Extract fields from the JSON object
                brand_id = brand['_id']['oid']
                barcode = brand.get('barcode')
                brand_code = brand.get('brandCode')
                name = brand.get('name')
                category = brand.get('category')
                category_code = brand.get('categoryCode')
                cpg = brand.get('cpg', {}).get('oid') if brand.get('cpg') else None
                top_brand = brand.get('topBrand')

                # Insert into the brands table
                cursor.execute('''
                    INSERT INTO brands (
                        brand_id, barcode, brand_code, name, category,
                        category_code, cpg, top_brand
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    brand_id, barcode, brand_code, name, category,
                    category_code, cpg, top_brand
                ))

                print(f"Inserted brand {brand_id} (Line {line_number})")
            except Exception as e:
                print(f"Error inserting brand on line {line_number}: {e}")
                continue  

    conn.commit()
    conn.close()
    print(f"Brands data loaded into '{db_name}'.")

def load_receipts_data(db_name, jsonl_file):

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    with open(jsonl_file, 'r', encoding='utf-8') as infile:
        for line_number, line in enumerate(infile, start=1):
            try:
                receipt = json.loads(line)
                # Extract fields for the receipts table
                receipt_id = receipt['_id']['oid']
                purchase_date = receipt.get('purchaseDate', {}).get('date')
                create_date = receipt.get('createDate', {}).get('date')
                date_scanned = receipt.get('dateScanned', {}).get('date')
                finished_date = receipt.get('finishedDate', {}).get('date')
                modify_date = receipt.get('modifyDate', {}).get('date')
                points_awarded_date = receipt.get('pointsAwardedDate', {}).get('date')
                total_spent = receipt.get('totalSpent')
                bonus_points_earned = receipt.get('bonusPointsEarned')
                bonus_points_earned_reason = receipt.get('bonusPointsEarnedReason')
                purchased_item_count = receipt.get('purchasedItemCount')
                rewards_receipt_status = receipt.get('rewardsReceiptStatus')
                user_id = receipt.get('userId')

                # Convert timestamps to ISO format strings if necessary
                def convert_timestamp(ts):
                    return datetime.datetime.fromtimestamp(ts / 1000).isoformat() if ts else None

                purchase_date = convert_timestamp(purchase_date)
                create_date = convert_timestamp(create_date)
                date_scanned = convert_timestamp(date_scanned)
                finished_date = convert_timestamp(finished_date)
                modify_date = convert_timestamp(modify_date)
                points_awarded_date = convert_timestamp(points_awarded_date)

                # Insert into the receipts table
                cursor.execute('''
                    INSERT INTO receipts (
                        receipt_id, purchase_date, create_date, date_scanned, finished_date,
                        modify_date, points_awarded_date, total_spent, bonus_points_earned,
                        bonus_points_earned_reason, purchased_item_count, rewards_receipt_status
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    receipt_id, purchase_date, create_date, date_scanned, finished_date,
                    modify_date, points_awarded_date, total_spent, bonus_points_earned,
                    bonus_points_earned_reason, purchased_item_count, rewards_receipt_status
                ))

                # Process receipt items
                receipt_items = receipt.get('rewardsReceiptItemList', [])
                for item in receipt_items:
                    # Extract fields for the receipt_items table
                    barcode = item.get('barcode')
                    brand_id = item.get('brand_id')  # Assuming brand_id is provided; if not, you may need to map it
                    description = item.get('description')
                    quantity_purchased = item.get('quantityPurchased')
                    item_price = item.get('itemPrice')
                    final_price = item.get('finalPrice')
                    needs_fetch_review = item.get('needsFetchReview')
                    partner_item_id = item.get('partnerItemId')
                    prevent_target_gap_points = item.get('preventTargetGapPoints')
                    user_flagged_barcode = item.get('userFlaggedBarcode')
                    user_flagged_new_item = item.get('userFlaggedNewItem')
                    user_flagged_price = item.get('userFlaggedPrice')
                    user_flagged_quantity = item.get('userFlaggedQuantity')
                    brand_code = item.get('brandCode')

                    # Insert into the receipt_items table
                    cursor.execute('''
                        INSERT INTO receipt_items (
                            user_id, barcode, receipt_id, brand_id, brand_code, description,
                            quantity_purchased, item_price, final_price, needs_fetch_review,
                            partner_item_id, prevent_target_gap_points, user_flagged_barcode,
                            user_flagged_new_item, user_flagged_price, user_flagged_quantity
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        user_id, barcode, receipt_id, brand_id, brand_code, description,
                        quantity_purchased, item_price, final_price, needs_fetch_review,
                        partner_item_id, prevent_target_gap_points, user_flagged_barcode,
                        user_flagged_new_item, user_flagged_price, user_flagged_quantity
                    ))

                print(f"Inserted receipt {receipt_id} and its items (Line {line_number})")
            except Exception as e:
                print(f"Error inserting receipt on line {line_number}: {e}")
                continue  
    conn.commit()
    conn.close()
    print(f"Receipts data loaded into '{db_name}'.")

def select_users_data(db_name):

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM users')
    rows = cursor.fetchall()

    if rows:
        column_names = [description[0] for description in cursor.description]
        print(f"{' | '.join(column_names)}")
        print('-' * 50)

        for row in rows:
            print(' | '.join(str(item) if item is not None else 'NULL' for item in row))
    else:
        print("No data found in the brands table.")

    conn.close()

def select_brands_data(db_name):
    # Function to select and display data from the brands table
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM brands')
    rows = cursor.fetchall()

    if rows:
        column_names = [description[0] for description in cursor.description]
        print(f"{' | '.join(column_names)}")
        print('-' * 50)

        for row in rows:
            print(' | '.join(str(item) if item is not None else 'NULL' for item in row))
    else:
        print("No data found in the brands table.")

    conn.close()

def select_receipts_data(db_name):
    # Function to select and display data from the receipts table
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM receipts')
    rows = cursor.fetchall()

    if rows:
        column_names = [description[0] for description in cursor.description]
        print(f"{' | '.join(column_names)}")
        print('-' * 50)

        for row in rows:
            print(' | '.join(str(item) if item is not None else 'NULL' for item in row))
    else:
        print("No data found in the receipts table.")

    conn.close()

def select_receipt_items_data(db_name):
    # Function to select and display data from the receipt_items table
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM receipt_items')
    rows = cursor.fetchall()

    if rows:
        column_names = [description[0] for description in cursor.description]
        print(f"{' | '.join(column_names)}")
        print('-' * 50)

        for row in rows:
            print(' | '.join(str(item) if item is not None else 'NULL' for item in row))
    else:
        print("No data found in the receipt_items table.")

    conn.close()

if __name__ == "__main__":
    database_name = 'fetch_data.db'

    # Create database
    create_database(database_name)

    # Load data into users table
    users_jsonl_file = 'users.jsonl'
    load_users_data(database_name, users_jsonl_file)

    # Load data into brands table
    brands_jsonl_file = 'brands.jsonl'
    load_brands_data(database_name, brands_jsonl_file)

    # Load data into receipts and receipt_items tables
    receipts_jsonl_file = 'receipts.jsonl'
    load_receipts_data(database_name, receipts_jsonl_file)

    # # Optional: Select and display data from the tables
    # select_users_data(database_name)
    # select_brands_data(database_name)
    # select_receipts_data(database_name)
    # select_receipt_items_data(database_name)
