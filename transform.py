import sqlite3


def top_5_brands_recent_month(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    query = """
    WITH most_recent_month AS (
      SELECT DATE(strftime('%Y-%m-01', MAX(date_scanned))) AS month_start
      FROM receipts
    ),
    receipts_in_month AS (
      SELECT receipt_id
      FROM receipts r
      JOIN most_recent_month mrm ON DATE(strftime('%Y-%m-01', r.date_scanned)) = mrm.month_start
    ),
    brand_receipt_counts AS (
      SELECT ri.barcode, COUNT(DISTINCT ri.receipt_id) AS receipt_count
      FROM receipt_items ri
      JOIN receipts_in_month rim ON ri.receipt_id = rim.receipt_id
      WHERE ri.barcode IS NOT NULL
      GROUP BY ri.barcode
    )
    SELECT b.name AS brand_name, brc.barcode, brc.receipt_count
    FROM brand_receipt_counts brc
    JOIN brands b ON brc.barcode = b.barcode
    ORDER BY brc.receipt_count DESC
    LIMIT 5;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    print("Top 5 Brands by Receipts Scanned for the Most Recent Month:")
    print("Brand Name | Barcode | Receipt Count")
    print("-" * 50)
    for row in results:
        print(f"{row[0]} | {row[1]} | {row[2]}")

    conn.close()

def compare_top_5_brands_recent_previous_month(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    query = """
    WITH months AS (
      SELECT DISTINCT DATE(strftime('%Y-%m-01', date_scanned)) AS month_start
      FROM receipts
      ORDER BY month_start DESC
      LIMIT 2
    ),
    ranked_months AS (
      SELECT month_start, ROW_NUMBER() OVER (ORDER BY month_start DESC) AS month_rank
      FROM months
    ),
    receipts_in_months AS (
      SELECT r.receipt_id, rm.month_rank
      FROM receipts r
      JOIN ranked_months rm ON DATE(strftime('%Y-%m-01', r.date_scanned)) = rm.month_start
    ),
    brand_receipt_counts AS (
      SELECT
        ri.barcode,
        b.name AS brand_name,
        rim.month_rank,
        COUNT(DISTINCT ri.receipt_id) AS receipt_count
      FROM receipt_items ri
      JOIN receipts_in_months rim ON ri.receipt_id = rim.receipt_id
      JOIN brands b ON ri.barcode = b.barcode
      WHERE ri.barcode IS NOT NULL
      GROUP BY ri.barcode, b.name, rim.month_rank
    ),
    ranked_brands AS (
      SELECT
        barcode,
        brand_name,
        month_rank,
        receipt_count,
        RANK() OVER (
          PARTITION BY month_rank
          ORDER BY receipt_count DESC
        ) AS brand_rank
      FROM brand_receipt_counts
    )
    SELECT
      rb_current.brand_name,
      rb_current.receipt_count AS current_month_count,
      rb_current.brand_rank AS current_month_rank,
      rb_previous.receipt_count AS previous_month_count,
      rb_previous.brand_rank AS previous_month_rank
    FROM ranked_brands rb_current
    LEFT JOIN ranked_brands rb_previous
      ON rb_current.barcode = rb_previous.barcode AND rb_previous.month_rank = 2
    WHERE rb_current.month_rank = 1 AND rb_current.brand_rank <= 5
    ORDER BY rb_current.brand_rank;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    print("Comparison of Top 5 Brands Between Recent and Previous Month:")
    print("Brand Name | Current Month Count | Current Rank | Previous Month Count | Previous Rank")
    print("-" * 90)
    for row in results:
        print(f"{row[0]} | {row[1]} | {row[2]} | {row[3] or 'N/A'} | {row[4] or 'N/A'}")

    conn.close()

def average_spend_by_receipt_status(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    query = """
    SELECT rewards_receipt_status, AVG(total_spent) AS average_spend
    FROM receipts
    WHERE rewards_receipt_status IN ('Accepted', 'Rejected')
    GROUP BY rewards_receipt_status;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    print("Average Spend for 'Accepted' vs 'Rejected' Receipts:")
    print("Receipt Status | Average Spend")
    print("-" * 40)
    for row in results:
        print(f"{row[0]} | {row[1]:.2f}")

    conn.close()

def total_items_purchased_by_receipt_status(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    query = """
    SELECT r.rewards_receipt_status, SUM(ri.quantity_purchased) AS total_items_purchased
    FROM receipt_items ri
    JOIN receipts r ON ri.receipt_id = r.receipt_id
    WHERE r.rewards_receipt_status IN ('Accepted', 'Rejected')
    GROUP BY r.rewards_receipt_status;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    print("Total Number of Items Purchased by Receipt Status:")
    print("Receipt Status | Total Items Purchased")
    print("-" * 50)
    for row in results:
        print(f"{row[0]} | {row[1]}")

    conn.close()

def top_brand_by_spend_recent_users(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    query = """
    WITH recent_users AS (
      SELECT user_id
      FROM users
      WHERE created_date >= DATE('now', '-6 months')
    ),
    recent_user_receipt_items AS (
      SELECT ri.barcode, ri.final_price
      FROM receipt_items ri
      JOIN recent_users ru ON ri.user_id = ru.user_id
      WHERE ri.barcode IS NOT NULL
    )
    SELECT
      b.name AS brand_name,
      ruri.barcode,
      SUM(ruri.final_price) AS total_spend
    FROM recent_user_receipt_items ruri
    JOIN brands b ON ruri.barcode = b.barcode
    GROUP BY ruri.barcode, b.name
    ORDER BY total_spend DESC
    LIMIT 1;
    """

    cursor.execute(query)
    result = cursor.fetchone()

    print("Brand with the Most Spend Among Users Created Within the Past 6 Months:")
    print("Brand Name | barcode | Total Spend")
    print("-" * 50)
    if result:
        print(f"{result[0]} | {result[1]} | {result[2]:.2f}")
    else:
        print("No data found.")

    conn.close()

def top_brand_by_transactions_recent_users(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    query = """
    WITH recent_users AS (
      SELECT user_id
      FROM users
      WHERE created_date >= DATE('now', '-6 months')
    ),
    recent_user_receipt_items AS (
      SELECT ri.barcode, ri.receipt_id
      FROM receipt_items ri
      JOIN recent_users ru ON ri.user_id = ru.user_id
      WHERE ri.barcode IS NOT NULL
    )
    SELECT
      b.name AS brand_name,
      ruri.barcode,
      COUNT(DISTINCT ruri.receipt_id) AS transaction_count
    FROM recent_user_receipt_items ruri
    JOIN brands b ON ruri.barcode = b.barcode
    GROUP BY ruri.barcode, b.name
    ORDER BY transaction_count DESC
    LIMIT 1;
    """

    cursor.execute(query)
    result = cursor.fetchone()

    print("Brand with the Most Transactions Among Users Created Within the Past 6 Months:")
    print("Brand Name | Brand ID | Transaction Count")
    print("-" * 60)
    if result:
        print(f"{result[0]} | {result[1]} | {result[2]}")
    else:
        print("No data found.")

    conn.close()

if __name__ == "__main__":

    database_name = 'fetch_data.db'  

    top_5_brands_recent_month(database_name)
    print("\n")

    compare_top_5_brands_recent_previous_month(database_name)
    print("\n")

    average_spend_by_receipt_status(database_name)
    print("\n")

    total_items_purchased_by_receipt_status(database_name)
    print("\n")

    top_brand_by_spend_recent_users(database_name)
    print("\n")

    top_brand_by_transactions_recent_users(database_name)