# fetch_takehome
Fetch Data-Take Home  

Data workflow is:

Extract_data.py >> Load_data.py >> Transform.py

### Extract_data.py: 
Parse JSON Object and then write the JSON object as a single line

### Load_data.py:

Create 4 tables depend on the data model we created and ingest data into 4 tables (3 dim: users, brands, and receipts, 1 fct: receipt_item) in data warehouse

### Transform.py

Answer 6 bullet points made by business stakeholders. 

### data_quality_issues.ipynb

Identify data quality issues for all 4 tables based on data consistency, data integrity, data accuracy.

## takehome.pdf

ERD Diagram to model the data in a data warehouse

## Slack/Email message

Hi, 

I have completed an initial review of the data and wanted to share some key observations about the current data quality and structure of datasets 

Some issues impact accuracy and reliability. There are critical fields such as item barcodes, purchase dates, brand_code, total_spent

Data Consistency Issues: Certain fields, like purchase and finish dates, show inconsistencies in the order they’re logged, which might lead to incorrect calculations in timeline-based reporting. 

Moreover, role column is not set to constant "CONSUMER" as expected. There are some duplicates in barcode of brands table, which does not make sense to me. 

Item price and final price match for each item are not matched in some cases where receipt_id is 60049d9d0a720f05f3000094. Receipt_id = 5f9c74f70a7214ad07000037 has null value for barcode column.  

There are many records duplicated in Users table. I think we should remove that from our database or add a PK and a constraint to that table. 

We need to know which fields are essential for analytics and reporting would allow us to prioritize cleaning and validation efforts effectively. we love to understand how this data is collected and entered could clarify certain placeholders and missing entries, and help us determine if there are steps in the pipeline where data loss might occur.


In terms of scalability, missing and inconsistent values could create problems in processing and impact the accuracy of any predictive models or reports we generate. As a solution, we could:

- Implement data validation checks to catch and flag discrepancies early in the data pipeline.

- Consider caching frequently accessed data to improve retrieval times for larger datasets.

Let’s connect soon to discuss these findings and any further questions you may have. 









