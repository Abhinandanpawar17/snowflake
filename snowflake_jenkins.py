# Run snowflake task and process with python jenkins 

import snowflake.connector

# Snowflake connection parameters
conn_params = {
                'user': 'abhinandan17',
                'password': 'Hexa@1234',
                'account': 'fv29345.ap-south-1',
                'database': 'falcon_200',
                'schema': 'sj'
                }

# Establish connection
conn = snowflake.connector.connect(**conn_params)
cursor = conn.cursor()

# Create source tables
create_source_table_query = '''
                            CREATE OR REPLACE TABLE source_table (
                            id INT,
                            name STRING,
                            value DECIMAL(10, 2)
                            )
                            '''
cursor.execute(create_source_table_query)

create_target_table_query = '''
                            CREATE OR REPLACE TABLE target_table (
                            id INT,
                            name STRING,
                            transformed_value DECIMAL(10, 2)
                            )
                            '''
cursor.execute(create_target_table_query)

# Insert data into source_table
data_to_insert = [
        (1, 'item1', 100.00),
        (2, 'item2', 150.50),
        (3, 'item3', 200.75)
        ]
insert_query = 'INSERT INTO source_table (id, name, value) VALUES (%s, %s, %s)'
cursor.executemany(insert_query, data_to_insert)
conn.commit()

# Process data and insert into target_table
select_query = 'SELECT id, name, value FROM source_table'
cursor.execute(select_query)
source_data = cursor.fetchall()

processed_data = []
for row in source_data:
    id, name, value = row
    # Perform data processing based on condition (example: double the value)
    transformed_value = value * 3
    processed_data.append((id, name, transformed_value))

# Insert processed data into target_table
insert_query = 'INSERT INTO target_table (id, name, transformed_value) VALUES (%s, %s, %s)'
cursor.executemany(insert_query, processed_data)
conn.commit()

# Close connections
cursor.close()
conn.close()