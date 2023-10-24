# Db_reader
Base class for reading data from SQL database using python mysql.connector.

---------------------------------------------------------------------------------------------------------------------------
Example usage for finding a suitable chunk size when importing the data (assuming 1 second response time is acceptable): 

credentials = ['host', 'usr_name', 'password'] \n

reader = DbReader(credentials, 'database_name') \n

df = reader.fetch_data(table_name='my_table', column_names=cols, evaluate_chunk_size=True)

The function reader.fetch_data() sends back the dataframe and prints the chunk size with the best performance
during the evaluation.

---------------------------------------------------------------------------------------------------------------------------

Example usage with specified chunk size to use and getting all columns in the table 
(if no chunk size is sent it will use the chunk size 100).

credentials = ['host', 'usr_name', 'password']

reader = DbReader(credentials, 'database_name')

df = reader.fetch_data(table_name='my_table', chunk_size=100)

---------------------------------------------------------------------------------------------------------------------------

