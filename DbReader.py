import mysql.connector
import pandas as pd
from tqdm import tqdm


class DbReader:
    def __init__(self, hup_list, database_name, debug=False):
        """

        :param hup_list: A list containing the host, username and password.
         Ex. ['192.168.1.100', 'my_username', 'my_password']
        :param database_name: Name of the database
        :param debug: Boolean variable to limit the load on the server and print relevant sample info
        """

        self.host = hup_list[0]
        self.user = hup_list[1]
        self.pwd = hup_list[2]
        self.db_name = database_name
        self.table = None
        self.columns = None
        self.debug = debug
        if self.debug:
            pd.set_option('display.max_rows', 50)
            pd.set_option('display.max_columns', 50)
            pd.set_option('display.width', 1000)

    def fetch_data(self, table_name, column_names=None, chunk_size=100, evaluate_chunk_size=False):
        """
        :param table_name: Name of the table to extract data from
        :param column_names: A list of strings containing the column names where the data is stored, default is all columns
        :param chunk_size: The chunk size of data we want to fetch. Default = 100
        :param evaluate_chunk_size: Boolean variable to try to optimize the chunk size to maximize
                iterations/second.
        :return: Returns a pandas dataframe containing the requested data.
        """
        if column_names is None:
            column_names = 'all'
        self.columns = column_names
        self.table = table_name
        connection = mysql.connector.connect(host=self.host, user=self.user, password=self.pwd, database=self.db_name)
        if not self.debug:
            if self.columns == 'all':
                query = f"SELECT * FROM {self.table};"
            else:
                columns_as_string = ', '.join(self.columns)
                query = f"SELECT {columns_as_string} FROM {self.table};"
        else:
            if self.columns == 'all':
                query = f"SELECT * FROM {self.table} LIMIT 2000;"
            else:
                columns_as_string = ', '.join(self.columns)
                query = f"SELECT {columns_as_string} FROM {self.table} LIMIT 2000"

        # Fetch all rows using a cursor
        cursor = connection.cursor()
        count_cursor = connection.cursor()
        count_cursor.execute(f"SELECT COUNT(*) FROM {self.table};")
        count_result = count_cursor.fetchone()
        cursor.execute(query)
        total_rows = count_result[0]
        df = None
        if evaluate_chunk_size:
            import time
            max_response_time = 1.0
            response_times = []
            chunk_s =[]
            with tqdm(total=total_rows, desc=f"Querying {total_rows} samples data from {self.table}".center(70),
                      leave=False) as pbar2:
                chunks = []
                while True:
                    start_time = time.time()
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                    chunks.append(rows)
                    end_time = time.time()
                    response_time = end_time - start_time
                    pbar2.update(len(rows))
                    chunk_s.append(chunk_size)
                    response_times.append(response_time)
                    if response_time > max_response_time:
                        # If response time is too high, decrease the chunk size
                        chunk_size = max(chunk_size // 2, 1)
                    else:
                        # If response time is acceptable, increase the chunk size
                        chunk_size = min(chunk_size * 2, total_rows)
                    df = pd.DataFrame([row for chunk in chunks for row in chunk], columns=cursor.column_names)
            pbar2.close()
            best = 0
            for chunk in range(len(chunk_s)):
                it = chunk_s[chunk] / (response_times[chunk] + 0.001)
                if it > best:
                    best = chunk_s[chunk]
                else:
                    pass
            print(f'OPTIMAL CHUNK SIZE: {best}')

        else:
            with tqdm(total=total_rows, desc=f"Querying {total_rows} samples data from {self.table}".center(70),
                      leave=False) as pbar2:
                chunks = []
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                    chunks.append(rows)
                    pbar2.update(len(rows))
                df = pd.DataFrame([row for chunk in chunks for row in chunk], columns=cursor.column_names)
                pbar2.close()
        return df





