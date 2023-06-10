import sys
import requests
import sqlite3
import logging

# Configuring logging
logging.basicConfig(filename='error.log', level=logging.ERROR)


class DataImporter:
    def __init__(self, api_url, db_path):
        self.api_url = api_url
        self.db_path = db_path

    def import_data(self):
        try:
            # Fetching data from the API endpoint
            response = requests.get(self.api_url)
            # Raising an exception for unsuccessful HTTP response
            response.raise_for_status()  
            data = response.json()

            # Storing the data into a SQLite database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Creating tables and populating data
            self.create_tables(cursor)
            self.populate_data(cursor, data)

            # Commiting and closing the database connection
            conn.commit()
            conn.close()
        except (requests.RequestException, sqlite3.Error) as e:
            logging.error(f"Error importing data: {str(e)}")

    def create_tables(self, cursor):
        try:
            # Creating tables if they don't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS countries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    country TEXT UNIQUE
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product TEXT UNIQUE
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS years (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    year INTEGER UNIQUE
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sales (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    country_id INTEGER,
                    product_id INTEGER,
                    year_id INTEGER,
                    sale INTEGER,
                    FOREIGN KEY (country_id) REFERENCES countries (id),
                    FOREIGN KEY (product_id) REFERENCES products (id),
                    FOREIGN KEY (year_id) REFERENCES years (id)
                )
            """)
        except sqlite3.Error as e:
            print(f"Error creating tables: {str(e)}")

    def populate_data(self, cursor, data):
        # Populating tables
        countries = {}
        products = {}
        years = {}

        for item in data:
            country_name = item["country"]
            product_name = item["petroleum_product"]
            year = int(item["year"])
            sale_value = int(item["sale"])

            # Adding country to the countries table if it doesn't exist
            if country_name not in countries:
                try:
                    cursor.execute("INSERT OR IGNORE INTO countries (country) VALUES (?)", (country_name,))
                    country_id = cursor.lastrowid
                    countries[country_name] = country_id
                except sqlite3.Error as e:
                    print(f"Error inserting country '{country_name}': {str(e)}")
            else:
                country_id = countries[country_name]

            # Adding product to the products table if it doesn't exist
            if product_name not in products:
                try:
                    cursor.execute("INSERT OR IGNORE INTO products (product) VALUES (?)", (product_name,))
                    product_id = cursor.lastrowid
                    products[product_name] = product_id
                except sqlite3.Error as e:
                    print(f"Error inserting product '{product_name}': {str(e)}")
            else:
                product_id = products[product_name]

            # Adding year to the years table if it doesn't exist
            if year not in years:
                try:
                    cursor.execute("INSERT OR IGNORE INTO years (year) VALUES (?)", (year,))
                    year_id = cursor.lastrowid
                    years[year] = year_id
                except sqlite3.Error as e:
                    print(f"Error inserting year '{year}': {str(e)}")
            else:
                year_id = years[year]

            # Adding sale data to the sales table
            try:
                cursor.execute("INSERT INTO sales (country_id, product_id, year_id, sale) VALUES (?, ?, ?, ?)",
                               (country_id, product_id, year_id, sale_value))
            except sqlite3.Error as e:
                print(f"Error inserting sale data: {str(e)}")


class QueryExecutor:
    def __init__(self, db_path):
        self.db_path = db_path

    def execute_query(self, query_name):
        try:
            # Connecting to the SQLite database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Checking the query name and executing the corresponding query
            if query_name == "totalsales":
                return self.execute_total_sales_query(cursor)
            elif query_name == "top3countries":
                return self.execute_top_countries_query(cursor)
            elif query_name == "averagesales":
                return self.execute_average_sales_query(cursor)
            else:
                print("Invalid query name")
                return None
        except sqlite3.Error as e:
            print(f"Database error: {str(e)}")
        finally:
            conn.close()

    def execute_total_sales_query(self, cursor):
        try:
            # Executing the query to get the total sale of each petroleum product
            cursor.execute("""
                SELECT products.product, SUM(sales.sale) AS total_sale
                FROM sales
                JOIN products ON sales.product_id = products.id
                GROUP BY sales.product_id
            """)

            # Fetching all the rows returned by the query
            results = cursor.fetchall()

            max_product_length = max(len(row[0]) for row in results)

            # Printing the total sale of each petroleum product
            print("Product".ljust(max_product_length), "Total Sale")
            print("-" * (max_product_length + 15))

            for row in results:
                product = row[0].ljust(max_product_length)
                total_sale = str(row[1])
                print(f"{product}   {total_sale:>5}")
        except sqlite3.Error as e:
            print(f"Error executing total sales query: {str(e)}")

    def execute_top_countries_query(self, cursor):
        try:
            # Retrieving the top 3 countries with the highest total sales
            cursor.execute("""
                SELECT countries.country, SUM(sales.sale) AS total_sale
                FROM sales
                JOIN countries ON sales.country_id = countries.id
                GROUP BY sales.country_id
                ORDER BY total_sale DESC
                LIMIT 3
            """)

            top_countries = cursor.fetchall()

            # Determining the maximum length of the country and total sale columns for top countries
            max_country_length = max(len(row[0]) for row in top_countries)
            max_total_sale_length = max(len(str(row[1])) for row in top_countries)

            # Printing the top countries with the highest total sales
            print("Top 3 countries with the highest total sales:")
            print("Country".ljust(max_country_length), "Total Sale".ljust(max_total_sale_length))
            # Adding extra padding
            print("-" * (max_country_length + max_total_sale_length + 5))  

            for country in top_countries:
                country_name = country[0].ljust(max_country_length)
                total_sale = str(country[1]).ljust(max_total_sale_length)
                print(f"{country_name}   {total_sale}")

            print()

            # Retrieving the top 3 countries with the lowest total sales
            cursor.execute("""
                SELECT countries.country, SUM(sales.sale) AS total_sale
                FROM sales
                JOIN countries ON sales.country_id = countries.id
                GROUP BY sales.country_id
                ORDER BY total_sale ASC
                LIMIT 3
            """)

            bottom_countries = cursor.fetchall()

            # Determining the maximum length of the country and total sale columns for bottom countries
            max_country_length = max(len(row[0]) for row in bottom_countries)
            max_total_sale_length = max(len(str(row[1])) for row in bottom_countries)

            # Printing the top countries with the lowest total sales
            print("Top 3 countries with the lowest total sales:")
            print("Country".ljust(max_country_length), "Total Sale".ljust(max_total_sale_length))
            print("-" * (max_country_length + max_total_sale_length + 5))  # Add extra padding

            for country in bottom_countries:
                country_name = country[0].ljust(max_country_length)
                total_sale = str(country[1]).ljust(max_total_sale_length)
                print(f"{country_name}   {total_sale}")
        except sqlite3.Error as e:
            print(f"Error executing top countries query: {str(e)}")

    def execute_average_sales_query(self, cursor):
        try:
            # Executing the query to get the average sale of each petroleum product for 4-year intervals
            cursor.execute("""
                SELECT products.product, 
                    CASE 
                        WHEN years.year <= 2010 THEN '2007-2010'
                        ELSE '2011-2014'
                    END AS year_interval,
                    AVG(sales.sale) AS average_sale
                FROM sales
                JOIN products ON sales.product_id = products.id
                JOIN years ON sales.year_id = years.id
                WHERE sales.sale > 0
                GROUP BY products.product, year_interval
                ORDER BY products.product, year_interval
            """)

            # Fetching all the rows returned by the query
            results = cursor.fetchall()

            # Determining the maximum length of the product, year, and average sale columns
            max_product_length = max(len(row[0]) for row in results)
            max_year_length = max(len(str(row[1])) for row in results)
            max_average_sale_length = max(len(str(row[2])) for row in results)

            # Printing the average sale of each petroleum product for 4-year intervals
            print("Product\t\t\tYear\t\tAvg")
            print("---------------------------------------------")
            for sale in results:
                product = sale[0].ljust(max_product_length)
                year = str(sale[1]).ljust(max_year_length)
                average_sale = str(sale[2]).ljust(max_average_sale_length)
                print(f"{product}   {year}   {average_sale}")
        except sqlite3.Error as e:
            print(f"Error executing average sales query: {str(e)}")


if __name__ == "__main__":
    # Initializing and running the data import process
    data_importer = DataImporter(
        "https://raw.githubusercontent.com/younginnovations/internship-challenges/master/programming/petroleum-report/data.json",
        "data/petroleum.db"
    )
    data_importer.import_data()
     
    # Checking if a query argument is provided
    if len(sys.argv) < 2:
        print("Please provide a query name as a command-line argument.")
        sys.exit(1)

    query_name = sys.argv[1]

    # Initializing and running the query execution process
    query_executor = QueryExecutor("data/petroleum.db")
    query_executor.execute_query(query_name)
