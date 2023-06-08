import requests
import sqlite3

# Fetch data from the API endpoint
response = requests.get("https://raw.githubusercontent.com/younginnovations/internship-challenges/master/programming/petroleum-report/data.json")
data = response.json()

# Store the data into a SQLite database
conn = sqlite3.connect("data/petroleum.db")
cursor = conn.cursor()

# Create tables
cursor.execute("""
    CREATE TABLE IF NOT EXISTS countries (
        id INTEGER PRIMARY KEY,
        country TEXT
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY,
        product TEXT
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS years (
        id INTEGER PRIMARY KEY,
        year INTEGER
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS sales (
        id INTEGER PRIMARY KEY,
        country_id INTEGER,
        product_id INTEGER,
        year_id INTEGER,
        sale INTEGER,
        FOREIGN KEY (country_id) REFERENCES countries (id),
        FOREIGN KEY (product_id) REFERENCES products (id),
        FOREIGN KEY (year_id) REFERENCES years (id)
    )
""")

# Populate tables
countries = {}
products = {}
years = {}

for item in data:
    country_name = item["country"]
    product_name = item["petroleum_product"]
    year = int(item["year"])
    sale_value = int(item["sale"])

    # Add country to the countries table if it doesn't exist
    if country_name not in countries:
        country_id = len(countries) + 1
        countries[country_name] = country_id
        cursor.execute("INSERT INTO countries (id, country) VALUES (?, ?)", (country_id, country_name))
    else:
        country_id = countries[country_name]

    # Add product to the products table if it doesn't exist
    if product_name not in products:
        product_id = len(products) + 1
        products[product_name] = product_id
        cursor.execute("INSERT INTO products (id, product) VALUES (?, ?)", (product_id, product_name))
    else:
        product_id = products[product_name]

    # Add year to the years table if it doesn't exist
    if year not in years:
        year_id = len(years) + 1
        years[year] = year_id
        cursor.execute("INSERT INTO years (id, year) VALUES (?, ?)", (year_id, year))
    else:
        year_id = years[year]

    # Add sale data to the sales table
    cursor.execute("INSERT INTO sales (country_id, product_id, year_id, sale) VALUES (?, ?, ?, ?)",
                   (country_id, product_id, year_id, sale_value))

conn.commit()
conn.close()
