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

# Populate tables
countries = {}
products = {}
years = {}

# Fetch existing countries from the database
cursor.execute("SELECT id, country FROM countries")
existing_countries = cursor.fetchall()

# Populate countries dictionary with existing countries
for country_id, country_name in existing_countries:
    countries[country_name] = country_id

# Fetch existing products from the database
cursor.execute("SELECT id, product FROM products")
existing_products = cursor.fetchall()

# Populate products dictionary with existing products
for product_id, product_name in existing_products:
    products[product_name] = product_id

# Fetch existing years from the database
cursor.execute("SELECT id, year FROM years")
existing_years = cursor.fetchall()

# Populate years dictionary with existing years
for year_id, year in existing_years:
    years[year] = year_id

for item in data:
    country_name = item["country"]
    product_name = item["petroleum_product"]
    year = int(item["year"])
    sale_value = int(item["sale"])

    # Add country to the countries table if it doesn't exist
    if country_name not in countries:
        cursor.execute("INSERT INTO countries (country) VALUES (?)", (country_name,))
        country_id = cursor.lastrowid
        countries[country_name] = country_id
    else:
        country_id = countries[country_name]

    # Add product to the products table if it doesn't exist
    if product_name not in products:
        cursor.execute("INSERT INTO products (product) VALUES (?)", (product_name,))
        product_id = cursor.lastrowid
        products[product_name] = product_id
    else:
        product_id = products[product_name]

    # Add year to the years table if it doesn't exist
    if year not in years:
        cursor.execute("INSERT INTO years (year) VALUES (?)", (year,))
        year_id = cursor.lastrowid
        years[year] = year_id
    else:
        year_id = years[year]

    # Add sale data to the sales table
    cursor.execute("INSERT INTO sales (country_id, product_id, year_id, sale) VALUES (?, ?, ?, ?)",
                   (country_id, product_id, year_id, sale_value))

conn.commit()
conn.close()
