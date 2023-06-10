# Code Instructions

This repository contains code for importing petroleum sales data from an API and executing queries on the imported data. The code is written in Python and uses SQLite as the database.

## Prerequisites

Before running the code, make sure you have the following prerequisites installed:

- Python 3.x
- requests library (`pip install requests`)
- SQLite library (comes with Python by default)
- SQLite database browser (optional)

## Instructions

Follow the steps below to run the code:

1. Clone the repository or download the code files.

2. Open a terminal or command prompt and navigate to the directory containing the code files.

3. Run the following command to install the required `requests` library if it's not already installed:

   ```
   pip install requests
   ```

4. Run the following command to execute the code and import the data:

   ```
   python main.py
   ```

   This command will import the petroleum sales data from the API and store it in a SQLite database (`data/petroleum.db`). The data import process may take some time depending on the size of the data.

5. After the data import is complete, you can run queries on the imported data. Use the following command to execute a query:

   ```
   python main.py <query_name>
   ```

   Replace `<query_name>` with one of the available query names:
   
   - `totalsales`: Retrieves the total sale of each petroleum product.
   - `top3countries`: Retrieves the top 3 countries with the highest and lowest total sales.
   - `averagesales`: Retrieves the average sale of each petroleum product for 4-year intervals.

   For example, to execute the `totalsales` query, use the following command:

   ```
   python main.py totalsales
   ```

   The query results will be printed in the terminal.

6. You can also view the data in the SQLite database using a SQLite database browser. Open the `data/petroleum.db` file with a SQLite browser of your choice to explore the imported data.

Note: Make sure you have an active internet connection to fetch the data from the API during the import process.

If you encounter any errors or need further assistance, please feel free to reach out.
