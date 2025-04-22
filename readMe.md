# SC4023 - BIG DATA Column Store Project

This project implements a column store for HDB resale price data in Singapore. It provides preprocessing and querying mechanisms to analyze large datasets. 

## Directory Structure

BigDataProject/
├── HDBResaleColumnStore.java         # Main Java source code
├── ResalePricesSingapore.csv         # Input CSV file (raw HDB data)
├── column_store/                     # Columnar storage files (one CSV per attribute)
│   ├── month.csv
│   ├── town.csv
│   ├── floor_area_sqm.csv
│   ├── resale_price.csv
│   └── ... (other attribute columns)
├── output/
│   ├── NormalQuery.csv                 # Output file for Normal Query
│   ├── SSQuery.csv                     # Output file for Shared Scan Query
│   ├── ZMQuery.csv                     # Output file for Zone Mapping Query
│   ├── ZMSSQuery.csv                   # Output file for Zone Mapping + Shared Scan Query
│   ├── SortedResalePrices.csv          # Output file for the sorted resale prices used for the queries 


## How to Run

To execute the project, follow these steps:

1. **Prepare the Input Data**:  
    Place the input CSV file (`ResalePricesSingapore.csv`) in the project directory.

2. **Compile the Code**:  
    Use a Java compiler to compile the `HDBResaleColumnStore.java` file.  
    ```bash
    javac HDBResaleColumnStore.java
    ```

3. **Run the Program**:  
    Execute the compiled Java program.  
    ```bash
    java HDBResaleColumnStore
    ```

4. **Provide User Input**:  
    Enter the required matriculation number when prompted. This will be used to extract the year, month, and town for querying.

5. **View Results**:  
    The query results will be saved as CSV files in the `output/` directory. Execution times for each query will also be displayed in the console.



## HDBResaleColumnStore.java is divided into four main sections: **Preprocessing**, **Queries**, **Query Helper Functions**, and **Main**.

### 1. Preprocessing

The preprocessing step prepares the data for efficient querying by sorting and splitting the dataset into columnar files.

### Functions:
- **`sortCSVByMonth(String inputCsvPath, String outputCsvPath)`**  
  Sorts the input CSV file by the `month` column and writes the sorted data to a new file.  
  - **Input**: Path to the input CSV file.  
  - **Output**: Path to the sorted CSV file.

- **`splitCSV(String csvPath)`**  
  Splits the sorted CSV file into separate files for each column and stores them in the `column_store` directory.  
  - **Input**: Path to the sorted CSV file.  
  - **Output**: Columnar files in the `column_store` directory.

- **`generateZones()`**  
  Generates zones (start and end indices) for each year based on the `month.csv` file.  
  - **Output**: A mapping of years to their respective start and end indices.
  
- **`MultiFileCSVAccess()`**  
  Builds an index lookup table for all required columns (e.g., `month.csv`, `town.csv`, `floor_area_sqm.csv`, `resale_price.csv`) to enable efficient random access during queries.  
  - **Purpose**: Improves query performance by allowing direct access to specific rows in columnar files without sequential scanning.
  - **Output**: A mapping of file paths to their respective byte offsets for each row.




### 2. Queries

The project supports four types of queries to analyze the data:

### Query Types:
1. **Normal Query (`normalQuery`)**  
   Performs a multi-stage filter on the data based on year, month, town, and area.

2. **Zone Mapping Query (`zmQuery`)**  
   Uses precomputed zones to limit the range of data scanned, improving query performance.

3. **Shared Scan Query (`ssQuery`)**  
   Combines all filtering stages into a single scan for improved efficiency.

4. **Zone Mapping + Shared Scan Query (`zmssQuery`)**  
   Combines zone mapping and shared scan techniques for optimal performance.

Each query computes statistics such as minimum price, average price, standard deviation, and minimum price per square meter, and writes the results to a CSV file.



### 3. Query Helper Functions

These functions are used internally to support the queries:

- **`normalScan(int year, int startMonth, String town, int zone_startIdx, int zone_endIdx)`**  
  Performs a multi-stage filter on the data based on time, town, and area.

- **`sharedScan(int year, int startMonth, String town, int zone_startIdx, int zone_endIdx)`**  
  Combines all filtering stages into a single scan for efficiency.

- **`computeStatistics(List<String[]> filteredData)`**  
  Computes statistics (e.g., minimum price, average price, standard deviation) on the filtered data.

- **`writeStatisticsToCSV(Map<String, Double> stats, int year, int startMonth, String town, String outputFile)`**  
  Writes the computed statistics to a CSV file.



### 4. Main

The `main` method orchestrates the entire process, from preprocessing to running queries.

### Workflow:
1. **Preprocessing**:  
   - Sorts the input CSV file by month.  
   - Splits the sorted CSV into columnar files.

2. **User Input**:  
   Prompts the user to enter a matriculation number, which is used to extract the year, month, and town for querying.

3. **Query Execution**:  
   Executes the following queries in sequence:
   - Normal Query
   - Shared Scan Query
   - Zone Mapping Query
   - Zone Mapping + Shared Scan Query



