import requests
import pandas as pd
import sqlalchemy as sa
from datetime import datetime

def fetch_bank_holidays():
    url = "https://www.gov.uk/bank-holidays.json"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: HTTP {response.status_code}")

def process_data(data):
    holidays_df = pd.DataFrame()
    all_holidays_dfs = []
    current_datetime = datetime.now()

    for region, holidays_info in data.items():
        region_holidays_df = pd.DataFrame(holidays_info['events'])
        region_holidays_df['region'] = region

        # Convert the 'date' column to datetime
        region_holidays_df['date'] = pd.to_datetime(region_holidays_df['date'])

        # Extract the day of the week
        region_holidays_df['Day of the Week'] = region_holidays_df['date'].dt.day_name()

        # Extract the month as a name
        region_holidays_df['Month'] = region_holidays_df['date'].dt.month_name()

        # Extract the year
        region_holidays_df['year'] = region_holidays_df['date'].dt.year

        # LastUpdated datetime field
        region_holidays_df['LastUpdated'] = current_datetime

        all_holidays_dfs.append(region_holidays_df)

    holidays_df = pd.concat(all_holidays_dfs, ignore_index=True)
    holidays_df = holidays_df.rename(columns={
        'title': 'Bank Holiday',
        'date': 'Date',
        'notes': 'Notes',
        'bunting': 'Bunting',
        'region': 'Region',
        'year' :  'Year'
    })
    
    holidays_df['Date_only'] = holidays_df['Date'].astype(str).str.split(' ').str[0]
    holidays_df.drop('Date', axis=1, inplace=True)
    holidays_df.rename(columns={'Date_only': 'Date'}, inplace=True)
    desired_column_order = ['Date','Region','Bank Holiday', 'Notes', 'Bunting', 'Day of the Week','Month','Year','LastUpdated']
    holidays_df = holidays_df[desired_column_order]

    return holidays_df

#load_to_sql function -- Loading data to the warehouse
def load_to_sql(df, table_name):
    dtype = {
        'Date' : sa.types.Date
        ,'Bank Holiday' : sa.types.String(length=60)
        ,'Notes' : sa.types.String(length=18)
        ,'Region' : sa.types.String(length =100)
        ,'Bunting' : sa.types.String(length=2)
        ,'Day of the Week ' : sa.types.String(length=10)
        ,'Month' : sa.types.String(length=10) #16
        ,'Year' : sa.types.String(length=4) #11
        ,'LastUpdated' : sa.types.DateTime #16
    }

    connection_string = ("Driver={ODBC Driver 17 for SQL Server};"
                    "Server=UK-SQL3;"
                    "Database=Sandbox;"
                    "UID=CEF-UK\MU006;"
                    "PWD=;"
                    "port=1433;"
                    "Trusted_Connection=yes;"
        )

    connection_url = sa.engine.URL.create(
    "mssql+pyodbc",
    query=dict(odbc_connect=connection_string)
        )

    engine = sa.create_engine(connection_url, fast_executemany=True)
    df.to_sql(table_name, engine, dtype=dtype, schema="Test", if_exists="replace", index=False)

#Executing the main function
def main():
    try:
        data = fetch_bank_holidays()
        holidays_df = process_data(data)
        load_to_sql(holidays_df, "Bank_Holidays_Gov")
        print("Data loaded successfully into SQL Server.")

    except Exception as e:
            print(str(e))

if __name__ == "__main__":
    main()

