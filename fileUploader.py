import sys
import pandas as pd
from utils.database_connection import DatabaseConnection


def insert_data(data):
    with DatabaseConnection() as connection:
        data.to_sql(name='AdjustmentDetails',
                    con=connection,
                    if_exists='replace')  # Value can also be append, but have used 'replace' since it is the same file
        rows = len(data.index)
        print("Insert successful for {} rows".format(rows))


def verify_table():
    with DatabaseConnection() as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM 'AdjustmentDetails'")
        rows = cursor.fetchall()
        for row in rows:
            print(row)


def main():
    try:
        with open("uni_data.txt", 'r') as f:
            # Skipping 11 rows so that headers in row 12 can be used
            df = pd.read_table(f, skiprows=11, skipinitialspace=True, sep="|", thousands=',')

            # Dropping first and last columns as "|" in beginning and end creates 2 additional empty columns
            df.drop(df.columns[[0, -1]], axis=1, inplace=True)

            # Strip spaces from all columns
            df.columns = df.columns.str.strip()

            # Replacing the space within the names by "_" for DB upload
            df.columns = df.columns.str.replace(' ', '_')

            # Dropping any NA values - should not be inserted into the table
            df.dropna(how='any', inplace=True)
            print(df.dtypes)

            # Convert Material to 'int'
            df["Material"] = df["Material"].astype('int64')

            # Convert TotWstVal to float
            df["TotWstVal"] = df["TotWstVal"].str.replace(',', '')
            mask = df["TotWstVal"].str.endswith('-')
            df.loc[mask, "TotWstVal"] = '-' + df.loc[mask, "TotWstVal"].str[:-1]
            df["TotWstVal"] = pd.to_numeric(df["TotWstVal"], errors='coerce')

            # Convert TotActVal to float
            df["TotActVal"] = df["TotActVal"].str.replace(',', '')
            mask = df["TotActVal"].str.endswith('-')
            df.loc[mask, "TotActVal"] = '-' + df.loc[mask, "TotActVal"].str[:-1]
            df["TotActVal"] = pd.to_numeric(df["TotActVal"], errors='coerce')

            # Convert Cons._Value to float
            df["Cons._Value"] = df["Cons._Value"].str.replace(',', '')
            mask = df["Cons._Value"].str.endswith('-')
            df.loc[mask, "Cons._Value"] = '-' + df.loc[mask, "Cons._Value"].str[:-1]
            df["Cons._Value"] = pd.to_numeric(df["Cons._Value"], errors='coerce')

            # Convert StAdjValue to float
            df["StAdjValue"] = df["StAdjValue"].str.replace(',', '')
            mask = df["StAdjValue"].str.endswith('-')
            df.loc[mask, "StAdjValue"] = '-' + df.loc[mask, "StAdjValue"].str[:-1]
            df["StAdjValue"] = pd.to_numeric(df["StAdjValue"], errors='coerce')

            # Convert TotWaste_% to float
            df["TotWaste_%"] = df["TotWaste_%"].str.replace(',', '')
            mask = df["TotWaste_%"].str.endswith('-')
            df.loc[mask, "TotWaste_%"] = '-' + df.loc[mask, "TotWaste_%"].str[:-1]
            df["TotWaste_%"] = pd.to_numeric(df["TotWaste_%"], errors='coerce')
            #
            # Convert TotAct.Qty to float
            df["TotAct.Qty"] = df["TotAct.Qty"].str.replace(',', '')
            mask = df["TotAct.Qty"].str.endswith('-')
            df.loc[mask, "TotAct.Qty"] = '-' + df.loc[mask, "TotAct.Qty"].str[:-1]
            df["TotAct.Qty"] = pd.to_numeric(df["TotAct.Qty"], errors='coerce').round(3)

            # Convert Cons._Qty. to float
            df["Cons._Qty."] = df["Cons._Qty."].str.replace(',', '')
            mask = df["Cons._Qty."].str.endswith('-')
            df.loc[mask, "Cons._Qty."] = '-' + df.loc[mask, "Cons._Qty."].str[:-1]
            df["Cons._Qty."] = pd.to_numeric(df["Cons._Qty."], errors='coerce').round(3)

            # Convert StAdjQty to float
            df["StAdjQty"] = df["StAdjQty"].str.replace(',', '')
            mask = df["StAdjQty"].str.endswith('-')
            df.loc[mask, "StAdjQty"] = '-' + df.loc[mask, "StAdjQty"].str[:-1]
            df["StAdjQty"] = pd.to_numeric(df["StAdjQty"], errors='coerce').round(3)
            #
            # Convert TotComScr% to float
            df["TotComScr%"] = df["TotComScr%"].str.replace(',', '')
            mask = df["TotComScr%"].str.endswith('-')
            df.loc[mask, "TotComScr%"] = '-' + df.loc[mask, "TotComScr%"].str[:-1]
            df["TotComScr%"] = pd.to_numeric(df["TotComScr%"], errors='coerce')

            df["PrTgtQty"] = df["PrTgtQty"].round(3)
            df["PrNetQty"] = df["PrNetQty"].round(3)


            print(df.dtypes)
            insert_data(df)
            verify_table()
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))
    except AttributeError:
        print("Unexpected error:", sys.exc_info()[0])


main()
