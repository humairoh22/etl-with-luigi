import pandas as pd


def validation_process(data: pd.DataFrame, table_name: str):
    print("================== Start Data Validation ==================\n")

    #check n rows and cols
    n_rows = data.shape[0]
    n_col = data.shape[1]

    print(f"Data {table_name} memiliki {n_rows} baris dan {n_col} kolom.\n")

    GET_COL = data.columns

    #check data type on each columns
    for col in GET_COL:
        print(f"Column {col} memiliki tipe data {data[col].dtypes}")

    print("")

    #check missing value on each columns
    for col in GET_COL:
        get_missing_values = round((data[col].isnull().sum() * 100) / len(data),3)

        print(f"Persentase missing value di kolom {col} sebesar {get_missing_values} %.")

    print("")

    print(f"jumlah data {table_name} yang terduplikat:\n {data.duplicated(keep='first').sum()}")
       
    print("")
    print("================== End of Data Validation ==================\n")