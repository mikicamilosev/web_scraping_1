import pandas as pd
import os

file1 = "file1.csv"
file2 = "file2.csv"

def save_to_csv(df):
    file_path = "extracted_data.csv"
    
    if os.path.isfile(file_path):
        past_df = pd.read_csv(file_path)
        combined_df = pd.concat([past_df, df], ignore_index=True)
        combined_df.to_csv(file_path, index=False)
        print(f"Data added to '{file_path}'")
    else:
        df.to_csv(file_path, index=False)
        print(f"Data stored in a new file '{file_path}'")

for i in range(27):
    file2 = f"extracted_data{i+1}.csv"

    df2 = pd.read_csv(file2)

    save_to_csv(df2)
