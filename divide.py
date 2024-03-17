import pandas as pd

# Step 1: Load the Excel file into a DataFrame
file_path = 'ref_link.xlsx'
df = pd.read_excel(file_path)
# Step 2: Calculate the number of rows per part
total_rows = len(df)
rows_per_part = total_rows // 54

# Step 3: Split the DataFrame into 9 parts and save each part to a separate Excel file
for i in range(54):
    start_index = i * rows_per_part
    end_index = (i + 1) * rows_per_part
    if i == 54:  # For the last part, take all remaining rows
        end_index = total_rows
    part_df = df.iloc[start_index:end_index, 0]
    part_df.to_excel(f'part_{i+1}.xlsx', index=False)

print("Excel file divided into 54 parts successfully.")