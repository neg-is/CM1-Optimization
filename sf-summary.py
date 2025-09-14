import pandas as pd
import chardet
import re

sf_file = 'SF.csv'  # Or whichever file you really want

# --- Detect SF encoding ---
with open('SF.csv', 'rb') as f:
    sf_encoding = chardet.detect(f.read(100000))['encoding']
    print("📦 Detected SF encoding:", sf_encoding)


sf_df = pd.read_csv(
    sf_file,
    encoding=sf_encoding,
    sep=',',
    quotechar='"',
    dtype=str,
    engine='python',
    on_bad_lines='warn'
)

print("📋 Extracted SF columns:", sf_df.columns.tolist())
print(f"📊 SF shape after preprocessing: {sf_df.shape}")

sf_df.rename(columns={'Invoice: Invoice No.': 'Invoice No', ' Invoice: Grand Total ': 'SF Amount', 'Invoice: Trip Detail: Trip Confirmation: Trip': 'TripNumber'}, inplace=True)
sf_df = sf_df.drop_duplicates(subset='Invoice No', keep='first')

# --- Aggregate ---
sf_df = sf_df.groupby('Invoice No').agg({
    'TripNumber': 'first',
    'SF Amount': 'sum',
}).reset_index()


sf_df.drop_duplicates(subset='Invoice No', keep='first', inplace=True)


print("📋 Extracted SF columns:", sf_df.columns.tolist())

# Step 2: Clean and standardize column names
sf_df.columns = sf_df.columns.str.strip()

sf_df.to_csv('41395-sf-summary.csv', index=False)


merged = pd.merge(datev_df, sf_df, on='Invoice No', how='inner')
merged = merged[['Invoice No', 'TripNumber', 'Datev Amount', 'SF Amount', 'Belegdatum', 'Month', 'Month Name']]
merged = merged.drop_duplicates(subset='TripNumber', keep='first')

merged['Difference'] = pd.to_numeric(merged['Datev Amount'], errors='coerce') + pd.to_numeric(merged['SF Amount'], errors='coerce')
print(merged.head(10))

merged.to_csv('41395-datev-vs-sf.csv', index=False)