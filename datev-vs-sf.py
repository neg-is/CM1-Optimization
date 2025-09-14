import pandas as pd
import chardet
import re
from datev_module import load_and_process_datev
datev_file_2023 = '4-Datev-2023.csv'
primanota_2023_file = 'Primanota-2023.csv'
primanota_2024_file = 'Primanota-2024.csv'
datev_df = load_and_process_datev()


with open('Primanota-2023.csv', 'rb') as f:
    primanota_2023_encoding = chardet.detect(f.read(100000))['encoding']
    print("📦 Detected SF encoding:", primanota_2023_encoding)

primanota_2023 = pd.read_csv(
    primanota_2023_file,
    encoding=primanota_2023_encoding,
    sep=',',
    quotechar='"',
    dtype=str,
    engine='python',
    on_bad_lines='warn'
)

print("📋 Extracted Primanota_2023 columns:", primanota_2023.columns.tolist())
print(f"📊 Primanota_2023 shape after preprocessing: {primanota_2023.shape}")

with open('Primanota-2024.csv', 'rb') as f:
    primanota_2024_encoding = chardet.detect(f.read(100000))['encoding']
    print("📦 Detected SF encoding:", primanota_2023_encoding)

primanota_2024 = pd.read_csv(
    primanota_2024_file,
    encoding=primanota_2024_encoding,
    sep=',',
    quotechar='"',
    dtype=str,
    engine='python',
    on_bad_lines='warn'
)

print("📋 Extracted Primanota_2024 columns:", primanota_2024.columns.tolist())
print(f"📊 Primanota_2024 shape after preprocessing: {primanota_2024.shape}")

primanota_df = pd.concat([primanota_2023, primanota_2024], ignore_index=True)

primanota_df.columns = primanota_df.columns.str.strip()

# Confirm the exact column names
print("📋 Extracted Primanota_df columns:",primanota_df.columns.tolist())

# Select only 'Stapel-Nr' and 'Bezeichnung' from primanota_df
primanota_subset = primanota_df[['Stapel-Nr.', 'Bezeichnung']]

# Confirm the exact column names
print("📋 Extracted primanota_subest columns:",primanota_subset.columns.tolist())

# Merge that into datev_df
datev_df = pd.merge(
    datev_df,
    primanota_subset,
    on='Stapel-Nr.',
    how='left'
)


sf_file = 'SF.csv'


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
    'SF Amount': 'sum',
    'TripNumber': 'first',
}).reset_index()


sf_df.drop_duplicates(subset='Invoice No', keep='first', inplace=True)


print("📋 Extracted SF columns:", sf_df.columns.tolist())

# Step 2: Clean and standardize column names
sf_df.columns = sf_df.columns.str.strip()

sf_df.to_csv('sf-summary-17062025.csv', index=False)

datev_df.columns = datev_df.columns.str.strip()
sf_df.columns = sf_df.columns.str.strip()

# Normalize Invoice No fields for matching
sf_df['Invoice No Clean'] = sf_df['Invoice No'].str.replace('-', '', regex=False)
datev_df['Invoice No Clean'] = datev_df['Invoice No'].astype(str).str.strip()

# Merge on normalized invoice number
merged_inv_level = pd.merge(
    datev_df,
    sf_df,
    on='Invoice No Clean',
    how='left',
    suffixes=('_DATEV', '_SF')
)
merged_inv_level = merged_inv_level[merged_inv_level['KOST1'].str.strip().str.upper() != 'COGS']

# Fix SF amount formatting early
merged_inv_level['SF Amount Clean'] = (
    merged_inv_level['SF Amount']
    .astype(str)
    .str.replace(',', '', regex=False)       # remove thousands separators
    .str.replace('(', '-', regex=False)      # convert (1234) to -1234
    .str.replace(')', '', regex=False)
)

# Convert to numeric for calculation
merged_inv_level['SF Amount Clean'] = pd.to_numeric(merged_inv_level['SF Amount Clean'], errors='coerce')

# Invoice-level Difference
merged_inv_level['Difference'] = (
    merged_inv_level['Datev Amount'].abs() -
    merged_inv_level['SF Amount Clean'].abs()
)

# Trip-level aggregation
trip_totals = merged_inv_level.groupby('TripNumber_DATEV').agg({
    'Datev Amount': 'sum',
    'SF Amount Clean': 'sum'
}).reset_index().rename(columns={
    'Datev Amount': 'Trip Total Datev',
    'SF Amount Clean': 'Trip Total SF'
})

# Merge trip totals back
merged_inv_level = pd.merge(
    merged_inv_level,
    trip_totals,
    on='TripNumber_DATEV',
    how='left'
)

# Trip-level Difference
merged_inv_level['Trip Difference'] = (
    merged_inv_level['Trip Total Datev'].abs() -
    merged_inv_level['Trip Total SF'].abs()
)

# Final Match Status
merged_inv_level['Match Status'] = merged_inv_level.apply(
    lambda row: 'Matched' if (
        -1000 <= row['Difference'] <= 1000 or
        -1000 <= row['Trip Difference'] <= 1000
    ) else 'No Match',
    axis=1
)

column_order = [
    'Invoice No_DATEV', 'Invoice No_SF', 'Match Status',
    'Datev Amount', 'SF Amount', 'Difference',
    'TripNumber_DATEV', 'TripNumber_SF',
    'Belegdatum', 'Month', 'Month Name',
    'Trip Total Datev', 'Trip Total SF', 'Trip Difference'
]

# Keep only existing columns from the list
column_order = [col for col in column_order if col in merged_inv_level.columns]
merged_inv_level = merged_inv_level[column_order + [col for col in merged_inv_level.columns if col not in column_order]]

print("📋 Extracted SF columns:", merged_inv_level.columns.tolist())
print(f"📊 SF shape after preprocessing: {merged_inv_level.shape}")

merged_inv_level.to_csv('4-datev-vs-sf-inv-level_all_rows-17062025.csv', index=False)
print("✅ Cleaned merge exported.")

#Optional: Show trip totals only on the first line per trip
merged_inv_level['Show Trip Total'] = ~merged_inv_level.duplicated('TripNumber_DATEV')
for col in ['Trip Total Datev', 'Trip Total SF', 'Trip Difference']:
    merged_inv_level[col] = merged_inv_level.apply(
        lambda row: row[col] if row['Show Trip Total'] else '', axis=1
    )
merged_inv_level.drop(columns='Show Trip Total', inplace=True)

column_order = [
    'Invoice No_DATEV', 'Invoice No_SF', 'Match Status',
    'Datev Amount', 'SF Amount', 'Difference',
    'TripNumber_DATEV', 'TripNumber_SF',
    'Belegdatum', 'Month', 'Month Name',
    'Trip Total Datev', 'Trip Total SF', 'Trip Difference'
]

# Keep only existing columns from the list
column_order = [col for col in column_order if col in merged_inv_level.columns]
merged_inv_level = merged_inv_level[column_order + [col for col in merged_inv_level.columns if col not in column_order]]

print("📋 Extracted SF columns:", merged_inv_level.columns.tolist())
print(f"📊 SF shape after preprocessing: {merged_inv_level.shape}")

# Export
merged_inv_level.to_csv('4-datev-vs-sf-inv-level-17062025.csv', index=False)
print("✅ Cleaned merge exported.")

# Fill NaN in SF columns with 0 to include unmatched invoices
merged_inv_level['SF Amount Clean'] = merged_inv_level['SF Amount Clean'].fillna(0)
merged_inv_level['SF Amount'] = merged_inv_level['SF Amount'].fillna(0)
merged_inv_level['Invoice No_SF'] = merged_inv_level['Invoice No_SF'].fillna('Not ih Datev')
merged_inv_level['TripNumber_SF'] = merged_inv_level['TripNumber_SF'].fillna('Not in SF')


# Trip-level aggregation
trip_totals = merged_inv_level.groupby('TripNumber_DATEV').agg({
    'Datev Amount': 'sum',
    'SF Amount Clean': 'sum'
}).reset_index().rename(columns={
    'Datev Amount': 'Trip Total Datev',
    'SF Amount Clean': 'Trip Total SF'
})


grouped = merged_inv_level.groupby(
    ['Konto', 'Match Status', 'KOST1', 'Year', 'Stapel-Nr.','Invoice No_DATEV','Invoice No_SF','TripNumber_SF', 'Bezeichnung']
).agg({
    'Datev Amount': 'sum',
    'SF Amount Clean': 'sum',
}).reset_index().rename(columns={
    'SF Amount Clean': 'SF Amount'
})


grouped.to_csv('5-datev-vs-sf-summary-by-konto-match-year-month-17062025.csv', index=False)
print("✅ Grouped summary exported.")


