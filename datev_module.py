import pandas as pd
import chardet
import re


def load_and_process_datev():
    datev_file = 'mkt_infl-3.csv'

    # --- Load and clean 2024 file ---
    with open(datev_file, 'rb') as f:
        encoding_2024 = chardet.detect(f.read())['encoding']
        print(f"🔍 2024 Encoding: {encoding_2024}")

    try:
        df = pd.read_csv(datev_file, encoding=encoding_2024, sep=';', quotechar='"', dtype=str, on_bad_lines='warn')
    except Exception:
        df = pd.read_csv(datev_file, encoding='ISO-8859-1', sep=';', quotechar='"', dtype=str, on_bad_lines='warn')

    df.columns = df.columns.str.strip().str.replace('\ufeff', '')
    df['Umsatz (mit Soll/Haben-Kz)'] = pd.to_numeric(df['Umsatz (mit Soll/Haben-Kz)'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False), errors='coerce')
    df['Betrag'] = df['Umsatz (mit Soll/Haben-Kz)']
    df['TripNumber'] = df['Buchungstext'].str.extract(r"(T-\d{6}-\d+)")
    df['KOST1'] = df['KOST1'].str.upper()
    df['KOST2'] = df['KOST2'].str.upper()


    def normalize(val):
        return re.sub(r'[^A-Za-z0-9]', '', str(val).upper()) if pd.notna(val) else ''


    df['Belegdatum'] = pd.to_datetime(df['Belegdatum'], errors='coerce', dayfirst=True)
    df['Month'] = df['Belegdatum'].dt.month
    df['Month Name'] = df['Belegdatum'].dt.strftime('%B')
    df['Year'] = df['Stapel-Nr.'].str.extract(r'-(\d{4})')

    grouped = df.groupby(['Belegfeld 1', 'Konto', 'Gegenkonto', 'TripNumber', 'KOST1', 'KOST2', 'Stapel-Nr.', 'Belegdatum', 'Year']).agg({
        'Betrag': 'sum'
    }).reset_index()

    grouped.rename(columns={'Belegfeld 1': 'Invoice No', 'Betrag': 'Datev Amount'}, inplace=True)
    grouped['Datev Amount'] = grouped['Datev Amount'] / 100

    grouped['KOST1'] = grouped['KOST1'].str.upper().fillna("")

    # 🔍 Filter only for KOST1 containing MKT INFL or MKTINFL
    grouped = grouped[grouped['KOST1'].str.contains(r'\bMKT\s*INFL\b', regex=True, na=False)]

    print("📋 Cleaned and aggregated DATEV columns:", grouped.columns.tolist())
    grouped.to_csv('datev-summary-3.csv', index=False)
    print("✅ Aggregation completed and exported.")

    return grouped

if __name__ == '__main__':
    load_and_process_datev()
