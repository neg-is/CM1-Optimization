import os
import csv

import chardet

# Path to a folder containing the CSV files
folder_path = r"C:\Users\USERNAME\Documents\New Folder"

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read(10_000))  # Read first 10,000 bytes for detection
        return result['encoding']




def is_csv_with_semicolon_delimiter(file_path):
    """
    Check if the CSV file uses semicolons as delimiters
    and if the first column values are in European numeric format (comma as decimal separator).
    """
    try:
        with open(file_path, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter=';')
            rows = list(reader)

            if not rows:
                print(f"File {file_path} is empty.")
                return False

            num_columns = len(rows[0])
            for row in rows:
                if len(row) != num_columns:
                    print(f"Inconsistent columns in {file_path}.")
                    return False

                if row[0]:
                    try:
                        float(row[0].replace(',', '.'))  # European format: convert comma to dot for float
                    except ValueError:
                        print(f"Invalid number format in first column of {file_path}.")
                        return False

            return True
    except Exception as e:
        print(f"Error checking {file_path}: {e}")
        return False

def convert_csv_to_semicolon(file_path):
    """
    Convert a CSV file from comma-delimited to semicolon-delimited,
    adjusting the first column’s number format to European style.
    """
    try:
        with open(file_path, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter=',')
            rows = list(reader)

        updated_rows = []
        for row in rows:
            updated_row = row.copy()

            # Adjust the first column if it's numeric
            if updated_row and updated_row[0]:
                try:
                    number = float(updated_row[0])  # Assumes it’s using dot as decimal separator
                    updated_row[0] = f"{number:.2f}".replace('.', ',')  # Format as European (comma)
                except ValueError:
                    pass  # Keep original if not a number

            updated_rows.append(updated_row)

        # Write back with semicolon delimiter
        with open(file_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerows(updated_rows)

        print(f"File converted: {file_path}")

    except Exception as e:
        print(f"Error converting {file_path}: {e}")

def process_csv_folder(folder_path):
    """
    Process all CSV files in the given folder, converting those
    that are comma-delimited to semicolon-delimited.
    """
    if not os.path.exists(folder_path):
        print(f"Folder does not exist: {folder_path}")
        return

    files = [f for f in os.listdir(folder_path) if f.lower().endswith('.csv')]
    if not files:
        print("No CSV files found.")
        return

    for filename in files:
        file_path = os.path.join(folder_path, filename)
        print(f"Checking file: {file_path}")

        if is_csv_with_semicolon_delimiter(file_path):
            print(f"Skipping (already in correct format): {filename}")
        else:
            convert_csv_to_semicolon(file_path)

if __name__ == "__main__":
    process_csv_folder(folder_path)

