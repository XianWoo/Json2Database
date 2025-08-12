# The purpose of this script is to convert a JSON file into a Microsoft Access Database (.mdb) file.
# This script requires a Windows machine with the Microsoft Access Database Engine installed.
# You will also need to install the pyodbc library: pip install pyodbc

import json
import pyodbc
import os


def create_mdb_from_json(json_filename, mdb_filename):
    """
    Reads a JSON file with a top-level 'd' object and 'results' array,
    creates a new MDB file, and populates two tables
    ('Organizations' and 'Communications') based on the JSON data.
    """

    # 1. Load the JSON data
    try:
        with open(json_filename, 'r', encoding = 'utf-8') as f:
            full_data = json.load(f)

        # Access the 'results' array from the top-level 'd' object
        data = full_data.get('d', {}).get('results', [])
        print(f"Successfully loaded JSON data from '{json_filename}'. Found {len(data)} organizations.")
        if not data:
            print("Warning: The 'results' array in the JSON file is empty. No data will be inserted.")
            return

    except FileNotFoundError:
        print(f"Error: The file '{json_filename}' was not found.")
        return
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{json_filename}'. Please check the file's format.")
        return

    # 2. Extract data for the two tables from the JSON
    organizations_data = []
    communications_data = []

    for org in data:
        # Extract data for the 'Organizations' table
        organizations_data.append((
            org.get('Id'),
            org.get('Name'),
            org.get('Language'),
            org.get('ManagedBy'),
            org.get('CoName'),
            org.get('HouseNumber'),
            org.get('Street'),
            org.get('Street2'),
            org.get('Street3'),
            org.get('Street4'),
            org.get('Street5'),
            org.get('District'),
            org.get('Building')
        ))

        # Extract data for the 'Communications' table
        if 'Communications' in org and 'results' in org['Communications']:
            for comm in org['Communications']['results']:
                communications_data.append((
                    comm.get('OrgId'),  # This is the foreign key to the Organizations table
                    comm.get('SequenceNumber'),
                    comm.get('Type'),
                    comm.get('Data'),
                    comm.get('Owner'),
                    comm.get('DoNotUse'),
                    comm.get('Official'),
                    comm.get('StdRecipient'),
                    comm.get('BrSeqNumber'),
                    # These fields were not present in the new JSON, so adding placeholders to prevent errors
                    # You may need to update this part if your JSON data changes again
                    None,  # Placeholder for MetadataId
                    None,  # Placeholder for MetadataVal
                    None,  # Placeholder for MetadataTyp
                    None  # Placeholder for Comments
                ))

    # 3. Define the MDB connection string
    # This string specifies the driver to use and the path for the new MDB file.
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        f'DBQ={mdb_filename};'
    )

    # 4. Connect to the MDB file (it will be created if it doesn't exist)
    conn = None
    try:
        # Check if the file exists and delete it to start fresh
        if os.path.exists(mdb_filename):
            os.remove(mdb_filename)
            print(f"Existing file '{mdb_filename}' deleted.")

        # Establish connection. The file will be created.
        conn = pyodbc.connect(conn_str, autocommit = True)
        cursor = conn.cursor()
        print(f"Successfully connected to the MDB file: '{mdb_filename}'.")

        # 5. Create the 'Organizations' table
        print("Creating table 'Organizations'...")
        cursor.execute(f"""
            CREATE TABLE Organizations (
                [Id] TEXT(255) PRIMARY KEY,
                [Name] TEXT(255),
                [Language] TEXT(255),
                [ManagedBy] TEXT(255),
                [CoName] TEXT(255),
                [HouseNumber] TEXT(255),
                [Street] TEXT(255),
                [Street2] TEXT(255),
                [Street3] TEXT(255),
                [Street4] TEXT(255),
                [Street5] TEXT(255),
                [District] TEXT(255),
                [Building] TEXT(255)
            );
        """)

        # 6. Create the 'Communications' table
        print("Creating table 'Communications'...")
        cursor.execute(f"""
            CREATE TABLE Communications (
                [OrgId] TEXT(255),
                [SequenceNumber] TEXT(255),
                [Type] TEXT(255),
                [Data] TEXT(255),
                [Owner] TEXT(255),
                [DoNotUse] YESNO,
                [Official] YESNO,
                [StdRecipient] YESNO,
                [BrSeqNumber] TEXT(255)
            );
        """)

        # 7. Insert data into the 'Organizations' table
        print("Inserting data into 'Organizations'...")
        insert_organizations_query = f"""
            INSERT INTO Organizations 
            ([Id], [Name], [Language], [ManagedBy], [CoName], [HouseNumber], 
            [Street], [Street2], [Street3], [Street4], [Street5], [District], [Building])
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        cursor.executemany(insert_organizations_query, organizations_data)

        # 8. Insert data into the 'Communications' table
        print("Inserting data into 'Communications'...")
        insert_communications_query = f"""
            INSERT INTO Communications 
            ([OrgId], [SequenceNumber], [Type], [Data], [Owner], [DoNotUse], 
            [Official], [StdRecipient], [BrSeqNumber])
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        cursor.executemany(insert_communications_query, communications_data)

        # 9. Commit changes and close the connection
        conn.commit()
        print(f"Data successfully inserted. File '{mdb_filename}' is ready.")

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"An ODBC error occurred: {sqlstate}")
        print(ex)
    finally:
        if conn:
            conn.close()


# Example usage:
if __name__ == '__main__':
    create_mdb_from_json('../database/Organizations.json', 'Organizations_Output.mdb')

