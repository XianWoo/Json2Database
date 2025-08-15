import json
import os
import shutil
import pyodbc
from datetime import datetime


class JsonToMdbConverter:
    def __init__(self, json_path, mdb_path, template_path=None):
        self.json_path = json_path
        self.mdb_path = mdb_path
        self.template_path = template_path
        self.org_schema = {}
        self.comm_schema = {}
        self.org_records = []
        self.comm_records = []

    def convert(self):
        self._load_json()
        self._create_mdb()
        with self._connect_mdb() as conn:
            self._create_tables(conn)
            self._insert_data(conn)
        print(f"✅ Conversion complete: {self.mdb_path}")

    def _load_json(self):
        try:
            with open(self.json_path, "r", encoding="utf-8") as f:
                full_data = json.load(f)
        except FileNotFoundError:
            raise RuntimeError(f"JSON file not found: {self.json_path}")
        except json.JSONDecodeError:
            raise RuntimeError(f"Invalid JSON format in {self.json_path}")

        data = full_data.get("d", {}).get("results", [])
        if not data:
            raise RuntimeError("No 'results' found in JSON")

        # Separate org and comms
        org_list = []
        comm_list = []
        for org in data:
            org_copy = {k: v for k, v in org.items() if k != "Communications"}
            org_list.append(org_copy)

            if "Communications" in org and "results" in org["Communications"]:
                for comm in org["Communications"]["results"]:
                    comm_list.append(comm)

        self.org_schema = self._extract_schema(org_list)
        self.comm_schema = self._extract_schema(comm_list)
        self.org_records = org_list
        self.comm_records = comm_list

    def _create_mdb(self):
        if os.path.exists(self.mdb_path):
            os.remove(self.mdb_path)
        if self.template_path:
            shutil.copy(self.template_path, self.mdb_path)
        else:
            raise RuntimeError("MDB template path not provided — cannot create file.")

    def _connect_mdb(self):
        try:
            return pyodbc.connect(
                f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={self.mdb_path};",
                autocommit=True
            )
        except pyodbc.Error as e:
            raise RuntimeError(f"Failed to connect to MDB: {e}")

    def _create_tables(self, conn):
        cursor = conn.cursor()

        def build_table_sql(table_name, schema):
            cols = []
            for field, ftype in schema.items():
                cols.append(f"[{field}] {ftype}")
            return f"CREATE TABLE {table_name} ({', '.join(cols)});"

        print("📄 Creating 'Organizations' table...")
        cursor.execute(build_table_sql("Organizations", self.org_schema))
        print("📄 Creating 'Communications' table...")
        cursor.execute(build_table_sql("Communications", self.comm_schema))
        conn.commit()

    def _insert_data(self, conn):
        cursor = conn.cursor()
        self._insert_records(cursor, "Organizations", self.org_schema, self.org_records)
        self._insert_records(cursor, "Communications", self.comm_schema, self.comm_records)
        conn.commit()

    def _insert_records(self, cursor, table_name, schema, records):
        placeholders = ", ".join("?" for _ in schema)
        insert_sql = f"INSERT INTO {table_name} ({', '.join(f'[{f}]' for f in schema)}) VALUES ({placeholders});"
        normalized = [self._normalize_record(record, schema) for record in records]
        cursor.executemany(insert_sql, normalized)

    @staticmethod
    def _extract_schema(records):
        schema = {}
        for record in records:
            for key, value in record.items():
                if key == "__metadata":  # Skip this column
                    continue
                if key not in schema:
                    schema[key] = JsonToMdbConverter._infer_type(value)
        return schema

    @staticmethod
    def _infer_type(value):
        if isinstance(value, bool):
            return "YESNO"
        if isinstance(value, int):
            return "INTEGER"
        if isinstance(value, float):
            return "DOUBLE"
        if JsonToMdbConverter._is_datetime(value):
            return "DATETIME"
        return "TEXT(255)"

    @staticmethod
    def _is_datetime(value):
        if isinstance(value, datetime):
            return True
        if isinstance(value, str):
            try:
                datetime.fromisoformat(value.replace("Z", "+00:00"))
                return True
            except ValueError:
                return False
        return False

    @staticmethod
    def _normalize_record(record, schema):
        def convert_value(value):
            if value is None:
                return None
            if isinstance(value, (str, int, float, bool, datetime)):
                return value
            return json.dumps(value, ensure_ascii=False)
        return tuple(convert_value(record.get(field)) for field in schema.keys())


if __name__ == "__main__":
    # Example usage
    converter = JsonToMdbConverter(
        json_path="../database/Organizations.json",
        mdb_path="../output/Organizations_Output1.mdb",
        template_path="../database/blank.mdb"
    )
    converter.convert()
