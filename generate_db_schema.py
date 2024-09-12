from sqlalchemy import create_engine, MetaData
import json
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def inspect_database(connection_string):
    if not connection_string:
        raise ValueError("Connection string is empty or None")

    engine = create_engine(connection_string)
    metadata = MetaData()
    metadata.reflect(engine)

    schema_info = []

    for table_name in metadata.tables:
        table = metadata.tables[table_name]

        primary_key = ", ".join([col.name for col in table.primary_key.columns])

        references = []
        for fk in table.foreign_keys:
            references.append(f"{fk.column.table.name}.{fk.column.name}")

        referenced_by = []
        for other_table in metadata.tables.values():
            for fk in other_table.foreign_keys:
                if fk.references(table):
                    referenced_by.append(other_table.name)

        schema_info.append(
            {
                "table": table_name,
                "primary_key": primary_key,
                "references": ", ".join(references),
                "referenced_by": ", ".join(set(referenced_by)),
                "num_references": len(references),
                "num_referenced_by": len(set(referenced_by)),
            }
        )

    return schema_info


if __name__ == "__main__":
    # Load the connection string from the .env file
    connection_string = os.getenv("DB_CONNECTION_STRING")

    if not connection_string:
        print(
            "ERROR: DB_CONNECTION_STRING environment variable is not set in the .env file."
        )
        print(
            "Please ensure you have a .env file with DB_CONNECTION_STRING properly set."
        )
        exit(1)

    print("Connection string loaded from .env file.")

    try:
        schema_info = inspect_database(connection_string)

        data = {"timestamp": datetime.now().isoformat(), "schema_info": schema_info}

        with open("db_schema.json", "w") as f:
            json.dump(data, f)

        print("Schema data saved to db_schema.json")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        print(
            "Please check your connection string and ensure you have access to the database."
        )
