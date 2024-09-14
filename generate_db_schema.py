from sqlalchemy import create_engine, MetaData, inspect
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def get_column_type(column):
    """Convert SQLAlchemy column type to DBML type"""
    type_str = str(column.type)
    if "INT" in type_str.upper():
        return "integer"
    elif "CHAR" in type_str.upper() or "TEXT" in type_str.upper():
        return "varchar"
    elif "TIMESTAMP" in type_str.upper() or "DATETIME" in type_str.upper():
        return "datetime"
    elif "BOOL" in type_str.upper():
        return "boolean"
    else:
        return type_str.lower()


def inspect_database(connection_string):
    if not connection_string:
        raise ValueError("Connection string is empty or None")

    engine = create_engine(connection_string)
    metadata = MetaData()
    metadata.reflect(engine)
    inspector = inspect(engine)

    athelas_ehr_core_tables = ["prior_auths", "appointments"]
    athelas_ehr_core_tag = "[ATHELAS_EHR_CORE_TABLE]"
    athelas_ehr_related_tag = "[ATHELAS_EHR_RELATED_TABLE]"

    def get_related_tables(table_name, visited=None):
        if visited is None:
            visited = set()
        if table_name in visited:
            return set()
        visited.add(table_name)
        related = set([table_name])
        table = metadata.tables[table_name]
        for fk in table.foreign_keys:
            related.update(get_related_tables(fk.column.table.name, visited))
        for other_table in metadata.tables.values():
            for fk in other_table.foreign_keys:
                if fk.references(table):
                    related.update(get_related_tables(other_table.name, visited))
        return related

    athelas_ehr_related_tables = set()
    for core_table in athelas_ehr_core_tables:
        athelas_ehr_related_tables.update(get_related_tables(core_table))

    dbml_content = [
        f"Project Normandy {{\n  database_type: 'PostgreSQL'\n  Note: '''\n    # Normandy Database\n    Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n  '''\n}}"
    ]

    for table_name, table in metadata.tables.items():
        dbml_content.append(f"Table {table_name} {{")

        # Add columns
        for column in table.columns:
            try:
                column_def = f"  {column.name} {get_column_type(column)}"
                attributes = []
                if column.primary_key:
                    attributes.append("pk")
                if not column.nullable:
                    attributes.append("not null")
                if column.server_default:
                    if hasattr(column.server_default, "arg"):
                        attributes.append(f"default: `{column.server_default.arg}`")
                    elif "Computed" in str(type(column.server_default)):
                        attributes.append("note: 'Computed column'")
                    else:
                        attributes.append(f"default: `{str(column.server_default)}`")
                if attributes:
                    column_def += f" [{', '.join(attributes)}]"
                dbml_content.append(column_def)
            except Exception as e:
                print(
                    f"Error processing column {column.name} in table {table_name}: {str(e)}"
                )
                dbml_content.append(
                    f"  {column.name} unknown [note: 'Error processing this column']"
                )

        # Add indexes
        indexes = inspector.get_indexes(table_name)
        if indexes:
            dbml_content.append("  Indexes {")
            for index in indexes:
                index_def = f"    ({', '.join(index['column_names'])})"
                index_attributes = []
                if index["unique"]:
                    index_attributes.append("unique")
                if index["name"]:
                    index_attributes.append(f"name: '{index['name']}'")
                if index_attributes:
                    index_def += f" [{', '.join(index_attributes)}]"
                dbml_content.append(index_def)
            dbml_content.append("  }")

        # Add note for Athelas EHR related tables
        if table_name in athelas_ehr_related_tables:
            if table_name in athelas_ehr_core_tables:
                dbml_content.append(f"Note: '{athelas_ehr_core_tag}'")
            else:
                dbml_content.append(f"Note: '{athelas_ehr_related_tag}'")

        dbml_content.append("}")

    # Add references
    for table_name, table in metadata.tables.items():
        for fk in table.foreign_keys:
            dbml_content.append(
                f"Ref: {table_name}.{fk.parent.name} > {fk.column.table.name}.{fk.column.name}"
            )

    return "\n".join(dbml_content)


if __name__ == "__main__":
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
        dbml_content = inspect_database(connection_string)

        with open("normandy_schema.dbml", "w") as f:
            f.write(dbml_content)

        print("Schema data saved to normandy_schema.dbml")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        print(
            "Please check your connection string and ensure you have access to the database."
        )
