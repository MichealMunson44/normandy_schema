import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, MetaData
import os
from dotenv import load_dotenv
import json
from pathlib import Path
from datetime import datetime

load_dotenv()

CACHE_FILE = Path("db_schema_cache.json")


def check_password():
    """Returns `True` if the user had the correct password."""
    st.set_page_config(layout="wide")

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["password"] == os.getenv("DASHBOARD_PASSWORD"):
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show input for password.
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        return False
    elif not st.session_state["password_correct"]:
        # Password not correct, show input + error.
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        st.error("😕 Password incorrect")
        return False
    else:
        # Password correct.
        return True


def inspect_database(connection_string):
    engine = create_engine(connection_string)
    metadata = MetaData()
    metadata.reflect(engine)

    schema_info = []

    for table_name in metadata.tables:
        table = metadata.tables[table_name]

        # Get primary key
        primary_key = ", ".join([col.name for col in table.primary_key.columns])

        # Get foreign keys (references)
        references = []
        for fk in table.foreign_keys:
            references.append(f"{fk.column.table.name}.{fk.column.name}")

        # Get tables referencing this table
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


def save_cache(data):
    cache_data = {"timestamp": datetime.now().isoformat(), "schema_info": data}
    with CACHE_FILE.open("w") as f:
        json.dump(cache_data, f)


def load_cache():
    if CACHE_FILE.exists():
        with CACHE_FILE.open("r") as f:
            return json.load(f)
    return None


def create_dashboard(df, last_updated):

    st.title("⭕ Normandy Database Schema Dashboard")

    formatted_time = datetime.fromisoformat(last_updated).strftime("%Y-%m-%d %H:%M:%S")
    st.write(f"Last updated: {formatted_time}")

    st.write("## Overview")
    st.write(f"Total number of tables: {len(df)}")

    st.write("## Schema Information")
    search_term = st.text_input("Enter table name to search")
    if search_term:
        filtered_df = df[df["table"].str.contains(search_term, case=False)]
        if not filtered_df.empty:
            st.write(f"Found {len(filtered_df)} table(s) matching '{search_term}':")
            st.dataframe(filtered_df, use_container_width=True)
        else:
            st.write(f"No tables found matching '{search_term}'")
    st.dataframe(df, use_container_width=True)

    st.write("## Most Referenced Tables")
    top_referenced = df.nlargest(5, "num_referenced_by")
    st.bar_chart(top_referenced.set_index("table")["num_referenced_by"])

    st.write("## Tables with Most References")
    top_references = df.nlargest(5, "num_references")
    st.bar_chart(top_references.set_index("table")["num_references"])


@st.cache_data
def get_schema_data():
    cached_data = load_cache()
    if cached_data:
        if (
            isinstance(cached_data, dict)
            and "schema_info" in cached_data
            and "timestamp" in cached_data
        ):
            return pd.DataFrame(cached_data["schema_info"]), cached_data["timestamp"]
        elif isinstance(cached_data, list):
            # Handle old cache format
            return pd.DataFrame(cached_data), datetime.now().isoformat()
        else:
            st.warning("Cache format is invalid. Refreshing data from database.")

    connection_string = os.getenv("DB_CONNECTION_STRING")
    if not connection_string:
        st.error("Database connection string not found. Please check your .env file.")
        return None, None

    schema_info = inspect_database(connection_string)
    save_cache(schema_info)
    return pd.DataFrame(schema_info), datetime.now().isoformat()


if __name__ == "__main__":
    if check_password():
        df, last_updated = get_schema_data()
        if df is not None:
            create_dashboard(df, last_updated)
    else:
        st.stop()  # Don't run the rest of the app
