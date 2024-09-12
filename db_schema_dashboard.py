import pandas as pd
import streamlit as st
import json
from pathlib import Path
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

SCHEMA_FILE = Path("db_schema.json")


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


def load_schema_data():
    if SCHEMA_FILE.exists():
        with SCHEMA_FILE.open("r") as f:
            data = json.load(f)
        return pd.DataFrame(data["schema_info"]), data["timestamp"]
    else:
        st.error(f"Schema file {SCHEMA_FILE} not found.")
        return None, None


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
    return load_schema_data()


if __name__ == "__main__":
    if check_password():
        df, last_updated = get_schema_data()
        if df is not None:
            create_dashboard(df, last_updated)
    else:
        st.stop()  # Don't run the rest of the app
