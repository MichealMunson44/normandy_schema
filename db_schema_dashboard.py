import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, MetaData


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

    return pd.DataFrame(schema_info)


def create_dashboard(df):
    st.title("Database Schema Dashboard")

    st.write("## Overview")
    st.write(f"Total number of tables: {len(df)}")

    st.write("## Search Table")
    search_term = st.text_input("Enter table name to search:")
    if search_term:
        filtered_df = df[df["table"].str.contains(search_term, case=False)]
        if not filtered_df.empty:
            st.write(f"Found {len(filtered_df)} table(s) matching '{search_term}':")
            st.dataframe(filtered_df)
        else:
            st.write(f"No tables found matching '{search_term}'")

    st.write("## Schema Information")
    st.dataframe(df)

    st.write("## Table Reference Visualization")
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(df["num_references"], df["num_referenced_by"], alpha=0.5)
    ax.set_xlabel("Number of Tables Referenced")
    ax.set_ylabel("Number of Tables Referencing")
    ax.set_title("Table Relationships")
    for i, txt in enumerate(df["table"]):
        ax.annotate(txt, (df["num_references"][i], df["num_referenced_by"][i]))
    st.pyplot(fig)

    st.write("## Most Referenced Tables")
    top_referenced = df.nlargest(5, "num_referenced_by")
    st.bar_chart(top_referenced.set_index("table")["num_referenced_by"])

    st.write("## Tables with Most References")
    top_references = df.nlargest(5, "num_references")
    st.bar_chart(top_references.set_index("table")["num_references"])


if __name__ == "__main__":
    connection_string = "postgresql://michealmunson@localhost:5432/normandy"
    df = inspect_database(connection_string)
    create_dashboard(df)
