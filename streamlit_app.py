# Import python packages
import streamlit as st
import pandas as pd
import time
from snowflake.snowpark.context import get_active_session
from snowflake.cortex import Complete
from snowflake.snowpark.functions import concat, lit


def get_options_models():
    """Define a list of models."""
    return [
        "llama2-70b-chat",
        "mistral-large",
        "mistral-7b",
        "mixtral-8x7b",
        "gemma-7b",
        "llama3",
    ]


def get_options_databases(session):
    """Define a list of databases."""
    options_databases = []
    for i in session.sql("SHOW DATABASES").collect():
        options_databases.append(i.name)
    return options_databases


def get_options_schemas(session, database):
    """Define a list of databases."""
    options_schemas = []
    for i in session.sql(f"SHOW SCHEMAS IN DATABASE {database}").collect():
        options_schemas.append(i.name)
    return options_schemas


def get_options_tables(session, database, table):
    """Define a list of tables."""
    options_tables = []
    for i in session.sql(f"SHOW TABLES IN SCHEMA {database}.{table}").collect():
        options_tables.append(i.name)
    return options_tables


def get_table(session, selected_option):
    """Get the table based on the selected option."""
    return session.table(selected_option)


def get_table_desc(selected_model, selected_option):
    """Get the table description."""
    return Complete(
        selected_model,
        f"As an AI assistant, provide concise and clear one-liner descriptions of tables based on their names. Focus solely on the overall purpose and content of the table:- {selected_option}",
    )


def get_table_df(session, t):
    """Get the dataframe of the table columns."""
    return session.create_dataframe(t.columns).to_df("Columns")


def get_table_column_desc(table_df, selected_model):
    """Get the dataframe with the column descriptions."""
    return table_df.withColumn(
        "Description",
        Complete(
            selected_model,
            concat(
                lit(
                    "As an AI assistant, provide concise and clear one-line description of columns in table based on their names. Please note that the descriptions will not include any column names, but will focus solely on the overall purpose and content of the table:- "
                ),
                "Columns",
            ),
        ),
    )


def main():
    try:
        # Define a dictionary with default session state values
        if st.session_state == {}:
            default_session_state = {
                "options_models": [],
                "selected_model": "",
                "options_databases": [],
                "selected_database": "",
                "options_schemas": [],
                "selected_schema": "",
                "options_tables": [],
                "selected_table": "",
                "modeled_table_desc": "",
                "edited_table_desc": "",
                "modeled_column_desc": pd.DataFrame(),
                "edited_column_desc": pd.DataFrame(),
                "selected_table_name": "",
            }

            # Iterate over each item in the default session state
            for key, default_value in default_session_state.items():
                # If the key is not in the session state, add it with the default value
                if key not in st.session_state:
                    st.session_state[key] = default_value

        # Write directly to the app
        st.title(
            ":memo: :rainbow[TableDoc]",
        )
        st.markdown(
            """
            📱 This application 🚀 generates **table details** 📊 and **descriptions** 📝 based on your **database schema** 🗃️. It's your handy tool for understanding and visualizing your database structure! 🎉
            """
        )

        # Get the current session
        session = get_active_session()

        # Get the list of models
        if st.session_state["options_models"] == []:
            st.session_state["options_models"] = get_options_models()
        selected_model = st.selectbox(
            ":blue[Select LLM Mode?]", st.session_state["options_models"]
        )

        # Get the list of databases
        if st.session_state["options_databases"] == []:
            st.session_state["options_databases"] = get_options_databases(session)
        selected_database = st.selectbox(
            ":blue[Select Database?]", st.session_state["options_databases"]
        )

        # Get the list of scehmas
        st.session_state["options_schemas"] = get_options_schemas(
            session, selected_database
        )
        selected_schema = st.selectbox(
            ":blue[Select Schemas?]", st.session_state["options_schemas"]
        )

        # Get the list of tables
        st.session_state["options_tables"] = get_options_tables(
            session, selected_database, selected_schema
        )
        selected_table = st.selectbox(
            ":blue[Select table?]", st.session_state["options_tables"]
        )

        if (
            st.session_state["selected_model"] != selected_model
            or st.session_state["selected_table"] != selected_table
            or st.session_state["selected_schema"] != selected_schema
            or st.session_state["selected_database"] != selected_database
        ):
            st.session_state["selected_model"] = selected_model
            st.session_state["selected_database"] = selected_database
            st.session_state["selected_schema"] = selected_schema
            st.session_state["selected_table"] = selected_table
            st.session_state["modeled_table_desc"] = get_table_desc(
                st.session_state["selected_model"], st.session_state["selected_table"]
            )
            st.session_state["selected_table_name"] = (
                f"{st.session_state['selected_database']}.{st.session_state['selected_schema']}.{st.session_state['selected_table']}"
            )
            table = get_table(session, st.session_state["selected_table_name"])
            table_columns_df = get_table_df(session, table)
            st.session_state["modeled_column_desc"] = get_table_column_desc(
                table_columns_df, st.session_state["selected_model"]
            )
        # Get the table and its description
        edited_table_desc = st.text_area(
            ":blue[Table Description]", st.session_state["modeled_table_desc"]
        )
        if st.session_state["edited_table_desc"] != edited_table_desc:
            st.session_state["edited_table_desc"] = edited_table_desc
            st.session_state["modeled_table_desc"] = edited_table_desc
            # st.write(st.session_state['edited_table_desc'])
        # Get the dataframe of the table columns and their descriptions
        st.markdown(":blue[Column Description]")
        edited_column_desc = st.data_editor(st.session_state["modeled_column_desc"])
        # if st.session_state['edited_column_desc'] != edited_column_desc:
        st.session_state["edited_column_desc"] = edited_column_desc
        st.session_state["modeled_column_desc"] = edited_column_desc
        if st.button("Save Description"):
            st.session_state["edited_table_desc"] = st.session_state[
                "edited_table_desc"
            ].replace("'", "''")
            session.sql(
                f"""COMMENT ON TABLE {st.session_state['selected_table_name']} IS '{st.session_state['edited_table_desc']}'"""
            )
            progress_text = "Operation in progress. Please wait."
            percent_factor = 1 / st.session_state["edited_column_desc"].shape[0]
            percent_complete = 0
            my_bar = st.progress(percent_complete, text=progress_text)
            for _, row in st.session_state["edited_column_desc"].iterrows():
                percent_complete += percent_factor
                time.sleep(0.1)
                my_bar.progress(percent_complete, text=progress_text)
                description = row["DESCRIPTION"].replace("'", "''")
                # st.write(f"""COMMENT ON COLUMN {st.session_state['selected_table']}.{row['COLUMNS']} IS '{description}'""")
                # st.write(session.sql(f"""COMMENT ON COLUMN {st.session_state['selected_table']}.{row['COLUMNS']} IS '{description}'"""))
                session.sql(
                    f"""COMMENT ON COLUMN {st.session_state['selected_table_name']}.{row['COLUMNS']} IS '{description}'"""
                ).collect()
            my_bar.empty()
            st.success(
                "Successfully Updated column descriptions",
            )
            # st.write('Table Description Updated Successfully')
        if st.button("Check Stored data"):
            st.dataframe(
                session.sql(
                    f"""DESCRIBE TABLE {st.session_state['selected_table_name']}"""
                ).collect()
            )
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
