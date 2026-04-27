
def export_to_zip(
    dict_df,
    file_name_suffix="default",
    is_snapshot=False,
    decimal='.',
    db_script_path="core/create_sql_database.py",
    readme_path="../assets/README.txt",
    license_path="../assets/LICENSE.txt",
    table_script_path="assets/sql_table_script.txt",
):

    import zipfile
    import io
    import pandas as pd

    sep = ";" if decimal == "," else ","

    outer_zip_path = f"data/airline_data_{file_name_suffix}.zip"

    # load static files
    with open(table_script_path, "r", encoding="utf-8") as f:
        sql_table_script = f.read()

    with open(db_script_path, "r", encoding="utf-8") as f:
        sql_db_script = f.read()

    db_script_call = f"""
    create_sql_database(
        dbc=db_config,
        decimal="{decimal}",
        customers_has_noise=customers_is_noisy,
        reset_database=is_rerun
    )
    """

    sql_db_script += db_script_call

    # build inner zip in memory

    inner_buffer = io.BytesIO()

    with zipfile.ZipFile(
        inner_buffer, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as inner_zf:

        inner_zf.writestr("create_sql_tables.txt", sql_table_script)

        for table_name, df in dict_df.items():

            csv_buffer = io.StringIO()

            df.to_csv(csv_buffer, sep=sep, decimal=decimal, index=False)

            inner_zf.writestr(f"tables/{table_name}.csv", csv_buffer.getvalue())

    inner_buffer.seek(0)

    # build outer zip

    with zipfile.ZipFile(outer_zip_path, mode="w", compression=zipfile.ZIP_STORED) as outer_zf:

        outer_zf.writestr("README.txt", readme_path)
        outer_zf.writestr("LICENSE.txt", license_path)

        outer_zf.writestr("sql_db_creator.py", sql_db_script)

        # include inner archive
        outer_zf.writestr("airline_data.zip", inner_buffer.getvalue())

        # snapshot excel (optional)
        if is_snapshot:

            excel_buffer = io.BytesIO()

            with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:

                for table_name, df in dict_df.items():
                    df.to_excel(writer, sheet_name=table_name[:31], index=False)

            excel_buffer.seek(0)

            outer_zf.writestr("airline_data_snapshot.xlsx", excel_buffer.getvalue())