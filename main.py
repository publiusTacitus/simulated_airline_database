
def generate_and_export(upload_assets=True, diagnostics=False):

    from core.generate_tables import create_tables
    from core.build_archives import export_to_zip
    from core.upload_release_assets import sync_release_assets

    configs = [

        {
            "label":"1y",
            "snapshot":False,
            "year":2025,
            "n_years":1
        },

        {
            "label":"2y",
            "snapshot":False,
            "year":2024,
            "n_years":2
        },

        {
            "label":"3y",
            "snapshot":False,
            "year":2023,
            "n_years":3
        },

        {
            "label":"snapshot",
            "snapshot":True,
            "year":2025,
            "n_years":1
        }

    ]

    created_files = []

    for cfg in configs:

        df_dict = create_tables(
            only_snapshot=cfg["snapshot"],
            start_year=cfg["year"],
            n_years=cfg["n_years"],
            month_snapshot=5,
            show_diagnostics=diagnostics
        )

        print(f"{cfg['label']} data created.")

        for decimal_sep in [".", ","]:

            suffix = (
                f"{cfg['label']}_"
                f"{'decimal_point' if decimal_sep=='.' else 'decimal_comma'}"
            )

            zip_path = export_to_zip(
                dict_df=df_dict,
                file_name_suffix=suffix,
                is_snapshot=cfg["snapshot"],
                decimal=decimal_sep
            )

            created_files.append(zip_path)

            print(f"{suffix} exported.")

    if upload_assets:
        sync_release_assets(file_paths=created_files)


if __name__ == "__main__":
    generate_and_export()