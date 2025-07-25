import pandas as pd
from loaders.BaseLoader import BaseLoaderStrategy


class DMLoader(BaseLoaderStrategy):
    """
    Domain Model Loader Strategy.
    This class implements the loading strategy for the domain model.
    """

    def load_data(self, df, postgres_conn) -> None:
        """
        Load data into the domain model.

        :param df: DataFrame containing the data to be loaded.
        :param postgres_conn: Postgres connection object.
        """

        print("Executing DMLoader strategy..")

        column_map = {
            "STUDYID": "studyid",
            "DOMAIN": "domain",
            "USUBJID": "usubjid",
            "SUBJID": "subjid",
            "RFSTDTC": "rfstdtc",
            "RFENDTC": "rfendtc",
            "RFXSTDTC": "rfxstdtc",
            "RFXENDTC": "rfxendtc",
            "RFICDTC": "rficdtc",
            "RFPENDTC": "rfpendtc",
            "DTHDTC": "dthdtc",
            "DTHFL": "dthfl",
            "SITEID": "siteid",
            "AGE": "age",
            "AGEU": "ageu",
            "SEX": "sex",
            "RACE": "race",
            "ETHNIC": "ethnic",
            "ARMCD": "armcd",
            "ARM": "arm",
            "ACTARMCD": "actarmcd",
            "ACTARM": "actarm",
            "COUNTRY": "country",
            "DMDTC": "dmdtc",
            "DMDY": "dmdy",
        }

        available_cols = [col for col in column_map.keys() if col in df.columns]
        if not available_cols:
            raise ValueError("None of the required columns were found in the CSV file.")
        df_to_load = df[available_cols].rename(columns=column_map)

        date_columns = [
            "rfstdtc",
            "rfendtc",
            "rfxstdtc",
            "rfxendtc",
            "rficdtc",
            "rfpendtc",
            "dthdtc",
            "dmdtc",
        ]

        for col in date_columns:
            df_to_load[col] = pd.to_datetime(df_to_load[col], errors="coerce")
            df_to_load[col] = (
                df_to_load[col].astype(object).where(pd.notna(df_to_load[col]), None)
            )

        db_columns = df_to_load.columns.tolist()
        cols_str = ", ".join(f'"{col}"' for col in db_columns)
        values_str = ", ".join(["%s"] * len(db_columns))

        print(f"cols_str: {cols_str}")
        print(f"values_str: {values_str}")

        update_str = ", ".join(
            f'"{col}" = EXCLUDED."{col}"' for col in db_columns if col != "usubjid"
        )
        sql = f"""
                    INSERT INTO dm ({cols_str})
                    VALUES ({values_str})
                    ON CONFLICT (usubjid) DO UPDATE SET
                        {update_str},
                        updated_at = NOW();
                """

        records_to_insert = list(df_to_load.itertuples(index=False, name=None))
        cur = postgres_conn.cursor()
        cur.executemany(sql, records_to_insert)
        postgres_conn.commit()
        cur.close()
        print("DMLoader strategy executed successfully.")
