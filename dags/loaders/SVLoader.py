import pandas as pd
from loaders.BaseLoader import BaseLoaderStrategy
import uuid

class SVLoader(BaseLoaderStrategy):
    """
    Subject Visits Loader Strategy.
    Loads data from SV domain into the visits table.
    """

    def load_data(self, df, postgres_conn) -> None:
        """
        Load SV data into the visits table.

        :param df: DataFrame containing SV data.
        :param postgres_conn: Postgres connection object.
        """
        print("Executing SVLoader strategy..")

        column_map = {
            "USUBJID": "subject_id",
            "STUDYID": "trial_id",
            "VISITNUM": "visit_number",
            "VISIT": "visit_name",
            "SVDTC": "visit_date",
            "SVSTDTC": "start_time",
            "SVENDTC": "end_time",
            "VISITDY": "visit_day",
        }

        available_cols = [col for col in column_map.keys() if col in df.columns]
        if not available_cols:
            raise ValueError("None of the required columns were found in the CSV file.")
        df_to_load = df[available_cols].rename(columns=column_map)

        # Convert date/time columns
        for col in ["visit_date", "start_time", "end_time"]:
            if col in df_to_load.columns:
                df_to_load[col] = pd.to_datetime(df_to_load[col], errors="coerce")
                df_to_load[col] = df_to_load[col].astype(object).where(pd.notna(df_to_load[col]), None)

        # Generate UUIDs for id column
        df_to_load["id"] = [str(uuid.uuid4()) for _ in range(len(df_to_load))]

        db_columns = ["id"] + [column_map[k] for k in available_cols]
        cols_str = ", ".join(f'"{col}"' for col in db_columns)
        values_str = ", ".join(["%s"] * len(db_columns))

        sql = f"""
            INSERT INTO visits ({cols_str})
            VALUES ({values_str})
            ON CONFLICT (id) DO NOTHING;
        """

        records_to_insert = list(df_to_load[db_columns].itertuples(index=False, name=None))
        cur = postgres_conn.cursor()
        cur.executemany(sql, records_to_insert)
        postgres_conn.commit()
        cur.close()
        print("SVLoader strategy executed successfully.")