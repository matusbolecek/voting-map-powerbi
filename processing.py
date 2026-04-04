import pandas as pd
from pathlib import Path


class Base:
    def __init__(self):
        self.out_path = Path("out")
        self.df = pd.DataFrame(columns=self.COLUMNS).astype(self.DTYPES)

    def dump(self, name):
        path = self.out_path / name
        self.df.to_csv(path, index=False)

    def load(self, name):
        path = self.out_path / name
        self.df = pd.read_csv(path).astype(self.DTYPES)


class Election:
    COLUMNS = [
        "election_year",
        "election_type",
        "district_id",
        "district_name",
        "total_district_votes",
        "original_party_name",
        "party_name",
        "votes",
    ]

    DTYPES = {
        "election_year": "Int64",
        "election_type": "string",
        "district_id": "string",
        "district_name": "string",
        "total_district_votes": "Int64",
        "party_name": "string",
        "votes": "Int64",
    }


class DemoMeta:
    pass


class National(Base, Election):
    @staticmethod
    def set_header_and_clean(df):
        df.columns = df.iloc[0]
        df = df.iloc[1:].copy()
        df.columns = df.columns.str.replace(r"\s+", " ", regex=True).str.strip()

        return df

    @staticmethod
    def preprocess_2002(df):
        df = df.dropna(subset=["Unnamed: 0"])
        df = df.drop(index=2)

        return df

    @staticmethod
    def add_foreign(df):
        new_row = {col: 0 for col in df.columns}
        new_row["Okres"] = "Cudzina"
        new_row["platných hlasov spolu"] = 0

        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

        return df

    def process_2002(self, df):
        id_vars = ["Okres", "platných hlasov spolu"]
        party_cols = [col for col in df.columns if col not in id_vars and pd.notna(col)]

        melted_df = pd.melt(
            df,
            id_vars=id_vars,
            value_vars=party_cols,
            var_name="original_party_name",
            value_name="votes",
        )

        melted_df["election_year"] = 2002
        melted_df["election_type"] = "NR SR"
        melted_df["district_name"] = melted_df["Okres"].str.strip()

        melted_df["total_district_votes"] = pd.to_numeric(
            melted_df["platných hlasov spolu"], errors="coerce"
        )
        melted_df["votes"] = pd.to_numeric(melted_df["votes"], errors="coerce")

        melted_df["district_id"] = pd.NA  # not a thing in this df

        # the party_name will be changed with a method later
        melted_df["party_name"] = melted_df["original_party_name"]

        self.df = melted_df[self.COLUMNS].astype(self.DTYPES)


class Euro(Base, Election):
    pass


class Demography(Base, DemoMeta):
    pass
