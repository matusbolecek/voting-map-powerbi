import pandas as pd
from pathlib import Path

from mappings import DISTRICT_TO_LAU1


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

    @staticmethod
    def set_header(df):
        df.columns = df.iloc[0]
        df = df.iloc[1:].copy()
        return df

    @staticmethod
    def clean(df):
        df.columns = df.columns.str.replace(r"\s+", " ", regex=True).str.strip()
        return df

    @staticmethod
    def fill_disctrict_codes(df):
        df["Kód Okresu"] = df["Okres"].map(DISTRICT_TO_LAU1)
        return df

    def _append(self, new_df):
        self.df = pd.concat(
            [self.df, new_df[self.COLUMNS].astype(self.DTYPES)], ignore_index=True
        )


class Election:
    COLUMNS = [
        "election_year",
        "election_type",
        "district_id",
        "district_name",
        "original_party_name",
        "party_name",
        "votes",
    ]

    DTYPES = {
        "election_year": "Int64",
        "election_type": "string",
        "district_id": "string",
        "district_name": "string",
        "party_name": "string",
        "votes": "Int64",
    }

    def _melt(self, df, id_vars, party_cols, election_year, election_type):
        melted_df = pd.melt(
            df,
            id_vars=id_vars,
            value_vars=party_cols,
            var_name="original_party_name",
            value_name="votes",
        )

        melted_df["election_year"] = election_year
        melted_df["election_type"] = election_type
        melted_df["district_name"] = melted_df["Okres"].str.strip()

        # This changed later - renaming just for consistency
        melted_df["district_name"] = melted_df["district_name"].replace(
            "Zahraničie", "Cudzina"
        )

        melted_df["votes"] = pd.to_numeric(
            melted_df["votes"].astype(str).str.replace(" ", "", regex=False),
            errors="coerce",
        ).fillna(0)

        melted_df["district_id"] = (
            melted_df["Kód Okresu"].astype(str).str.replace(r"\.0$", "", regex=True)
        )

        melted_df["party_name"] = melted_df["original_party_name"]

        return melted_df

    def process_wide(self, df, year):
        id_vars = ["Okres", "Kód Okresu"]
        party_cols = [col for col in df.columns if col not in id_vars and pd.notna(col)]

        melted_df = self._melt(
            df,
            id_vars=id_vars,
            party_cols=party_cols,
            election_year=year,
            election_type=self.type,
        )

        self._append(melted_df)

    def process_long(self, df, year):
        melted_df = df[["Kód Okresu", "Okres"]].copy()
        melted_df["election_year"] = year
        melted_df["election_type"] = self.type
        melted_df["district_id"] = df["Kód Okresu"].astype(str)
        melted_df["district_name"] = (
            df["Okres"].str.strip().replace("Zahraničie", "Cudzina")
        )
        melted_df["original_party_name"] = df["party_name"]
        melted_df["party_name"] = df["party_name"]
        melted_df["votes"] = pd.to_numeric(df["votes"], errors="coerce").fillna(0)

        self._append(melted_df)

    @staticmethod
    def preprocess_2020(df):
        """Standard preprocessing for new format:
        Works for national and euro elections - the new long format
        Since 2019 in euro, and 2020 in national.
        """
        df = df.iloc[1:]
        df = National.set_header(df)
        df = National.clean(df)
        df = df.dropna(subset=["Názov okresu"])

        df = df[
            [
                "Kód okresu",
                "Názov okresu",
                "Názov politického subjektu",
                "Počet platných hlasov",
            ]
        ]

        df = df.rename(
            columns={
                "Kód okresu": "Kód Okresu",
                "Názov okresu": "Okres",
                "Názov politického subjektu": "party_name",
                "Počet platných hlasov": "votes",
            }
        )

        return df


class National(Base, Election):
    def __init__(self):
        super().__init__()
        self.type = "NR SR"

    @staticmethod
    def add_foreign(df):
        new_row = {col: 0 for col in df.columns}
        new_row["Okres"] = "Cudzina"

        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

        return df

    @staticmethod
    def preprocess_2002(df):
        df = df.dropna(subset=["Unnamed: 0"])
        df = df.drop(index=2)
        df = df[~df["Unnamed: 0"].astype(str).str.contains("Spolu za SR")]

        df = Base.set_header(df)
        df = Base.clean(df)

        df = National.add_foreign(df)
        df = Base.fill_disctrict_codes(df)

        df = df.drop(columns=["platných hlasov spolu"])

        return df

    @staticmethod
    def preprocess_2006(df):
        df = df.drop(index=0)
        df = National.set_header(df)
        df = National.clean(df)

        # 'platnych hlasov spolu - total'
        df = df.drop(columns=[df.columns[1]])

        df = df.rename(columns={df.columns[0]: "Okres"})

        df = df.dropna(subset=["Okres"])
        df = df[~df["Okres"].str.contains("kraj", na=False)]

        # obce...
        df = df.drop(index=[195, 196, 198])

        # spolu
        df = df[~df["Okres"].astype(str).str.contains("Spolu za SR")]

        df = National.add_foreign(df)
        df = Base.fill_disctrict_codes(df)

        return df

    # This one also works for 2012
    @staticmethod
    def preprocess_2010(df):
        df = Base.clean(df)
        df = df.loc[:, ~df.columns.str.contains("Počet")]

        df = df.rename(columns={df.columns[0]: "Kód Okresu", df.columns[1]: "Okres"})

        return df

    @staticmethod
    def preprocess_2016(df):
        df = df.iloc[1:]

        df = National.set_header(df)
        df = National.clean(df)

        # This just keeps total vote counts for party-district
        df = df.loc[:, df.columns.notna()]

        # Some empty lines (index=2,3)
        df = df.dropna(subset=["Okres"])

        df = df.loc[:, ~df.columns.str.contains("Počet")]

        df = df.rename(columns={df.columns[0]: "Kód Okresu"})

        return df


class Euro(Base, Election):
    def __init__(self):
        super().__init__()
        self.type = "EP"

    @staticmethod
    def preprocess_2004(df):
        df = df.iloc[1:]
        df = Base.set_header(df)
        df = Base.clean(df)

        cols = list(df.columns)
        cols[0] = "Okres"
        cols[1] = "Total"
        df.columns = cols

        df = df.dropna(subset=["Okres"])
        df = df.drop(df.columns[1], axis=1)

        df = Base.fill_disctrict_codes(df)

        return df

    @staticmethod
    def preprocess_2009(df):
        df = df.iloc[1:]
        df = Base.set_header(df)
        df = Base.clean(df)

        cols = list(df.columns)
        cols[0] = "Okres"
        cols[1] = "Total"
        df.columns = cols

        df = df.dropna(subset=["Okres"])
        df = df.drop(df.columns[1], axis=1)

        df = Base.fill_disctrict_codes(df)

        df = df.loc[:, df.columns.notna()]

        return df

    @staticmethod
    def preprocess_2014(df):
        df = Euro.preprocess_2009(df)

        df = df.drop(index=2)

        return df


class Demo(Base):
    COLUMNS = [
        "year",
        "district_id",
        "district_name",
        "statistic",
        "percentage",
    ]

    DTYPES = {
        "year": "Int64",
        "district_id": "string",
        "district_name": "string",
        "statistic": "string",
        "percentage": "Float64",
    }

    def __init__(self):
        self.df = None
        self.year = None
        self.id_vars = None

    @staticmethod
    def preprocess_2021(df):
        df = df.drop(columns=[col for col in df.columns if "(abs.)" in col])
        df = df.drop(columns="Total")
        return df

    def set_year(self, year):
        """Set year and id + vars
        This method allows for an external call that changes the year attrribute
        as well as the corresponding district code + name fields.
        The code should be the first entry in the id_vars and the name should be the second
        """
        self.year = year
        match year:
            case 2021:
                self.id_vars = ["Kód", "Territory unit"]

    def _melt(self, df, stat_cols):
        melted = df.melt(
            id_vars=self.id_vars,
            value_vars=stat_cols,
            var_name="statistic",
            value_name="percentage",
        )

        assert self.id_vars is not None
        melted = melted.rename(
            columns={
                self.id_vars[0]: "district_id",
                self.id_vars[1]: "district_name",
            }
        )

        melted["year"] = self.year
        melted = melted[self.COLUMNS]
        melted = melted.astype(
            {col: dtype for col, dtype in self.DTYPES.items() if col in melted.columns}
        )
        return melted

    def process(self, df, columns=None):
        assert self.year is not None
        assert self.id_vars is not None

        stat_cols = (
            columns
            if columns
            else [
                col for col in df.columns if col not in self.id_vars and pd.notna(col)
            ]
        )

        melted_df = self._melt(df, stat_cols)
        self._append(melted_df)
