from pathlib import Path
from typing import Literal
import pandas as pd
import json

from processing import National, Euro
from mappings import UNIVERSAL_PARTY_NAMES, PARTY_COLORS


class Dimension:
    def __init__(self):
        self.out_path = Path("out")
        self.out_path.mkdir(parents=True, exist_ok=True)

    def dump(self):
        export_path = self.out_path / f"{self.name}.csv"

        assert self.df is not None
        self.df.to_csv(export_path, index=False)


class ElectionBuilder(Dimension):
    def __init__(self):
        self.name = "DimResults"
        super().__init__()

        self.nr_df = None
        self.ep_df = None
        self.df = None

    def partyname_helper(self):
        """Returns all party names in all elections.
        Since parties changed their names, we want to utilize a LLM
        to group the same party together. This helper just returns a dict with
        all the names.
        """
        assert self.df is not None
        election_parties = (
            self.df.groupby(["election_year", "election_type"])["original_party_name"]
            .unique()
            .reset_index()
        )

        parties_dict_str = {
            f"{row['election_year']} - {row['election_type']}": row[
                "original_party_name"
            ].tolist()
            for _, row in election_parties.iterrows()
        }

        print(json.dumps(parties_dict_str, ensure_ascii=False, indent=2))

    def _build_nr(self):
        nr = National()

        df_02 = pd.read_excel("data/nrsr/nrsr2002.xls")
        df_02 = nr.preprocess_2002(df_02)
        nr.process_wide(df_02, 2002)

        df_06 = pd.read_excel("data/nrsr/nrsr2006.xls")
        df_06 = National.preprocess_2006(df_06)
        nr.process_wide(df_06, 2006)

        df_10 = pd.read_excel("data/nrsr/nrsr2010.xls")
        df_10 = National.preprocess_2010(df_10)
        nr.process_wide(df_10, 2010)

        df_12 = pd.read_excel("data/nrsr/nrsr2012.xls")
        df_12 = National.preprocess_2010(df_12)
        nr.process_wide(df_12, 2012)

        df_16 = pd.read_excel("data/nrsr/nrsr2016.xlsx")
        df_16 = National.preprocess_2016(df_16)
        nr.process_wide(df_16, 2016)

        df_20 = pd.read_excel("data/nrsr/nrsr2020.xlsx")
        df_20 = National.preprocess_2020(df_20)
        nr.process_long(df_20, 2020)

        df_23 = pd.read_excel("data/nrsr/nrsr2023.xlsx")
        df_23 = National.preprocess_2020(df_23)
        nr.process_long(df_23, 2023)

        self.nr_df = nr.df

    def _build_ep(self):
        ep = Euro()

        df_04 = pd.read_excel("data/ep/ep2004.xlsx")
        df_04 = Euro.preprocess_2004(df_04)
        ep.process_wide(df_04, 2004)

        df_09 = pd.read_excel("data/ep/ep2009.xls")
        df_09 = Euro.preprocess_2009(df_09)
        ep.process_wide(df_09, 2009)

        df_14 = pd.read_excel("data/ep/ep2014.xlsx")
        df_14 = Euro.preprocess_2014(df_14)
        ep.process_wide(df_14, 2014)

        df_19 = pd.read_excel("data/ep/ep2019.xlsx")
        df_19 = Euro.preprocess_2020(df_19)
        ep.process_long(df_19, 2019)

        df_24 = pd.read_excel("data/ep/ep2024.xlsx")
        df_24 = Euro.preprocess_2020(df_24)
        ep.process_long(df_24, 2024)

        self.ep_df = ep.df

    def _merge_and_map(self):
        assert self.nr_df is not None and self.ep_df is not None

        self.df = pd.concat([self.nr_df, self.ep_df], ignore_index=True)

        self.df["party_name"] = (
            self.df["original_party_name"]
            .map(UNIVERSAL_PARTY_NAMES)
            .fillna(self.df["original_party_name"])
        )

    def build_election(self):
        self._build_ep()
        self._build_nr()
        self._merge_and_map()


class DemographyBuilder(Dimension):
    def __init__(self):
        self.name = "DimDemography"
        self.in_path = Path("data") / "demo"
        self.src_year = None

        super().__init__()


class Coloring(Dimension):
    def __init__(self, election_df):
        self.name = "DimParties"
        self.election_df = election_df
        super().__init__()

    def process(self):
        df = self.election_df[["party_name"]].drop_duplicates().reset_index(drop=True)
        df["color"] = df["party_name"].apply(lambda x: PARTY_COLORS.get(x, "#CCCCCC"))

        self.df = df.copy()


class Districts(Dimension):
    def __init__(self, election_df):
        self.name = "DimDistricts"
        self.election_df = election_df
        super().__init__()

    def process(self):
        # Does not matter which one we pick - all data is the same
        df_old = self.election_df[
            (self.election_df["election_year"] == 2002)
            & (self.election_df["election_type"] == "NR SR")
        ]

        df = df_old[["district_id", "district_name"]]

        # Some sheets have this scheme, thus it is also added
        df["district_id_alt"] = "SK" + df["district_id"].astype(str)

        df["district_name_alt"] = "Okres " + df["district_name"].astype(str)

        # English fields
        df["district_name_en"] = "District of " + df["district_name"].astype(str)

        self.df = df


if __name__ == "__main__":
    build_main = ElectionBuilder()
    build_main.build_election()
    build_main.dump()

    color = Coloring(build_main.df)
    color.process()
    color.dump()

    district = Districts(build_main.df)
    district.process()
    district.dump()
