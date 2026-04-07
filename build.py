from pathlib import Path
from typing import Literal
import pandas as pd
import json

from processing import National, Euro
from mappings import UNIVERSAL_PARTY_NAMES


class Builder:
    def __init__(self):
        self.nr_df = None
        self.ep_df = None
        self.demo_df = None

        self.out_path = Path("out")
        self.out_path.mkdir(parents=True, exist_ok=True)

        self.election_df = None

    def partyname_helper(self):
        """Returns all party names in all elections.
        Since parties changed their names, we want to utilize a LLM
        to group the same party together. This helper just returns a dict with
        all the names.
        """
        assert self.election_df is not None
        election_parties = (
            self.election_df.groupby(["election_year", "election_type"])[
                "original_party_name"
            ]
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

        self.election_df = pd.concat([self.nr_df, self.ep_df], ignore_index=True)

        self.election_df["party_name"] = (
            self.election_df["original_party_name"]
            .map(UNIVERSAL_PARTY_NAMES)
            .fillna(self.election_df["original_party_name"])
        )

    def dump(self, type: Literal["election", "demography"]):
        export_path = self.out_path / f"{type}.csv"

        if type == "election":
            assert self.election_df is not None
            self.election_df.to_csv(export_path, index=False)

        elif type == "demography":
            assert self.demo_df is not None
            self.demo_df.to_csv(export_path, index=False)

    def build_election(self):
        self._build_ep()
        self._build_nr()
        self._merge_and_map()

    def _build_demo(self):
        pass


if __name__ == "__main__":
    build = Builder()

    build.build_election()
    build.dump("election")
