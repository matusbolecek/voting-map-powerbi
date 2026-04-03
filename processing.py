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
    pass


class Euro(Base, Election):
    pass


class Demography(Base, DemoMeta):
    pass
