import pandas as pd


class Dumping:
    def dump(self, name):
        self.df.to_csv(name, index=False)


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


class National(Election, Dumping):
    def __init__(self):
        self.df = pd.DataFrame(columns=self.COLUMNS).astype(self.DTYPES)

