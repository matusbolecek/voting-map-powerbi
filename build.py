import pandas as pd
from processing import National, Base, Euro


class Builder:
    def build_nr(self):
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

        return nr.df

    def build_ep(self):
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

        return ep.df

    def build_demo(self):
        pass

    def main(self):
        pass
