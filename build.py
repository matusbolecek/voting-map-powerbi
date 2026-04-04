import pandas as pd
from processing import National, Base

def build_nr():
    nr = National()

    df_02 = pd.read_excel('data/nrsr/nrsr2002.xls')
    df_02 = nr.preprocess_2002(df_02)
    nr.process_a(df_02, 2002)

    df_06 = pd.read_excel('data/nrsr/nrsr2006.xls')
    df_06 = National.preprocess_2006(df_06)
    nr.process_a(df_06, 2006)

    df_10 = pd.read_excel('data/nrsr/nrsr2010.xls')
    df_10 = National.preprocess_2010(df_10)
    nr.process_a(df_10, 2010)

    df_12 = pd.read_excel('data/nrsr/nrsr2012.xls')
    df_12 = National.preprocess_2010(df_12)
    nr.process_a(df_12, 2012)

    df_16 = pd.read_excel('data/nrsr/nrsr2016.xlsx')
    df_16 = National.preprocess_2016(df_16)
    nr.process_a(df_16, 2016)

    df_20 = pd.read_excel('data/nrsr/nrsr2020.xlsx')
    df_20 = National.preprocess_2020(df_20)
    nr.process_b(df_20, 2020)

    df_23 = pd.read_excel('data/nrsr/nrsr2023.xlsx')
    df_23 = National.preprocess_2020(df_23)
    nr.process_b(df_23, 2023)

    return nr.df

def build_ep():
    pass

def build_demo():
    pass

def main():
    pass