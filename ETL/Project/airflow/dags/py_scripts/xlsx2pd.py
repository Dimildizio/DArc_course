import pandas as pd


def get_df(filepath: str, sheet_name: str, indexcol=None) -> pd.DataFrame:
    if not indexcol:
        return pd.read_excel(filepath, sheet_name=sheet_name)
    return pd.read_excel(filepath, sheet_name=sheet_name, index_col=indexcol)
