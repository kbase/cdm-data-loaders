import pandas as pd


def _not_empty_mask(series: pd.Series) -> pd.Series:
    return series.notna() & (series.str.strip() != "")


def apply_prefix(df: pd.DataFrame, col: str, prefix: str) -> None:
    if col not in df.columns:
        return
    mask = _not_empty_mask(df[col])
    df.loc[mask, col] = prefix + df.loc[mask, col].str.strip()


def apply_split_prefix(df: pd.DataFrame, col: str, delim: str, prefix: str) -> None:
    if col not in df.columns:
        return
    mask = _not_empty_mask(df[col])

    def split_and_prefix(val: str):
        return [f"{prefix}{v.strip()}" for v in val.split(delim) if v.strip()]

    df.loc[mask, col] = df.loc[mask, col].apply(split_and_prefix)


def apply_date_parse(df: pd.DataFrame, col: str, keep_time: bool = False) -> None:
    if col not in df.columns:
        return
    mask = _not_empty_mask(df[col])
    df[col] = df[col].astype(object)
    parsed = pd.to_datetime(df.loc[mask, col], errors="coerce")
    df.loc[mask, col] = parsed if keep_time else parsed.dt.date
    df.loc[~mask, col] = None
