import streamlit as st
import pandas as pd

_SUP_MAP = str.maketrans("0123456789", "⁰¹²³⁴⁵⁶⁷⁸⁹")

def to_superscript(n: int) -> str:
    return str(n).translate(_SUP_MAP)


def rank_color(rank: int, total_teams: int = 30) -> str:
    third = total_teams // 3
    if rank <= third:
        return "green"
    elif rank <= 2 * third:
        return "orange"
    return "red"


@st.cache_data
def parse_matchup(m: str) -> tuple[str, str]:
    m = m.replace("vs.", "vs").replace(" @ ", "@").replace(" vs ", "vs").strip()
    if "@" in m:
        away, home = m.split("@")
    else:
        home, away = m.split("vs")
    return away.strip(), home.strip()


@st.cache_data
def add_home_away(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df[["away_team", "home_team"]] = df["matchup"].apply(parse_matchup).tolist()
    df["opp_team"] = df.apply(
        lambda r: r.away_team if r.home_team == r.team else r.home_team, axis=1
    )
    return df


@st.cache_data
def with_month(df: pd.DataFrame, date_col: str = "game_date") -> pd.DataFrame:
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df["month"] = df[date_col].dt.month
    months = {
        1: "JAN", 2: "FEB", 3: "MAR", 4: "APR", 5: "MAY", 6: "JUN",
        7: "JUL", 8: "AUG", 9: "SEP", 10: "OCT", 11: "NOV", 12: "DEC",
    }
    df["month_name"] = df["month"].map(months)
    df["month"] = pd.Categorical(df["month"], categories=list(months.keys()))
    return df