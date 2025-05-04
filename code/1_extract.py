import pandas as pd
import numpy as np
import streamlit as st
import pandaslib as pl
  
#Data Extraction

import pandas as pd
import numpy as np
import streamlit as st
from concurrent.futures import ThreadPoolExecutor

# Cache the sheet reads so rerunning the script (or rerendering the app) won’t re-download
@st.cache_data
def load_survey(url: str) -> pd.DataFrame:
    df = pd.read_csv(url)
    # vectorized datetime → year extraction
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
    df['year'] = df['Timestamp'].dt.year
    return df

@st.cache_data
def load_states(url: str) -> pd.DataFrame:
    return pd.read_csv(url)

def fetch_col_year(year: int) -> pd.DataFrame:
    """Grab the Numbeo table for a single year and tag it."""
    tables = pd.read_html(
        f"https://www.numbeo.com/cost-of-living/rankings.jsp?title={year}&displayColumn=0",
        flavor='bs4'
    )
    # the data you want is always in tables[1]
    col = tables[1]
    col['year'] = year
    return col

def main():
    survey_url = (
        "https://docs.google.com/spreadsheets/d/1IPS5dBSGtwYVbjsfbaMCYlWnOuRmJcbequohNxCyGVw"
        "/export?resourcekey=&gid=1625408792&format=csv"
    )
    states_url = (
        "https://docs.google.com/spreadsheets/d/14wvnQygIX1eCVo7H5B7a96W1v5VCg6Q9yeRoESF6epw"
        "/export?format=csv"
    )

    survey = load_survey(survey_url)
    state_table = load_states(states_url)

    # write out  cleaned survey & states once
    survey.to_csv('cache/survey.csv', index=False)
    state_table.to_csv('cache/states.csv', index=False)

    # pull each year's cost-of-living table in parallel
    years = survey['year'].dropna().unique().astype(int)
    with ThreadPoolExecutor(max_workers=5) as pool:
        col_dfs = list(pool.map(fetch_col_year, years))

    # stitch them all into one DataFrame
    all_col = pd.concat(col_dfs, ignore_index=True)
    all_col.to_csv('cache/cost_of_living_by_year.csv', index=False)

    st.write(" Data extraction complete:")
    st.write(f"• survey ({len(survey)} rows), years {years.min()}–{years.max()}")
    st.write(f"• states ({len(state_table)} rows)")
    st.write(f"• cost-of-living ({len(all_col)} rows)")

if __name__ == "__main__":
    main()
