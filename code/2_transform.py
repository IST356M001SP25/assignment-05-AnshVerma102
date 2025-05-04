import pandas as pd
import streamlit as st
from pathlib import Path
import pandaslib as pl

@st.cache_data
def load_and_transform(
    survey_path: str = "cache/survey.csv",
    states_path: str = "cache/states.csv",
    col_glob: str = "cache/col_*.csv"
) -> pd.DataFrame:
    # 1) Load survey & states
    survey = pd.read_csv(survey_path, parse_dates=["Timestamp"])
    states = pd.read_csv(states_path)

    # 2) Clean / derive columns on survey
    survey["_country"] = pl.clean_country_usa( survey["What country do you work in?"] )
    survey["_full_city"] = (
        survey["What city do you work in?"].str.strip()
        + ", "
        + survey["If you're in the U.S., what state do you work in?"].map(
            dict(zip(states["State"], states["Abbreviation"]))
        )
        + ", "
        + survey["_country"]
    )
    survey["year"] = survey["Timestamp"].dt.year

    # 3) Read & concat all cost‐of‐living files at once
    all_cols = pd.concat(
        (pd.read_csv(fp) for fp in Path().glob(col_glob)),
        ignore_index=True,
    )

    # 4) Merge survey ↔ states (to drop non-US) then ↔ cost-of-living
    df = (
        survey
        .merge(states, left_on="If you're in the U.S., what state do you work in?",
                         right_on="State", how="inner")
        .merge(all_cols, left_on=["year", "_full_city"], right_on=["year", "City"], how="inner")
    )

    # 5) Clean salaries & compute adjusted salary in one vectorized pass
    df["_annual_salary_cleaned"] = pl.clean_currency(df[
        "What is your annual salary? (You'll indicate the currency in a later question. "
        "If you are part-time or hourly, please enter an annualized equivalent -- what you "
        "would earn if you worked the job 40 hours a week, 52 weeks a year.)"
    ])
    df["_annual_salary_adjusted"] = df["_annual_salary_cleaned"] * (100.0 / df["Cost of Living Index"])

    return df

def main():
    combined = load_and_transform()
    combined.to_csv("cache/survey_dataset.csv", index=False)

    # 6) Generate two pivot tables
    by_age = combined.pivot_table(
        index="_full_city",
        columns="How old are you?",
        values="_annual_salary_adjusted",
        aggfunc="mean"
    )
    by_edu = combined.pivot_table(
        index="_full_city",
        columns="What is your highest level of education completed?",
        values="_annual_salary_adjusted",
        aggfunc="mean"
    )

    # 7) Save & display
    by_age.to_csv("cache/annual_salary_adjusted_by_location_and_age.csv")
    by_edu.to_csv("cache/annual_salary_adjusted_by_location_and_education.csv")

    st.write("### Avg. Adjusted Salary by Location & Education")
    st.dataframe(by_edu)

if __name__ == "__main__":
    main()

