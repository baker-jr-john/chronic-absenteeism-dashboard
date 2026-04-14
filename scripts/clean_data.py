"""
Clean NYSED SRC CSVs into tidy parquet files for the dashboard.

Inputs (all gitignored):

    data/raw/src{YYYY}/
        annual_em_ela.csv
        acc_em_chronic_absenteeism.csv
        boces_and_n_rc.csv
        annual_regents_exams.csv
        acc_hs_chronic_absenteeism.csv
    data/raw/enrollment{YYYY}/
        beds_day_enrollment.csv

SRC release subdirs are sorted lexicographically so src2019 < src2022 < src2024
and drop_duplicates(keep="last") resolves overlapping rows to the latest release.
Enrollment releases overlap on year 2022 — same dedup strategy applies.

Outputs:
    data/processed/district_year.parquet   — district x year x subgroup panel
    data/processed/statewide_year.parquet  — statewide totals by year
    data/processed/subgroup_year.parquet   — subgroup x year (statewide)
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

RAW = Path(__file__).resolve().parents[1] / "data" / "raw"
PROCESSED = Path(__file__).resolve().parents[1] / "data" / "processed"
PROCESSED.mkdir(parents=True, exist_ok=True)

SRC_DIRS = sorted(p for p in RAW.glob("src*") if p.is_dir())
ENROLLMENT_DIRS = sorted(p for p in RAW.glob("enrollment*") if p.is_dir())

STATEWIDE_ENTITY_CD = "111111111111"

NRC_DISPLAY = {
    "1": "New York City",
    "2": "Large City Districts",
    "3": "High Need Urban-Suburban",
    "4": "High Need Rural",
    "5": "Average Need",
    "6": "Low Need",
    "7": "Charter Schools",
}

REGENTS_ELA_SUBJECTS = {
    "REG_COMENG",
    "Regents Common Core English Language Art",
    "Regents Comprehensive English",
}


def read_across_dirs(dirs: list[Path], filename: str) -> pd.DataFrame:
    """Concatenate the same CSV across a set of release subdirs. Later
    releases appear later so drop_duplicates(keep='last') resolves overlap."""
    frames = []
    for d in dirs:
        path = d / filename
        if path.exists():
            frames.append(pd.read_csv(path, dtype=str, low_memory=False))
    return pd.concat(frames, ignore_index=True)


def to_numeric(series: pd.Series) -> pd.Series:
    """NYSED stores numeric fields as text(255) with 's' for suppressed values.
    The ELA table also emits YEAR values like '2023.' with a trailing period."""
    return pd.to_numeric(
        series.astype(str).str.strip().str.rstrip("."),
        errors="coerce",
    )


def is_district_row(entity_cd: pd.Series) -> pd.Series:
    return entity_cd.astype(str).str.endswith("0000")


def load_nrc_lookup() -> pd.DataFrame:
    df = read_across_dirs(SRC_DIRS, "boces_and_n_rc.csv")
    df = df.rename(columns=str.lower)
    df["year"] = to_numeric(df["year"]).astype("Int64")
    df["nrc_category"] = df["needs_index"].map(NRC_DISPLAY).fillna("Unknown")
    df["entity_cd"] = df["district_cd"].str.strip() + "0000"
    latest = df.sort_values("year").drop_duplicates("entity_cd", keep="last")
    return latest[["entity_cd", "district_name", "county_name", "nrc_category"]]


def _load_absenteeism(filename: str, prefix: str) -> pd.DataFrame:
    df = read_across_dirs(SRC_DIRS, filename)
    df = df.rename(columns=str.lower)
    df["year"] = to_numeric(df["year"]).astype("Int64")
    df = df.rename(columns={"subgroup_name": "subgroup"})
    df = df.drop_duplicates(
        subset=["entity_cd", "year", "subgroup"], keep="last"
    )
    out = pd.DataFrame({
        "entity_cd": df["entity_cd"],
        "entity_name": df["entity_name"],
        "year": df["year"],
        "subgroup": df["subgroup"],
        f"{prefix}_enrollment": to_numeric(df["enrollment"]),
        f"{prefix}_absent_count": to_numeric(df["absent_count"]),
        f"{prefix}_absent_rate": to_numeric(df["absent_rate"]),
    })
    return out


def load_em_absenteeism() -> pd.DataFrame:
    return _load_absenteeism("acc_em_chronic_absenteeism.csv", "em")


def load_hs_absenteeism() -> pd.DataFrame:
    return _load_absenteeism("acc_hs_chronic_absenteeism.csv", "hs")


def load_em_ela() -> pd.DataFrame:
    """EM 3–8 ELA proficiency, option B: NUM_PROF / TOTAL_COUNT.

    TOTAL_COUNT is the enrollment base (tested + not-tested), which treats
    opt-outs as non-proficient. Participation rate is also emitted for the
    Methodology page's transparency note.
    """
    df = read_across_dirs(SRC_DIRS, "annual_em_ela.csv")
    df = df.rename(columns=str.lower)
    df["year"] = to_numeric(df["year"]).astype("Int64")
    df = df[df["assessment_name"] == "ELA3_8"]
    df = df.rename(columns={"subgroup_name": "subgroup"})
    df = df.drop_duplicates(
        subset=["entity_cd", "year", "subgroup"], keep="last"
    )
    num_tested = to_numeric(df["num_tested"])
    num_prof = to_numeric(df["num_prof"])
    not_tested = to_numeric(df["not_tested"]) if "not_tested" in df.columns else pd.Series(pd.NA, index=df.index)
    if "total_count" in df.columns:
        total_count = to_numeric(df["total_count"])
        total_count = total_count.fillna(num_tested + not_tested)
    else:
        total_count = num_tested + not_tested
    out = pd.DataFrame({
        "entity_cd": df["entity_cd"],
        "entity_name": df["entity_name"],
        "year": df["year"],
        "subgroup": df["subgroup"],
        "em_ela_total_count": total_count,
        "em_ela_num_tested": num_tested,
        "em_ela_num_prof": num_prof,
        "em_ela_prof_rate": (num_prof / total_count) * 100,
        "em_ela_participation": (num_tested / total_count) * 100,
    })
    return out


def load_regents_ela() -> pd.DataFrame:
    """Regents ELA proficiency (HS).

    The Regents table has no TOTAL_COUNT column — only TESTED — so option B
    cannot be computed. We fall back to NUM_PROF / TESTED (i.e. PER_PROF).
    This asymmetry is documented on the Methodology page.

    SUBJECT labels drift across releases:
      - src2019 uses the short code "REG_COMENG"
      - src2022/2024 use "Regents Common Core English Language Art"
      - pre-Common-Core "Regents Comprehensive English" is accepted if present
    """
    df = read_across_dirs(SRC_DIRS, "annual_regents_exams.csv")
    df = df.rename(columns=str.lower)
    df["year"] = to_numeric(df["year"]).astype("Int64")
    df = df[df["subject"].isin(REGENTS_ELA_SUBJECTS)]
    df = df.rename(columns={"subgroup_name": "subgroup"})
    df = df.drop_duplicates(
        subset=["entity_cd", "year", "subgroup"], keep="last"
    )
    tested = to_numeric(df["tested"])
    num_prof = to_numeric(df["num_prof"])
    out = pd.DataFrame({
        "entity_cd": df["entity_cd"],
        "entity_name": df["entity_name"],
        "year": df["year"],
        "subgroup": df["subgroup"],
        "hs_ela_tested": tested,
        "hs_ela_num_prof": num_prof,
        "hs_ela_prof_rate": (num_prof / tested) * 100,
    })
    return out


def load_enrollment() -> pd.DataFrame:
    """True K–12 enrollment from the separate NYSED Enrollment Database.

    Keyed by (entity_cd, year). Covers every year in the SRC panel because
    enrollment releases overlap the SRC release years.
    """
    df = read_across_dirs(ENROLLMENT_DIRS, "beds_day_enrollment.csv")
    df.columns = [c.lower() for c in df.columns]
    df["year"] = to_numeric(df["year"]).astype("Int64")
    df["k12_enrollment"] = to_numeric(df["k12"])
    df = df.drop_duplicates(subset=["entity_cd", "year"], keep="last")
    return df[["entity_cd", "year", "k12_enrollment"]]


def _combine_absenteeism(em: pd.Series, hs: pd.Series) -> pd.Series:
    """Sum EM + HS counts. If only one side is present, use it; if both are
    missing the result is NaN. Count-based math means we can feed the sums
    into a single rate calculation without rate-of-rates bias."""
    out = em.fillna(0) + hs.fillna(0)
    return out.mask(em.isna() & hs.isna())


def build_district_panel() -> pd.DataFrame:
    em_ab = load_em_absenteeism()
    hs_ab = load_hs_absenteeism()
    em_ela = load_em_ela()
    rg_ela = load_regents_ela()
    enroll = load_enrollment()
    nrc = load_nrc_lookup()

    keys = ["entity_cd", "year", "subgroup"]

    ab = em_ab.merge(hs_ab, on=keys, how="outer", suffixes=("_em", "_hs"))
    ab["entity_name"] = ab["entity_name_em"].fillna(ab["entity_name_hs"])
    ab = ab.drop(columns=["entity_name_em", "entity_name_hs"])

    combined_absent_count = _combine_absenteeism(
        ab["em_absent_count"], ab["hs_absent_count"]
    )
    combined_enrollment = _combine_absenteeism(
        ab["em_enrollment"], ab["hs_enrollment"]
    )
    ab["combined_absent_count"] = combined_absent_count
    ab["combined_enrollment"] = combined_enrollment
    ab["combined_absent_rate"] = (
        combined_absent_count / combined_enrollment * 100
    )
    ab["absent_rate"] = ab["combined_absent_rate"]

    ela = em_ela.merge(rg_ela, on=keys, how="outer", suffixes=("_em", "_rg"))
    ela["entity_name"] = ela["entity_name_em"].fillna(ela["entity_name_rg"])
    ela = ela.drop(columns=["entity_name_em", "entity_name_rg"])
    ela["ela_prof_rate"] = ela["em_ela_prof_rate"]

    panel = ab.merge(ela, on=keys, how="outer", suffixes=("_ab", "_ela"))
    panel["entity_name"] = panel["entity_name_ab"].fillna(panel["entity_name_ela"])
    panel = panel.drop(columns=["entity_name_ab", "entity_name_ela"])

    panel = panel.merge(enroll, on=["entity_cd", "year"], how="left")

    candidate = panel[is_district_row(panel["entity_cd"])].copy()
    districts = candidate.merge(nrc, on="entity_cd", how="inner")
    districts["district_name"] = districts["district_name"].fillna(districts["entity_name"])

    return districts


def build_statewide_series() -> pd.DataFrame:
    em_ab = load_em_absenteeism()
    hs_ab = load_hs_absenteeism()
    em_ela = load_em_ela()
    rg_ela = load_regents_ela()
    enroll = load_enrollment()

    def state(df: pd.DataFrame) -> pd.DataFrame:
        return df[
            (df["entity_cd"] == STATEWIDE_ENTITY_CD)
            & (df["subgroup"] == "All Students")
        ]

    em_ab_s = state(em_ab)
    hs_ab_s = state(hs_ab)
    em_ela_s = state(em_ela)
    rg_ela_s = state(rg_ela)

    ab = em_ab_s.merge(hs_ab_s, on=["entity_cd", "year", "subgroup"], how="outer")
    combined_count = _combine_absenteeism(ab["em_absent_count"], ab["hs_absent_count"])
    combined_enr = _combine_absenteeism(ab["em_enrollment"], ab["hs_enrollment"])
    ab["combined_absent_rate"] = combined_count / combined_enr * 100
    ab["absent_rate"] = ab["combined_absent_rate"]

    ela = em_ela_s.merge(rg_ela_s, on=["entity_cd", "year", "subgroup"], how="outer")
    ela["ela_prof_rate"] = ela["em_ela_prof_rate"]

    out = ab.merge(ela, on=["entity_cd", "year", "subgroup"], how="outer")
    enroll_s = enroll[enroll["entity_cd"] == STATEWIDE_ENTITY_CD]
    out = out.merge(enroll_s, on=["entity_cd", "year"], how="left")

    return out[[
        "year",
        "absent_rate", "combined_absent_rate",
        "em_absent_rate", "hs_absent_rate",
        "ela_prof_rate", "em_ela_prof_rate", "hs_ela_prof_rate",
        "em_ela_participation",
        "k12_enrollment",
    ]].sort_values("year").reset_index(drop=True)


def build_subgroup_series() -> pd.DataFrame:
    em_ab = load_em_absenteeism()
    hs_ab = load_hs_absenteeism()
    em_ela = load_em_ela()
    rg_ela = load_regents_ela()

    def state(df: pd.DataFrame) -> pd.DataFrame:
        return df[df["entity_cd"] == STATEWIDE_ENTITY_CD]

    ab = state(em_ab).merge(
        state(hs_ab), on=["entity_cd", "year", "subgroup"], how="outer"
    )
    combined_count = _combine_absenteeism(ab["em_absent_count"], ab["hs_absent_count"])
    combined_enr = _combine_absenteeism(ab["em_enrollment"], ab["hs_enrollment"])
    ab["absent_rate"] = combined_count / combined_enr * 100

    ela = state(em_ela).merge(
        state(rg_ela), on=["entity_cd", "year", "subgroup"], how="outer"
    )
    ela["ela_prof_rate"] = ela["em_ela_prof_rate"]

    out = ab.merge(ela, on=["entity_cd", "year", "subgroup"], how="outer")
    return out[[
        "year", "subgroup",
        "absent_rate", "em_absent_rate", "hs_absent_rate",
        "ela_prof_rate", "em_ela_prof_rate", "hs_ela_prof_rate",
    ]]


def main() -> None:
    panel = build_district_panel()
    statewide = build_statewide_series()
    subgroup = build_subgroup_series()

    panel.to_parquet(PROCESSED / "district_year.parquet", index=False)
    statewide.to_parquet(PROCESSED / "statewide_year.parquet", index=False)
    subgroup.to_parquet(PROCESSED / "subgroup_year.parquet", index=False)

    print(f"Wrote {len(panel):,} district-year-subgroup rows")
    print(f"Wrote {len(statewide):,} statewide rows")
    print(f"Wrote {len(subgroup):,} statewide subgroup rows")


if __name__ == "__main__":
    main()
