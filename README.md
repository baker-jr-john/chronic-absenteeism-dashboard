# Chronic Absenteeism and ELA Proficiency in New York State

An interactive Quarto dashboard examining the relationship between chronic absenteeism and Grade 3–8 ELA proficiency across New York State public school districts, grouped by NYSED's Need-to-Resource Capacity (N/RC) peer categories.

**[View the live dashboard →](https://baker-jr-john.github.io/chronic-absenteeism-dashboard/)**

---

## What it shows

- **The correlation** between chronic absenteeism and ELA proficiency, plotted per peer group with per-group OLS trendlines
- **Trends over time** (2018–2024, minus the cancelled 2020 assessment year) for both statewide rates and broken out by N/RC peer group
- **A district explorer** covering all ~700 NYS public school districts, with K–12 enrollment, absenteeism rate, and ELA proficiency for the latest reporting year
- **Methodology** documenting data sources, proficiency formula choices, the K–2 literacy gap, suppression rules, and the N/RC peer-grouping framework

Charts adapt to light and dark mode and use a colorblind-safe (Okabe-Ito) palette throughout.

---

## Data sources

All data is from the New York State Education Department public downloads at [data.nysed.gov/downloads.php](https://data.nysed.gov/downloads.php):

- **SRC (ESSA School Report Card)** — `SRC2019.zip`, `SRC2022.zip`, `SRC2024.zip`
- **Enrollment Database** — `enrollment_2019.zip`, `enrollment_2022.zip`, `enrollment_2024.zip`

Raw extracts are not committed (they are large Microsoft Access databases). The cleaned, deduplicated parquet files under `data/processed/` are committed and are the inputs the dashboard reads at render time.

---

## Local setup

**Requirements:** Python 3.12.6 (via pyenv), [Quarto](https://quarto.org/docs/download/), and `mdbtools` (for raw extraction only).

### Install Python dependencies

```bash
/Users/john/.pyenv/versions/3.12.6/bin/pip install -r requirements.txt
```

### Render the dashboard

The processed parquets are already in the repo, so you can render immediately without re-running the data pipeline:

```bash
quarto render index.qmd
```

Output is `index.html` plus `index_files/`.

### Re-run the data pipeline (optional)

Only needed if you add a new SRC release. See the "Adding a new SRC release" steps in `CLAUDE.md` (local only, not committed) or follow this summary:

1. Download and unzip the SRC `.mdb` and matching Enrollment `.mdb`
2. `bash scripts/extract_tables.sh <SRC.mdb> data/raw/src{YYYY}/`
3. `bash scripts/extract_enrollment.sh <ENROLL.mdb> data/raw/enrollment{YYYY}/`
4. `/Users/john/.pyenv/versions/3.12.6/bin/python scripts/clean_data.py`
5. `quarto render index.qmd`

---

## Stack

| Layer | Tool |
|---|---|
| Dashboard framework | [Quarto](https://quarto.org/) dashboard format |
| Charts | [Plotly](https://plotly.com/python/) |
| Table | [itables](https://mwouts.github.io/itables/) |
| Data pipeline | pandas + pyarrow |
| Themes | Bootstrap (Cosmo / Darkly) + custom SCSS |
| Hosting | GitHub Pages |

---

## Methodology notes

Proficiency rates in this dashboard use `NUM_PROF / TOTAL_COUNT` rather than NYSED's published `PER_PROF`. This is the conservative opt-out-adjusted formula: non-testers are counted in the denominator and treated as non-proficient. Rates here will not match data.nysed.gov. See the Methodology page in the dashboard for full details.
