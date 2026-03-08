**Credit Contagion & Corporate Default Risk:**

A Data Integration and Curation Project

IS477 – Data Management, Curation & Reproducibility

University of Illinois Urbana-Champaign – Spring 2026

**Gabriel Reisen & Christina Jordan**

March 8, 2026

# **Overview**

This project builds a curated, reproducible data pipeline that integrates corporate financial fundamentals with macroeconomic indicators and bankruptcy records to study how financial distress propagates across interconnected firms. Corporate credit risk is traditionally modeled at the firm level, but the 2008 financial crisis and the COVID-19 supply chain disruptions demonstrated that firm interconnections amplify idiosyncratic shocks into systemic events. Understanding these contagion dynamics requires linking heterogeneous datasets that are rarely combined in a single reproducible workflow.

We will acquire, clean, integrate, and document at least two complementary datasets from institutional sources, Compustat corporate fundamentals via WRDS and the FRED macroeconomic database via the St. Louis Fed, and enrich them with bankruptcy event labels from the LoPucki Bankruptcy Research Database. The integrated panel dataset will span 1990–2024 and link firms to macroeconomic conditions at the time of financial distress events. Our end-to-end workflow will be fully automated via a Snakemake pipeline, with all code, metadata, and documentation published on GitHub for reproducibility.

# **Team**

| Member | Role | Responsibilities |
| :---- | :---- | :---- |
| **Gabriel Reisen** | Data Engineering & Integration Lead | FRED & WRDS (Compustat) data acquisition, dataset integration scripts, network feature construction, Snakemake workflow, reproducibility packaging |
| **Christina Jordan** | Data Curation & Documentation Lead | LoPucki & WRDS (CRSP) data acquisition, data quality profiling and cleaning, metadata/data dictionary creation, ethical/legal compliance documentation, final report writing |

# **Research Questions**

Our project addresses the following questions through data integration and exploratory analysis:

* How do firm-level financial fundamentals (leverage, profitability, liquidity) relate to corporate bankruptcy outcomes when combined with macroeconomic conditions at the time of distress?

* Do firms that enter bankruptcy during economic downturns (recessions, high credit spreads, elevated VIX) exhibit systematically different financial profiles than those that default during expansions?

* Can an integrated panel linking Compustat fundamentals, FRED macro indicators, and LoPucki bankruptcy events reveal patterns consistent with credit contagion, where macroeconomic stress amplifies firm-level vulnerability beyond what fundamentals alone predict?

# **Datasets**

We will use three complementary datasets from distinct institutional sources, each accessed through a different method and contributing different dimensions of information. All three share common temporal attributes (year/quarter) and can be linked through firm identifiers or time-based joins.

## **Dataset 1: Compustat Corporate Fundamentals (WRDS)**

| Attribute | Detail |
| :---- | :---- |
| **Source** | Standard & Poor’s Compustat via WRDS (Wharton Research Data Services) |
| **Access Method** | SQL query via WRDS PostgreSQL server (wrds-cloud.wharton.upenn.edu) |
| **Coverage** | 1950–2026; project scope filtered to 1990–2024 |
| **Format** | Tabular (CSV export from SQL); annual frequency |
| **Key Variables** | GVKEY (firm ID), fiscal year, total assets, total debt, revenue, net income, current assets/liabilities, SIC industry code, state of incorporation |
| **Integration Key** | GVKEY \+ fiscal year (links to LoPucki); calendar year (links to FRED) |
| **License** | WRDS academic license via UIUC; data cannot be redistributed |

## **Dataset 2: FRED Macroeconomic Indicators (St. Louis Fed)**

| Attribute | Detail |
| :---- | :---- |
| **Source** | Federal Reserve Economic Data (FRED), Federal Reserve Bank of St. Louis |
| **Access Method** | FRED API (RESTful JSON) via fredapi Python package |
| **Coverage** | 1980–2024; 15 series across five categories |
| **Format** | Time series (JSON → CSV); mixed frequencies (daily/monthly/quarterly) aligned to quarterly |
| **Key Variables** | Fed Funds Rate, 10Y Treasury, BAA-AAA credit spread, ICE BofA High Yield spread, VIX, GDP growth, unemployment rate, industrial production, TED spread, M2 money supply |
| **Integration Key** | Calendar year-quarter (joined to Compustat fiscal year-quarter) |
| **License** | Public domain; free for academic and commercial use with attribution |

## **Dataset 3: LoPucki Bankruptcy Research Database**

| Attribute | Detail |
| :---- | :---- |
| **Source** | LoPucki Bankruptcy Research Database (UCLA Law / academic distribution) |
| **Access Method** | Direct download (Excel file from academic website) |
| **Coverage** | 1979–present; major U.S. public company Chapter 11 bankruptcies (assets \> $100M in 1980 dollars) |
| **Format** | Tabular (XLS → CSV); event-level records |
| **Key Variables** | Company name, filing date, assets at filing, industry, bankruptcy outcome (reorganized/liquidated/acquired), case duration |
| **Integration Key** | Company name \+ filing year matched to Compustat GVKEY via fuzzy matching and manual validation |
| **License** | Academic use permitted with attribution |

These three datasets differ in access method (SQL query, REST API, direct download), format (relational tabular, time series JSON, spreadsheet (csv)), and granularity (firm-year, date-series, event-level), satisfying the IS477 requirement for heterogeneous data sources that must be integrated.

# **Timeline**

| Week | Dates | Task | Owner | Requirement |
| :---- | :---- | :---- | :---- | :---- |
| **1–2** | Mar 2–10 | Acquire Compustat data via WRDS SQL; download FRED series via API; download LoPucki BRD; document acquisition steps | Gabriel (WRDS), Christina (FRED, LoPucki) | Data collection |
| **3** | Mar 10–28 | Profile all datasets: null counts, distributions, outliers, type checks; document quality issues; run initial cleaning (missing values, duplicates, type coercion) | Christina, Gabriel  | Data quality & cleaning |
| **4** | Mar 29–Apr 4 | Integrate datasets: match LoPucki to Compustat; align FRED quarterly series to Compustat fiscal years; construct derived features (leverage, Altman Z-score, distress indicators); document integration logic | Gabriel, Christina  | Data integration |
| **5** | Apr 5–11 | Build Snakemake workflow automating acquisition, cleaning, integration, and analysis; create requirements.txt; test end-to-end reproducibility | Gabriel, Christina  | Workflow & reproducibility |
| **6** | Apr 12–18 | Submit interim status report (StatusReport.md); address any feedback from project plan grading | Both | Milestone 3 |
| **7–8** | Apr 19–May 2 | Exploratory analysis and visualization: bankruptcy rates by macro regime, firm characteristics pre-default vs. surviving firms, time-series trends; generate figures | Both | Analysis & visualization |
| **9–10** | May 3–16 | Write final README.md report; create data dictionary and DCAT metadata; add LICENSE files; package Box links for large data; final reproducibility check; tag and release | Christina (report, metadata), Gabriel (packaging, release) | Final submission |

# **Constraints**

* WRDS data redistribution: Compustat data is licensed for academic use only through UIUC’s WRDS subscription and cannot be included directly in the GitHub repository. We will store raw data on Illinois Box with a shareable link and provide acquisition scripts so that anyone with WRDS access can reproduce the download.

* LoPucki-Compustat matching: The LoPucki database does not include GVKEY identifiers. We will use company name and filing year to match records via fuzzy string matching (Python fuzzywuzzy), supplemented by manual validation for ambiguous cases and SIC Code validation. Match quality will be documented. 

* FRED frequency alignment: FRED series are published at daily, monthly, or quarterly frequencies; Compustat fundamentals are annual. We will aggregate all series to a quarterly frequency using end-of-quarter values (rates) or period averages (GDP, unemployment), then map to Compustat fiscal year-quarters.

* Large file sizes: Compustat and CRSP data may exceed GitHub’s 50MB limit. Files exceeding this threshold will be hosted on Illinois Box with automated download scripts integrated into the Snakemake workflow.

* Survivorship bias: Compustat covers primarily active public firms; firms that delist or go private may be underrepresented. We will document this limitation and assess its impact on the bankruptcy analysis.

# **Gaps**

* We have not yet confirmed the exact LoPucki-to-Compustat match rate. If the fuzzy matching approach yields a low match rate (below 80%), we may need to supplement with manual EDGAR lookups or use Compustat delisting codes as an alternative bankruptcy indicator.

* The optimal set of FRED macro indicators for credit risk analysis is an open question. We plan to start with the 15 series already identified (interest rates, credit spreads, VIX, GDP, unemployment, industrial production, TED spread, M2) and may refine them based on exploratory correlation analysis.

* Data quality issues in the source datasets are not fully known until profiling is complete. We anticipate missing values in Compustat (particularly for smaller firms and earlier years) and potential inconsistencies in LoPucki company names, but the exact scope of cleaning required will be determined.

# **Data Lifecycle**

The acquisition phase involves collecting data from three heterogeneous sources using distinct access methods (SQL, API, download). The curation phase encompasses profiling, quality assessment, cleaning, and integration across schemas. The documentation phase produces metadata conforming to DCAT standards, a data dictionary, and a codebook. The preservation phase packages the workflow for reproducibility via Snakemake, dependency specifications, and Box-hosted data with checksums. The analysis phase produces exploratory findings that demonstrate the value of the integrated dataset.

# **Storage and Organization**

The project repository will follow a clear directory structure separating raw data, processed data, scripts, documentation, and outputs. Raw acquired files (CSV, XLS, JSON) will be stored in a data/raw/ directory and will not be modified after acquisition. Processed and integrated datasets will be stored in data/processed/. All acquisition, cleaning, integration, and analysis scripts will be in a scripts/ directory. The Snakemake workflow file, requirements.txt, and configuration files will reside in the repository root. Documentation including the data dictionary, metadata, and license files will be in a docs/ directory. Large files exceeding GitHub’s size limits will be hosted on Illinois Box with download URLs documented in the README and automated in the Snakemake workflow.

# **Ethical Data Handling**

All datasets are used in compliance with their respective licenses and terms of use. Compustat data is accessed under UIUC’s WRDS academic subscription, which permits research use but prohibits redistribution; raw Compustat files will not be included in the public repository. FRED data is not fully redistributable and requires attribution to the Federal Reserve Bank of St. Louis. The LoPucki BRD is distributed for academic research purposes. All data concerns publicly traded corporations and publicly available financial information; no individual-level personal data or PII is involved. We will include appropriate LICENSE files and attribution statements in the repository.