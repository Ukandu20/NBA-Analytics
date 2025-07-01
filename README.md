# NBA Analytics

This project provides data pipelines and interactive dashboards for exploring NBA statistics. It includes scripts for scraping and cleaning data along with Streamlit apps to visualize players, teams and MVP award trends.

## Setup

1. Create and activate a virtual environment (optional):

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

2. Install the required packages:

   ```bash
   pip install -r requirements.txt
   ```

## Running the Dashboards

- **Main Dashboard** – multiple tabs for team and player metrics:

  ```bash
  streamlit run notebooks/nba_dashboard.py
  ```

- **MVP Dashboard** – focused on historical MVP award data:

  ```bash
  streamlit run notebooks/mvp_dashboard.py
  ```

- **TEAM Dashboard** – focused on a teams seasonal revview data:

  ```bash
  streamlit run notebooks/mvp_dashboard.py
  ```

Run the commands from the repository root and the dashboards will open in your browser.
