# NYC TAXI TRIP EXPLORER
Full-stack web application that analyzes the NYC taxi trip patterns.

## Team Members
- Dianah Shimwa Gasasira - Backend, Algorithm
- Ayobamidele Aiyedogbon - Frontend, Visualizations
- Jesse Nkubito - Data Processing, Documentation
- Bior Aguer Kuir - Database, Documentation

## Video Demo & team task sheet

1. https://docs.google.com/spreadsheets/d/1WQV7vqSB8P43lSL9Z7yuMnFIPaApOHZrcsmOsyzrytY/edit?usp=sharing
2. https://youtu.be/qOVUIs6VaN8

## Project Structure
```
├── backend/
│   ├── app.py                 # Flask API (runs on port 5000)
│   ├── database.py            # Database helpers
│   ├── data_processor.py      # Data cleaning & ingestion scripts
│   ├── custom_algorithm.py    # Ranking algorithm used by the API
│   └── requirements.txt       # Python dependencies
├── frontend/
│   ├── index.html             # Dashboard HTML
│   ├── style.css              # CSS (beige + dark-brown theme)
│   └── app.js                 # Frontend JS & API calls
├── data/                      # Sample datasets and shapefiles
└── README.md
```

Quick start (Python)

1. Create and activate a Python virtual environment (recommended):

```bash
python -m venv .venv
# Windows (PowerShell)
.\.venv\Scripts\Activate.ps1
# macOS / Linux
source .venv/bin/activate
```

2. Install backend dependencies:

```bash
pip install -r backend/requirements.txt
```

3. Prepare the database

- If you have a prebuilt SQLite file (e.g. `nyc_taxi.db`) place it at the repository root or update the `DATABASE` path in `backend/app.py`.
- Alternatively, run `backend/data_processor.py` (it contains ingestion helpers) to build/ingest CSV data into the database.

4. Start the backend API:

```bash
python backend/app.py
```

The API will be available at http://localhost:5000. Example endpoints:

- `GET /api/stats` — overview stats (total trips, average fare, revenue, etc.)
- `GET /api/hourly` — hourly aggregated values for charts
- `GET /api/top-zones?limit=10` — top pickup zones ranked by revenue
- `GET /api/trips?limit=100` — sample trips (supports filters)

Frontend

The frontend is a static dashboard in `frontend/`. For development you can open `frontend/index.html` directly in a browser, or serve the directory with a lightweight HTTP server (recommended):

```bash
# from repo root
python -m http.server 8000 --directory frontend
# then open http://localhost:8000
```

Notes & customization
- The frontend styling lives in `frontend/style.css` and uses a warm beige background with dark-brown accents and a muted teal for complementary highlights. Tweak the CSS variables at the top of that file to change the theme quickly.
- `backend/custom_algorithm.py` contains the `TaxiZoneRanker` used by the `/api/top-zones` endpoint.
- The app samples trips in some endpoints for performance (see `app.py` query comments). Adjust sampling or add indices if working with a full dataset.

Contributing

If you'd like to add features or fixes:

1. Create an issue describing the change.
2. Open a branch, make your changes, and submit a pull request.


Contact

For questions about the code or dataset, contact the members of the group.
