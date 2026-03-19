# Job Tracker

Job Tracker is a lightweight Flask-based dashboard that pulls LinkedIn job application emails from Gmail, parses them into structured records, and helps you track your application pipeline from one place.

It is built for a simple workflow:

- collect LinkedIn application activity automatically
- store everything in a local CSV file
- review the pipeline in a clean web UI
- mark favorites and follow-ups
- manually repair parser misses when needed

## Features

- Reads LinkedIn notification emails through the Gmail API
- Extracts structured fields such as `company`, `job_title`, `location`, and `job_url`
- Merges records into `jobs.csv` while reducing duplicates
- Supports both incremental sync and full sync
- Lets you search, filter, and sort records in the dashboard
- Toggle `downloaded`, `favorite`, and `follow_up_done` states from the UI
- Includes a dedicated Favorites page
- Includes an Insights page for follow-up candidates
- Sends parser failures to a Needs Review workflow instead of silently losing data
- Saves manual corrections so similar emails can be handled better later

## Screenshots

### Main Overview

![Main overview](./image.png)

### Jobs Dashboard

![Jobs dashboard](./jobs.png)

### Favorites Page

![Favorites page](./favorites.png)

### Insights Page

![Insights page](./insights.png)

### Full Sync Dialog

![Full sync dialog](./full_sync.png)

### Sync In Progress

![Sync in progress](./syncing.png)

## Project Structure

```text
job-tracker/
|- dashboard.py
|- sync_service.py
|- gmail_client.py
|- linkedin_parser.py
|- repository.py
|- review_repository.py
|- set_downloaded.py
|- jobs.csv
|- templates/
|  |- base.html
|  |- dashboard.html
|  |- insights.html
|  |- needs_review.html
|  `- review_detail.html
|- image.png
|- jobs.png
|- favorites.png
|- insights.png
|- full_sync.png
`- syncing.png
```

## Core Files

- `dashboard.py`: Flask routes, page rendering, filters, and toggle endpoints
- `sync_service.py`: end-to-end Gmail -> parser -> CSV sync flow
- `gmail_client.py`: Gmail API helpers for listing and reading messages
- `linkedin_parser.py`: parsing and normalization logic for LinkedIn emails
- `repository.py`: CSV read/write, merge, deduplication, and toggle helpers
- `review_repository.py`: storage and workflow for parser review items and manual corrections
- `set_downloaded.py`: small CLI helper to mark a record as downloaded from the terminal

## Requirements

- Python 3.10+
- Gmail API credentials created in Google Cloud
- `credentials.json`
- `token.json` after the first OAuth login

Suggested installation:

```bash
pip install flask google-api-python-client google-auth-httplib2 google-auth-oauthlib
```

## Setup

### 1. Clone the repository

```bash
git clone <repo-url>
cd job-tracker
```

### 2. Install dependencies

```bash
pip install flask google-api-python-client google-auth-httplib2 google-auth-oauthlib
```

### 3. Add Gmail credentials

After enabling Gmail API access in Google Cloud, place `credentials.json` in the project root.

When you run the app for the first time, the OAuth flow will create `token.json`.

## Running the Project

### Sync emails

```bash
python sync_service.py
```

This command:

- fetches matching LinkedIn emails from Gmail
- parses the email content
- updates `jobs.csv`
- writes review items when parsing is incomplete
- stores the latest sync metadata in `.sync_state.json`

### Start the dashboard

```bash
python dashboard.py
```

Then open:

```text
http://127.0.0.1:5000
```

### Optional: mark a record as downloaded from the terminal

```bash
python set_downloaded.py
```

## Application Pages

### 1. Jobs

The main dashboard shows all tracked records. From here you can:

- search in real time
- filter by `viewed`, `downloaded`, and `rejected`
- sort by applied time or viewed time
- open the job link directly
- mark a role as favorite
- toggle downloaded status with one click

### 2. Favorites

The Favorites page shows only starred roles. It is useful for separating the opportunities you want to revisit or keep an eye on.

### 3. Insights

The Insights page highlights viewed jobs that have not been rejected and may be ready for follow-up.

- Jobs with `downloaded=True` get a shorter follow-up threshold
- Other viewed jobs use a longer waiting threshold
- You can mark follow-up items as done from the interface

### 4. Needs Review

If the parser cannot extract enough information from an email, the record is not discarded. Instead, it is sent to the review workflow.

This page lets you:

- inspect incomplete parser results
- manually fix missing fields
- save corrections for future reuse

That makes the system more resilient over time.

## Data Files

- `jobs.csv`: the main application dataset
- `.sync_state.json`: last sync metadata and query history
- review data files: queued parser failures that need manual attention
- manual correction data: saved fixes that can improve future syncs

## Common Commands

```bash
python sync_service.py
python dashboard.py
python set_downloaded.py
```

## Security Notes

It is a good idea to keep these files out of version control:

- `credentials.json`
- `token.json`
- real `jobs.csv` files containing personal data
- any other config file containing OAuth or API secrets

Make sure your `.gitignore` covers them properly.

## Possible Improvements

- export and import support
- richer analytics on the dashboard
- automated tests
- Docker setup
- `requirements.txt` or `pyproject.toml`
- adapters for sources beyond Gmail

## License

This repository does not currently include a license file. If you want to make reuse clearer, adding an `MIT` license is a good next step.
