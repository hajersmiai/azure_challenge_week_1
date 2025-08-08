# 🚆 Azure Belgian Train Data Pipeline

##  Project Overview

This project builds a **cloud-native data pipeline** that fetches real-time **Belgian train data** from the [iRail API](https://docs.irail.be/), processes it, and stores it in an **Azure SQL Database** for future business intelligence and analytics.

We collect three main types of data:
-  Station metadata
-  Liveboard departures for major stations
-  Route connections between popular city pairs

---

##  Architecture & Technologies

- **Backend**: Azure Functions (Python)
- **Database**: Azure SQL Server
- **API Source**: [iRail API](https://docs.irail.be/)
- **Libraries**: `requests`, `pandas`, `pyodbc`, `sqlalchemy`, `azure.functions`
- **Deployment**: GitHub Actions → Azure Function App
- **Data Ingestion Triggers**:
  - `@http_trigger` to fetch data on demand
  - `@timer_trigger` to fetch data every 5 or 15 minutes

---

##  Database Schema

The schema follows a **star-schema** logic to optimize analytical queries.


###  Dimension Tables

- **Train**:
  - `TrainID`, `TrainNumber`, `TrainType`, `Operator`, `id` (unique)

- **Station**:
  - `StationID`, `StationName`, `Latitude`, `Longitude`, `id`, `iri_url`

- **DateDimension**:
  - `DateID`, `FullDate`, `Day`, `Month`, `Year`, `Hour`, `Minute`, `Second`

###  Fact Tables

- **TrainMovements**:
  - Captures each movement with delay, scheduled vs actual time, stations, dates, platform.
  - Foreign keys: `TrainID`, `DepartureStationID`, `ArrivalStationID`, `DepartureDateID`, `ArrivalDateID`

- **TrainFeedback**:
  - User-based feedback with `occupancy`, `vehicleUrl`, `stationUrl`, `feedbackDate`

- **TrainCompositionUnit**:
  - Describes the physical train composition: comfort, orientation, seats, amenities, etc.

- **Disturbance**:
  - Logs real-time operational issues (e.g. delays, incidents, weather disruptions)

---

##  Components & Class Responsibilities

### `DirectInsertor`
Handles **direct ingestion** of raw API data to SQL (mainly for debug and full refreshes).

- `insert_stations_direct()` – populates `Station`
- `insert_sample_liveboards()` – populates `Train`, `DateDimension`, and `TrainMovements`
- `insert_disturbances_direct()` – inserts into `Disturbance`
- `cleanup_duplicates()` – utility to remove redundant rows

### `TrainDataRepository`
Core interface between iRail API, data transformation logic, and SQL database operations.

---

##  Azure Functions & Their Roles

This project uses a modular function-based architecture, powered by Azure Functions, each with a distinct purpose in the train data ingestion pipeline. Below is a breakdown of each relevant function and its role:

## function.py
A diagnostic and testing function used to verify that dependencies (e.g., pymssql, SQLAlchemy, requests) are correctly installed and that the database connection is functional.

It also tests the successful instantiation of core classes like TrainDataRepository, IRailAPI, and DirectInsertor.

## iRail_API.py
Contains the API client for accessing the iRail API.

Implements functions to fetch:

Station data

Train departures

Train connections

Disturbances

## train_data_repository.py

Responsible for interfacing with the Azure SQL Database.

Includes methods to insert:

Train departures

Disturbances

Connections

Acts as the core bridge between raw API data and the database.

## train_data_ingestor.py

Orchestrates the full ingestion pipeline using both iRail_API.py and train_data_repository.py.

Main responsibility: calls the API, transforms the data, and stores it into the database with business logic (e.g., filtering, formatting).

## DirectInsertor.py

A utility class used to directly insert raw iRail data into the database, bypassing any checks or normalization steps.

Mainly used for initial bulk data population, testing, or debugging without enforcing schema constraints or data validation.

## Train_repository.py

A simplified variant of train_data_repository.py used together with DirectInsertor.py to test the database insertions with minimal abstraction.

Mainly useful for debugging and direct SQL write operations without full API orchestration.

## train-http-ingestor/__init__.py

HTTP-triggered Azure Function that exposes an endpoint to manually trigger the ingestion of:

Departures

Disturbances

Connections

Can be invoked from external tools or the browser for manual pipeline execution.

## train-timer-ingestor/__init__.py

Timer-triggered Azure Function that runs periodically (e.g., every 15 minutes) to automatically ingest new train data.

Ensures data freshness by continuously polling the iRail API and updating the database.
---

##  Challenges & Solutions Implemented

During the development and deployment of this Azure-based train data ingestion pipeline, several technical challenges were encountered. Below is a summary of the main ones and the solutions (or current limitations) implemented:

1. **Azure SQL Connectivity & Secure Environment Variables**

**Challenge:**

Connecting to Azure SQL securely in a cloud context required injecting sensitive credentials (server, database, user, password) as environment variables, not hardcoding them.

**Solution Implemented:**

Environment variables were defined and retrieved dynamically using os.environ.get().

Connection strings were built using pyodbc with encrypted channels and fail-safe timeouts.

Secure connection parameters (Encrypt=yes;TrustServerCertificate=no) were used for compliance.

2.  **Timer-Based Automation & Stability**

**Challenge:**

Azure Functions triggered by a CRON timer (every 5 or 15 minutes) must remain stateless, performant, and resilient to API latency or failure.

**Solution Implemented:**

Functions were designed to be idempotent and lightweight.

Concurrent data collection used ThreadPoolExecutor with timeouts to avoid hanging jobs.

Robust retry logic with exponential backoff was added for API failures (iRail endpoints).

3.  **Complex Star Schema Normalization**

**Challenge:**

The project was architected to insert real-time iRail data into a star-schema database (fact and dimension tables), requiring normalization of:

Time fields (to the DateDimension table)

Station names/IDs (to Station)

Train identifiers (to Train)

**Solution Implemented:**

Logic was introduced in the train_data_repository.py to handle foreign key resolution (e.g., linking a TrainID to a TrainMovement).

However, fully automating these joins dynamically requires further work (e.g., caching lookups or enforcing referential integrity on insert).

4. **Diagnostic Debugging vs Production Behavior**

**Challenge:**

While all components (API calls, database logic, data transformations) function perfectly in local testing (with live data retrieved and parsed), the deployed version on Azure runs without error—but does not insert data into the database.

**Observations & Current Status:**

The deployed Azure Functions log successful execution.

Debug messages confirm the API is reachable and the pipeline logic is running.

No exception is raised during insertion, but the Azure SQL tables remain empty.

This suggests an environmental or permission issue between the Azure App Service and the SQL Server (e.g., firewall, network rules, or missing driver).

**Solution in Progress:**

Verified the run_direct_insertion and process_and_store_* functions locally.

Diagnostic logs were added for every step.

Next step involves validating firewall rules, App Service networking, or switching to SQLAlchemy for clearer error propagation in cloud deployments.

## Deployment

The Azure Functions-based data pipeline was deployed using two different methods:

1.  **Visual Studio Code (VS Code) Deployment**
Azure Functions were deployed directly from VS Code using the Azure Functions extension.

This method allowed quick iteration and immediate deployment feedback.

Environment configuration and runtime logs were accessed through the Azure portal for debugging.

2. **GitHub Actions CI/CD**
A GitHub Actions workflow was configured to automatically deploy the Azure Function App upon pushing to the main branch.

The .github/workflows/ directory includes a YAML definition that builds the function, installs dependencies, and pushes the package to Azure.

This CI/CD setup ensures version control and reliable reproducibility.

Both deployment strategies were successfully tested and ensured the Azure Function App remained up-to-date and consistent with the repository codebase.

##  How to Deploy

1. **Configure Azure SQL Database**  
   Set env variables in Azure:
   - `SQL_SERVER`
   - `SQL_DATABASE`
   - `SQL_USERNAME`
   - `SQL_PASSWORD`

2. **Deploy via GitHub Actions** or VSCode  
   Push to GitHub + configure Function App to auto-deploy.

3. **Test it**  
   Call:
   - `/api/auto_collect_all`
   - Or wait for timer triggers

---

##  Future Improvements

-  Real-time event processing with Azure Event Hub
-  Power BI dashboard based on SQL data
-  Add logging/monitoring via Application Insights
-  Retry logic with exponential backoff
-  Full data cataloging with column definitions

##  Author

This project was created and deployed by **Hajer Smiai Ep Fridhi** as part of a professional Azure Data Engineering challenge.