# SalesFlow360
# SalesFlow360: BigQuery Sales Process Automation

## Overview

SalesFlow360 automates and tracks the multi-stage sales process across various categories using Google BigQuery SQL. It supports partner and non-partner flows, ensures data consistency, and provides analytics-ready outputs.

## Features

- Automated lead categorization and status tracking
- Multi-stage process (LC1–LC4) with real-time updates
- Data synchronization and integrity checks
- Modular SQL structure for easy maintenance

## Project Structure

- `sql/`: All SQL scripts organized by function (extract, transform, load, pipeline)
- `docs/`: Documentation and architecture diagrams

## Getting Started

1. Clone the repository.
2. Review the `pipeline.sql` file for the main workflow.
3. Run scripts in order: extract → transform → load.
