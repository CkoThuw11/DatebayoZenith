# CDC Northwind Pipeline

## Overview
This is the root repository for the Northwind CDC (Change Data Capture) Pipeline. It acts as the structural foundation and contract between the Source, Backbone, and Sink teams.

## Structure
- `docs/`: Critical architecture and contract definitions.
- `docker-compose.yaml`: The single source of truth for local infrastructure (Backbone-owned).
- `connectors/`: Integration boundary for Source and Sink configurations.
- `scripts/`: Shared standalone utilities.

## Getting Started
1. Review `docs/architecture.md` and `docs/contracts.md`.
2. Copy `.env.example` to `.env` and provide your local configurations.
3. Bring up the infrastructure (Requires `docker-compose`).

