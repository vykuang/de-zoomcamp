# Week 1 - Setup for GCP, Docker, Terraform, Postgres

## Ingest csv to postgres

Set the whole `data/` directory as a mount for my `postgres` container; how it's got really strict permissions that prevents `black` and `git` commands.

- `sudo chmod -R 770 data/` to restore read/write permission

pandas has a tool that can read the header row of a csv and create the table schema in SQL