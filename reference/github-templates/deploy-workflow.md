Below is a sample Github Workflow file that shows how to implement a post-merge deployment process with dbt.

Note: You should update the pip install and other commands to meet your needs. This is for sample purposes.

name: Post Merge Deploy (Production)

on:
  pull_request:
    types:
      - closed

  workflow_dispatch:

jobs:
  build-and-deploy:
    if: github.event.pull_request.merged == true
    runs-on: ubuntu-latest
    env:
      DBT_USER: ${{ secrets.DBT_USER }}
      DBT_PASSWORD: ${{ secrets.DBT_PASSWORD }}
      DATABASE_HOST: ${{ secrets.DATABASE_HOST }}

    steps:
      - name: Checkout branch
        uses: actions/checkout@v3

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
          
      - name: Install dbt
        run: pip install dbt-snowflake

      - name: Install dbt Packages
        run: dbt deps

      - name: Deploy & Test Models (Prod)
        run: > 
          dbt build
          --profiles-dir _project_docs/automation
          --target prod