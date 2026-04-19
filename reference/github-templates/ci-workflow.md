Below is a sample Github Workflow file that shows how to implement a CI process with dbt.

name: CI

on:
  pull_request:
    branches: [ "main" ]

jobs:
  build-and-deploy:
    runs-on: ubuntu-latest
    env:
      DBT_USER: ${{ secrets.DBT_USER }}
      DBT_PASSWORD: ${{ secrets.DBT_PASSWORD }}
      DATABASE_HOST: ${{ secrets.DATABASE_HOST }}
      DATABASE_PORT: ${{ secrets.DATABASE_PORT }}

    steps:
      - name: Checkout branch
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
          
      - name: Install dbt
        run: pip install dbt-core==1.8.7 dbt-postgres==1.8.2

      - name: Install dbt Packages
        run: dbt deps

      - name: Deploy & Test Models (CI)
        run: > 
          dbt build
          --profiles-dir _project_docs/automation
          --target ci