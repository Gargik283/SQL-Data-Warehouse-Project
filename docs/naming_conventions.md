### Naming Conventions

This document outlines the naming conventions used for schemas, tables, views, columns, and other objects in the data
warehouse.

## Table of Contents

  1. General Principles
  2. Table Naming Conventions
       - Bronze Rules
       - Silver Rules
       - Gold Rules
  3. Column Naming Conventions
       - Surrogate Keys
       - Technical Procedure
  4. Stored Procedures

## General Principles

  - **Naming Conventions**: Use snake_case, with lowercase letters and underscores(_) to seperate words.
  - **Language**: Use English for all names.
  - **Avoid Reserved Words**: Do not use SQL reserved words as object names.

## Table Naming Conventions
# Bronze Rules
  - All names must start with the source system name, and table names must watch their original names without
    renaming.
  - <sourcesystem>_<entity>
    - <sourcesystem>:Name of the source system (e.g., crm, erp).
    - <entity>:Exact table name from the source system.
    - Example crm_customer_info -> Customer information from the CRM system.
 # Silver Rules
  - All names must start with the source system name, and table names must watch their original names without
    renaming.
  - <sourcesystem>_<entity>
    - <sourcesystem>: Name of the source system (e.g., crm, erp).
    - <entity>: Exact table name from the source system.
    - Example crm_customer_info -> Customer information from the CRM system.     

  # Gold Rules
  - All names must use meaningful, buisness-aligned names for tables, startingf with the category prefix.
  - <category>_<entity>
    - <category>: Describe the role of the table, such as dim (dimension) or fact (fact table).
    - <entity>: Descriptive name of the table, aligned with the buisness domain(e.g., customers, products, sales)
    - Examples:
        - dim_customers -> Dimension table for customer data.
        - fact_sales -> Fact table containing sales transactions.

# Glossary of Category Patterns
  |**Pattern** |**Meaning**     |**Example(s)**
  |:-----------|:---------------|:------------------------------------
  |dim_        |Dimension table |dim_customer, dim_product
  |fact_       |Fact table      |fact_sales
  |report_     |Report table    |report_customers, report_sales_monthly















