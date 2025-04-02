# Automated Snowflake View Creation

## Overview

This project automates the creation of multiple **Snowflake views** for a given table, allowing structured data access without modifying the original table. A **Snowflake Stored Procedure** dynamically generates views based on predefined logic, enabling controlled data access. While the procedure does not assign permissions, administrators can use **Role-Based Access Control (RBAC)** to manage user access.

## Features

- **Dynamically creates views** based on the provided table name.
- **Supports structured data segmentation** for different user groups.
- **Does not directly grant access**, allowing administrators to configure RBAC as needed.
- **Improves efficiency** by automating view creation and reducing manual work.
- **Enhances security** by ensuring users only access relevant data.

## Usage

To execute the stored procedure and create views for a specific table, use the following command in Snowflake:

```sql
CALL create_user_access_view('HIST_DB.PUBLIC.WORLD_TRADE_DATA');
