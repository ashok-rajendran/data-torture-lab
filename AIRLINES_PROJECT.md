✈️ Airlines Data Engineering Project
BigQuery-Based Analytical Simulation

Project Overview

This project simulates a real-world airline data platform built using Google BigQuery. It is designed to represent how a production-grade data warehouse supports operational analytics, revenue optimization, passenger intelligence, and disruption management.

Rather than building a simple dataset for basic SQL queries, this project models realistic airline operational behavior such as missed connections, overbooking conflicts, delay propagation, passenger loyalty prioritization, and route-level performance analysis.

Architectural Design

The dataset is structured to reflect a layered data warehouse approach. Raw operational data is represented through relational tables that capture flight schedules, passenger profiles, bookings, and historical flight status events.

The core dataset (etl_db) contains five primary tables:

Airports

Flights

Passengers

Bookings

Flight Status History

Each table is intentionally structured to support advanced analytical use cases across operational, commercial, and customer-experience dimensions.

Data Model Philosophy

The data model is designed around real airline operational realities:

Flights may be delayed multiple times before departure.

Aircraft may be intentionally overbooked.

Passengers may have multi-leg journeys.

Loyalty tiers influence operational decisions.

Operational disruptions can cascade across routes.

Business Use Cases Modeled

The dataset supports five advanced analytical scenarios designed to reflect real-world airline operations.

1. Missed Connection Detection

Passengers traveling on multi-leg journeys may miss their second flight if the first leg is delayed beyond the layover window.

This use case analyzes arrival delays, scheduled departure gaps, and journey groupings to identify passengers who require rebooking or compensation.

It simulates real-world disruption management systems.

2. Overbooking Conflict Resolution

Airlines often oversell flights assuming some passengers will not show up.

This scenario compares aircraft capacity against confirmed bookings and uses loyalty priority scoring to determine boarding order.

It models the balance between revenue optimization and customer-tier prioritization.

3. Delay Propagation Analysis

When an aircraft is delayed on one route, the delay can cascade into its next scheduled flight.

By tracking aircraft registration across flights and analyzing turnaround windows, this use case models operational efficiency and downstream delay impact.

4. High-Value Passenger Risk Detection

Not all passengers contribute equally to revenue.

This scenario identifies premium-tier frequent flyers and analyzes their cancellation behavior to detect churn risk.

It supports strategic retention and loyalty management initiatives.

5. Route-Level Performance Evaluation

Flights may operate at full capacity but still underperform financially due to refunds, rebookings, or denied boarding compensation.

This scenario aggregates ticket revenue, subtracts refund exposure, and evaluates operational risk to assess route profitability.

It supports strategic decisions such as pricing adjustments, capacity allocation, and route optimization.

Summary

This project was intentionally designed to simulate realistic airline business scenarios rather than focusing solely on schema creation or basic querying. The objective was to explore how real-world operational challenges can be translated into structured analytical use cases.

Using ChatGPT as a thought partner, I first defined complex, business-driven scenarios such as missed connections, overbooking conflicts, delay propagation, loyalty prioritization, and route-level profitability analysis. I then engineered the data model and crafted sample datasets specifically to support those scenarios.

Repository Structure

The complete implementation of this project, including table creation scripts, sample data generation, and analytical SQL use cases, is available within the repository under:

etl/airlines_project