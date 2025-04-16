# Innowise task 1: Python Introduction
This repository contains the implementation of Task 1 for the Innowise DE Trainee program.
## Objective:
- Design a schema using MySQL (or another relational database, e.g., PostgreSQL) to match the structure of the provided files (many-to-one relationship).
- Write a script to load data from the two files into the database.
## Database Queries Requirements:
- List of rooms and the number of students in each room.
- Five rooms with the lowest average student age.
- Five rooms with the highest age difference among students.
- List of rooms where students of different gender live.
## Expectation and Notes:
- Propose query optimization strategies using indexes.
- Generate an SQL script to add necessary indexes.
- Export the results in JSON or XML format.
- All calculations should be performed at the database level.
- The command-line interface should support the following input parameters:
1. students: Path to the student data file.
2. rooms: Path to the room data file.
3. format: Output format (XML or JSON).
- Follow SOLID principles in object-oriented programming.
- Avoid using ORM; write raw SQL queries.
## Installation 
Copy the repository contents, install packages from requirements.txt and fill your credentials to PostgreSQL Database in config.ini.
```bash
git clone https://github.com/xeqbz/innowise_task_1
pip install -r requirements.txt
```
Or use Docker.
```bash
git clone https://github.com/xeqbz/innowise_task_1
docker-compose up --build
```
## Usage
```bash
main.py [-h] --students STUDENTS --rooms ROOMS [--format {xml,json}]

Insert data in database

options:
  -h, --help           show this help message and exit
  --students STUDENTS  Path to students JSON file
  --rooms ROOMS        Path to rooms JSON file
  --format {xml,json}  Output format(JSON or XML)

```