-----------------------------------------------------------------------------------------------------------------
Overview:
The script generator script was created to automate deployment and rollback scripts for MSSQL server.
-----------------------------------------------------------------------------------------------------------------
Dependencies:
1. PyYAML
2. RedGate SQL Compare

Input(s):
Input is a YAML file with information provided by development.

Output(s):
1. Copy of original yaml file (for reference).
2. Deployment scripts for tables, views, functions, stored procedures.
3. Rollback scripts for the deployed objects.

Process:
1. Create YAML file.
2. Run script.
3. Run scripts.
