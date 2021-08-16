# datium-etl-test

Create an ETL framework that is able to handle the following scenarios:

  - Scenario 1 - ingest, process and load raw data from a csv file to a SQL database
  - Scenario 2 - ingest, process and load raw data from a json file to a SQL database
  
The developed ETL framework should have the following features:
- 1. clear data transformation and cleaning functions
- 2. extendable to allow ingestion of new raw data sources
- 3. extendable to allow loading into new data stores
     - e.g. storing the output in an object store instead of a SQL database
- 4. consist of modular and reusable components
- 5. incorporates unit tests
- 6. logging
      
The expected submission is a working demonstration of the created framework that can be
independently run by us. The submission must consist of the following:

      ● source code of your ETL framework hosted on a public github repo
      ● a runnable docker container that hosts your developed ETL framework
      ● a runnable docker container that hosts a SQL database that acts as the target to load
      the processed data from the ETL framework
      ● instructions on how to run your demonstration and explanation of the expected
      outputs
      
You will be provided with the following resources:

      ● A csv file called test.csv
      ● A json file called test.json
      
We would like your framework to ingest the provided files


SQL table requirements:
- 1. Create a table called test from the provided csv file. The table must conform to the following requirements:
     - each row must have a unique identifier
     - Columns created_at and last_login must be of type timestamp
     - Column is_claimed must be of type boolean
     - Column paid_amount must be of type numeric with 2 decimal places
- 2. Create 3 tables called users, telephone_numbers and jobs_history from the provided json file. The created tables are to have the following requirements:
     - Tables user, telephone_numbers and jobs_history must be linked to each other via the appropriate foreign key relationship
     - Columns created_at, updated_at, and logged_at must be of type timestamp
     - Columns dob, start, and end must be of type date
            
** Additional requirement: Personally Identifiable Information (PII) or sensitive data should be masked where applicable **
