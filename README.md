# export-API

ClarityNLP module for exporting results to third party sources (OMOP, FHIR, etc.)

### Exporting results to OMOP database

- Endpoint: `~/export_ohdsi`
- Methods: `POST, GET`
- `GET` Request:
  - Checks whether the API is up and running, i.e. an API health check.
- `POST` Request:
  - Request Body
    ```
    {
      job_id: 1234,
      result_name: Widowed,
      omop_domain: Condition,
      concept_id: 9732
    }
    ```
  - Return value: JSON
    - Sucess: 200
      ```
      {
        Successfully exported results
      }
      ```
    - Error: 400, 500
      ```
      {
        <Error Message>
      }
      ```
