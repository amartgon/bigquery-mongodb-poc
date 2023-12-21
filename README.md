# bigquery-mongodb-poc

Instructions to run BigQuery to MongoDB pipeline:

1. Open Google Cloud Shell
2. Clone this repository: `git clone https://github.com/amartgon/bigquery-mongodb-poc.git`
3. Change directory: `cd bigquery-mongodb-poc/`
4. Compile and run with maven:
   ```
   mvn -Pdataflow-runner compile exec:java -Dexec.mainClass=org.mongodb.poc.BigQueryToMongoDb -Dexec.args="--inputTableSpec=BIGQUERY_TABLE --mongoDbUri=MONGO_CONNECTION_STRING --database=DB_NAME --collection=COLLECTION_NAME --project=GCP_PROJECT --runner=DataflowRunner --region=us-central1 --numWorkers=NUM_WORKERS"
   ```
6. The new job should appear in the Dataflow GUI
