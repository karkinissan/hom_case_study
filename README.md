# A case study

This data pipeline has the following components: 
* Data Generation 
* Data Processing 

The Data Generation code is present in the file `data_generator.py`. 
It is invoked in the file `data_generator_gcp.py`, which is the source code for a cloud function.
The cloud function generates the mock data and uploads it to a Google cloud bucket.

The Data Processing code is present in the `activity_processor.py`. 
It is invoked in the `activity_processor_bq.py`, which is also the source code for a cloud function. 
The cloud function reads the file list from GCS, compares it to the logs to find new files, 
processes said files and uploads the contents to BQ tables.  

Tests are present in the `tests/` directory. 