# UCLA MSBA Data Management Project - Impact of Wildfires on Air Pollution Levels: Sparsh Sharma

## 1. Project Overview

This project analyzes how wildfire events impact air pollution levels over time and geography, with a focus on California and Arizona.

Rather than treating this as a pure data engineering task, I framed it as a data science impact analysis problem, where the goal was to:

- Engineer meaningful features from raw environmental data

- Quantify pollution changes before and after wildfire events

- Interpret results responsibly using observational data

- Deliver insights through an interactive, stakeholder-friendly dashboard

The final output includes:

- A reproducible ETL + analytics pipeline

- Analysis-ready datasets

- An interactive Tableau dashboard for exploration and decision-making

## 2. Problem Framing (Data Science Perspective)

**Core Question**

How do wildfire events affect air pollution levels, and does fire severity influence the magnitude and duration of impact?

**Why this matters**

- Wildfire-related air pollution poses serious public health risks

- Understanding which fires matter most helps prioritize response and mitigation

- Quantifying post-fire pollution persistence informs public advisories

This framing intentionally avoids over-claiming causality and instead focuses on consistent, interpretable patterns in real-world data.

## 3. Data Sources

**Dataset	Description**
US Wildfires (1992–2015)	Fire size, cause, dates, and locations (SQLite)
Link: https://drive.google.com/file/d/1GyFFy_ifvy3TrLUj_vWZ6xrH7mtEskUs/view?usp=sharing

US Air Pollution (2000–2016)	AQI and pollutant measurements including NO₂ (CSV)
Link: https://drive.google.com/file/d/1hCR16nGVYeptgQ-SLy4rCix5fPoPb2jJ/view?usp=sharing

**Key data decisions**

- Scoped analysis to California and Arizona to balance signal quality and compute cost

- Prioritized data completeness over fully automated geocoding

- Selected AQI and NO₂ as primary risk-sensitive metrics
  

## 4. Feature Engineering & Analytical Approach

Most of the data science work in this project happens during transformation.

**Engineered Features**

1. Pre- vs Post-Wildfire AQI windows

2. Fire size classes (severity-based categorical features)

3. Peak daily AQI to capture worst-case exposure

4. Aggregations at event, date, and regional levels
 
**Analytical Strategy**

1. Pre/post comparisons within the same geography

2. Correlation analysis between fire size and pollution change

3. Stratified analysis by wildfire severity class

This approach emphasizes interpretability and robustness over complex modeling, given the observational nature of the data.


## 5. Pipeline Architecture

Raw Data \n
   ↓ \n
Apache Spark (cleaning, joins, feature engineering) \n
   ↓ \n
Parquet \n
   ↓
DuckDB (analytical SQL & aggregations)
   ↓
CSV outputs
   ↓
Tableau Dashboard

**Why this design**

- Spark enables scalable transformations beyond pandas
- DuckDB provides fast analytical SQL without heavy infrastructure
- Clean separation between compute, analytics, and visualization

Outputs are reproducible and BI-ready
This project processes data using PySpark and DuckDB to generate aggregated CSV files for visualization in Tableau. It involves the following main steps:

1. Setting up a Google Cloud instance.

2. Installing required software and dependencies.

3. Cloning the repository containing the input data and pipeline scripts.

4. Running the data processing pipeline to generate output CSVs that can be used in Tableau or other visualization tools.

## 1. Activating Google Cloud and Creating an Instance

Create a Google Cloud account if you don't already have one

Activate billing for your account to access compute resources.

Launch your VM instance by navigating to Compute Engine -> VM Instance -> Create Instance and selecting your preferences

## 2. Installing all pre-requisites required

### Install Java (required for PySpark)

sudo apt install openjdk-11-jdk -y

### Verify Java Installation

java -version

### Install Python and pip

sudo apt install python3 python3-pip python3-venv -y

### Verify Python installation

python3 --version 

pip3 --version

### Install Hadoop (required for PySpark)

wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz

sudo tar -xzvf hadoop-3.3.6.tar.gz -C /opt/

sudo mv /opt/hadoop-3.3.6 /opt/hadoop

### Set Hadoop environment variables (add these lines to your .bashrc):

echo 'export HADOOP_HOME=/opt/hadoop' >> ~/.bashrc

echo 'export PATH=$PATH:$HADOOP_HOME/bin' >> ~/.bashrc

source ~/.bashrc

### Verify Hadoop installation:

hadoop version

### Dowload the files from google sheets and upload to the GCP

Pollution data: https://drive.google.com/file/d/1vdUKFzU0-SG7IaZFACPymmEy5WO5ad0M/view?usp=sharing

Fire data: https://drive.google.com/file/d/1U9cHiZIfkBjSviUwvjrpoRDxiPA9VTKE/view?usp=sharing

### Install DuckDB
pip install duckdb

### Install PySpark
pip install pyspark

## 3. Cloning this Repository to Get Input Files and Pipeline Scripts

git clone https://github.com/Prof-Rosario-UCLA/team16.git

### Navigate into the directory 

Make sure the following files are present after cloning:

1. 2 Input CSV files (used by PySpark script)

2. spark_job.py: PySpark script for initial data processing and joining CSV files into Parquet format. (update the location CSV files)

3. queries_v2.sql: SQL queries for DuckDB aggregations. (use the output file of spark_job as base)

4. pipeline_test_2.sh: Python script orchestrating the entire pipeline execution. (update the location spark_job, queries_v2, output CSV location)

## 4. Running the Data Processing Pipeline

Run the complete pipeline script (pipeline_test_2.sh) which will execute:

PySpark processing (spark_job.py) to create Parquet files.

DuckDB SQL queries (queries_v2.sql) to generate aggregated CSV outputs.

Execute the pipeline with:

bash pipeline_test_2.sh

After successful execution, aggregated output CSV files will be available in the output directory

## Next Steps: Visualization in Tableau

Once you have generated your aggregated CSV outputs, you can import these files directly into Tableau Desktop or Tableau Cloud for visualization purposes.

Tableau public dashboard: https://public.tableau.com/app/profile/sparsh.sharma1162/viz/ImpactofWildfiresonAirQualityDashboard/Dashboard?publish=yes
