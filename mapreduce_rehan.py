import pandas as pd
from collections import defaultdict

# Load the dataset
file_path = "gs://rehanbucket/retailstore.csv"  (my csv file path)
data = pd.read_csv(file_path)

# Define the mapper function
def mapper(row):
    """
    Processes a single row of the dataset and emits a key-value pair.
    Key: Country
    Value: 1 (represents one customer)
    """
    country = row['Country']
    return (country, 1)

# Define the reducer function
def reducer(key, values):
    """
    Aggregates values for a given key.
    Key: Country
    Values: List of integers (e.g., [1, 1, 1])
    """
    return (key, sum(values))

# Simulate the MapReduce job
def mapreduce_job(data):
    """
    Runs the MapReduce job on the given dataset.
    """
    # Map phase
    intermediate_results = []
    for _, row in data.iterrows():
        intermediate_results.append(mapper(row))
    
    # Shuffle phase (group by key)
    grouped_data = defaultdict(list)
    for key, value in intermediate_results:
        grouped_data[key].append(value)
    
    # Reduce phase
    final_results = []
    for key, values in grouped_data.items():
        final_results.append(reducer(key, values))
    
    return final_results

# Run the MapReduce job
results = mapreduce_job(data)

# Display the results
for country, count in results:
    print(f"{country}: {count}")
