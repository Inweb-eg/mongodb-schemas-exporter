import pymongo

# Replace with your actual connection details
client = pymongo.MongoClient("mongodb+srv://your_user:your_password@your_cluster.mongodb.net/your_database?retryWrites=true&w=majority")
db = client["togo"]  # Replace "togo" with your actual database name

def export_schema(collection_name):
  collection = db[collection_name]
  # Get a sample document to analyze the schema
  sample_doc = collection.find_one()

  if not sample_doc:
    print(f"Collection '{collection_name}' is empty. Skipping schema export.")
    return

  # Extract field names and data types from the sample document
  schema = {}
  for field, value in sample_doc.items():
    if field == "_id":
      continue  # Skip the _id field

    # Infer data type based on the sample value
    if isinstance(value, str):
      data_type = "string"
    elif isinstance(value, int):
      data_type = "integer"
    elif isinstance(value, bool):
      data_type = "boolean"
    elif isinstance(value, list):
      data_type = "array"
    else:
      data_type = "unknown"  # Handle other data types cautiously

    schema[field] = data_type

  # Write the schema to a JSON file
  with open(f"{collection_name}_schema.json", "w") as f:
    import json
    json.dump(schema, f, indent=4)
    print(f"Schema for collection '{collection_name}' exported to {collection_name}_schema.json")

# Get all collection names
collection_names = db.list_collection_names()

for collection_name in collection_names:
  export_schema(collection_name)

client.close()
