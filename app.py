import json
import os
import boto3
import pandas as pd
from io import StringIO
from flask import Flask, request, jsonify

app = Flask(__name__)

# Load AWS Credentials explicitly
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")  # Default to us-east-1

print(AWS_ACCESS_KEY_ID)
print(AWS_SECRET_ACCESS_KEY)
print(AWS_REGION)
# Initialize S3 client with explicit credentials
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

BUCKET_NAME = "sample-bucket-cognito"

@app.route("/process-file", methods=["POST"])
def process_file():
    try:
        # Parse request data (JSON)
        data = request.get_json()
        file_name = data.get("file_name")  # File to search
        new_file_name = data.get("new_file_name")  # New file name for upload

        if not file_name or not new_file_name:
            return jsonify({"error": "Missing file_name or new_file_name"}), 400

        # Search for the file in S3
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=file_name)
        if "Contents" not in response:
            return jsonify({"error": "File not found"}), 404

        # Get the exact file key
        file_key = response["Contents"][0]["Key"]  # Assuming exact match

        # Get the file from S3
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
        file_data = obj["Body"].read().decode("utf-8")  # Read and decode file

        # Check if file is a CSV
        if file_key.endswith(".csv"):
            # Convert CSV string to DataFrame
            df = pd.read_csv(StringIO(file_data))

            # Add an empty row
            df.loc[len(df)] = [None] * len(df.columns)

            # Convert back to CSV
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()

            # Upload the modified file with the new name
            new_file_key = new_file_name if new_file_name.endswith(".csv") else new_file_name + ".csv"
            s3.put_object(Bucket=BUCKET_NAME, Key=new_file_key, Body=csv_content)

            return jsonify({
                "message": "CSV file updated and saved with new file name",
                "original_file": file_key,
                "new_file": new_file_key,
                "rows": len(df),
                "columns": list(df.columns)
            })

        # If it's not a CSV, just return file details
        return jsonify({
            "message": "File found but not a CSV, no modifications made",
            "file_name": file_key
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
