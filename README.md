### * If the code is useful to you, please consider leaving a star.

# This script copies a large file of several gigabytes to the s3 bucket with the following advantages:

- Sends the file in pieces (configurable)
  
The files will be sent in smaller parts and at the end they will be joined again in 1 file.


- Retries to send if failure occurs (configurable)

Each part sent, if failures occur, it will try again.


- Sends several parts simultaneously (configurable)

Sends multiple parts at the same time, making the file sending very fast.

How to use

### 1 - Install Python

### 2 - Create a Virtual Environment:
python -m venv venv

### 3 - Activate the Virtual Environment:
venv\Scripts\activate

### 4 - Install BOTO3
pip install boto3

### 5 - Run:
python copy-larger-files-to-s3.py
