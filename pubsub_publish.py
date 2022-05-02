"""Publishes multiple messages to a Pub/Sub topic with an error handler."""
from concurrent import futures
from google.cloud import pubsub_v1
import sys
import csv
from datetime import date
import json
from itertools import islice
import time

file = open('dataset/loan_data_sample.csv')
#file = open('dataset/loan_data_3y_trim.csv')

csvreader = csv.reader(file)
print(csvreader)
header = next(csvreader)
print(header)

# TODO(developer)
project_id = "gcp_project_id"
topic_id = "gcp-hackathon"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)
publish_futures = []

#for row in csvreader:
for row in islice(csvreader,  1, 5000):
    print(row)
    time.sleep(2)
    print("csv record to be processed... : ", row)
    final_msg = {"TransactionID": str(row[0]),
    "CustomerID": str(row[1]),
    "CustomerName": str(row[2]),
    "CustomerDOB": str(row[3]),
    "CustAccountBalance": str(row[4]),
    "LoanAmount": str(row[5]),
    "CustomerEmail": str(row[6]),
    "CustLocation": str(row[7]), #datetime.now(),
    "TotalAmountPaid": float(row[8]),
    "LoanDuration": int(row[9]),
    "LoanRate": float(row[10]),
    "LoanSanctDate": str(row[11]),
    "LastCycleDate": str(row[12]),
    "IssuerBank": str(row[13]),
    "LoanEndDate": str(row[14]),
    "RemainingLoan": int(row[15])
    }
    print("Final msg to be pushed ... : ", final_msg)
    print("type of msg :", type(final_msg))
    final_msg = json.dumps(final_msg)
    print("type before laoding the data : ", final_msg)
    print("----------------------------------------")

    #data = str(i)
    # When you publish a message, the client returns a future.
    
    publish_future = publisher.publish(topic_path, final_msg.encode('utf-8'))
    
    # Non-blocking. Publish failures are handled in the callback function.
    #publish_future.add_done_callback(get_callback(publish_future, final_msg))
    
    publish_futures.append(publish_future)

# Wait for all the publish futures to resolve before exiting.
futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

print(f"Published messages with error handler to {topic_path}.")

