import boto3
import time
import statistics
import itertools
import argparse
from boto3.dynamodb.conditions import Key, Attr


dynamodb = boto3.resource('dynamodb')
bsm_data_table = dynamodb.Table('BedSideMonitorData')
bsm_agg_table = dynamodb.Table('BedSideMonitorAggData')

#Creating arguments to accept device id, starting and ending time
parser = argparse.ArgumentParser()
parser.add_argument("-d", "--deviceId", action="store", required=True, dest="deviceId", help="Your Device ID")
parser.add_argument("-s", "--startingTime", action="store", required=True, dest="startingTime", help="Starting Time", type=str)
parser.add_argument("-e", "--endingTime", action="store", required=True, dest="endingTime", help="Ending Time", type=str)

args = parser.parse_args()
deviceid = args.deviceId
startingTime = args.startingTime
endingTime = args.endingTime

hour_data = Key('timestamp').between(startingTime,endingTime) & Key('deviceid').eq(deviceid);
response = bsm_data_table.query(KeyConditionExpression=hour_data)

# Group the sorted response table by items per minute
items_by_minute = itertools.groupby(
    sorted(response["Items"], key=lambda x: (x['datatype'], x["timestamp"][:16])), 
    key=lambda x: (x['datatype'], x["timestamp"][:16]) 
)

# Calculate and store the average, minimum and maximum values taking all values within a minute into account 
# Calculate the statistics for each minute
for minute, items in items_by_minute:
    values_per_minute = [item["value"] for item in items]
    avg = statistics.mean(values_per_minute)
    min_value = min(values_per_minute)
    max_value = max(values_per_minute)
    response = bsm_agg_table.put_item(
        Item = {
        "datatype" : minute[0],
        "timestamp" : minute[1], 
        "deviceid" : deviceid,
        "min" : min_value,
        "max" : max_value, 
        "avg": avg
    })
    
    print(f"Datatype : {minute[0]} / Time : {minute[1]} / Average {avg} / Min {min_value} / Max {max_value}")

