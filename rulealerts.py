import boto3
import time
import json
import statistics
import itertools
import argparse
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')
bsm_agg_table = dynamodb.Table('BedSideMonitorAggData')
bsm_alerts_table = dynamodb.Table('BedSideMonitorAlerts')

#Creating arguments to accept device id, starting, ending time and rule configuration file
parser = argparse.ArgumentParser()
parser.add_argument("-d", "--deviceId", action="store", required=True, dest="deviceId", help="Your Device ID")
parser.add_argument("-s", "--startingTime", action="store", required=True, dest="startingTime", help="Starting Time", type=str)
parser.add_argument("-e", "--endingTime", action="store", required=True, dest="endingTime", help="Ending Time", type=str)
parser.add_argument("-c", "--configFile", action="store", required=True, dest="configFile", help="Rule config file path")
parser.add_argument("-t", "--dataType", action="store", required=True, dest="dataType", help="Data Type to generate alert")

args = parser.parse_args()
deviceid = args.deviceId
startingTime = args.startingTime
endingTime = args.endingTime
configFile = args.configFile 
dataType = args.dataType

#Filtering the table based on the datatype argument
hour_data = Key('timestamp').between(startingTime,endingTime) & Key('deviceid').eq(deviceid);
datatype_filter = Attr('datatype').eq(dataType)
response = bsm_agg_table.query(KeyConditionExpression=hour_data, FilterExpression=datatype_filter)

#Reading in the rules configuration file
config = open(str(configFile), "r")
rules = json.loads(config.read())
#Filter rule by datatype
rule = list(filter(lambda x: x['datatype'] == dataType, rules["rules"]))[0]

for item in response['Items']:
    if item['min'] < rule['min'] or item['max'] > rule['max']:
        response = bsm_alerts_table.put_item(
        Item = {
        "datatype" : item['datatype'],
        "timestamp" : item['timestamp'], 
        "deviceid" : deviceid,
        "min" : item['min'],
        "max" : item['max'], 
        })
        print(f"Alert Item data {item['datatype']} with min {item['min']} and max {item['max']}") 