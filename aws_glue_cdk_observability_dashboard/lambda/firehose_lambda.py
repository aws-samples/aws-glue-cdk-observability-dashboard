from __future__ import print_function
import base64
import json
import datetime


def lambda_handler(firehose_records_input, context):
    print("Received records for processing from DeliveryStream: " + firehose_records_input['deliveryStreamArn']
          + ", Region: " + firehose_records_input['region']
          + ", and InvocationId: " + firehose_records_input['invocationId'])
    firehose_records_output = {'records': []}

    for firehose_record_input in firehose_records_input['records']:
        # Get user payload
        payload = base64.b64decode(firehose_record_input['data']).decode('utf-8')
        print(f"Payload: {payload}")
        lines = payload.split("\n")
        print("split lines: ")
        print(*lines)

        output_payload = ""
        partition_keys = {}
        for line in lines:
            if not line:
                continue
            print(f"split line: {line}")
            json_value = json.loads(line)
            print("Record that was received")
            print(json_value)
            print("\n")

            firehose_record_output = {}
            event_timestamp = datetime.datetime.fromtimestamp(json_value['timestamp'] / 1000)
            partition_keys = {
                "account_id": json_value['account_id'],
                "region": json_value['region'],
                "year": event_timestamp.strftime('%Y'),
                "month": event_timestamp.strftime('%m'),
                "day": event_timestamp.strftime('%d'),
                "hour": event_timestamp.strftime('%H')
            }
            del json_value['account_id']
            del json_value['region']
            output_payload += json.dumps(json_value) + "\n"
        print(f"output_payload: {output_payload}")
        print(f"partition keys: {partition_keys}")

        if partition_keys != {}:
            firehose_record_output = {'recordId': firehose_record_input['recordId'],
                                      'data': base64.b64encode(output_payload.encode('utf-8')),
                                      'result': 'Ok',
                                      'metadata': {'partitionKeys': partition_keys}}

            firehose_records_output['records'].append(firehose_record_output)

    return firehose_records_output
