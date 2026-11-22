from xml.dom.minidom import Identified
from boto3 import Session
import json
import boto3
import os

IS_LOCAL = os.getenv('ISLOCAL')
PROFILE_NAME = os.getenv("PROFILENAME")
# IS_LOCAL = True
# PROFILE_NAME = "valentine"


def session_inicializer(isLocal: bool, profile_name: str = None) -> Session:
    if isLocal:
        return boto3.Session(profile_name=profile_name)
    return boto3.Session()


session_local = session_inicializer(IS_LOCAL, PROFILE_NAME)


def receive_messages(queue_name: str) -> list:
    client = session_local.client("sqs")
    # n = whoami()
    response = client.get_queue_url(QueueName=queue_name)
    queue_url = response["QueueUrl"]

    # get messagese here
    response = client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=2
    )

    return response.get("Messages", [])


def whoami() -> dict[str:str]:
    values = {}
    sts = session_local.client("sts")
    identity = sts.get_caller_identity()
    values["Account"] = identity["Account"]
    values["Identity"] = identity["Arn"]
    values["Region"] = session_local.region_name
    return values


def send_message_to_queue(queue_name: str = None, message_body: str = None,  message_atrib_dict: dict[str:str] = {}) -> str | None:
    queue_url = None
    queue = None
    client = session_local.client("sqs")
    list_raw = client.list_queues()
    result = list_raw["QueueUrls"]
    for i in result:
        if i.split("/")[-1] == queue_name:
            queue = i.split("/")[-1]
            queue_url = i
            break
    if queue_url is None:
        return None
    string_message = str(message_body)
    response = client.send_message(QueueUrl=queue_url, DelaySeconds=10,
                                   MessageAttributes=message_atrib_dict, MessageBody=string_message)
    return response


def receive_and_process_messages(queue_name: str) -> int:
    client = session_local.client("sqs")

    # 1. gettin url of queue
    queue_response = client.get_queue_url(
        QueueName=queue_name
    )
    queue_url = queue_response["QueueUrl"]

    # 2. getting 10 messages
    response = client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=5,
        VisibilityTimeout=60
    )

    messages = response.get("Messages", [])
    processed_count = 0

    # 3. proces each message...  in future i will implement some training with db 
    for message in messages:
        try:
            body = json.loads(message["Body"])

            print("Order ID:", body["order_id"])
            print("Product:", body["product"])
            print("Price:", body["price"])

            # here i will add some bis. logic
            # as example saves somethibg to db... 
            # 4. delete messages which is processed 
            client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message["ReceiptHandle"]
            )

            processed_count += 1
            print("Message processed and deleted")

        except Exception as error:
            print(f"Processing failed: {error}")
        

    return processed_count



def publish_order(topic_arn: str, order: dict) -> str:
    client = session_local.client("sns")
    response = client.publish(
        TopicArn=topic_arn,
        Message=json.dumps(order)
    )
    return response["MessageId"]
