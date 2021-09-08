
import argparse
import random
import json
import datetime, time
from google.cloud import pubsub_v1 # google-cloud-pubsub==2.7.0


async def write_to_pubsub(publisher_client, project_id, topic_id, json_payload):
    try:
        topic_path = publisher_client.topic_path(project_id, topic_id)
        
        data = json.dumps(json_payload).encode('utf-8')
        
        future = publisher_client.publish(topic_path, data)
        #print(future.result())
        #print(f'[ INFO ] Published message to {topic_path}.')
    except Exception as e:
        print(f'[ EXCEPTION ] {e}')


if __name__ == "__main__":
    
    # Parse Args
    parser = argparse.ArgumentParser()
    parser.add_argument('--gcp_project_id',  type=str, help='Google Cloud Project ID',           required=True)
    parser.add_argument('--pubsub_topic_id', type=str, help='Google PubSub Topic ID',            required=True)
    parser.add_argument('--event_count',     type=int, help='Number of events to simulate',      required=True)
    parser.add_argument('--event_delay',     type=int, help='Delay (in seconds) between events', required=True)
    args = parser.parse_args()
    
    # Initialize PubSub Client
    publisher_client  = pubsub_v1.PublisherClient()
    
    # Initial Variables
    random.seed(12345)
    current_timestamp = datetime.datetime.strptime('2021-01-01 00:00:00','%Y-%m-%d %H:%M:%S')
    usernames = [
        'overkill','zodiac','thebully','riotstarter','55pixels','alpha','beta','hyperfox','spyshooter','joker'
    ]
    
    # Simulate events
    print('[ INFO ] Starting simulation')
    tic = time.perf_counter()
    for i in range(args.event_count):
        current_timestamp = current_timestamp + datetime.timedelta(seconds=random.randint(1,30))
        text_message = f'Chat message {i} at {datetime.datetime.now()}'
        payload = {
            'username':  random.choice(usernames),
            "timestamp": time.mktime(datetime.datetime.now().timetuple()), # unix timestamp
            'text':      text_message
        }
        
        # Write to PubSub
        write_to_pubsub(publisher_client, args.gcp_project_id, args.pubsub_topic_id, payload)
        
        # Sleep
        if args.event_delay > 0:
            time.sleep(args.event_delay)
    
    toc = time.perf_counter()
    total_seconds = toc - tic
    events_per_second = args.event_count / total_seconds
    
    print(f'[ INFO ] Simulation Complate')
    print(f'[ INFO ] Events per second: {events_per_second}')
    print(f'[ INFO ] Seconds: {total_seconds}')



#ZEND