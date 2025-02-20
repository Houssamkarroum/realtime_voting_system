import time
from datetime import datetime
import random

import psycopg2
from confluent_kafka import Consumer, KafkaException, KafkaError , SerializingProducer
import simplejson as json

from main import delivery_report

conf = {
    'bootstrap.servers': 'localhost:9092',

}
consumer = Consumer(conf | {
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
                            })

producer = SerializingProducer(conf)

if __name__ == '__main__':
    conn = psycopg2.connect("dbname=voting user=postgres password=postgres host=localhost")
    cur = conn.cursor()

    candidates_query = cur.execute("""
    
                SELECT row_to_json(col)
                FROM (
                SELECT * FROM candidates
                ) col;
                 
                """

                )
    candidates = [candidate[0] for candidate in cur.fetchall()]
    print("Candidates:", candidates)

    if len(candidates) == 0:
        raise Exception("No candidates found")
    else:
        print("Found {} candidates".format(len(candidates)))

    consumer.subscribe(topics=['voters_topic'])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print("Error: %s" % msg.error())
                    break
            else:
                voter = json.loads(msg.value().decode('utf-8'))

                chosen_candidate = random.choice(candidates)
                print("Chosen: {}".format(chosen_candidate))


                vote = voter | chosen_candidate | {
                    'voting_time' : datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    'vote': 1
                }

                try:
                    print("User {} is voting for candidate: {}".format(vote['voter_id'], vote['candidat_id']))

                    print("Attempting to insert:", vote)

                    cur.execute("""
                    INSERT INTO votes (voter_id,candidate_id,voting_time)
                    VALUES (%s, %s, %s)
                    """, (vote['voter_id'], vote['candidat_id'], vote['voting_time'])
                    )
                    conn.commit()
                    producer.produce(
                        topic='votes_topic',
                        key=vote['voter_id'],
                        value=json.dumps(vote),
                        on_delivery=delivery_report
                    )
                    producer.poll(0)
                except Exception as e:
                    print("exception here {}".format(e))
                    conn.rollback()
            time.sleep(0.5)
    except Exception as e:
        print(e)