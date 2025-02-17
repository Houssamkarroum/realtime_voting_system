import random
import requests
import psycopg2
import simplejson as json
from confluent_kafka import SerializingProducer

BASE_URL = 'https://randomuser.me/api/?nat=gb'
random.seed(42)

def create_table(conn, cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS candidates (
        candidat_id VARCHAR(255) PRIMARY KEY,
        candidat_name VARCHAR(255) NOT NULL,
        party_affiliation VARCHAR(255) NOT NULL,
        biography text ,
        campaign text ,
        photo_url text
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS voters (
        voter_id VARCHAR(255) PRIMARY KEY,
        voter_name VARCHAR(255),
        date_of_birth DATE,
        gender VARCHAR(255) ,
        nationality VARCHAR(255) ,
        registration_number VARCHAR(255) ,
        address_street VARCHAR(255),
        address_city VARCHAR(255),
        address_state VARCHAR(255),
        address_country VARCHAR(255),
        address_postcode VARCHAR(255),
        email VARCHAR(255),
        phone_number VARCHAR(255),
        picture TEXT,
        registered_age INTEGER
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS votes (
        voter_id VARCHAR(255),
        candidate_id VARCHAR(255),
        voting_time TIMESTAMP,
        vote int DEFAULT 1,
        PRIMARY KEY (voter_id , candidate_id)
        )
        """
    )

    conn.commit()

def insert_candidate(conn, cur, candidat_id, candidat_name, party_affiliation, biography, campaign, photo_url):
    try:
        cur.execute(
            """
            INSERT INTO candidates (candidat_id, candidat_name, party_affiliation, biography, campaign, photo_url)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (candidat_id) DO UPDATE 
            SET candidat_name = EXCLUDED.candidat_name,
                party_affiliation = EXCLUDED.party_affiliation,
                biography = EXCLUDED.biography,
                campaign = EXCLUDED.campaign,
                photo_url = EXCLUDED.photo_url
            """,
            (candidat_id, candidat_name, party_affiliation, biography, campaign, photo_url)
        )
        conn.commit()
        print("Candidate inserted/updated successfully.")
    except Exception as e:
        conn.rollback()
        print("Error inserting candidate:", e)

def generate_voter_data():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        return {
            "voter_id": user_data['login']['uuid'],
            "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data['dob']['date'],
            "gender": user_data['gender'],
            "nationality": user_data['nat'],
            "registration_number": user_data['login']['username'],
            "address": {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": user_data['location']['city'],
                "state": user_data['location']['state'],
                "country": user_data['location']['country'],
                "postcode": user_data['location']['postcode']
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data['registered']['age']
        }
    else:
        return "Error fetching data"

def insert_voters(conn, cur, voter):
    cur.execute("""INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality, registration_number, address_street, address_city, address_state, address_country, address_postcode, email, phone_number, picture, registered_age)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s)
                        """,
                (voter["voter_id"], voter['voter_name'], voter['date_of_birth'], voter['gender'],
                 voter['nationality'], voter['registration_number'], voter['address']['street'],
                 voter['address']['city'], voter['address']['state'], voter['address']['country'],
                 voter['address']['postcode'], voter['email'], voter['phone_number'],
                 voter['picture'], voter['registered_age'])
                )
    conn.commit()
    print("Voter inserted/updated successfully.")

def delivery_report(err,msg):
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print(f"Message delivery successful to {msg.topic()}")


if __name__ == '__main__':
    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})
    try:
        conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
        cur = conn.cursor()
        create_table(conn,cur)
        cur.execute(
            """SELECT * FROM candidates"""
        )
        candidates = cur.fetchall()
        print(candidates)
        '''    if len(candidates) == 0:
            insert_candidate(
                conn, cur,
                candidat_id="CAND1",
                candidat_name="Aziz Akhannouch",
                party_affiliation="the National Rally of Independents party",
                biography="Aziz Akhannouch has been involved in politics for over 14 years...",
                campaign="A better future for me...",
                photo_url="https://upload.wikimedia.org/wikipedia/commons/8/87/Fumio_Kishida_and_Aziz_Akhannouch_before_the_funeral_of_Shinzo_Abe_%281%29_%28cropped%29.jpg"
            )
            insert_candidate(
                conn, cur,
                candidat_id="CAND12",
                candidat_name="Abdelilah Benkirane",
                party_affiliation="The Justice and Development Party",
                biography="Benkirane became Prime Minister on 29 November 2011, The Justice and Development Party retained the majority of seats in the 2016 Moroccan general election",
                campaign="A better future for all...",
                photo_url="https://upload.wikimedia.org/wikipedia/commons/4/40/Abdelilah_Benkirane_2014-08-05.jpg"
            )
            insert_candidate(
                conn, cur,
                candidat_id="CAND123",
                candidat_name="Abdellatif Wahbi",
                party_affiliation="Authenticity and Modernity Party (PAM)",
                biography="Abdellatif Wahbi is a prominent figure in Moroccan politics. He is a member of the Authenticity and Modernity Party (PAM), which is a political party in Morocco",
                campaign="NEVER JUDGE...",
                photo_url="https://www.moroccojewishtimes.com/wp-content/uploads/2020/02/wahbi-1-678x381.jpg"
            )
            conn.commit()
            '''
        for i in range(1001):
            voter_data = generate_voter_data()
            insert_voters(conn,cur,voter_data)
            producer.produce(
                "voters_topic",
                key=voter_data['voter_id'],
                value=json.dumps(voter_data),
                on_delivery=delivery_report
            )

            producer.flush()

    except Exception as e:
        print(e)