import time

import numpy as np
import pandas as pd
import psycopg2
import streamlit as st
from kafka import KafkaConsumer
import simplejson as json
from streamlit import bar_chart
import matplotlib.pyplot as plt
from voting import consumer
from streamlit_autorefresh import st_autorefresh



def fetch_voting_stats():
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    curr = conn.cursor()
    #fetch total number of voters
    curr.execute("""
    SELECT COUNT(*) voters_count  FROM voters
    """)
    voters_count = curr.fetchone()[0]
    #fetch_total_candidates
    curr.execute("""
    
    SELECT COUNT(*) candidates_count FROM candidates""")

    candidates_count = curr.fetchone()[0]

    return voters_count, candidates_count

def create_kafka_consumer(topic):
    consumer1 = KafkaConsumer(topic,
                             bootstrap_servers='localhost:9092',
                             auto_offset_reset='earliest',
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')),)
    return consumer1

def fetch_data_from_kafka(consumer):
    messages = consumer.poll(timeout_ms=1000)
    data = []
    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)
    return data

def plot_bar_chart(results):
    data_type = results['candidate_name']
    colors = plt.cm.viridis(np.linspace(0, 1, len(data_type)))
    plt.bar(data_type, results['total_votes'], color=colors)
    plt.xlabel('candidate')
    plt.ylabel('total votes')
    plt.title('total votes per candidates')
    plt.xticks(rotation=90)
    return plt

def plot_donut_chart(data):
    labels = list(data['candidate_name'])
    sizes = list(data['total_votes'])
    colors = plt.cm.viridis(np.linspace(0, 1, len(sizes)))
    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels,autopct='%1.1f%%' , startangle=140 , colors=colors)
    ax.axis('equal')
    plt.title('candidates votes')
    return fig

def side_bar():
    if 'key' not in st.session_state:
        st.session_state['key'] = 'auto'
        st.session_state.key = 'auto'

    st.write(st.session_state.key)
    if st.session_state.get('latest_update') is None:
        st.session_state['latest_update'] = time.time()

    # Initialize the 'refresh_interval' key with a default value if it's not set
    if 'refresh_interval' not in st.session_state:
        st.session_state['refresh_interval'] = 0.5 # default value, can be adjusted

    # Use a slider to select the refresh interval
    refresh_interval = st.slider(
        'Refresh interval (in seconds)',
        min_value=0.5,
        max_value=15.0,
        value=st.session_state['refresh_interval'],
        step=0.5,
        format="%.1f"
    )

    # Save the selected value to session state
    st.session_state['refresh_interval'] = refresh_interval

    # Set the auto-refresh interval (milliseconds)
    st_autorefresh(refresh_interval * 1000, key="auto")

    if st.sidebar.button('refresh Data'):
        update_data()




def pagination_table(table_data, rows_per_page=16):
    # Ensure table_data is a DataFrame
    if not isinstance(table_data, pd.DataFrame):
        table_data = pd.DataFrame(table_data)

    # Initialize session state variables
    if 'current_page' not in st.session_state:
        st.session_state['current_page'] = 1
    if 'sort_column' not in st.session_state:
        st.session_state['sort_column'] = table_data.columns[0]  # Default to first column
    if 'sort_order' not in st.session_state:
        st.session_state['sort_order'] = True  # True = Ascending, False = Descending

    # Dropdown to select sort column
    sort_col = st.selectbox("Sort by:", table_data.columns,
                            index=list(table_data.columns).index(st.session_state['sort_column']))

    # Sort button
    if st.button("Sort"):
        # Toggle sorting order if the same column is selected again
        if sort_col == st.session_state['sort_column']:
            st.session_state['sort_order'] = not st.session_state['sort_order']
        else:
            st.session_state['sort_order'] = True  # Default to ascending if a new column is selected
        st.session_state['sort_column'] = sort_col

    # Apply sorting
    sorted_data = table_data.sort_values(by=st.session_state['sort_column'], ascending=st.session_state['sort_order'])

    # Get total number of pages
    total_rows = len(sorted_data)
    total_pages = (total_rows // rows_per_page) + (1 if total_rows % rows_per_page > 0 else 0)

    # Pagination controls
    col1, col2, col3 = st.columns([1, 2, 1])

    with col1:
        if st.button("⬅️ Previous", disabled=st.session_state['current_page'] == 1):
            st.session_state['current_page'] -= 1

    with col3:
        if st.button("Next ➡️", disabled=st.session_state['current_page'] == total_pages):
            st.session_state['current_page'] += 1

    # Get start and end indices for slicing
    start_idx = (st.session_state['current_page'] - 1) * rows_per_page
    end_idx = start_idx + rows_per_page

    # Display the paginated and sorted data
    st.write(
        f"Page {st.session_state['current_page']} of {total_pages} (Sorted by {st.session_state['sort_column']}, {'Ascending' if st.session_state['sort_order'] else 'Descending'})")

    # Display large table
    st.data_editor(
        sorted_data.iloc[start_idx:end_idx],
        use_container_width=True,
        height=600
    )


def update_data():
    last_refresh = st.empty()
    last_refresh.text(f"last refresh: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    #fetch voting statistics
    voters_count , candidates_count = fetch_voting_stats()

    st.markdown("""---""")
    col1, col2 = st.columns(2)
    col1.metric(f"total voters",voters_count)
    col2.metric(f"Total candidates",candidates_count)

    consumer = create_kafka_consumer(topic_name)
    data = fetch_data_from_kafka(consumer)
    results = pd.DataFrame(data)
    #identify the leading candidat
    results = results.loc[results.groupby('candidate_id')['total_votes'].idxmax()]
    leading_candidat = results.loc[results['total_votes'].idxmax()]

    #display it
    st.markdown("""---""")
    st.header("Leading Candidat")
    col1, col2 = st.columns(2)
    with col1:
        st.image(leading_candidat['photo_url'], width=200 , use_column_width=True)
    with col2:
        st.header(leading_candidat['candidate_name'])
        st.subheader(leading_candidat['party_affiliation'])
        st.subheader('total votes{}'.format(leading_candidat['total_votes']))
    #display statistics and visualisation
    st.markdown("""---""")
    st.header("Voting Statistics")
    results = results[['candidate_id','candidate_name','party_affiliation', 'total_votes']]
    results = results.reset_index(drop=True)
    #Display the bar chart and Donute chart
    col1, col2 = st.columns(2)
    with col1:
        bar_fig = plot_bar_chart(results)
        st.pyplot(bar_fig)
    with col2:
        donut_fig = plot_donut_chart(results)
        st.pyplot(donut_fig)
    st.table(results)

    #fetch data from second topic aggregated turnout by location
    location_consumer = create_kafka_consumer('aggregated_turnout_by_location')
    location_data = fetch_data_from_kafka(location_consumer)
    location_result = pd.DataFrame(location_data)
    st.markdown("""---""")
    # max location voted
    location_result = location_result.loc[location_result.groupby('state')['count'].idxmax()]
    location_result.reset_index(drop=True)
    # disply it
    st.markdown("""---""")
    st.header('location of voters')
    pagination_table(location_result)

st.title("RealTime moroccan Election 2025 simulation Dashboard")
topic_name= "aggregated_votes_per_candidate"
side_bar()
update_data()
