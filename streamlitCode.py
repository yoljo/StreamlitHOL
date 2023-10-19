# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

# Get the current credentials
session = get_active_session()

# Write directly to the app
st.title("The Important Questions: What Are We Eating?")
st.write(
    "This guide will help you choose."
)



# Working with Dataframes: https://docs.snowflake.com/en/developer-guide/snowpark/python/working-with-dataframes
restaurants_df = session.table("MARKETPLACE_RESTAURANTS.UNIFIED_SCHEMA.LOCATIONS_SAMPLE")
# st.write(restaurants_df)

with st.expander('Source Table'):
    st.write(restaurants_df)

# Transforming Dataframes: https://docs.snowflake.com/en/developer-guide/snowpark/python/working-with-dataframes#specifying-how-the-dataset-should-be-transformed
open_restaurants_df = session.table("MARKETPLACE_RESTAURANTS.UNIFIED_SCHEMA.LOCATIONS_SAMPLE").filter(col("LOCATION_CLOSED_DATE").isNull())
no_of_open_restaurants = open_restaurants_df.count()
# st.write(no_of_open_restaurants)

zipcodes_df = open_restaurants_df.select(col("LOCATION_ZIP_POSTAL")).distinct()
no_of_zipcodes = zipcodes_df.count()
# st.write(no_of_zipcodes)

# Columns Layout: https://docs.streamlit.io/library/api-reference/layout/st.columns
metric_col_1, metric_col_2 = st.columns(2)
with metric_col_1:
    # Metric Object: https://docs.streamlit.io/library/api-reference/data/st.metric
    st.metric('# of Open Restaurants',"{:,}".format(no_of_open_restaurants))

with metric_col_2:
    # Metric Object: https://docs.streamlit.io/library/api-reference/data/st.metric
    st.metric('# of Zip Codes',"{:,}".format(no_of_zipcodes))

with st.expander('Open Restaurant Details'):
    st.write(open_restaurants_df)

delivery_vs_dinein_options_df = open_restaurants_df.select(
    col("LOCATION_NAME"), 
    (
        col("LOCATION_DELIVERY_DOOR_DASH") 
        + col("LOCATION_DELIVERY_POST_MATES")
        + col("LOCATION_DELIVERY_UBER_EATS")
    ).as_("DELIVERY_OPTIONS"),
    (
        col("LOCATION_RESERVATION_GOOGLE") 
        + col("LOCATION_RESERVATION_OPEN_TABLE")
        + col("LOCATION_RESERVATION_RESY")
    ).as_("RESERVATIONS_OPTIONS")
)
# st.write(delivery_vs_dinein_options_df)

no_of_delivery_restaurants = delivery_vs_dinein_options_df.filter(col("DELIVERY_OPTIONS") > 0).count()
no_of_dinein_restaurants = delivery_vs_dinein_options_df.filter(col("RESERVATIONS_OPTIONS") > 0).count()
delivery_vs_dinein_aggregates_df = session.create_dataframe([['Delivery', no_of_delivery_restaurants],['Dine-in', no_of_dinein_restaurants]], schema=["TYPE", "# OF RESTAURANTS"])
# st.write(delivery_vs_dinein_aggregates_df)

# Bar Chart Object: https://docs.streamlit.io/library/api-reference/data/st.metric
st.bar_chart(data=delivery_vs_dinein_aggregates_df,x="TYPE",y="# OF RESTAURANTS")

# Session State: https://docs.streamlit.io/library/api-reference/session-state
if 'chosen_option_field' not in st.session_state:
    st.session_state.chosen_option_field = ''

# Button: https://docs.streamlit.io/library/api-reference/widgets/st.button
st.write('Will you be ordering in or dining out?')    
delivery_button_col_1, dinein_button_col_2 = st.columns(2)
with delivery_button_col_1:
    # Radio Button Selector: https://docs.streamlit.io/library/api-reference/widgets/st.radio
    delivery_option = st.radio(
    'Delivery Option:',
    ('Door Dash', 'Postmates', 'Uber Eats'),
    label_visibility='collapsed')
    delivery_button = st.button("Choose Delivery")

with dinein_button_col_2:
    dinein_option = st.radio(
    'Dine-in Option:',
    ('Google', 'Open Table', 'Resy'),
    label_visibility='collapsed')
    dinein_button = st.button("Choose Dine-in")

# Session State: https://docs.streamlit.io/library/api-reference/session-state
if delivery_button:
    if delivery_option == 'Door Dash': st.session_state.chosen_option_field = 'LOCATION_DELIVERY_DOOR_DASH' 
    if delivery_option == 'Postmates': st.session_state.chosen_option_field = 'LOCATION_DELIVERY_POST_MATES' 
    if delivery_option == 'Uber Eats': st.session_state.chosen_option_field = 'LOCATION_DELIVERY_UBER_EATS'

if dinein_button:
    if dinein_option == 'Google':     st.session_state.chosen_option_field = 'LOCATION_RESERVATION_GOOGLE' 
    if dinein_option == 'Open Table': st.session_state.chosen_option_field = 'LOCATION_RESERVATION_OPEN_TABLE' 
    if dinein_option == 'Resy':       st.session_state.chosen_option_field = 'LOCATION_RESERVATION_RESY'

if 'chosen_restaurant' not in st.session_state:
    st.session_state.chosen_restaurant = ''

if st.session_state.chosen_option_field != '':
    final_restaurants_df = open_restaurants_df.filter(col(st.session_state.chosen_option_field) == 1)
    
    # Dropdown Selectbox: https://docs.streamlit.io/library/api-reference/widgets/st.selectbox
    restaurant_selector = st.selectbox(
    'Restaurant Options:',
    final_restaurants_df.select(col("LOCATION_NAME")).distinct().toPandas(),
    label_visibility='visible')

    choose_button = st.button("Choose Restaurant", use_container_width = True)
    if choose_button:
        st.session_state.chosen_restaurant = restaurant_selector




        
if st.session_state.chosen_restaurant != '':
    final_restaurant = final_restaurants_df.filter(col("LOCATION_NAME") == restaurant_selector).select(col("LOCATION_ID"), col("LOCATION_NAME"),col("LOCATION_FULL_ADDRESS")).collect()
    
    restaurant_details_col_1, restaurant_feedback_col_2 = st.columns(2)
    with restaurant_details_col_1:
        final_restaurant_id = st.text_input(f"**Restaurant ID:**", final_restaurant[0][0])
        final_restaurant_name = st.text_input(f"**Restaurant Name:**", final_restaurant[0][1])
        final_restaurant_address = st.text_input(f"**Restaurant Address:**", final_restaurant[0][2])
    with restaurant_feedback_col_2:
        final_restaurant_comments = st.text_input('Feedback:')
        final_restaurant_rating = st.radio(
            "Restaurant Score:",
            ('1', '2', '3', '4', '5'),
            index=0,
            horizontal=True
        )

    feedback_submitted = st.button("Submit your Feedback! (Save to Snowflake Table)", use_container_width = True)
    if feedback_submitted:
        with st.spinner("Submitting"):
            session.sql("CREATE DATABASE IF NOT EXISTS STREAMLIT_LAB;").collect()
            session.sql("CREATE TABLE IF NOT EXISTS STREAMLIT_LAB.PUBLIC.USER_FEEDBACK (LOCATION_ID VARCHAR, LOCATION_NAME VARCHAR, LOCATION_ADDRESS VARCHAR, USER_COMMENTS VARCHAR, USER_RATING VARCHAR)").collect()    
            
            final_restaurant_feedback = session.create_dataframe(
                [[final_restaurant_id, final_restaurant_name, final_restaurant_address, final_restaurant_comments, final_restaurant_rating]],
                schema=["LOCATION_ID", "LOCATION_NAME", "LOCATION_ADDRESS", "USER_COMMENTS", "USER_RATING"],
            )
            final_restaurant_feedback.write.save_as_table("STREAMLIT_LAB.PUBLIC.USER_FEEDBACK", mode="append")
            
            st.success("✅ Successfully saved your rating!")
            # Celebrate: https://docs.streamlit.io/library/api-reference/status/st.snow
            st.snow()

            feedback_history = session.table("STREAMLIT_LAB.PUBLIC.USER_FEEDBACK").collect()
            with st.expander('Feedback History'):
                st.write(feedback_history)
