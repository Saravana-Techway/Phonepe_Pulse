import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from Database_Phonepe import Database_Management
from phonepe_json import json_file_df
from Phonepe_visual import phonepe_graph
pd.options.display.float_format = '{:.2f}'.format

# Create an instance of Database_Management
db_manage = Database_Management()
json_ins = json_file_df()
graph_ins = phonepe_graph()

custom_css = """
<style>
body {
    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; /* Use a clean sans-serif font */
    background-color: #f5f5f5; /* Set a light gray background color */
    color: #333; /* Set text color to dark gray */
    margin: 0; /* Reset margin */
    padding: 0; /* Reset padding */
    display: flex;
    justify-content: center;
    align-items: center;
    height: 100vh; /* Full viewport height */
}

.container {
    text-align: center;
    padding: 50px;
    background-color: #ffffff; /* White background */
    border-radius: 10px;
    box-shadow: 0px 5px 15px rgba(0, 0, 0, 0.1); /* Soft box shadow */
    max-width: 80%; /* Limit container width */
}

.header {
    font-size: 48px;
    margin-bottom: 20px;
    color: #6f42c1; /* Purple color for header */
}

.subheader {
    font-size: 24px;
    color: #888; /* Light gray color for subheader */
    margin-bottom: 30px;
}

.btn {
    display: inline-block;
    padding: 12px 24px;
    font-size: 18px;
    font-weight: bold;
    color: #ffffff;
    background-color: #6f42c1; /* Purple color for button */
    border: none;
    border-radius: 5px;
    cursor: pointer;
    text-decoration: none;
    transition: background-color 0.3s ease;
}

.btn:hover {
    background-color: #8248c9; /* Darker purple on hover */
}
</style>
"""

# Render the custom CSS within the Streamlit app
st.markdown(custom_css, unsafe_allow_html=True)

# Streamlit content using the defined styles

st.markdown("<h1 class='header'>PhonePe Data Exploration</h1>", unsafe_allow_html=True)
st.markdown("<p class='subheader'>Explore the Data Visualization!</p>", unsafe_allow_html=True)
st.markdown("</div>", unsafe_allow_html=True)

#Render homepage content within a styled container
with st.container():

    # Button to explore data
    if st.button("Explore Data", key="explore_button"):
        st.success("Let's get started with data exploration!")


# Sidebar menu for navigation and options
with st.sidebar:
    st.title("Navigation Menu")

    # Selection box for main options
    select_option = st.selectbox("Select Option", ["HOME", "DATA EXPLORE", "TOP CHARTS"])
    # Sidebar title with emoji
    st.sidebar.title("PhonePe Actions 📱💸")

    # Open PhonePe App (using a custom URL scheme)
    st.sidebar.markdown("- [Open PhonePe App](phonepe://open)")

    # Send Money via PhonePe (simulated link)
    st.sidebar.markdown("- [Send Money via PhonePe](https://fake.phonepe.com/send-money)")

    # View Transaction History on PhonePe (simulated link)
    st.sidebar.markdown("- [View Transaction History](https://fake.phonepe.com/transaction-history)")

    # PhonePe Customer Support (simulated link)
    st.sidebar.markdown("- [PhonePe Customer Support](https://fake.phonepe.com/support)")

def df_state_namcon(df):
    df["State"] = df["State"].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar')
    df["State"] = df["State"].str.title()
    df["State"] = df["State"].str.replace('Dadra-&-Nagar-Haveli-&-Daman-&-Diu', 'Dadra and Nagar Haveli and Daman and Diu')
    df["State"] = df["State"].str.replace("-", " ")

# Create the database and tables if they don't exist
db_manage.create_database()
db_manage.table_creation()


agg_trans = pd.DataFrame(json_ins.agg_transactions())
df_state_namcon(agg_trans)
table_name="agg_transactions"
db_manage.df_to_sql(agg_trans, table_name)

agg_user=pd.DataFrame(json_ins.agg_users())
agg_user["Device_Share_Percentage"] = agg_user["Device_Share_Percentage"].apply(lambda x: "{:.2f}".format(x * 100))
df_state_namcon(agg_user)
table_name="agg_users"
db_manage.df_to_sql(agg_user, table_name)

map_user = pd.DataFrame(json_ins.map_users())
df_state_namcon(map_user)
table_name = "map_users"
db_manage.df_to_sql(map_user, table_name)

map_transaction =  pd.DataFrame(json_ins.map_transactions())
df_state_namcon(map_transaction)
table_name = "map_transactions"
db_manage.df_to_sql(map_transaction, table_name)

top_transaction_pincodes = pd.DataFrame(json_ins.top_transactions())
df_state_namcon(top_transaction_pincodes)
table_name="top_transactions"
db_manage.df_to_sql(top_transaction_pincodes, table_name)

top_users_pincodes = pd.DataFrame(json_ins.top_users())
df_state_namcon(top_users_pincodes)
table_name = "top_users"
db_manage.df_to_sql(top_users_pincodes, table_name)

agg_transaction_query = "select State, Year, Quarter, Transaction_Type, Total_Transactions, Transaction_Amount from agg_transactions;"
agg_transaction_query_result = db_manage.Query_Output(agg_transaction_query)
if agg_transaction_query_result:
    agg_transactions_df = pd.DataFrame(agg_transaction_query_result, columns=['State', 'Year','Quarter','Transaction_Type','Total_Transactions','Transaction_Amount'])

agg_user_query = "select State, Year, Quarter, Device_Brand, User_Count, Device_Share_Percentage from agg_users;"
agg_user_query_result = db_manage.Query_Output(agg_user_query)
if agg_user_query_result:
    agg_users_df = pd.DataFrame(agg_user_query_result, columns=['State', 'Year','Quarter','Device_Brand','User_Count','Device_Share_Percentage'])

top_transaction_query = "select State, Year, Quarter, Pincode, Total_Transactions, Transaction_Amount from top_transactions;"
top_transaction_query_result = db_manage.Query_Output(top_transaction_query)
if top_transaction_query_result:
    top_transactions_df = pd.DataFrame(top_transaction_query_result, columns=['State', 'Year','Quarter','Pincode','Total_Transactions','Transaction_Amount'])

top_user_query = "select State, Year, Quarter, Pincode, User_Count from top_users;"
top_user_query_result = db_manage.Query_Output(top_user_query)
if top_user_query_result:
    top_users_df = pd.DataFrame(top_user_query_result, columns=['State', 'Year','Quarter','Pincode','User_Count'])

# map_transaction_query = "select a.State as State,a.Year as Year,a.Quarter as Quarter,a.District as District, User_Count,Total_Used_Apps,Total_Transactions,Transaction_Amount from map_users a,map_transactions b where a.State=b.State and a.Year=b.Year and a.Quarter=b.Quarter and a.District=b.District;"
map_transaction_query = "select a.State as State, sum(User_Count) as User_Count,sum(Total_Used_Apps) as Total_Used_Apps,Sum(Total_Transactions) as Total_Transactions,Sum(Transaction_Amount) as Transaction_Amount from map_users a,map_transactions b where a.State=b.State and a.Year=b.Year and a.Quarter=b.Quarter and a.District=b.District group by a.State;"
map_transaction_query_result = db_manage.Query_Output(map_transaction_query)
if map_transaction_query_result:
    # map_transactions_df = pd.DataFrame(map_transaction_query_result, columns=['State', 'Year','Quarter','District','User_Count','Total_Used_Apps','Total_Transactions','Transaction_Amount'])
    map_transactions_df = pd.DataFrame(map_transaction_query_result, columns=['State', 'User_Count', 'Total_Used_Apps','Total_Transactions', 'Transaction_Amount'])

if select_option == "HOME":
    # Display key insights with color and emojis
    st.markdown("### Key Insights 💡📊")
    st.write("- Total number of transactions processed.")
    st.write("- Top states with the highest transaction amounts.")
    st.write("- Most frequent transaction types.")
    st.write("- Distribution of user demographics.")

    st.markdown("---")  # Horizontal rule

    # Update the message with color and emojis
    st.markdown("### Why Explore Your Data? 🕵️‍♂️🔍")
    st.write("Analyzing your data uncovers hidden patterns and actionable insights. 📈✨")

    # Additional styling for each insight and message
    st.markdown("---")  # Horizontal rule

    # Total number of transactions processed
    st.markdown("### Total Number of Transactions Processed 🔄")
    st.write("Understanding the overall volume of transactions provides insights into business activity.")

    # Top states with the highest transaction amounts
    st.markdown("### Top States with Highest Transaction Amounts 🌟📊")
    st.write("Identifying which states generate the most revenue can inform targeted marketing strategies.")

    # Most frequent transaction types
    st.markdown("### Most Frequent Transaction Types 🔄📊")
    st.write("Recognizing popular transaction types helps optimize inventory and service offerings.")

    # Distribution of user demographics
    st.markdown("### Distribution of User Demographics 🧑‍🤝‍🧑🌎")
    st.write("Understanding your user base demographics aids in personalized marketing and product development.")

# Display content based on user selection
if select_option == "TOP CHARTS":
    st.header("Explore Top Charts")

    # Define SQL queries corresponding to user choices
    sql_queries = {
        "Q1: What are all the states have the highest total transaction amounts?": '''
            SELECT state, SUM(transaction_amount) AS transaction_amount
            FROM agg_transactions
            GROUP BY state
            ORDER BY transaction_amount DESC
            LIMIT 10;
        ''',
        "Q2: What are all the states have the lowest total transaction amounts?": '''
            SELECT state, SUM(transaction_amount) AS transaction_amount
            FROM agg_transactions
            GROUP BY state
            ORDER BY transaction_amount ASC
            LIMIT 10;
        ''',
        "Q3: What are all the states have the highest total number of users?": '''
            SELECT state, SUM(User_count) AS Total_User_Count
            FROM agg_users
            GROUP BY state
            ORDER BY Total_User_Count DESC
            LIMIT 10;
        ''',
        "Q4: What are all the states have the lowest total number of users?": '''
            SELECT state, SUM(User_count) AS Total_User_Count
            FROM agg_users
            GROUP BY state
            ORDER BY Total_User_Count ASC
            LIMIT 10;
        ''',
        "Q5: Need to know the top 10 area codes having the highest transaction amount?": '''
            SELECT Pincode, SUM(Transaction_Amount) as Transaction_Amount, SUM(Total_Transactions) as Total_Transactions 
            FROM top_transactions
            GROUP BY Pincode
            ORDER BY Transaction_Amount DESC
            LIMIT 10;
        ''',
        "Q6: Need to know the top 10 area codes having the lowest transaction amount?": '''
            SELECT Pincode, SUM(Transaction_Amount) as Transaction_Amount, SUM(Total_Transactions) as Total_Transactions 
            FROM top_transactions
            GROUP BY Pincode
            ORDER BY Transaction_Amount ASC
            LIMIT 10;
        ''',
        "Q7: What are all the states have the highest number of registered users?": '''
            SELECT state, SUM(User_Count) AS "Registered Users"
            FROM top_users
            GROUP BY state
            ORDER BY 2 DESC
            LIMIT 10;
        ''',
        "Q8: What are all the states have the lowest number of registered users?": '''
            SELECT state, SUM(User_Count) AS "Registered Users"
            FROM top_users
            GROUP BY state
            ORDER BY 2 ASC
            LIMIT 10;
        ''',
        "Q9: Need to know which transaction sector having the highest total transaction amounts?": '''
            SELECT Transaction_Type, SUM(Transaction_amount) AS Total_Transaction_Amount
            FROM agg_transactions
            GROUP BY Transaction_Type
            ORDER BY Total_Transaction_Amount DESC
            LIMIT 10;
        '''
    }

    # Display dropdown menu to select the query
    selected_query = st.selectbox(
        "Select a query:",
        list(sql_queries.keys())
    )

    # Display the selected query above the "Execute Query" button
    st.write(f"Chosen Query: {selected_query}")

    # Execute selected SQL query when the user clicks the button
    if st.button("Execute Query"):
        if selected_query in sql_queries:
            user_query = sql_queries[selected_query]
            # Execute SQL query to fetch data
            query_result = db_manage.Query_Output(user_query)
            # If query result is obtained, create a DataFrame from the result
            if query_result:
                result_df = pd.DataFrame(query_result)
                # Determine the appropriate chart type based on the query choice
                if "transaction_amount" in result_df.columns:
                    # Create a bar chart for transaction amounts
                    fig = px.bar(
                        result_df,
                        x="state",
                        y="transaction_amount",
                        title="Top States Vs Transaction Amounts",
                        color="state",
                        height=650,
                        width=800
                    )
                elif "Total_User_Count" in result_df.columns:
                    # Create a scatter plot for total user counts with correct marker sizes
                    fig = px.area(
                        result_df,
                        x="state",
                        y="Total_User_Count",
                        title="Top States Vs Total User Count",
                        height=650,
                        width=800
                    )
                elif "Transaction_Amount" in result_df.columns and "Total_Transactions" in result_df.columns:
                    # Create a line chart for top transactions by amount
                    result_df["Pincode"]=result_df["Pincode"].astype(str)
                    fig = px.line(
                        result_df,
                        x="Pincode",
                        y="Transaction_Amount",
                        title="Top 10 Transactions based on Transaction Amounts",
                        line_shape="linear",
                        height=650,
                        width=800
                    )
                    # Update x-axis type to categorical
                    fig.update_xaxes(type='category')
                elif "Registered Users" in result_df.columns:
                    # Create a pie chart for registered users per state
                    fig = px.pie(
                        result_df,
                        names="state",
                        values="Registered Users",
                        title="Top 10 States based on Registered Users",
                        height=650,
                        width=800
                    )
                elif "Total_Transaction_Amount" in result_df.columns:
                    # Create a pie chart for transaction types by total transaction amount
                    fig = px.pie(
                        result_df,
                        names="Transaction_Type",
                        values="Total_Transaction_Amount",
                        title="Top Transaction Sectors based on Total Transaction Amount",
                        height=650,
                        width=800
                    )
                # Display the Plotly chart in Streamlit
                st.plotly_chart(fig)

            else:
                st.warning("No data available for the selected query.")

        else:
            st.warning("Invalid query selection. Please select a valid query.")



if select_option == "DATA EXPLORE":
    st.header("Data Exploration")

    # Data exploration category selection
    explore_category = st.radio("Select Exploration Category", ["Aggregated", "Map", "Top"])

    if explore_category == "Aggregated":
        st.subheader("Aggregated Data Exploration")
        explore_type = st.radio("Select Explore Type", ["Transaction", "User"])

        if explore_type == "Transaction":
            st.write("Explore Aggregated Transaction Data")

            selected_state = st.selectbox("Select State", agg_transactions_df["State"].unique())
            # Use the selected year to process the data (example function call)
            data_viz_agg_trans = graph_ins.aggregated_transaction_graph(agg_transactions_df, selected_state)
            st.plotly_chart(data_viz_agg_trans)

            # Allow user to select state, year, and quarter
            selected_state = st.selectbox("Select States", agg_transactions_df["State"].unique())
            selected_year = st.selectbox("Select Years", agg_transactions_df["Year"].unique())
            selected_qtr = st.selectbox("Select Quarters", agg_transactions_df["Quarter"].unique())
            # Use the selected parameters to process the data
            # chart=graph_file.Transaction_details_by_three(agg_transactions_df, state=selected_state, year=selected_year, qtr=selected_qtr)
            data_viz_agg_trans_det = graph_ins.aggregated_transaction_graph_detail(agg_transactions_df, selected_state, selected_year,selected_qtr)
            st.plotly_chart(data_viz_agg_trans_det)

        elif explore_type == "User":
            st.write("Explore Aggregated User Data")

            # Display a selectbox to choose the year for user data with a unique key
            selected_year = st.selectbox("Select Years", agg_users_df["Year"].unique())
            # Use the selected year to process the data (example function call)
            data_viz_agg_users = graph_ins.aggregated_user_graph(agg_users_df, selected_year)
            st.plotly_chart(data_viz_agg_users)

            # Allow user to select state, year, and quarter
            selected_state = st.selectbox("Select State", agg_users_df["State"].unique())
            selected_year = st.selectbox("Select Year", agg_users_df["Year"].unique())
            selected_qtr = st.selectbox("Select Quarter", agg_users_df["Quarter"].unique())
            # Call Brand_usage_details_by_three function with selected parameters
            data_viz_agg_users_det = graph_ins.aggregated_user_graph_detail(agg_users_df, selected_state, selected_year,selected_qtr)
            st.plotly_chart(data_viz_agg_users_det)

    elif explore_category == "Map":
        st.subheader("Map Data Exploration")
        data_viz_map_trans = graph_ins.map_transaction_user(map_transactions_df)
        st.plotly_chart(data_viz_map_trans)


    elif explore_category == "Top":
        st.subheader("Top Data Exploration")
        explore_type = st.radio("Select Explore Type", ["Transaction", "User"])
        if explore_type == "Transaction":
            st.write("Explore Top Transaction Data")

            # Display a selectbox to choose the year for user data with a unique key
            selected_state = st.selectbox("Select State", top_transactions_df["State"].unique())
            selected_year = st.selectbox("Select Year", top_transactions_df["Year"].unique())
            # Call Brand_usage_details_by_three function with selected parameters
            data_viz_top_trans = graph_ins.top_transaction_graph(top_transactions_df, selected_state, selected_year)
            st.plotly_chart(data_viz_top_trans)

        if explore_type == "User":
            st.write("Explore Top User Data")

            # Display a selectbox to choose the year for user data with a unique key
            selected_state = st.selectbox("Select States", top_users_df["State"].unique())
            selected_year = st.selectbox("Select Years", top_users_df["Year"].unique())
            # Call Brand_usage_details_by_three function with selected parameters
            data_viz_top_users = graph_ins.top_user_graph(top_users_df, selected_state, selected_year)
            st.plotly_chart(data_viz_top_users)