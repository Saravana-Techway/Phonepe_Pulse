import plotly.express as px
import requests
class phonepe_graph:
    def aggregated_transaction_graph(self,aggregated_transaction, state):
        # Filter the aggregated_user DataFrame for the specified year
        agg_tran_df = aggregated_transaction[aggregated_transaction["State"] == state].reset_index(drop=True)

        # Create static treemap chart
        agg_tran_graph = px.treemap(agg_tran_df,
                        path=["Year", "Quarter", "Transaction_Type"],
                        values="Transaction_Amount",
                        color="Transaction_Type",
                        color_discrete_sequence=px.colors.qualitative.Bold,
                        title=f"Performances of Transaction Sector - {state}",
                        hover_name="Transaction_Type",  # Set the hover name to "Transaction_Type"
                        hover_data={"State": True,"Quarter": True, "Total_Transactions": True, "Transaction_Amount": True},
                        height=800)
        return agg_tran_graph

    def aggregated_transaction_graph_detail(self, aggregated_transaction, state, year, qtr):
        agg_tran_det_df = aggregated_transaction[(aggregated_transaction["State"] == state) &
                                       (aggregated_transaction["Year"] == year) &
                                       (aggregated_transaction["Quarter"] == qtr)].reset_index(drop=True)

        # Create pie chart
        agg_tran_det_graph = px.pie(agg_tran_det_df, values="Transaction_Amount",
                     names="Transaction_Type",
                     title=f"Transaction Sector Performance - {state} For the Year {year}",
                     color="Transaction_Type",
                     color_discrete_sequence=px.colors.qualitative.Bold,
                     height=800,
                     hole=0.5)
        return agg_tran_det_graph

    def aggregated_user_graph(self, aggregated_user, year):
        # Filter the aggregated_user DataFrame for the specified year
        agg_user_df = aggregated_user[aggregated_user["Year"] == year].reset_index(drop=True)

        # Create static treemap chart
        agg_user_graph = px.treemap(agg_user_df,
                         path=["State", "Quarter", "Device_Brand"],
                         values="User_Count",
                         color="State",
                         color_discrete_sequence=px.colors.qualitative.Bold,
                         title=f"Smart-Device's Brand Utilization - {year}",
                         hover_name="Device_Brand",  # Set the hover name to "Brands"
                         hover_data={"Quarter": True, "User_Count": True, "Device_Share_Percentage": True},
                         height=800)
        return agg_user_graph

    def aggregated_user_graph_detail(self, aggregated_user, state, year, qtr):

        agg_user_det_df = aggregated_user[(aggregated_user["State"] == state) &
                                (aggregated_user["Year"] == year) &
                                (aggregated_user["Quarter"] == qtr)].reset_index(drop=True)

        # Create pie chart
        agg_user_det_graph = px.pie(agg_user_det_df, values="Device_Share_Percentage",
                     names="Device_Brand",
                     title=f"Smart-Device's Brand Utilization - {state} For the Year {year}",
                     color="Device_Brand",
                     color_discrete_sequence=px.colors.qualitative.Bold,
                     height=800,
                     hole=0.5)
        return agg_user_det_graph

    def top_transaction_graph(self, top_transaction, state, year):

        top_trans_df = top_transaction[(top_transaction["State"] == state) & (top_transaction["Year"] == year)].reset_index(drop=True)

        # Create a sunburst chart to visualize transaction metrics across quarters and pincodes
        top_trans_graph = px.sunburst(top_trans_df,
                          path=["Quarter", "Pincode"],
                          values="Transaction_Amount",
                          title=f"Area wise Transaction Report - {state} For the Year {year}",
                          template="plotly_white",
                          color="Pincode",  # Color segments based on Pincodes
                          hover_data={"Total_Transactions": True, "Transaction_Amount": True},
                          )

        # Update layout and axis labels
        top_trans_graph.update_layout(
            font=dict(family="Arial", size=12, color="black"),
            plot_bgcolor='White',  # Set background color
            hoverlabel=dict(font_size=12, font_family="Arial"),

        )
        return top_trans_graph

    def top_user_graph(self, top_users, state, year):
        top_users_df = top_users[(top_users["State"] == state) & (top_users["Year"] == year)].reset_index(drop=True)
       # Create grouped bar chart
        top_users_graph = px.bar(top_users_df,
                     x="Quarter",
                     y="User_Count",
                     color="Pincode",
                     title=f"Area wise Registered Users Report - {state} For the Year {year}",
                     labels={"Quarter": "Quarter", "User_Count": "Registered Users Count"},
                     barmode="group",
                     hover_data=["Pincode"],
                     category_orders={"Quarter": sorted(top_users_df["Quarter"].unique())}
                     )

        # Update layout and axis labels
        top_users_graph.update_layout(
            xaxis_title="Quarter",
            yaxis_title="Registered Users Count",
            font=dict(family="Arial", size=12, color="black"),
            plot_bgcolor='white',
            hovermode="x",
            hoverlabel=dict(font_size=12, font_family="Arial"),
            autosize=True
        )
        return top_users_graph

    def map_transaction_user(self,map_tran_user):
        # Filter the merged DataFrame for the specified year
        map_tran_user_df = map_tran_user
        # map_tran_user_df = map_tran_user_df.groupby(["State"]).agg({
        #     "Total_Transactions": "sum",
        #     "Transaction_Amount": "sum",
        #     "User_Count": "sum",
        #     "Total_Used_Apps": "sum"
        # }).reset_index()

        # URL to fetch the GeoJSON data for Indian states
        geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"

        # Fetch the GeoJSON data for Indian states
        response = requests.get(geojson_url)
        geojson_data = response.json()
        # Create choropleth map using Plotly Express
        map_tran_user_graph = px.choropleth(
            map_tran_user_df,
            geojson=geojson_data,
            locations="State",
            featureidkey="properties.ST_NM",
            color="State",
            hover_name="State",
            hover_data={
                "Transaction_Amount": True,
                "Total_Transactions": ":,",
                "User_Count": ":,",
                "Total_Used_Apps": ":,"
            },
            title=f"Overall Users and Revenue Report ",
            color_continuous_scale="Viridis",
            range_color=(0, map_tran_user_df["User_Count"].max()),  # Set the color range based on Transaction_count
            width=800,
            height=600
        )

        map_tran_user_graph.update_geos(visible=False)
        return map_tran_user_graph

