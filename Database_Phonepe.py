from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, VARCHAR,BIGINT,DECIMAL, text,UniqueConstraint
from sqlalchemy.exc import IntegrityError


class Database_Management:
    def __init__(self):

        self.db_connection = f"mysql+mysqlconnector://root:password@localhost:3306"
        self.database_name = "phonepe_pulse_project"
        self.engine = create_engine(self.db_connection)


        # self.db_connection = "mysql+mysqlconnector://admin:Sara9789!@saramysql.cxiugc2cspx0.us-east-2.rds.amazonaws.com:3306"
        # self.database_name = "phonepe_pulse_project"
        # self.engine = create_engine(self.db_connection)

    def create_database(self):
        try:
            # Establish a connection to the MySQL server
            connection = self.engine.connect()

            # Create the database if it doesn't exist
            create_db_statement = text(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
            connection.execute(create_db_statement)

            # Close the connection explicitly
            connection.close()
        except Exception as e:
            print(f"An error occurred while creating the database: {e}")
            raise

    def table_creation(self):
        try:
            # Establish a connection to the MySQL server
            connection = self.engine.connect()

            # Use the database
            use_db_statement = text(f"USE {self.database_name}")
            connection.execute(use_db_statement)

            # Define metadata
            metadata = MetaData()

            # Define tables
            agg_transactions_table = Table(
                'agg_transactions', metadata,
                Column('State', VARCHAR(100)),
                Column('Year', VARCHAR(5)),
                Column('Quarter', VARCHAR(2)),
                Column('Transaction_Type', VARCHAR(100)),
                Column('Total_Transactions', BIGINT),
                Column('Transaction_Amount', DECIMAL(50,2)),
                UniqueConstraint('State', 'Year', 'Quarter', 'Transaction_Type',name='uq_agg_transactions_constraint'),
                schema=self.database_name
            )

            # Create the table if it doesn't exist
            agg_transactions_table.create(self.engine, checkfirst=True)

            agg_users_table = Table(
                'agg_users', metadata,
                Column('State', VARCHAR(100)),
                Column('Year', VARCHAR(5)),
                Column('Quarter', VARCHAR(2)),
                Column('Device_Brand', VARCHAR(100)),
                Column('User_Count', Integer),
                Column('Device_Share_Percentage', Float),
                UniqueConstraint('State', 'Year', 'Quarter', 'Device_Brand', name='uq_agg_users_constraint'),
                schema=self.database_name
            )

            # Create the table if it doesn't exist
            agg_users_table.create(self.engine, checkfirst=True)

            map_transactions_table = Table(
                'map_transactions', metadata,
                Column('State', VARCHAR(100)),
                Column('Year', VARCHAR(5)),
                Column('Quarter', VARCHAR(2)),
                Column('District', VARCHAR(100)),
                Column('Total_Transactions', BIGINT),
                Column('Transaction_Amount', DECIMAL(50,2)),
                UniqueConstraint('State', 'Year', 'Quarter', 'District', name='uq_map_transactions_constraint'),
                schema=self.database_name
            )

            # Create the table if it doesn't exist
            map_transactions_table.create(self.engine, checkfirst=True)

            map_users_table = Table(
                'map_users', metadata,
                Column('State', VARCHAR(100)),
                Column('Year', VARCHAR(5)),
                Column('Quarter', VARCHAR(2)),
                Column('District', VARCHAR(100)),
                Column('User_Count', Integer),
                Column('Total_Used_Apps', Integer),
                UniqueConstraint('State', 'Year', 'Quarter', 'District', name='uq_map_users_constraint'),
                schema=self.database_name
            )

            # Create the table if it doesn't exist
            map_users_table.create(self.engine, checkfirst=True)

            top_transactions_table = Table(
                'top_transactions', metadata,
                Column('State', VARCHAR(100)),
                Column('Year', VARCHAR(5)),
                Column('Quarter', VARCHAR(2)),
                Column('Pincode', VARCHAR(8)),
                Column('Total_Transactions', BIGINT),
                Column('Transaction_Amount', DECIMAL(50,2)),
                UniqueConstraint('State', 'Year', 'Quarter', 'Pincode', name='uq_top_transactions_constraint'),
                schema=self.database_name
            )

            # Create the table if it doesn't exist
            top_transactions_table.create(self.engine, checkfirst=True)

            top_users_table = Table(
                'top_users', metadata,
                Column('State', VARCHAR(100)),
                Column('Year', VARCHAR(5)),
                Column('Quarter', VARCHAR(2)),
                Column('Pincode', VARCHAR(8)),
                Column('User_Count', Integer),
                UniqueConstraint('State', 'Year', 'Quarter', 'Pincode', name='uq_top_users_constraint'),
                schema=self.database_name
            )

            # Create the table if it doesn't exist
            top_users_table.create(self.engine, checkfirst=True)

            # Close the connection explicitly
            connection.close()

        except IntegrityError as ie:
            print(f"An integrity error occurred while creating the tables: {ie}")
            raise

        except Exception as e:
            print(f"An error occurred while creating the database: {e}")
            raise


    def df_to_sql(self, df_details, table_name):
        try:
            # Establish a connection to the MySQL server
            connection = self.engine.connect()

            # Use the database
            use_db_statement = text(f"USE {self.database_name}")
            connection.execute(use_db_statement)

            # Push the dataframe to sql
            df_details.to_sql(table_name, con=self.engine, if_exists='append', index=False, schema=self.database_name)

            # Close the connection explicitly
            connection.close()

        except IntegrityError as e:
            # Handle the IntegrityError (duplicate entry)
            # print(f"Duplicate entry error: {e}")
            pass
            # You can log the error or handle it in any other appropriate way
        except Exception as e:
            # Handle other exceptions
            print(f"An error occurred: {e}")
            raise


    def Query_Output(self, Chnl_Chk_Query):
        try:
            # Establish a connection to the MySQL server
            connection = self.engine.connect()

            # Use the database
            use_db_statement = text(f"USE {self.database_name}")
            connection.execute(use_db_statement)

            select_statement = text(Chnl_Chk_Query)
            result = connection.execute(select_statement)
            channel_result = result.fetchall()

            if channel_result:
                return channel_result
            else:
                return None
            connection.close()

        except Exception as e:
            print(f"An error occurred while creating the database: {e}")
            raise