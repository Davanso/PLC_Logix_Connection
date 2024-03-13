import mysql.connector   # Import the library for connecting to the MySQL database
from pylogix import PLC  # Import the library for communication with the PLC Logix
import schedule          # Import the library for task scheduling
import time              # Import the time library for time management
import json              # Import the JSON library for file operations

def read_and_insert_data():
    # Load the access information from the JSON file
    with open((r'Grafana\config.json'), 'r') as f:
        config = json.load(f)

    # Configure the connection to the MySQL database
    db_connection = mysql.connector.connect(
        host=config['mysql']['host'],
        user=config['mysql']['user'],
        password=config['mysql']['password'],
        database=config['mysql']['database']
    )

    # Create a cursor to execute SQL queries
    cursor = db_connection.cursor()

    # Configure the connection to the PLC
    with PLC() as comm:
        comm.IPAddress = 'your_plc_ip'

        # List of tags you want to read
        tags_to_read = ['Tag1', 'Tag2', 'Tag3', 'Tag4', 'Tag5', 'Tag6']

        for tag_name in tags_to_read:
            try:
                ret = comm.Read(tag_name)

                # Insert the read data into the MySQL table
                sql = "INSERT INTO your_table (tag_name, tag_value, tag_status) VALUES (%s, %s, %s)"
                val = (ret.TagName, ret.Value, ret.Status)
                cursor.execute(sql, val)

                # Confirm the transaction
                db_connection.commit()

                print("Data for tag", tag_name, "successfully inserted.")
            except Exception as e:
                print("Error reading or inserting data for tag", tag_name + ":", str(e))

    # Close the connection to the database
    cursor.close()
    db_connection.close()

# Schedule the function to run every 10 seconds
schedule.every(10).seconds.do(read_and_insert_data)

# Loop to keep the program running
while True:
    schedule.run_pending()
    time.sleep(1)
