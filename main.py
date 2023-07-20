import mysql.connector

# Replace these values with your actual MySQL credentials and database
# information
mysql_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'pentafox'
}


def connect_to_mysql():
    try:
        # Connect to the MySQL server
        connection = mysql.connector.connect(**mysql_config)

        if connection.is_connected():
            print("Connected to MySQL database!")
            return connection

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None


def create_table(connection, table_name, columns):
    try:
        cursor = connection.cursor()
        column_defs = ', '.join(columns)
        query = f"CREATE TABLE {table_name} ({column_defs})"
        cursor.execute(query)
        connection.commit()
        print(f"Table '{table_name}' created successfully.")
        cursor.close()

    except mysql.connector.Error as err:
        print(f"Error: {err}")


def insert_data(connection, table_name, data):
    try:
        cursor = connection.cursor()
        placeholders = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        cursor.execute(query, data)
        connection.commit()
        print("Data inserted successfully.")
        cursor.close()

    except mysql.connector.Error as err:
        print(f"Error: {err}")


def bulk_insert(connection, table_name, data):
    try:
        cursor = connection.cursor()
        placeholders = ', '.join(['%s'] * len(data[0]))
        query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        cursor.executemany(query, data)
        connection.commit()
        print("Bulk data inserted successfully.")
        cursor.close()

    except mysql.connector.Error as err:
        print(f"Error: {err}")


def update_data(connection, table_name, set_values, condition):
    try:
        cursor = connection.cursor()
        set_clause = ', '.join([f"{key} = %s" for key in set_values.keys()])
        query = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"
        cursor.execute(query, tuple(set_values.values()))
        connection.commit()
        print("Data updated successfully.")
        cursor.close()

    except mysql.connector.Error as err:
        print(f"Error: {err}")


def delete_data(connection, table_name, condition):
    try:
        cursor = connection.cursor()
        query = f"DELETE FROM {table_name} WHERE {condition}"
        cursor.execute(query)
        connection.commit()
        print("Data deleted successfully.")
        cursor.close()

    except mysql.connector.Error as err:
        print(f"Error: {err}")


# This procedure is used to insert data as it is the frequently used option in
# a company as recruiting is often happening...
def run_procedure(connection, arg1, arg2, arg3):
    try:
        cursor = connection.cursor()
        procedure_name = "add_data"
        args = (arg1, arg2, arg3)
        cursor.callproc(procedure_name,args)
        connection.commit()
        print("Data inserted successfully..")
        cursor.close()


    except mysql.connector.Error as err:
        print(f"Error: {err}")


def select_data(connection, table_name):
    try:
        cursor = connection.cursor()
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return []


if __name__ == "__main__":
    connection = connect_to_mysql()
    if connection:
        # Test the functions here
        table_name = "Company_table"
        columns = ["id INT AUTO_INCREMENT PRIMARY KEY", "name VARCHAR(255)",
                   "age INT"]
        create_table(connection, table_name, columns)

        # ch is the variable which is used as choice as it is a
        # menu-driven code
        ch = 15
        # Menu
        while ch != 0:
            print("\nMenu:\n1.Insert data\n2.Update data\n3.Delete data\n"
                  "4.Insert bulk data\n5.Run procedure\n6.Select data\n")
            ch = int(input("Enter your choice:"))
            if ch == 1:
                print("Insertion of data:")
                print("--------------------")
                id1 = int(input("Enter id:"))
                name = input("Enter name:")
                age = int(input("Enter age:"))
                data_to_insert = (id1, name, age)
                insert_data(connection, table_name, data_to_insert)

            elif ch == 2:
                print("Updation of data:")
                print("--------------------")
                data = input("Enter data to be updated:")
                val = int(input("Enter value to be updated:"))
                name1 = input("Enter whose data to be updated:")
                data_to_update = {f"{data}": val}
                update_data(connection, table_name, data_to_update,
                            f"name='{name1}'")

            elif ch == 3:
                print("Deletion of data:")
                print("--------------------")
                name1 = input("Enter name:")
                data_to_delete = f"name='{name1}'"
                delete_data(connection, table_name, data_to_delete)

            elif ch == 4:
                print("Insertion of bulk data:")
                print("------------------------")
                data_for_bulk_insert = []
                num = int(input("Enter number of inputs:"))
                for i in range(1,num+1):
                    id1 = int(input("Enter id:"))
                    name1 = input("Enter name:")
                    age1 = int(input("Enter age:"))
                    data_for_bulk_insert.append((id1, name1, age1))
                bulk_insert(connection, table_name, data_for_bulk_insert)

            elif ch == 5:
                print("Running procedure")
                print("------------------")
                procedure_name = "add_data"
                id1 = int(input("Enter id:"))
                name1 = input("Enter name:")
                age1 = int(input("Enter age:"))
                # procedure_args = [id1, name1, age1]
                run_procedure(connection, id1, name1, age1)

            elif ch == 6:
                # Select data
                print("Display data:")
                print("---------------")
                result = select_data(connection, table_name)
                print("Selected data:")
                for row in result:
                    print(row)
        connection.close()
