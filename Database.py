import pymysql
from datetime import datetime
import time
import copy
sale_products_count = 0
client_db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'dilpasand_offline_fast_test'
}


def get_records_from_client():
    try:
        connection = pymysql.connect(**client_db_config)
        cursor = connection.cursor()

        sql = f"SELECT * FROM sale_counter WHERE is_merge = 0"
        cursor.execute(sql)
        records = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        records_dicts = []
        for record in records:
            records_dicts.append(dict(zip(columns, record)))

        return records_dicts
    except pymysql.Error as error:
        print("Error retrieving Sale from client database:", error)
        return []
    finally:
        if 'connection' in locals() and connection.open:
            cursor.close()
            connection.close()

def get_sale_products_for_sale(sale_id):
    try:
        connection = pymysql.connect(**client_db_config)
        cursor = connection.cursor()

        sql = "SELECT * FROM sale_product_counter WHERE sale_id = %s"
        cursor.execute(sql, (sale_id,))
        records = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        records_dicts = []
        for record in records:
            
            records_dicts.append(dict(zip(columns, record)))

        return records_dicts
    except pymysql.Error as error:
        print(f"Error retrieving Sale Products for sale_id {sale_id} from client database:", error)
        return []
    finally:
        if 'connection' in locals() and connection.open:
            cursor.close()
            connection.close()

def update_records_in_client(records):
    try:
        connection = pymysql.connect(**client_db_config)
        cursor = connection.cursor()

        for record in records:
            update_sql = "DELETE FROM sale_counter WHERE sale_id = %s"
            cursor.execute(update_sql, (record['sale_id'],)) 
            connection.commit()
            
            update_product_sql = "DELETE FROM sale_product_counter WHERE sale_id = %s"
            cursor.execute(update_product_sql, (record['sale_id'],)) 
            connection.commit()

        print("Records updated in client database successfully")
    except pymysql.Error as error:
        print("Error updating records in client database:", error)

def insert_records_into_server(records):
    global sale_products_count
    try:
        connection = pymysql.connect(**client_db_config)
        cursor = connection.cursor()
        sale_products_count = 0
        for record in records:
            # Insert sale record
            sale_columns = ", ".join(record.keys())
            sale_values_template = ', '.join(['%s'] * len(record))
            sale_sql = f"INSERT INTO sale ({sale_columns}) VALUES ({sale_values_template})"
            cursor.execute(sale_sql, tuple(record.values()))
            connection.commit()
            sale_data_copy = copy.deepcopy(record)
            sale_id = sale_data_copy['sale_id']
            # print(sale_id)
         
            
            sale_products = get_sale_products_for_sale(record['sale_id'])
            sale_products_count = len(sale_products)
            for sale_product in sale_products:
                
                sale_product['sale_id'] = sale_id
                sale_product.pop('sale_product_id', None)
                sale_product_columns = ", ".join(sale_product.keys())
                sale_product_values_template = ', '.join(['%s'] * len(sale_product))
                sale_product_sql = f"INSERT INTO sale_product ({sale_product_columns}) VALUES ({sale_product_values_template})"
                cursor.execute(sale_product_sql, tuple(sale_product.values()))
                connection.commit()
            connection.commit()

        print("Records inserted into server successfully")
    except pymysql.Error as error:
        print("Error inserting records into server database:", error)
    finally:
        if 'connection' in locals() and connection.open:
            cursor.close()
            connection.close()

def main():
    
    records = get_records_from_client()
    
    if records:
        insert_records_into_server(records)
        update_records_in_client(records)

    print(len(records), "Sales uploaded")

if __name__ == "__main__":
    while True:
        time.sleep(5)
        main()
