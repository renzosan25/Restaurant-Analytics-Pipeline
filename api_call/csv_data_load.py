import psycopg2
from data_insert import connect_to_db



def csv_export():
    try:
        conn = connect_to_db()
        cursor = conn.cursor()

        sql = "COPY (SELECT * FROM dev.restaurants_competitiveness) TO STDOUT WITH CSV HEADER DELIMITER ','"
        with open("/opt/airflow/exports/kepler_data.csv", "w", encoding= "utf-8") as file:
            cursor.copy_expert(sql, file)
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f" Error al exportar el CSV : {e}")




