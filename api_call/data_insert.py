import json
import os
import h3 # H3 es una libreria de código abierto de Uber para generar cuadriculas hexagonales
import folium #Una libreria de datos geoespaciales para visualizar datos
from api_call.api_request import api_call, mock_api_call
from api_call.generate_data import generate_mock_places
import psycopg2

#HEXAGONAL GRID

def generate_polygon_h3(coordinates: tuple):
    try:
        if not coordinates or len(coordinates)%2 !=0:
            raise ValueError("Las coordenadas deben ser un tuple de longitud par.")
        v1= []
        v2= []
        for i in range(len(coordinates)):
            if i%2==0:
                v1.append(coordinates[i])
            if i%2!=0:
                v2.append(coordinates[i])
        
        polygon_coords=list(zip(v1,v2))
        polygons= h3.LatLngPoly(polygon_coords)
        return polygons
    except Exception as e:
        raise RuntimeError(f"Error en generate_polygon_h3 : {e}")

def generate_hex_ids():
    try:
        #Coordenadas del poligono
        coordinates = (-12.060113662381214, -76.97328624205664,
                -12.056168626735527, -76.98298511017731,
                -12.051803838845045, -76.9889074278793,
                -12.043157991738733, -76.98701915266997,
                -12.039296649707989, -76.9894224120273,
                -12.038541163253237, -76.97362956482198,
                -12.029894889244733, -76.96144160665266,
                -12.025865462672531, -76.94273051594202,
                -12.045088641936193, -76.93603572201803
                )
        #Generate polygon based on its coordinates
        polygon=generate_polygon_h3(coordinates)
        # Resolution h3
        resolution=9
        #Generates hexagonal grid
        hex_ids= h3.polygon_to_cells(polygon, resolution)
        if not hex_ids:
            raise ValueError("No se generaron celdas H3. Revisar coordenadas o resolución")
        return hex_ids
    except Exception as e:
        raise RuntimeError(f"Error en generate_hex_ids: {e}")

def api_request_preview(hex_ids, output_path="map_preview.html"):
    try:
        m = folium.Map(location=[-12.1000, -77.0300], zoom_start = 14)
        centros= [h3.cell_to_latlng(hid) for hid in hex_ids]
        for centro in centros:
            folium.Circle(
                location= centro,
                radius=250,
                color="blue",
                fill=True,
                fill_opacity=0.3
            ).add_to(m)
        m.save(output_path)
    except Exception as e:
        raise RuntimeError(f"Error en api_request_preview: {e}")
    

#POSTGRES CONNECTION

def connect_to_db(retries=5, delays=5):
    print("Connecting to PostgresSQL database")
    for attempt in range(1, retries +1):
        try:
            conn=psycopg2.connect(
                host="db",
                port=5432,
                dbname="db",
                user="db_user",
                password="db_password"
            )
            return conn
        except psycopg2.Error as e:
            print(f"Attempt {attempt} failed: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                print(f"Database connection failed, all attempts failed: {e}")
                raise

def create_table(conn):
    print("Creating table if not exists...")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;
            CREATE TABLE IF NOT EXISTS dev.raw_restaurants_data(
            id SERIAL PRIMARY KEY,
            id_location TEXT,
            latitude FLOAT,
            longitude FLOAT,
            rating FLOAT,
            userRatingCount INT,
            place_name TEXT,
            hasDelivery BOOL,
            hex_id TEXT,
            inserted_at TIMESTAMP DEFAULT NOW()
            );
            """)
        conn.commit()
        print("Table was created. ")
    except psycopg2.Error as e:
        print(f"Failed to create table: {e}")
        raise


def create_table_hex(conn):
    print("Creating table if not exists...")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;
            CREATE TABLE IF NOT EXISTS dev.raw_hex_data(
            id SERIAL PRIMARY KEY,
            hex_id TEXT,
            hex_contour TEXT,
            inserted_at TIMESTAMP DEFAULT NOW()
            );
            """)
        conn.commit()
        print("Table was created. ")
    except psycopg2.Error as e:
        print(f"Failed to create table: {e}")
        raise



def insert_data(conn, data):
    try:
        print("Insert restaurant data into database...")
        data_list=[]

        places = data.get("places") if isinstance(data, dict) else None
        if not places:
            print("No 'places' in response or empty list. Skipping.")
            return 0

        for items in places:
            place_id = items.get("id")
            latitude = items.get("location",{}).get("latitude")
            longitude = items.get("location",{}).get("longitude")
            rating = items.get("rating")
            userRatingCount = items.get("userRatingCount")
            place_name = items.get("displayName",{}).get("text")
            delivery = items.get("delivery",False)
            hex_id = h3.latlng_to_cell(latitude, longitude, 9)
            if None in [place_id,latitude,longitude,place_name]:
                continue

            data_list.append((
                        place_id,
                        latitude,
                        longitude,
                        rating,
                        userRatingCount,
                        place_name,
                        delivery,
                        hex_id
                        ))


        if not data_list:
            print("No valid rows remaining...")
            return 0

        cursor=conn.cursor()

        #Una función muy poderosa para poder insertar data rapidamente a la base de datos
        if data_list:
            args_str = b','.join((cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,NOW())",row )
            for row in data_list))
        
        query= b"""
        INSERT INTO dev.raw_restaurants_data(
        id_location,
        latitude,
        longitude,
        rating,
        userRatingCount,
        place_name,
        hasdelivery,
        hex_id,
        inserted_at
        )
        VALUES """ + args_str
        cursor.execute(query)
        conn.commit()
        print(f"{len(data_list)} rows inserted sucessfully")
    except psycopg2.Error as e:
        print(f"Error inserting data into database...")
        raise



def insert_data_hex(conn, data):
    try:
        print("Insert hex data into database...")
        cursor=conn.cursor()
        data_list=[]
        
        places = data.get("places") if isinstance(data, dict) else None
        if not places:
            print("No 'places' in response or empty list. Skipping.")
            return 0

        for items in places:
            place_latitude = items.get("location",{}).get("latitude")
            place_longitude = items.get("location",{}).get("longitude")
            hex_id = h3.latlng_to_cell(place_latitude, place_longitude, 9)
            hex_coords=h3.cell_to_boundary(hex_id)
            hex_polygon = [[lon, lat] for lat, lon in hex_coords]
            hex_polygon_str = json.dumps(hex_polygon)
            
            if None in [place_latitude,place_longitude]:
                continue
            
            data_list.append((
                        hex_id,
                        hex_polygon_str
                       ))

        #Una función muy poderosa para poder insertar data rapidamente a la base de datos
        
        if not data_list:
            print("No valid rows remaining...")
            return 0
        
        if data_list:
            args_str = b','.join((cursor.mogrify("(%s,%s,NOW())",row )
            for row in data_list))
        
        query= b"""
        INSERT INTO dev.raw_hex_data(
        hex_id,
        hex_contour,
        inserted_at
        )
        VALUES """ + args_str
        cursor.execute(query)
        conn.commit()
        print(f"{len(data_list)} rows inserted sucessfully")
    except psycopg2.Error as e:
        print(f"Error inserting data into database...")
        raise

def create_table_init():
    try:
        conn=connect_to_db()
        create_table(conn)
        create_table_hex(conn)
    except psycopg2.Error as e:
        print(f"Error creating tables....")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.")



def main():
    try:
        conn=connect_to_db()
        create_table(conn)
        create_table_hex(conn)
        hex_ids=generate_hex_ids()
        for hid in hex_ids:
            centro=h3.cell_to_latlng(hid)
            data=mock_api_call()
            #data=api_call(centro[0],centro[1])
            insert_data(conn, data)
            insert_data_hex(conn, data)
    except Exception as e:
        print(f"An error ocurred during execution: {e}")
    finally:
        if'conn' in locals():
            conn.close()
            print("Database connection closed.")
        
