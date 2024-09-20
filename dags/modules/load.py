import psycopg2
from psycopg2.extras import execute_values
from redshift_conn import connect_redshift
from config import REDSHIFT_SCHEMA


# Carga de datos a Redshift
def load_data(df):
    
    # Conexion a Redshift
    conn = connect_redshift()

    try:
        with conn.cursor() as cursor:

            # Crear la tabla stock_data en el esquema especificado
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA}.Api_NASA(
                    Fecha DATE PRIMARY KEY,
                    Explicacion VARCHAR(2000),
                    Url_Contenido VARCHAR(255),
                    Tipo_Contenido VARCHAR(50),
                    Version_Servicio VARCHAR(10),
                    Titulo_Contenido VARCHAR(255),
                    Url VARCHAR(255),
                    Autor VARCHAR(255),
                    Fecha_Carga DATE
);


            );
            """)
            print("Tabla en Redshift lista!")

            # Preparar datos para inserción en bloque
            block_size = 100  # Tamaño del bloque
            for start in range(0, len(df), block_size):
                end = start + block_size
                block_df = df.iloc[start:end]
                    
                # Obtener IDs existentes en la tabla
                cursor.execute(f"SELECT id FROM {REDSHIFT_SCHEMA}.Api_NASA;")
                existing_ids = {row[0] for row in cursor.fetchall()}
                    
                # Filtrar datos no duplicados basados en el ID
                new_rows = block_df[~block_df['id'].isin(existing_ids)]

                # Si la fila esta vacia continuar con la siguente    
                if new_rows.empty:
                    print(f"No se agregaron registros nuevos.")
                    continue
                    
                # Filas a insertar
                rows_to_insert = list(new_rows.to_records(index=False))
                    
                # Insercion en bloque
                insert_query = f"""
                INSERT INTO {REDSHIFT_SCHEMA}.Api_NASA (Fecha, Explicacion, Url_Contenido, Tipo_Contenido, Version_Servicio, Titulo_Contenido, Url, 
Autor, Fecha_Carga)
                VALUES %s
                """
                execute_values(cursor, insert_query, rows_to_insert)

                # Commit de la insercion    
                conn.commit()
                print(f"Se ha agregado un bloque de {len(rows_to_insert)} registros a Redshift.")

    except psycopg2.Error as e:
        print(f"Error cargando datos a Redshift: {e}")
    finally:
        conn.close()

