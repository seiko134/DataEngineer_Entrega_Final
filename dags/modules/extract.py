import pandas as pd
import requests
from datetime import datetime,timedelta
from config import  API_KEY, URL


def extract_data():
    # Inicializa un DataFrame vacío para almacenar los datos extraídos de cada símbolo
    df = pd.DataFrame()
    #Extraccion de Hora tanto de inicio como actual(depende del endpoint que se utilice)
    hora_actual = datetime.now()
    fecha_actual= datetime(hora_actual.year,hora_actual.month,hora_actual.day,hora_actual.hour,hora_actual.minute,hora_actual.second)

    fecha_inicial = fecha_actual - timedelta(days=30)
    fecha_inicial = fecha_inicial.date()
    fecha_actual = fecha_actual.date()

    #Endpoint con fecha de inicio y final, para navegar entre las fechas que ofrece el endpoint
    #url_base = f'https://api.nasa.gov/planetary/apod?api_key=7oNCkWUAqsODsa7LWMdaDiZwQ07z7lyYh3xTLrHo&start_date={fecha_inicial}&end_date={fecha_actual}'

    #endpoint que obtiene info solo del dia
    #url_base = f'https://api.nasa.gov/planetary/apod?api_key={API_KEY}&start_date={fecha_actual}'
    params = {
                    'api_key': API_KEY,  # Clave de API que debe estar definida
                    'start_date': fecha_actual  # Define la zona horaria
                }
    response = requests.get(URL, params=params)

    #Normalizacion de json
    datos = response.json()
    data = pd.json_normalize(datos)
    data = data.rename(columns={
        "copyright": "Autor",
        "date": "fecha",
        "explanation": "explicacion",
        "hdurl": "Url_Contenido",
        "media_type": "Tipo_Contenido",
        "service_version": "Version_Servicio",
        "title": "Titulo_Contenido",
        "url": "url",
        })
    #Hora de carga en chile
    horaCarga= datetime(hora_actual.year,hora_actual.month,hora_actual.day,hora_actual.hour,hora_actual.minute,hora_actual.second) - timedelta(hours=4)
    data['Fecha_Carga'] =horaCarga
    #print(data)
    #print(data.info())
    print("Api Consumida con exito:\n")
    df.to_csv('/tmp/extracted_data.csv', index=False)
