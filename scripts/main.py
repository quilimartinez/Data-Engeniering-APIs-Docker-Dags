# # Instalar librerías

from configparser import ConfigParser
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
import requests 
from datetime import datetime
from sqlalchemy import text
from datetime import datetime
import logging
from datetime import datetime
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
import smtplib


# # Definir funciones


def build_conn_string(config_path, config_section):
    """
    Construye la cadena de conexión a la base de datos
    a partir de un archivo de configuración.
    """

    # Lee el archivo de configuración
    parser = ConfigParser()
    parser.read(config_path)

    # Lee la sección de configuración de PostgreSQL
    config = parser[config_section]
    host = config['host']
    port = config['port']
    dbname = config['dbname']
    username = config['username']
    pwd = config['pwd']

    # Construye la cadena de conexión
    conn_string = f'postgresql://{username}:{pwd}@{host}:{port}/{dbname}?sslmode=require'

    return conn_string


build_conn_string('config/config.ini','redshift')


def connect_to_db(conn_string):
    """
    Crea una conexión a la base de datos.
    """
    engine = sa.create_engine(conn_string)
    conn = engine.connect()
    return conn, engine


def load_to_sql(df, table_name, engine, if_exists="append"):
    """
    Carga un DataFrame en la base de datos especificada.

    Parameters:
    df (pandas.DataFrame): El DataFrame a cargar en la base de datos.
    table_name (str): El nombre de la tabla en la base de datos.
    engine (sqlalchemy.engine.base.Engine): Un objeto de conexión a la base de datos.
    if_exists (str, opcional): Especifica qué hacer si la tabla ya existe ("fail", "replace" o "append").
                                 Por defecto, se reemplazan los datos existentes.

    """
    try:
        df.to_sql(
            table_name,
            engine,
            if_exists=if_exists,
            index=False,
            method="multi"
        )
        print("Datos cargados exitosamente en la base de datos")
    except Exception as e:
        print(f"Error al cargar los datos en la base de datos: {e}")
        # Levanta la excepción para que pueda ser manejada en un nivel superior
        raise e




# # Preparación de los datos

# DEFINO API A CONSULTAR, OBTENGO ARCHIVOS JSON Y CREO DATAFRAME

base_url = "https://criptoya.com/api"
criptomonedas = ["btc", "eth", "dai"]
cotizaciones = {}

for cripto in criptomonedas:
    endpoint = f"{cripto}/usd/0.1"
    endpoint_url = f"{base_url}/{endpoint}"
    
    r = requests.get(endpoint_url)
    
    if r.status_code == 200:
        json_data = r.json()
        cotizaciones[cripto] = json_data
        print(f"JSON COTIZACION {cripto.upper()}:")
        print(json_data)
    else:
        print(f"Error al obtener la cotización de {cripto} - Código de estado: {r.status_code}")

# Crear un diccionario para los DataFrames de cada criptomoneda
dfs = {}

for cripto, json_data in cotizaciones.items():
    df = pd.DataFrame(json_data).T.reset_index()
    df.rename(columns={"index": "broker"}, inplace=True)
    df.insert(0, "cripto", cripto)
    dfs[cripto] = df

# Concatenar los DataFrames
df_cotizaciones = pd.concat(list(dfs.values()), axis=0)



## Creo las 3 tablas que se utilizaran para Stage y Normalizo

## CRIPTOMONEDAS

data_cripto = {
    "cripto_id": [1, 2, 3],  
    "cripto_name": ["btc", "eth", "dai"] 
}

# Crear el DataFrame
criptomonedas = pd.DataFrame(data_cripto)


## BROKERS

data_broker = {
    "broker_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],  
    "broker_name": ["binancep2p", "fiwind", "letsbit", "tiendacrypto", "bitsoalpha", "calypso", "decrypto", "banexcoin", "buenbit", "fluyez", "vitawallet"]  
}

# Crear el DataFrame
brokers = pd.DataFrame(data_broker)

# COTIZACIONES

nuevos_nombres = {
    "cripto": "cripto_name",
    "broker": "broker_name",
    "totalAsk": "p_compra",
    "totalBid": "p_venta",
    "time": "fecha_cotizacion"
}

df_cotizaciones = df_cotizaciones.rename(columns=nuevos_nombres)

## Normalizo datos

merged_df = pd.merge(df_cotizaciones, criptomonedas, on='cripto_name', how='inner')
merged_df_1 = pd.merge(merged_df, brokers, on='broker_name', how='inner')
merged_df_1=merged_df_1[["cripto_id","broker_id","p_compra","p_venta","fecha_cotizacion"]]

## Normalizo la fecha
merged_df_1['fecha_cotizacion'] = pd.to_datetime(merged_df_1['fecha_cotizacion'], unit='s')

## Elimino duplicados si existen en el DataFrame antes de subir los datos en AWS Redshift
merged_df_1 = merged_df_1.drop_duplicates()

# # INSERTO DATOS EN REDSHIFT

# Conexión a Redshift
config_dir = "config/config.ini"
conn_string = build_conn_string(config_dir, "redshift")
conn, engine = connect_to_db(conn_string)


## Actualizo tabla cotizaciones

with engine.connect() as conn, conn.begin():

    conn.execute("TRUNCATE TABLE stg_cotizaciones")

    load_to_sql(merged_df_1, "stg_cotizaciones", conn)

    conn.execute("""
        MERGE INTO cotizaciones
        USING stg_cotizaciones
        ON cotizaciones.cripto_id = stg_cotizaciones.cripto_id and cotizaciones.broker_id = stg_cotizaciones.broker_id
        WHEN MATCHED THEN
            UPDATE SET
                cripto_id = stg_cotizaciones.cripto_id,
                broker_id = stg_cotizaciones.broker_id,
                p_compra = stg_cotizaciones.p_compra,
                p_venta = stg_cotizaciones.p_venta, 
                fecha_cotizacion = stg_cotizaciones.fecha_cotizacion
        WHEN NOT MATCHED THEN
            INSERT (cripto_id, broker_id, p_compra, p_venta, fecha_cotizacion)
            VALUES (stg_cotizaciones.cripto_id, stg_cotizaciones.broker_id, stg_cotizaciones.p_compra, stg_cotizaciones.p_venta, GETDATE())
        """)


## Actualizo tabla Criptomonedas

with engine.connect() as conn, conn.begin():

    conn.execute("TRUNCATE TABLE stg_criptomonedas")

    load_to_sql(criptomonedas, "stg_criptomonedas", conn)

    conn.execute("""
        MERGE INTO criptomonedas
        USING stg_criptomonedas
        ON criptomonedas.cripto_id = stg_criptomonedas.cripto_id 
        WHEN MATCHED THEN
            UPDATE SET
                cripto_id = stg_criptomonedas.cripto_id,
                cripto_name = stg_criptomonedas.cripto_name,
                fecha_carga = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (cripto_id, cripto_name, fecha_carga)
            VALUES (stg_criptomonedas.cripto_id, stg_criptomonedas.cripto_name, GETDATE())
        """)


## Actualizo tabla Brokers

with engine.connect() as conn, conn.begin():

    conn.execute("TRUNCATE TABLE stg_brokers")

    load_to_sql(brokers, "stg_brokers", conn)

    conn.execute("""
        MERGE INTO brokers
        USING stg_brokers
        ON brokers.broker_id = stg_brokers.broker_id 
        WHEN MATCHED THEN
            UPDATE SET
                broker_id = stg_brokers.broker_id,
                broker_name = stg_brokers.broker_name,
                fecha_carga = GETDATE() 
        WHEN NOT MATCHED THEN
            INSERT (broker_id, broker_name,fecha_carga)
            VALUES (stg_brokers.broker_id, stg_brokers.broker_name,GETDATE() )
        """)


## Envio de alertas vía email cuando el precio de Bitcoin supere los $50.000 en el Broker 1

# Define la función que enviará la alerta por correo electrónico
def enviar_alerta(precio_compra_bitcoin):
    destinatario = 'quilimartinez@gmail.com'
    remitente = 'quilimartinez@gmail.com'
    asunto = 'Alerta: Precio de Bitcoin superó los $50,000'
    mensaje = f'El precio de compra actual de Bitcoin es {precio_compra_bitcoin}. ¡Supera los $50,000!'

    # Configuración del servidor SMTP
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587
    smtp_user = 'quilimartinez@gmail.com'
    smtp_password = 'msdd pqol ggxo bxaz'

    # Crear el objeto MIMEText con el cuerpo del mensaje
    msg = MIMEMultipart()
    msg.attach(MIMEText(mensaje, 'plain'))

    # Configurar el servidor SMTP
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)

        # Enviar el correo electrónico
        server.sendmail(remitente, destinatario, msg.as_string())

# Define la función que enviará la alerta cuando el precio supere los $50,000

def enviar_alerta_por_correo(**kwargs):
    # Conexión a Redshift
    config_dir = "config/config.ini"
    conn_string = build_conn_string()
    conn, engine = connect_to_db(conn_string)

    # Consultar el precio de compra de Bitcoin para cripto_id=1 y broker_id=1
    query = text("""
        SELECT p_compra
        FROM cotizaciones
        WHERE cripto_id = 1 AND broker_id = 1
        ORDER BY fecha_cotizacion DESC
        LIMIT 1
    """)
    result = conn.execute(query).fetchone()

    if result:
        precio_compra_bitcoin = result[0]
        limite_alerta = 50000

        if precio_compra_bitcoin > limite_alerta:
            # Envia la alerta por correo electrónico
            enviar_alerta(precio_compra_bitcoin)