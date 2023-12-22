
# OBJETIVO

El presente repositorio tiene los siguientes objetivos:

-  La extraccion de datos de cotización de criptomonedas de un sitio web denominado "CRYPTO YA" a través de una API pública y gratuita
-  La cración 3 tablas de almacenamiento en RedShift (Data WareHouse de AWS)
-  El proceso diario de ETL de creación, almacenamiento y actualización de las tablas de Redshift a partir de la información extraída de la API
-  dfdfdf

# REPOSITORIO  

En el repositorio se encuentran las siguientes carpetas: 

SCRIPTS: Contiene el codigo de extracción de datos de API Publica "CriptoYa" con las cotizaciones diarias de compra y venta de 3 Criptomonedas en los principales Brokers del mercado, la normalizacion de los datos y el proceso de carga de datos en tablas SQL en RedShift AWS 

SQL: Contiene el código SQL de creación tablas en RedShift AWS para el posterior armado del ETL correspondiente

CONFIG: Contiene las credenciales de acceso a AWS Redshift

DAGS: Contiene la secuencia de DAGS programadas para que se pueda ejecutar en un contenedor de Apache Airflow

Proyecto_Final.ipynb contiene el codigo inicial (sin dags) en formato Jupyter Notebook

# API "CRYPTO YA"


Las cotizaciones se actualizan cada 1 minuto.
Límite: 200 requests por minuto.

Endpoint: /api/{coin}/{fiat}/{volumen}

"coin":{str} Cripto a operar.

"fiat":{str} Moneda contra la que se opera, puede ser fiat o stablecoin.

"volumen":{float} Volumen a operar, utilizar el punto como separador decimal.

### Valores posibles:
{coin}: BTC , DAI , ETH , USDT , USDC , BNB , AAVE , ADA , AXS , BAT , CAKE , DOGE , DOT , EOS , LINK , LTC , MANA , PAXG , SAND , SHIB , SLP , SOL , TRX , UNI , XLM , XRP , AVAX , BCH , BUSD , CHZ , FTM , MATIC , ALGO , NUARS , DOC , USDP , UXD

{fiat}: ARS , BRL , PEN , CLP , COP , MXN , USD , UYU , VES


### Salida: 

"ask":{float} Precio de compra reportado por el exchange, sin sumar comisiones.

"totalAsk":{float} Precio de compra final incluyendo las comisiones de transferencia y trade.

"bid":{float} Precio de venta reportado por el exchange, sin restar comisiones.

"totalBid":{float} Precio de venta final incluyendo las comisiones de transferencia y trade.

"time":{int} Timestamp del momento en que fue actualizada esta cotización.



# BASE DE DATOS EN REDSHIFT

Información sobre las tablas creadas en RedShift

### Brokers

Contiene la información de los principales brokers

Estructura:

broker_id INTEGER Contiene el ID identificatorio de cada broker

broker_name VARCHAR(100) Contiene el nombre comercial de cada broker

Sortkey (broker_Id) 

Distribution Style ALL  

La distribución ALL es apropiada para mesas de movimiento relativamente lento; es decir, tablas que no se actualizan con frecuencia ni de forma exhaustiva como es el caso de los Brokers.
El broker_id es un campo numerico que sirve de Sorkey tipicamente.

### Criptomonedas

Contiene la información de las criptomonedas consultadas en cada brokers

Estructura:

cripto_id INTEGER Contiene el ID identificatorio de cada criptomoneda

cripto_name VARCHAR(100) Contiene el nombre comercial de cada criptomoneda

Sortkey (cripto_Id) 

Distribution Style ALL  

La distribución ALL es apropiada para mesas de movimiento relativamente lento; es decir, tablas que no se actualizan con frecuencia ni de forma exhaustiva como es el caso de los nombres de las criptomonedas.
El cripto_id es un campo numerico que sirve de Sorkey tipicamente.

### Cotizaciones

Contiene la tabla fáctica que contiene las cotizaciones que se actualizan diariamente con informacion de cada broker

Estructura:

cripto_id INTEGER Contiene el ID identificatorio de cada criptomoneda

broker_id INTEGER Contiene el ID identificatorio de cada broker

p_compra FLOAT Precio de compra de cata criptomoneda

p_venta FLOAT Precio de venta de cada criptomoneda

fecha_cotizacion DATE Fecha de cotizacion consultada

Sortkey (cripto_Id) 

Distribution Style KEY  

Se elige el estilo de distribución AUTO ya que ésta tabla crecerá y será mas eficiente que Redshift asigne automaticamente el estilo de distribución mas conveniente para mayor eficiencia en el almacenamiento y consulta a la base de datos.

El campo fecha_cotizacion es un campo de fecha que sirve de Sorkey tipicamente.

# PROCESO ETL EN PYTHON

El archivo main.py obtiene los datos de la API en archivos JSON, los convierte en Dataframes, los normaliza, elimina duplicados y luego los convierte en base de datos SQL.

# DAGS

El archivo dag_critpo.py contiene el pipeline creado en Python para funcionar en Apache Airflow con 3 tareas definidas:

. Conectarse a la base de datos 

. Extraer la informacion del Broker, cargar la data a Redshift y normalizar

. Enviar una alerta por email cuando el precio del Bitcoin supere los $50.000


