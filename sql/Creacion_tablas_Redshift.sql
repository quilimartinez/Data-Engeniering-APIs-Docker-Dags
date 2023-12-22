--- GENERACION DE TABLAS DE STAGE

DROP table quilimartinez_coderhouse.stg_brokers;
CREATE TABLE IF NOT EXISTS quilimartinez_coderhouse.stg_brokers
(
	broker_id INTEGER NOT NULL,
	broker_name VARCHAR(100)
)
DISTSTYLE ALL
sortkey(broker_id)
;


DROP table quilimartinez_coderhouse.stg_criptomonedas;
CREATE TABLE IF NOT EXISTS quilimartinez_coderhouse.stg_criptomonedas
(
	cripto_id INTEGER NOT NULL,
	cripto_name VARCHAR(100)

)
DISTSTYLE ALL
 SORTKEY (
	cripto_id
	)
;


DROP TABLE quilimartinez_coderhouse.stg_cotizaciones;
CREATE TABLE IF NOT EXISTS quilimartinez_coderhouse.stg_cotizaciones
(
    cripto_id INTEGER NOT NULL, 
    broker_id INTEGER NOT NULL,
    p_compra FLOAT NOT NULL,
	p_venta FLOAT,
	fecha_cotizacion TIMESTAMP
)
DISTSTYLE AUTO
 SORTKEY (
	fecha_cotizacion
	)
;


--- GENERACION DE TABLAS FINALES

DROP table quilimartinez_coderhouse.brokers;
CREATE TABLE IF NOT EXISTS quilimartinez_coderhouse.brokers
(
	broker_id INTEGER,
	broker_name VARCHAR(100),
	fecha_carga TIMESTAMP   
)
DISTSTYLE ALL
sortkey(broker_id)
;

DROP table quilimartinez_coderhouse.criptomonedas;
CREATE TABLE IF NOT EXISTS quilimartinez_coderhouse.criptomonedas
(
	cripto_id INTEGER,
	cripto_name VARCHAR(100),
	fecha_carga TIMESTAMP   

)
DISTSTYLE ALL
 SORTKEY (
	cripto_id
	)
;

DROP TABLE quilimartinez_coderhouse.cotizaciones;
CREATE TABLE IF NOT EXISTS quilimartinez_coderhouse.cotizaciones
(
	cripto_id INTEGER, 
	broker_id INTEGER,
	p_compra FLOAT,
	p_venta FLOAT,
	fecha_cotizacion DATE 
)
DISTSTYLE key
DISTKEY (cripto_id)
 SORTKEY (
	fecha_cotizacion
	)
;