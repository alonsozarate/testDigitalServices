# Plataforma de Analítica de Datos - Digital Services Inc.

## Contexto del Proyecto

Este repositorio contiene la solución técnica para el reto de Ingeniería de Datos de **"Digital Services Inc."**. El objetivo es construir los cimientos de una plataforma de datos moderna que unifique la visión del usuario a través de múltiples fuentes (Streaming, Batch y Relacional), consolidando la información en una capa **Gold (Medallion Architecture)** para análisis avanzado.

El pipeline sigue una **Arquitectura Medallion (Bronze, Silver, Gold)** implementada con **PySpark**, diseñada para ser escalable en la nube (**AWS**).

## Parte 1: Desarrollo caso Kashio

Para el siguiente caso se ha desarrollado un pipeline ETL que ingiere datos de eventos, transacciones y usuarios, los cuales son procesados y consolidados en una capa **Gold (Medallion Architecture)** para análisis avanzado.

### 1. Diseño de Esquema (Capa Gold)

La tabla final `user_session_analysis` se diseñó desnormalizada para facilitar el análisis OLAP.

**DDL Optimizado (Redshift):**

```sql
CREATE TABLE user_session_analysis (
    session_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(50) DISTKEY,          -- Optimización: Distribución por Usuario
    user_country VARCHAR(50),
    user_device VARCHAR(50),
    session_start_time TIMESTAMP SORTKEY, -- Optimización: Ordenamiento por Tiempo
    total_events INT DEFAULT 0,
    event_type VARCHAR(100),
    transaction_id VARCHAR(50),
    amount DECIMAL(10,2),
    currency VARCHAR(3),
    is_conversion BOOLEAN DEFAULT FALSE
);

### Criterio de selección de columnas:

* **session_id:** Identificador único presente en eventos y transacciones que permite agrupar la interacción.
* **user_id:** Clave de distribución (`DISTKEY`) que conecta las tres fuentes (Eventos, Transacciones y Usuarios) en un mismo nodo.
* **user_country:** Dato dimensional de la tabla de usuarios desnormalizado para análisis geográfico sin necesidad de JOINS.
* **user_device:** Dato dimensional desnormalizado para segmentar el análisis según la tecnología del cliente.
* **session_start_time:** Clave de ordenamiento (`SORTKEY`) derivada de los eventos para optimizar consultas temporales.
* **total_events:** Métrica agregada que cuantifica la intensidad de la actividad del usuario durante la sesión.
* **event_type:** Categoriza la interacción principal ocurrida en la sesión (streaming).
* **transaction_id:** Vincula la sesión con el archivo batch; su existencia confirma una conversión.
* **amount:** Métrica financiera crítica proveniente de las transacciones para calcular el valor de la sesión.
* **currency:** Necesario para interpretar correctamente el monto monetario de la transacción.
* **is_conversion:** Bandera booleana derivada que facilita identificar rápidamente si la sesión cumplió el objetivo de venta.

### Estrategia de Optimización:

* **DISTKEY --> user_id**: Agrupa datos del mismo usuario en el mismo nodo para acelerar JOINs históricos.
* **SORTKEY --> session_start_time**: Acelera las consultas de rango de tiempo.

### 2. Manejo de Escenarios Críticos

* **a. Datos tardíos:**
  Se implementa una lógica de "Upsert" o reprocesamiento de particiones en la capa Silver basada en *Event Time* y no solo en *Processing Time*. Spark permite leer particiones antiguas y actualizar los registros si llega un evento con timestamp pasado.

* **b. Calidad y Duplicados:**
  *Deduplicación:* En la capa Silver, se utilizan Window Functions (`ROW_NUMBER`) particionadas por ID y ordenadas por timestamp descendente para garantizar que siempre prevalezca el estado más reciente del dato.

* **c. Validación:**
  Se sugiere implementar **Great Expectations** en el pipeline de Airflow para bloquear la carga si el porcentaje de nulos supera un umbral.


### 2. Manejo de Escenarios Críticos

* **Datos tardíos:**
Se implementa una lógica de "Upsert" o reprocesamiento de particiones en la capa Silver basada en Event Time y no solo en Processing Time. Spark permite leer particiones antiguas y actualizar los registros si llega un evento con timestamp pasado.

* **Calidad y Duplicados:**
Deduplicación: En la capa Silver, se utilizan Window Functions (ROW_NUMBER) particionadas por ID y ordenadas por timestamp descendente para garantizar que siempre prevalezca el estado más reciente del dato.

* **Validación:**
Se sugiere implementar Great Expectations en el pipeline de Airflow para bloquear la carga si el porcentaje de nulos supera un umbral.


Parte 2: Ejecución del Prototipo (Instrucciones)

Este repositorio incluye un generador de datos sintéticos y un pipeline ETL desarrollado en PySpark que simula el flujo completo (Bronze -> Silver -> Gold) localmente.

### Estructura del Proyecto

kashio_challenge_v2/
├── config/             # Configuración centralizada (YAML)
|   └── etl_config.yaml  # Configuración del pipeline
├── data/               # Simulador de Data Lake (No se sube a Git)
|   ├── bronze/         # Datos Raw
|   |   ├── events/     # Eventos
|   |   ├── transactions/ # Transacciones
|   |   └── users/      # Usuarios
|   ├── silver/         # Datos Limpios excepto users (viene estructurada)
|   |   ├── events/     # Eventos
|   |   └── transactions/ # Transacciones
|   └── gold/           # Datos Analíticos
|   |   └── user_session_analysis/ # Tabla final para reporte
├── design/             # DDLs y diagramas
|   └── user_session_analysis.sql  # DDL de la tabla final
├── src/                # Código fuente Python
|   ├── data_generator.py  # Generador de Mock Data
|   └── etl_pipeline.py    # Lógica ETL (PySpark)
├── requirements.txt    # Dependencias
└── README.md           # Documentación


### Prueba y ejecución:

Opción A (Recomendada - Linux/Colab): 
Google Colab o entorno Linux con Java 8/11 instalado.

Opción B (Windows Local): 
Requiere configuración manual de winutils.exe y HADOOP_HOME.

### Pasos para Ejecutar

Clonar el repositorio:

git clone https://github.com/alonsozarate/testDigitalServices.git
cd kashio_challenge_v2

### Instalar dependencias:

pip install -r requirements.txt

### Generar Datos de Prueba --> Capa Bronze:
Este script crea archivos JSON y CSV aleatorios en data/bronze/.

python src/data_generator.py

### Ejecutar el Pipeline ETL --> Silver -> Gold:
Procesa los datos, limpia, estandariza y genera la tabla analítica.

python src/etl_pipeline.py

### Verificar Resultados:
Los archivos procesados (Parquet/CSV) se encontrarán en:

data/silver/ (Datos limpios)

data/gold/user_session_analysis/ (Tabla final para reporte)

Nota: El pipeline ha sido validado exitosamente en entornos Linux (Google Colab) para garantizar compatibilidad nativa con Spark.


### Parte 3: Estrategia de IA/ML (Visión Futura)

#### a.- Infraestructura para Detección de Fraude en Tiempo Real

La arquitectura Batch actual tiene una latencia de horas-días, lo cual esinsuficiente para detener un fraude en el momento. Para soportar ML en tiempo real, se requiere:

Streaming Ingestion: 
Migrar de archivos S3 a Amazon Kinesis Data Streams o Kafka.

Motor de Procesamiento: 
Implementar Spark Structured Streaming o Apache Flink para calcular ventanas de tiempo (ej. "más de 3 transacciones en 1 minuto").

Feature Store: 
Implementar una Feature Store (ej. Amazon SageMaker Feature Store o Redis) para servir variables pre-calculadas al modelo con latencia de milisegundos.

#### b.- GenAI en Ingeniería de Datos

Caso de Uso: Generación Automática de Tests de Calidad (Data Quality).

Implementación: Integrar un modelo LLM (como Gemini o GPT-4) en el pipeline de CI/CD. Al hacer un Pull Request con una nueva transformación SQL/PySpark, el modelo analiza la lógica de negocio y genera automáticamente:

Casos de prueba unitarios (pytest).

Reglas de calidad para dbt (unique, not_null, accepted_values).

Documentación técnica del dataset para el catálogo de datos.
