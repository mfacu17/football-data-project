# ⚽ Proyecto de Análisis de Datos de Fútbol con Airflow, PySpark, GCS, BigQuery y PowerBI

## 📝 Descripción

Este proyecto lo inicié con el objetivo de explorar y poner en práctica nuevas herramientas del ecosistema de ingeniería de datos. Implementa una arquitectura medallón y un proceso ETL orientado a la transformación y análisis de datos de futbol. El objetivo es transformar datos crudos en información útil que permita visualizar estadísticas detalladas sobre clubes, jugadores y transferencias.

---
## 🛠️ Tecnologías y Herramientas

  - **Apache Airflow**: Orquestación de tareas ETL.
  - **PySpark**: Procesamiento distribuido de datos.
  - **Google Cloud Storage (GCS)**: Archivos crudos, almacenamiento intermedio y trazabilidad de errores.
  - **BigQuery**: Almacenamiento y consulta de datos estructurados.
  - **PowerBI**: Visualización de dashboards interactivos.
  - **Conectores BigQuery y GCS para Spark**: Integración entre Spark y GCP.
---

## Arquitectura del pipeline

El flujo de datos se divide en tres capas principales:

1. **Raw**:
     - Contiene los archivos CSV crudos almacenados en GCS.
     - Se cargan sin modificaciones en tablas de BigQuery.
2. **Stage**:
     - Contiene los datos curados: sin nulos, duplicados, tipos corregidos e imputación de nulos, en algunos casos.
     - Esta limpieza se realiza con PySpark.
3. **Analytics**:
     - Genera métricas y agregaciones relevantes para análisis. 
     - Los scripts de PySpark ejecutan consultas SQL complejas y guardan los resultados directamente en BigQuery.

## Scripts principales

- `etl_raw.py`: Contiene los DAGs correspondientes a la carga de cada una de las tablas en la capa raw de BigQuery.
- `etl_stage.py`: Contiene los DAGs que se ejecutan para realizar las transformaciones necesarias sobre las tablas raw, mediante la importación de jobs definidos con PySpark.
- `etl_analytics.py`: Contiene los DAGs necesarios para generar las tablas con métricas y agregaciones en la capa analytics, destinadas a su posterior visualización.

## ✅ Consideraciones
- Se utilizan variables de entorno para manejar credenciales y configuraciones.
- Los datos inválidos (nulos y duplicados) son registrados en GCS para mejorar su trazabilidad.
