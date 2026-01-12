# 🚀 Pipeline ETL Emisión SCTR - Databricks Medallion Architecture

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Runtime%2015.x-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![VS Code](https://img.shields.io/badge/VS%20Code-Databricks%20Connect-007ACC?style=for-the-badge&logo=visualstudiocode&logoColor=white)](https://code.visualstudio.com/)
[![License: CC BY-NC 4.0](https://img.shields.io/badge/License-CC%20BY--NC%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by-nc/4.0/)

Este proyecto implementa una arquitectura **ELT (Extract, Load, Transform)** moderna para el procesamiento masivo de archivos Excel de pólizas SCTR. 

Utiliza un flujo de trabajo híbrido donde el desarrollo y control de versiones se realizan localmente en **VS Code** (Windows), mientras que la ejecución de cómputo pesado ocurre en un clúster de **Databricks** mediante **Databricks Connect**.

---

## 🏗️ Arquitectura del Flujo

* **Origen:** Archivos Excel en Volúmenes de Unity Catalog (`/Volumes/...`).
* **Procesamiento:** Databricks (PySpark).
* **Orquestación:** Databricks Workflows / Ejecución interactiva vía Databricks Connect.
* **Capas (Medallion):**
    * 🥉 **Bronze:** Ingesta cruda (Raw Data).
    * 🥈 **Silver:** Limpieza, Eliminación de Duplicados y Tipado.
    * 🥇 **Gold:** Agregaciones de negocio (Consolidado BI).
* **Salida:** Tablas Delta conectadas a Power BI.

---

## 🛠️ Requisitos Previos

* **Sistema Operativo:** Windows 10/11 (Probado), macOS o Linux.
* **Python:** Versión `3.11` requerida (Gestionado con `uv`).
* **Databricks Workspace:** Unity Catalog habilitado.
* **Cluster:** Runtime 14.3 LTS o superior (Compatible con Databricks Connect).
* **VS Code:** Con la extensión oficial *Databricks for Visual Studio Code*.

---

## 🚀 Instalación y Configuración del Entorno de Desarrollo (Setup)

Sigue estos pasos si estás clonando este repositorio por primera vez.

### 1. Clonar y Preparar Entorno (uv)
1.  Clonar el repositorio

```sh
git clone [https://github.com/SefreesDev29/Databricks_ELT_Medallion_Emision_SCTR.git](https://github.com/SefreesDev29/Databricks_ELT_Medallion_Emision_SCTR.git)
```

2.  Este proyecto utiliza `uv` para la gestión de dependencias. Instala las librerías necesarias para que el linter local y Jupyter funcionen:

```sh
# Sincronizar entorno virtual
uv sync

# O instalar manualmente las dependencias clave
uv add databricks-connect databricks-sdk ipykernel
```

### 2. Configuración en Databricks
1.  Generar un **Personal Access Token (PAT)**:
    * Ir a *User Settings* -> *Developer* -> *Access Tokens* -> *Generate New Token*.
    * Guardar el token , se necesitará para conectar VS Code.
2.  Tener un Clúster encendido para desarrollo.

### 3. Configuración Local (VS Code)
1.  Instalar la extensión: **Databricks for Visual Studio Code**.
2.  Configurar la autenticación:
    * Clic en el icono de Databricks en la barra lateral.
    * Seleccionar **Configure Databricks**.
    * Ingresar la URL del Workspace y el Token generado.
    * Seleccionar el **Cluster** objetivo.
    * Esto generará un perfil en (ej. `Emision_SCTR_Databricks`).
    * Dirigirse al archivo `src/utilities.py` y forzar la variable de entorno antes de crear la sesión Spark.

```python
    import os
    from pyspark.sql import SparkSession

    os.environ['DATABRICKS_CONFIG_PROFILE'] = 'Emision_SCTR_Databricks' #Ejemplo de Profile
    spark = SparkSession.builder.getOrCreate()
```