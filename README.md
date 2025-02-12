# Hudi, Spark y Airflow con Docker Compose

🚀 Proyecto para crear un **Data Lakehouse** con **Apache Hudi, Spark y Airflow** usando Docker Compose.

## 📌 Tecnologías Utilizadas
- **Docker & Docker Compose** - Orquestación de servicios.
- **MinIO** - Almacenamiento tipo S3 para Hudi.
- **Apache Spark** - Motor de procesamiento distribuido.
- **Apache Hudi** - Framework para manejar datos transaccionales en el Data Lake.
- **Apache Airflow** - Orquestador de tareas ETL.

## 📌 Cómo Ejecutar el Proyecto
1. Clona este repositorio:
   ```bash
   git clone https://github.com/tu-usuario/hudi-spark-airflow.git
   cd hudi-spark-airflow
   ```
2. Ejecuta los servicios con:
   ```bash
   docker-compose up -d
   ```
3. Accede a las interfaces:
   - **MinIO:** `http://localhost:9001` (usuario: `admin`, contraseña: `password`)
   - **Spark UI:** `http://localhost:8080`
   - **Airflow UI:** `http://localhost:8080`

## 📌 Cómo Detener y Eliminar los Contenedores
```bash
docker-compose down
