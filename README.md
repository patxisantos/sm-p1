# SM - Práctica 1 (AgroCeuta)

Práctica de la asignatura Sistemas Multidimensionales basada en el diseño e implementación de un entorno OLTP departamental, su transformación a un Data Warehouse en esquema estrella, una variante snowflake y un benchmark comparativo de consultas analíticas.

## Autor

Francisco Javier Santos Rivas

## Contenido

- `sql/creacion_agroceuta.sql`: creación completa de esquemas OLTP, esquema estrella, esquema snowflake, carga de datos y ETL básico.
- `benchmark_agroceuta.py`: script Python para benchmarking de consultas OLTP vs STAR vs SNOWFLAKE.
- `docs/`: diagramas ER y OLAP utilizados en la documentación.
- `practica1_SANTOSRIVAS.pdf`: memoria final entregada.

## Requisitos

- PostgreSQL
- Python 3.8 o superior
- Librería `psycopg2` para Python

## Cómo montar la base de datos

1. Crear una base de datos vacía en PostgreSQL, por ejemplo:
   ```sql
   CREATE DATABASE agroceuta_dw;
   ```

2. Ejecutar el script:
   ```bash
   psql -U postgres -d agroceuta_dw -f sql/creacion_agroceuta.sql
   ```

## Cómo ejecutar el benchmark

1. Instalar dependencia:
   ```bash
   pip install psycopg2-binary
   ```

2. Editar `benchmark_agroceuta.py` y cambiar:
   - `user`
   - `password`

   por los valores de tu instalación local de PostgreSQL.

3. Ejecutar:
   ```bash
   python benchmark_agroceuta.py
   ```