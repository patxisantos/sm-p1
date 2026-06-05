# SM - AgroCeuta (Sistemas Multidimensionales)

Repositorio de prácticas de la asignatura **Sistemas Multidimensionales** de la Universidad de Granada, basadas en el diseño e implementación completo de un sistema de inteligencia de negocio para **AgroCeuta S.L.**, 
una empresa ficticia del sector agroalimentario dedicada a la exportación de productos desde Ceuta.

## Autor

Francisco Javier Santos Rivas

***

## Estructura del repositorio

```
.
├── csv/                              # Datos meteorológicos y resultados ETL
│   ├── estaciones_meteorologicas.csv
│   ├── observaciones_diarias.csv
│   ├── objetivos_familias.csv
│   └── analisis_ventas_agroceuta.csv
├── docs/                             # Diagramas ER y OLAP
├── images/                           # Imágenes utilizadas en la documentación
├── pentaho/                          # Transformaciones Pentaho (.ktr)
│   ├── limpieza_meteo_completo.ktr
│   ├── etl_agroceuta.ktr
│   └── analisis_olap_agroceuta.ktr
├── sql/                              # Scripts SQL
│   ├── creacion_agroceuta.sql
│   └── ampliacion_agroceuta.sql
├── benchmark_agroceuta.py
├── practica1_SANTOSRIVAS.pdf
├── documento_practica2_SANTOS_RIVAS.pdf
├── practica3_SANTOSRIVAS.pdf
└── README.md
```

***

## Práctica 1 - Diseño de Sistemas OLAP

Diseño e implementación de un entorno OLTP departamental, su transformación a un Data Warehouse en esquema estrella y snowflake, y un benchmark comparativo de consultas analíticas.

### Contenido relevante

- `sql/creacion_agroceuta.sql` - creación de esquemas OLTP, estrella y snowflake, carga de datos y ETL básico.
- `benchmark_agroceuta.py` - benchmarking de consultas OLTP vs STAR vs SNOWFLAKE.
- `docs/` - diagramas ER y OLAP.
- `practica1_SANTOSRIVAS.pdf` - memoria final entregada.

### Requisitos

- PostgreSQL
- Python 3.8 o superior
- Librería `psycopg2`

### Cómo montar la base de datos

1. Crear una base de datos vacía en PostgreSQL:
   ```sql
   CREATE DATABASE agroceuta_dw;
   ```

2. Ejecutar el script:
   ```bash
   psql -U postgres -d agroceuta_dw -f sql/creacion_agroceuta.sql
   ```

### Cómo ejecutar el benchmark

1. Instalar dependencia:
   ```bash
   pip install psycopg2
   ```

2. Editar `benchmark_agroceuta.py` y cambiar `user` y `password` por los valores de tu instalación local de PostgreSQL.

3. Ejecutar:
   ```bash
   python benchmark_agroceuta.py
   ```

***

## Práctica 2 - Integración de Sistemas (Diseño del Componente ETL)

Automatización del proceso ETL usando **Pentaho Data Integration (PDI)**. Incluye data profiling sobre un dataset meteorológico externo, diseño e implementación de transformaciones ETL con arquitectura de 3 capas (OLTP → Staging → DW), e integración de un sistema externo con el modelo OLAP.

### Contenido relevante

- `pentaho/limpieza_meteo_completo.ktr` - limpieza y carga del dataset meteorológico en `staging.observaciones_clean` y `olap_snow.dim_clima`.
- `pentaho/etl_agroceuta.ktr` - limpieza y carga de clientes AgroCeuta con arquitectura de 3 capas y log de errores.
- `pentaho/analisis_olap_agroceuta.ktr` - análisis OLAP de ingresos vs. objetivos por familia de producto.
- `csv/objetivos_familias.csv` - fichero CSV externo con objetivos de ingresos por familia (sistema externo del Hito 4).
- `csv/analisis_ventas_agroceuta.csv` - resultados generados por `analisis_olap_agroceuta.ktr`.
- `documento_practica2_SANTOS_RIVAS.pdf` - memoria final entregada.

### Requisitos adicionales

- Pentaho Data Integration (PDI) 11
- Java 17 (OpenJDK)
- Driver JDBC de PostgreSQL (`postgresql-42.x.x.jar`) copiado en `[directorio_pentaho]/lib/`

### Cómo ejecutar las transformaciones

1. Lanzar Pentaho Spoon:
   ```bash
   cd /opt/pentaho/data-integration/
   ./spoon.sh
   ```

2. Configurar la conexión a PostgreSQL en **File > New > Database Connection**:
   - Host: `localhost`, Port: `5432`
   - Database: `agroceuta_dw`
   - User/Password: los de tu instalación local

3. Abrir y ejecutar las transformaciones en orden:
   1. `limpieza_meteo_completo.ktr`
   2. `etl_agroceuta.ktr` *(ejecutar dos veces: primera carga staging, segunda carga OLAP)*
   3. `analisis_olap_agroceuta.ktr`

***

## Práctica 3 - Consultas multidimensionales, optimización y puesta en valor en un sistema BI

Explotación analítica del Data Warehouse mediante consultas SQL multidimensionales, optimización de planes de ejecución e implementación de un dashboard de Business Intelligence con Metabase.

**Hito 1 - Consultas analíticas:** formulación de las cinco preguntas de negocio de AgroCeuta como consultas SQL ejecutables sobre los tres modelos (OLTP, estrella y snowflake), con identificación de las operaciones multidimensionales que materializan (roll-up, drill-down, slice, dice) y verificación de resultados.

**Hito 2 - Optimización:** análisis de planes de ejecución con `EXPLAIN ANALYZE` pre y post-optimización, creación de un índice compuesto sobre las claves de join de `fact_exportaciones` y materialización de la consulta más costosa (C1) en una vista materializada. Benchmark comparativo con script Python.

**Hito 3 - Business Intelligence:** conexión del DW a Metabase, construcción de un dashboard con cinco visualizaciones (una por pregunta de negocio), tres KPIs (ingresos netos, margen neto, precio medio/kg) y cuatro filtros interactivos que materializan operaciones OLAP reales (slice, dice, drill-down, roll-up).

### Contenido relevante

- `sql/ampliacion_agroceuta.sql` - ampliación de la población del DW (de 180 a 360 filas en `fact_exportaciones`) para mejorar las visualizaciones en Metabase.
- `practica3_SANTOSRIVAS.pdf` - memoria final entregada.

### Requisitos adicionales

- Metabase (versión JAR o Docker)
- El DW debe estar montado previamente con `sql/creacion_agroceuta.sql` (Práctica 1)

### Cómo ampliar la población del DW

Con la base de datos ya cargada, ejecutar:

```bash
psql -U postgres -d agroceuta_dw -f sql/ampliacion_agroceuta.sql
```

### Cómo conectar Metabase al DW

1. Lanzar Metabase:
   ```bash
   java -jar metabase.jar
   ```

2. En el asistente de configuración inicial, añadir una conexión PostgreSQL:
   - Host: `localhost`, Port: `5432`
   - Database: `agroceuta_dw`, Schema: `olap_star`
   - User/Password: los de tu instalación local
