CREATE INDEX idx_fact_exportaciones_sks
ON olap_star.fact_exportaciones (tiempo_sk, producto_sk, cliente_sk, finca_sk, calidad_sk, logistica_sk, campania_sk, geografia_sk);

CREATE MATERIALIZED VIEW olap_star.mv_ingresos_trimestre AS
SELECT t.anio, t.trimestre, p.nombre_producto, p.variedad,
       g.pais, ca.nombre_campania,
       SUM(f.importe_neto)      AS ingresos,
       SUM(f.coste_producto)    AS coste_prod,
       SUM(f.coste_logistico)   AS coste_log,
       SUM(f.margen)            AS margen
FROM olap_star.fact_exportaciones f
JOIN olap_star.dim_tiempo t     ON t.tiempo_sk    = f.tiempo_sk
JOIN olap_star.dim_producto p   ON p.producto_sk  = f.producto_sk
JOIN olap_star.dim_geografia g  ON g.geografia_sk = f.geografia_sk
JOIN olap_star.dim_campania ca  ON ca.campania_sk = f.campania_sk
GROUP BY t.anio, t.trimestre, p.nombre_producto, p.variedad, g.pais, ca.nombre_campania
WITH DATA;

-- Refresco cuando entren nuevos datos:
REFRESH MATERIALIZED VIEW olap_star.mv_ingresos_trimestre;
