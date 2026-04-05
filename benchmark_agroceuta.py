import psycopg2
import time
import statistics

conn = psycopg2.connect(    
    host="localhost",    
    database="agroceuta_dw",    
    user="TU_USUARIO",    
    password="TU_PASSWORD")

cur = conn.cursor()

# ------------------------------# CONSULTAS # ------------------------------

QUERIES = {
    "Q1_oltp": """
        SELECT
            EXTRACT(YEAR FROM p.fecha_pedido) AS anio,
            EXTRACT(QUARTER FROM p.fecha_pedido) AS trimestre,
            ca.nombre_campania,
            cu.familia_producto,
            cu.nombre_producto,
            cu.variedad,
            c.pais,
            SUM(d.importe_neto) AS ingresos,
            SUM(d.kilos_solicitados * cl.coste_por_kg) AS costeproducto,
            SUM(d.kilos_solicitados * clog.coste_por_kg) AS costelogistico,
            SUM(d.importe_neto - (d.kilos_solicitados * cl.coste_por_kg) - (d.kilos_solicitados * clog.coste_por_kg)) AS margen
        FROM oltp_ventas.detalle_pedido d
        JOIN oltp_ventas.pedidos p ON p.pedido_id = d.pedido_id
        JOIN oltp_ventas.clientes c ON c.cliente_id = p.cliente_id
        JOIN oltp_logistica.envios e ON e.pedido_id = p.pedido_id
        JOIN oltp_finanzas.costes_logisticos clog ON clog.envio_id = e.envio_id
        JOIN oltp_finanzas.costes_lote cl ON cl.lote_id = d.lote_id
        JOIN oltp_produccion.lotes_origen lo ON lo.lote_id = d.lote_id
        JOIN oltp_produccion.cosechas co ON co.cosecha_id = lo.cosecha_id
        JOIN oltp_produccion.cultivos cu ON cu.cultivo_id = co.cultivo_id
        JOIN oltp_produccion.campanias ca ON ca.campania_id = co.campania_id
        GROUP BY
            EXTRACT(YEAR FROM p.fecha_pedido),
            EXTRACT(QUARTER FROM p.fecha_pedido),
            ca.nombre_campania,
            cu.familia_producto,
            cu.nombre_producto,
            cu.variedad,
            c.pais;
    """,

    "Q1_star": """
        SELECT
            t.anio,
            t.trimestre,
            ca.nombre_campania,
            p.familia_producto,
            p.nombre_producto,
            p.variedad,
            g.pais,
            SUM(f.importe_neto) AS ingresos,
            SUM(f.coste_producto) AS costeproducto,
            SUM(f.coste_logistico) AS costelogistico,
            SUM(f.margen) AS margen
        FROM olap_star.fact_exportaciones f
        JOIN olap_star.dim_tiempo t ON t.tiempo_sk = f.tiempo_sk
        JOIN olap_star.dim_producto p ON p.producto_sk = f.producto_sk
        JOIN olap_star.dim_geografia g ON g.geografia_sk = f.geografia_sk
        JOIN olap_star.dim_campania ca ON ca.campania_sk = f.campania_sk
        GROUP BY
            t.anio,
            t.trimestre,
            ca.nombre_campania,
            p.familia_producto,
            p.nombre_producto,
            p.variedad,
            g.pais;
    """,

    "Q1_snow": """
        SELECT
            t.anio,
            t.trimestre,
            ca.nombre_campania,
            fp.familia_producto,
            p.nombre_producto,
            p.variedad,
            g.pais,
            SUM(f.importe_neto) AS ingresos,
            SUM(f.coste_producto) AS costeproducto,
            SUM(f.coste_logistico) AS costelogistico,
            SUM(f.margen) AS margen
        FROM olap_snow.fact_exportaciones f
        JOIN olap_snow.dim_tiempo t ON t.tiempo_sk = f.tiempo_sk
        JOIN olap_snow.dim_producto p ON p.producto_sk = f.producto_sk
        JOIN olap_snow.dim_familia_producto fp ON fp.familia_sk = p.familia_sk
        JOIN olap_snow.dim_geografia g ON g.geografia_sk = f.geografia_sk
        JOIN olap_snow.dim_campania ca ON ca.campania_sk = f.campania_sk
        GROUP BY
            t.anio,
            t.trimestre,
            ca.nombre_campania,
            fp.familia_producto,
            p.nombre_producto,
            p.variedad,
            g.pais;
    """,

    "Q2_oltp": """
        SELECT
            fi.nombre_finca,
            ca.nombre_campania,
            cu.familia_producto,
            cu.nombre_producto,
            SUM(d.kilos_solicitados) AS kilos_vendidos,
            SUM(d.importe_neto - (d.kilos_solicitados * cl.coste_por_kg) - (d.kilos_solicitados * clog.coste_por_kg)) AS margen
        FROM oltp_ventas.detalle_pedido d
        JOIN oltp_ventas.pedidos p ON p.pedido_id = d.pedido_id
        JOIN oltp_logistica.envios e ON e.pedido_id = p.pedido_id
        JOIN oltp_finanzas.costes_logisticos clog ON clog.envio_id = e.envio_id
        JOIN oltp_finanzas.costes_lote cl ON cl.lote_id = d.lote_id
        JOIN oltp_produccion.lotes_origen lo ON lo.lote_id = d.lote_id
        JOIN oltp_produccion.cosechas co ON co.cosecha_id = lo.cosecha_id
        JOIN oltp_produccion.parcelas pa ON pa.parcela_id = co.parcela_id
        JOIN oltp_produccion.fincas fi ON fi.finca_id = pa.finca_id
        JOIN oltp_produccion.cultivos cu ON cu.cultivo_id = co.cultivo_id
        JOIN oltp_produccion.campanias ca ON ca.campania_id = co.campania_id
        GROUP BY
            fi.nombre_finca,
            ca.nombre_campania,
            cu.familia_producto,
            cu.nombre_producto;
    """,

    "Q2_star": """
        SELECT
            fi.nombre_finca,
            ca.nombre_campania,
            p.familia_producto,
            p.nombre_producto,
            SUM(f.kilos_vendidos) AS kilos_vendidos,
            SUM(f.margen) AS margen
        FROM olap_star.fact_exportaciones f
        JOIN olap_star.dim_finca fi ON fi.finca_sk = f.finca_sk
        JOIN olap_star.dim_producto p ON p.producto_sk = f.producto_sk
        JOIN olap_star.dim_campania ca ON ca.campania_sk = f.campania_sk
        GROUP BY
            fi.nombre_finca,
            ca.nombre_campania,
            p.familia_producto,
            p.nombre_producto;
    """,

    "Q2_snow": """
        SELECT
            fi.nombre_finca,
            ca.nombre_campania,
            fp.familia_producto,
            p.nombre_producto,
            SUM(f.kilos_vendidos) AS kilos_vendidos,
            SUM(f.margen) AS margen
        FROM olap_snow.fact_exportaciones f
        JOIN olap_snow.dim_finca fi ON fi.finca_sk = f.finca_sk
        JOIN olap_snow.dim_producto p ON p.producto_sk = f.producto_sk
        JOIN olap_snow.dim_familia_producto fp ON fp.familia_sk = p.familia_sk
        JOIN olap_snow.dim_campania ca ON ca.campania_sk = f.campania_sk
        GROUP BY
            fi.nombre_finca,
            ca.nombre_campania,
            fp.familia_producto,
            p.nombre_producto;
    """,

    "Q3_oltp": """
        SELECT
            cu.familia_producto,
            c.nombre_cliente,
            c.pais,
            SUM(d.kilos_solicitados) AS kilos_vendidos,
            AVG(d.importe_neto / NULLIF(d.kilos_solicitados, 0)) AS precio_medio_kg,
            SUM(d.importe_neto - (d.kilos_solicitados * cl.coste_por_kg) - (d.kilos_solicitados * clog.coste_por_kg)) AS margen
        FROM oltp_ventas.detalle_pedido d
        JOIN oltp_ventas.pedidos p ON p.pedido_id = d.pedido_id
        JOIN oltp_ventas.clientes c ON c.cliente_id = p.cliente_id
        JOIN oltp_logistica.envios e ON e.pedido_id = p.pedido_id
        JOIN oltp_finanzas.costes_logisticos clog ON clog.envio_id = e.envio_id
        JOIN oltp_finanzas.costes_lote cl ON cl.lote_id = d.lote_id
        JOIN oltp_produccion.lotes_origen lo ON lo.lote_id = d.lote_id
        JOIN oltp_produccion.cosechas co ON co.cosecha_id = lo.cosecha_id
        JOIN oltp_produccion.cultivos cu ON cu.cultivo_id = co.cultivo_id
        GROUP BY
            cu.familia_producto,
            c.nombre_cliente,
            c.pais;
    """,

    "Q3_star": """
        SELECT
            p.familia_producto,
            c.nombre_cliente,
            g.pais,
            SUM(f.kilos_vendidos) AS kilos_vendidos,
            AVG(f.precio_medio_kg) AS precio_medio_kg,
            SUM(f.margen) AS margen
        FROM olap_star.fact_exportaciones f
        JOIN olap_star.dim_producto p ON p.producto_sk = f.producto_sk
        JOIN olap_star.dim_cliente c ON c.cliente_sk = f.cliente_sk
        JOIN olap_star.dim_geografia g ON g.geografia_sk = f.geografia_sk
        GROUP BY
            p.familia_producto,
            c.nombre_cliente,
            g.pais;
    """,

    "Q3_snow": """
        SELECT
            fp.familia_producto,
            c.nombre_cliente,
            g.pais,
            SUM(f.kilos_vendidos) AS kilos_vendidos,
            AVG(f.precio_medio_kg) AS precio_medio_kg,
            SUM(f.margen) AS margen
        FROM olap_snow.fact_exportaciones f
        JOIN olap_snow.dim_producto p ON p.producto_sk = f.producto_sk
        JOIN olap_snow.dim_familia_producto fp ON fp.familia_sk = p.familia_sk
        JOIN olap_snow.dim_cliente c ON c.cliente_sk = f.cliente_sk
        JOIN olap_snow.dim_geografia g ON g.geografia_sk = f.geografia_sk
        GROUP BY
            fp.familia_producto,
            c.nombre_cliente,
            g.pais;
    """,

    "Q4_oltp": """
        SELECT
            cu.familia_producto,
            tr.nombre_transitario,
            ps.nombre_puerto AS puerto_salida,
            pl.nombre_puerto AS puerto_llegada,
            AVG(e.fecha_entrega_real - e.fecha_envio) AS tiempo_medio_entrega,
            AVG(clog.coste_por_kg) AS costelogisticomediokg
        FROM oltp_logistica.envios e
        JOIN oltp_ventas.pedidos p ON p.pedido_id = e.pedido_id
        JOIN oltp_ventas.detalle_pedido d ON d.pedido_id = p.pedido_id
        JOIN oltp_produccion.lotes_origen lo ON lo.lote_id = d.lote_id
        JOIN oltp_produccion.cosechas co ON co.cosecha_id = lo.cosecha_id
        JOIN oltp_produccion.cultivos cu ON cu.cultivo_id = co.cultivo_id
        JOIN oltp_logistica.rutas_logisticas r ON r.ruta_id = e.ruta_id
        JOIN oltp_logistica.transitarios tr ON tr.transitario_id = r.transitario_id
        JOIN oltp_logistica.puertos ps ON ps.puerto_id = r.puerto_salida_id
        JOIN oltp_logistica.puertos pl ON pl.puerto_id = r.puerto_llegada_id
        JOIN oltp_finanzas.costes_logisticos clog ON clog.envio_id = e.envio_id
        GROUP BY
            cu.familia_producto,
            tr.nombre_transitario,
            ps.nombre_puerto,
            pl.nombre_puerto;
    """,

    "Q4_star": """
        SELECT
            p.familia_producto,
            l.nombre_transitario,
            l.puerto_salida,
            l.puerto_llegada,
            AVG(f.tiempo_entrega_dias) AS tiempo_medio_entrega,
            AVG(f.coste_logistico / NULLIF(f.kilos_vendidos, 0)) AS costelogisticomediokg
        FROM olap_star.fact_exportaciones f
        JOIN olap_star.dim_producto p ON p.producto_sk = f.producto_sk
        JOIN olap_star.dim_logistica l ON l.logistica_sk = f.logistica_sk
        GROUP BY
            p.familia_producto,
            l.nombre_transitario,
            l.puerto_salida,
            l.puerto_llegada;
    """,

    "Q4_snow": """
        SELECT
            fp.familia_producto,
            l.nombre_transitario,
            l.puerto_salida,
            l.puerto_llegada,
            AVG(f.tiempo_entrega_dias) AS tiempo_medio_entrega,
            AVG(f.coste_logistico / NULLIF(f.kilos_vendidos, 0)) AS coste_logistico_medio_kg
        FROM olap_snow.fact_exportaciones f
        JOIN olap_snow.dim_producto p ON p.producto_sk = f.producto_sk
        JOIN olap_snow.dim_familia_producto fp ON fp.familia_sk = p.familia_sk
        JOIN olap_snow.dim_logistica l ON l.logistica_sk = f.logistica_sk
        GROUP BY
            fp.familia_producto,
            l.nombre_transitario,
            l.puerto_salida,
            l.puerto_llegada;
    """,

    "Q5_oltp": """
        WITH calibredominante AS (
            SELECT lote_id AS loteid, calibre
            FROM (
                SELECT
                    lote_id,
                    calibre,
                    kilos_calibre AS kiloscalibre,
                    ROW_NUMBER() OVER (PARTITION BY lote_id ORDER BY kilos_calibre DESC, calibre) AS rn
                FROM oltp_calidad.calibres
            ) x
            WHERE rn = 1
        )
        SELECT
            cu.familia_producto,
            CASE
                WHEN cc.porcentaje_primera_categoria >= 70 THEN 'Primera'
                ELSE 'Segunda'
            END AS categoriacomercial,
            cd.calibre,
            cert.tipo_certificado,
            AVG(d.importe_neto / NULLIF(d.kilos_solicitados, 0)) AS preciomediokg,
            SUM(d.importe_neto - (d.kilos_solicitados * cl.coste_por_kg) - (d.kilos_solicitados * clog.coste_por_kg)) AS margentotal
        FROM oltp_ventas.detalle_pedido d
        JOIN oltp_ventas.pedidos p ON p.pedido_id = d.pedido_id
        JOIN oltp_logistica.envios e ON e.pedido_id = p.pedido_id
        JOIN oltp_finanzas.costes_logisticos clog ON clog.envio_id = e.envio_id
        JOIN oltp_finanzas.costes_lote cl ON cl.lote_id = d.lote_id
        JOIN oltp_produccion.lotes_origen lo ON lo.lote_id = d.lote_id
        JOIN oltp_produccion.cosechas co ON co.cosecha_id = lo.cosecha_id
        JOIN oltp_produccion.cultivos cu ON cu.cultivo_id = co.cultivo_id
        JOIN oltp_calidad.controles_calidad cc ON cc.lote_id = d.lote_id
        JOIN calibredominante cd ON cd.loteid = d.lote_id
        JOIN oltp_calidad.certificados cert ON cert.lote_id = d.lote_id
        GROUP BY
            cu.familia_producto,
            CASE
                WHEN cc.porcentaje_primera_categoria >= 70 THEN 'Primera'
                ELSE 'Segunda'
            END,
            cd.calibre,
            cert.tipo_certificado;
    """,

    "Q5_star": """
        SELECT
            p.familia_producto,
            q.categoria_comercial,
            q.calibre,
            q.tipo_certificado,
            AVG(f.precio_medio_kg) AS preciomediokg,
            SUM(f.margen) AS margentotal
        FROM olap_star.fact_exportaciones f
        JOIN olap_star.dim_producto p ON p.producto_sk = f.producto_sk
        JOIN olap_star.dim_calidad q ON q.calidad_sk = f.calidad_sk
        GROUP BY
            p.familia_producto,
            q.categoria_comercial,
            q.calibre,
            q.tipo_certificado;
    """,

    "Q5_snow": """
        SELECT
            fp.familia_producto,
            q.categoria_comercial,
            q.calibre,
            q.tipo_certificado,
            AVG(f.precio_medio_kg) AS preciomediokg,
            SUM(f.margen) AS margentotal
        FROM olap_snow.fact_exportaciones f
        JOIN olap_snow.dim_producto p ON p.producto_sk = f.producto_sk
        JOIN olap_snow.dim_familia_producto fp ON fp.familia_sk = p.familia_sk
        JOIN olap_snow.dim_calidad q ON q.calidad_sk = f.calidad_sk
        GROUP BY
            fp.familia_producto,
            q.categoria_comercial,
            q.calibre,
            q.tipo_certificado;
    """
}

# ------------------------------# 
# FUNCIÓN DE MEDICIÓN # 
# ------------------------------# 

def medir(query, repeticiones=20):
    tiempos = []
    for _ in range(repeticiones):
        inicio = time.time()
        cur.execute(query)
        cur.fetchall()
        fin = time.time()
        tiempos.append(fin - inicio)
    return tiempos

# ------------------------------# 
# EJECUCIÓN # 
# ------------------------------#

def mostrar(nombre, tiempos):
    print(f"\n--- {nombre} ---")
    print("Promedio:", round(statistics.mean(tiempos), 6), "segundos")
    print("Desv Std:", round(statistics.stdev(tiempos), 6))

for nombre, query in QUERIES.items():
    print(f"Ejecutando {nombre}...")
    tiempos = medir(query)
    mostrar(nombre, tiempos)

cur.close()
conn.close()