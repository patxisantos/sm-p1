BEGIN;

-- 1. Nuevos pedidos en 2022, 2023 y 2025
INSERT INTO oltp_ventas.pedidos (cliente_id, contrato_id, fecha_pedido, fecha_entrega_solicitada, estado, observaciones)
SELECT
  (g - 1) % 60 + 1,
  (g - 1) % 60 + 1,
  CASE
    WHEN g <= 60  THEN DATE '2022-02-01' + (g * 5)
    WHEN g <= 120 THEN DATE '2023-02-01' + ((g - 60) * 5)
    ELSE               DATE '2025-02-01' + ((g - 120) * 5)
  END,
  CASE
    WHEN g <= 60  THEN DATE '2022-02-06' + (g * 5)
    WHEN g <= 120 THEN DATE '2023-02-06' + ((g - 60) * 5)
    ELSE               DATE '2025-02-06' + ((g - 120) * 5)
  END,
  'entregado',
  'Pedido histórico ampliación demo'
FROM generate_series(1, 180) g;

-- 2. Detalle de pedido para los nuevos pedidos (reutiliza lotes 1-180 en ciclo)
INSERT INTO oltp_ventas.detalle_pedido (pedido_id, lote_id, producto, variedad, kilos_solicitados, cajas_solicitadas, precio_unitario_kg, descuento_porcentaje, importe_bruto, importe_neto)
SELECT
  p.pedido_id,
  ((p.pedido_id - 181) % 180) + 1,
  cu.nombre_producto,
  cu.variedad,
  ROUND((120 + p.pedido_id % 60 * 4)::numeric, 2),
  10 + p.pedido_id % 8,
  ROUND((CASE cu.nombre_producto
    WHEN 'Tomate'    THEN 2.20 WHEN 'Pimiento'   THEN 2.50
    WHEN 'Pepino'    THEN 1.95 WHEN 'Aguacate'   THEN 3.80
    WHEN 'Fresa'     THEN 3.20 WHEN 'Naranja'    THEN 1.85
    WHEN 'Limon'     THEN 2.05 WHEN 'Calabacin'  THEN 1.90
    WHEN 'Frambuesa' THEN 4.10 WHEN 'Arandano'   THEN 4.40
    ELSE 2.10 END + p.pedido_id % 5 * 0.10)::numeric, 2),
  con.descuento_general_porcentaje,
  ROUND((120 + p.pedido_id % 60 * 4) *
    (CASE cu.nombre_producto
      WHEN 'Tomate'    THEN 2.20 WHEN 'Pimiento'   THEN 2.50
      WHEN 'Pepino'    THEN 1.95 WHEN 'Aguacate'   THEN 3.80
      WHEN 'Fresa'     THEN 3.20 WHEN 'Naranja'    THEN 1.85
      WHEN 'Limon'     THEN 2.05 WHEN 'Calabacin'  THEN 1.90
      WHEN 'Frambuesa' THEN 4.10 WHEN 'Arandano'   THEN 4.40
      ELSE 2.10 END + p.pedido_id % 5 * 0.10)::numeric, 2),
  ROUND((120 + p.pedido_id % 60 * 4) *
    (CASE cu.nombre_producto
      WHEN 'Tomate'    THEN 2.20 WHEN 'Pimiento'   THEN 2.50
      WHEN 'Pepino'    THEN 1.95 WHEN 'Aguacate'   THEN 3.80
      WHEN 'Fresa'     THEN 3.20 WHEN 'Naranja'    THEN 1.85
      WHEN 'Limon'     THEN 2.05 WHEN 'Calabacin'  THEN 1.90
      WHEN 'Frambuesa' THEN 4.10 WHEN 'Arandano'   THEN 4.40
      ELSE 2.10 END + p.pedido_id % 5 * 0.10) *
    (1 - con.descuento_general_porcentaje / 100.0)::numeric, 2)
FROM oltp_ventas.pedidos p
JOIN oltp_ventas.contratos con ON con.contrato_id = p.contrato_id
JOIN oltp_produccion.lotes_origen lo ON lo.lote_id = ((p.pedido_id - 181) % 180) + 1
JOIN oltp_produccion.cosechas co ON co.cosecha_id = lo.cosecha_id
JOIN oltp_produccion.cultivos cu ON cu.cultivo_id = co.cultivo_id
WHERE p.pedido_id BETWEEN 181 AND 360;

-- 3. Envíos para los nuevos pedidos
INSERT INTO oltp_logistica.envios (pedido_id, ruta_id, transitario_id, fecha_envio, fecha_entrega_estimada, fecha_entrega_real, kilos_totales, numero_contenedor, coste_envio_total, coste_por_kg, estado)
SELECT
  p.pedido_id,
  ((p.pedido_id - 181) % 120) + 1,
  r.transitario_id,
  p.fecha_pedido + 1,
  p.fecha_pedido + 1 + r.duracion_estimada_dias,
  p.fecha_pedido + 1 + r.duracion_estimada_dias + p.pedido_id % 2,
  d.kilos_solicitados,
  'CONT-' || LPAD(p.pedido_id::text, 6, '0'),
  ROUND(d.kilos_solicitados * r.coste_base_kg::numeric, 2),
  ROUND(r.coste_base_kg::numeric, 4),
  'entregado'
FROM oltp_ventas.pedidos p
JOIN oltp_ventas.detalle_pedido d ON d.pedido_id = p.pedido_id
JOIN oltp_logistica.rutas_logisticas r ON r.ruta_id = ((p.pedido_id - 181) % 120) + 1
WHERE p.pedido_id BETWEEN 181 AND 360;

-- 4. Costes logísticos para los nuevos envíos
INSERT INTO oltp_finanzas.costes_logisticos (envio_id, coste_transporte, coste_aduana, coste_almacenaje, coste_seguro, coste_total_logistica, coste_por_kg)
SELECT
  e.envio_id,
  ROUND(e.kilos_totales * e.coste_por_kg * 0.70::numeric, 2),
  ROUND((10 + e.envio_id % 8 * 2)::numeric, 2),
  ROUND((8 + e.envio_id % 6 * 1.5)::numeric, 2),
  ROUND((5 + e.envio_id % 5 * 1.2)::numeric, 2),
  ROUND((e.kilos_totales * e.coste_por_kg * 0.70 + 10 + e.envio_id % 8 * 2 + 8 + e.envio_id % 6 * 1.5 + 5 + e.envio_id % 5 * 1.2)::numeric, 2),
  ROUND((e.kilos_totales * e.coste_por_kg * 0.70 + 10 + e.envio_id % 8 * 2 + 8 + e.envio_id % 6 * 1.5 + 5 + e.envio_id % 5 * 1.2) / e.kilos_totales::numeric, 4)
FROM oltp_logistica.envios e
WHERE e.pedido_id BETWEEN 181 AND 360;

-- 5. Poblar dim_tiempo con las nuevas fechas
INSERT INTO olap_star.dim_tiempo (fecha, dia, mes, trimestre, anio, semana, dia_semana, es_festivo)
SELECT DISTINCT
  p.fecha_pedido,
  EXTRACT(DAY   FROM p.fecha_pedido)::INT,
  EXTRACT(MONTH FROM p.fecha_pedido)::INT,
  EXTRACT(QUARTER FROM p.fecha_pedido)::INT,
  EXTRACT(YEAR  FROM p.fecha_pedido)::INT,
  EXTRACT(WEEK  FROM p.fecha_pedido)::INT,
  TO_CHAR(p.fecha_pedido, 'Day'),
  CASE WHEN EXTRACT(ISODOW FROM p.fecha_pedido) IN (6,7) THEN TRUE ELSE FALSE END
FROM oltp_ventas.pedidos p
WHERE p.pedido_id BETWEEN 181 AND 360
ON CONFLICT (fecha) DO NOTHING;

-- 6. Insertar en fact_exportaciones los nuevos registros en olap_star
INSERT INTO olap_star.fact_exportaciones (
  tiempo_sk, producto_sk, cliente_sk, geografia_sk, finca_sk,
  calidad_sk, logistica_sk, campania_sk,
  kilos_vendidos, cajas_vendidas, importe_bruto, descuento,
  importe_neto, coste_producto, coste_logistico, coste_total,
  margen, precio_medio_kg, tiempo_entrega_dias)
SELECT
  dt.tiempo_sk, dp.producto_sk, dc.cliente_sk, dg.geografia_sk, df.finca_sk,
  dq.calidad_sk, dl.logistica_sk, dca.campania_sk,
  d.kilos_solicitados, d.cajas_solicitadas, d.importe_bruto,
  ROUND((d.importe_bruto - d.importe_neto)::numeric, 2),
  d.importe_neto,
  ROUND((d.kilos_solicitados * cl.coste_por_kg)::numeric, 2),
  ROUND((d.kilos_solicitados * clog.coste_por_kg)::numeric, 2),
  ROUND((d.kilos_solicitados * cl.coste_por_kg + d.kilos_solicitados * clog.coste_por_kg)::numeric, 2),
  ROUND((d.importe_neto - d.kilos_solicitados * cl.coste_por_kg - d.kilos_solicitados * clog.coste_por_kg)::numeric, 2),
  ROUND((d.importe_neto / d.kilos_solicitados)::numeric, 4),
  (e.fecha_entrega_real - e.fecha_envio)
FROM oltp_ventas.detalle_pedido d
JOIN oltp_ventas.pedidos p          ON p.pedido_id = d.pedido_id
JOIN oltp_ventas.clientes c         ON c.cliente_id = p.cliente_id
JOIN oltp_logistica.envios e        ON e.pedido_id = p.pedido_id
JOIN oltp_finanzas.costes_logisticos clog ON clog.envio_id = e.envio_id
JOIN oltp_finanzas.costes_lote cl   ON cl.lote_id = d.lote_id
JOIN oltp_produccion.lotes_origen lo ON lo.lote_id = d.lote_id
JOIN oltp_produccion.cosechas co    ON co.cosecha_id = lo.cosecha_id
JOIN oltp_produccion.cultivos cu    ON cu.cultivo_id = co.cultivo_id
JOIN oltp_produccion.parcelas pa    ON pa.parcela_id = co.parcela_id
JOIN oltp_produccion.fincas fi      ON fi.finca_id = pa.finca_id
JOIN oltp_produccion.campanias ca   ON ca.campania_id = co.campania_id
JOIN olap_star.dim_tiempo dt        ON dt.fecha = p.fecha_pedido
JOIN olap_star.dim_producto dp      ON dp.cultivo_id = cu.cultivo_id
JOIN olap_star.dim_cliente dc       ON dc.cliente_id = c.cliente_id
JOIN olap_star.dim_geografia dg     ON dg.pais = c.pais AND dg.ciudad = c.ciudad
JOIN olap_star.dim_finca df         ON df.finca_id = fi.finca_id AND df.parcela = pa.nombre_parcela
JOIN olap_star.dim_calidad dq       ON dq.lote_id = d.lote_id
JOIN olap_star.dim_logistica dl     ON dl.ruta_id = e.ruta_id
JOIN olap_star.dim_campania dca     ON dca.campania_id = ca.campania_id
WHERE p.pedido_id BETWEEN 181 AND 360;

-- 7. Insertar en olap_snow.dim_tiempo las nuevas fechas para los nuevos pedidos
INSERT INTO olap_snow.dim_tiempo (fecha, dia, mes, trimestre, anio, semana, dia_semana, es_festivo)
SELECT DISTINCT
  p.fecha_pedido,
  EXTRACT(DAY   FROM p.fecha_pedido)::INT,
  EXTRACT(MONTH FROM p.fecha_pedido)::INT,
  EXTRACT(QUARTER FROM p.fecha_pedido)::INT,
  EXTRACT(YEAR  FROM p.fecha_pedido)::INT,
  EXTRACT(WEEK  FROM p.fecha_pedido)::INT,
  TO_CHAR(p.fecha_pedido, 'Day'),
  CASE WHEN EXTRACT(ISODOW FROM p.fecha_pedido) IN (6,7) THEN TRUE ELSE FALSE END
FROM oltp_ventas.pedidos p
WHERE p.pedido_id BETWEEN 181 AND 360
ON CONFLICT (fecha) DO NOTHING;

-- 8. Insertar en fact_exportaciones los nuevos registros en olap_snow
INSERT INTO olap_snow.fact_exportaciones (
  tiempo_sk, producto_sk, cliente_sk, geografia_sk, finca_sk,
  calidad_sk, logistica_sk, campania_sk,
  kilos_vendidos, cajas_vendidas, importe_bruto, descuento,
  importe_neto, coste_producto, coste_logistico, coste_total,
  margen, precio_medio_kg, tiempo_entrega_dias)
SELECT
  dt.tiempo_sk, dp.producto_sk, dc.cliente_sk, dg.geografia_sk, df.finca_sk,
  dq.calidad_sk, dl.logistica_sk, dca.campania_sk,
  d.kilos_solicitados, d.cajas_solicitadas, d.importe_bruto,
  ROUND((d.importe_bruto - d.importe_neto)::numeric, 2),
  d.importe_neto,
  ROUND((d.kilos_solicitados * cl.coste_por_kg)::numeric, 2),
  ROUND((d.kilos_solicitados * clog.coste_por_kg)::numeric, 2),
  ROUND((d.kilos_solicitados * cl.coste_por_kg + d.kilos_solicitados * clog.coste_por_kg)::numeric, 2),
  ROUND((d.importe_neto - d.kilos_solicitados * cl.coste_por_kg - d.kilos_solicitados * clog.coste_por_kg)::numeric, 2),
  ROUND((d.importe_neto / d.kilos_solicitados)::numeric, 4),
  (e.fecha_entrega_real - e.fecha_envio)
FROM oltp_ventas.detalle_pedido d
JOIN oltp_ventas.pedidos p          ON p.pedido_id = d.pedido_id
JOIN oltp_ventas.clientes c         ON c.cliente_id = p.cliente_id
JOIN oltp_logistica.envios e        ON e.pedido_id = p.pedido_id
JOIN oltp_finanzas.costes_logisticos clog ON clog.envio_id = e.envio_id
JOIN oltp_finanzas.costes_lote cl   ON cl.lote_id = d.lote_id
JOIN oltp_produccion.lotes_origen lo ON lo.lote_id = d.lote_id
JOIN oltp_produccion.cosechas co    ON co.cosecha_id = lo.cosecha_id
JOIN oltp_produccion.cultivos cu    ON cu.cultivo_id = co.cultivo_id
JOIN oltp_produccion.parcelas pa    ON pa.parcela_id = co.parcela_id
JOIN oltp_produccion.fincas fi      ON fi.finca_id = pa.finca_id
JOIN oltp_produccion.campanias ca   ON ca.campania_id = co.campania_id
JOIN olap_snow.dim_tiempo dt        ON dt.fecha = p.fecha_pedido
JOIN olap_snow.dim_producto dp      ON dp.cultivo_id = cu.cultivo_id
JOIN olap_snow.dim_cliente dc       ON dc.cliente_id = c.cliente_id
JOIN olap_snow.dim_geografia dg     ON dg.pais = c.pais AND dg.ciudad = c.ciudad
JOIN olap_snow.dim_finca df         ON df.finca_id = fi.finca_id AND df.parcela = pa.nombre_parcela
JOIN olap_snow.dim_calidad dq       ON dq.lote_id = d.lote_id
JOIN olap_snow.dim_logistica dl     ON dl.ruta_id = e.ruta_id
JOIN olap_snow.dim_campania dca     ON dca.campania_id = ca.campania_id
WHERE p.pedido_id BETWEEN 181 AND 360;

COMMIT;
