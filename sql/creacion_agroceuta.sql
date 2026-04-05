BEGIN;

DROP SCHEMA IF EXISTS olap_snow CASCADE;
DROP SCHEMA IF EXISTS olap_star CASCADE;
DROP SCHEMA IF EXISTS oltp_finanzas CASCADE;
DROP SCHEMA IF EXISTS oltp_logistica CASCADE;
DROP SCHEMA IF EXISTS oltp_ventas CASCADE;
DROP SCHEMA IF EXISTS oltp_calidad CASCADE;
DROP SCHEMA IF EXISTS oltp_produccion CASCADE;

CREATE SCHEMA oltp_produccion;
CREATE SCHEMA oltp_calidad;
CREATE SCHEMA oltp_ventas;
CREATE SCHEMA oltp_logistica;
CREATE SCHEMA oltp_finanzas;

-- OLTP PRODUCCIÓN

CREATE TABLE oltp_produccion.fincas (
    finca_id SERIAL PRIMARY KEY,
    nombre_finca VARCHAR(100) NOT NULL,
    superficie_total_hectareas NUMERIC(10,2) NOT NULL CHECK (superficie_total_hectareas > 0),
    municipio VARCHAR(100) NOT NULL,
    provincia VARCHAR(100) NOT NULL,
    zona_agricola VARCHAR(100) NOT NULL,
    tipo_suelo VARCHAR(100) NOT NULL,
    sistema_riego VARCHAR(100) NOT NULL
);

CREATE TABLE oltp_produccion.parcelas (
    parcela_id SERIAL PRIMARY KEY,
    finca_id INT NOT NULL REFERENCES oltp_produccion.fincas(finca_id),
    nombre_parcela VARCHAR(100) NOT NULL,
    superficie_hectareas NUMERIC(8,2) NOT NULL CHECK (superficie_hectareas > 0),
    cultivo_actual VARCHAR(100) NOT NULL,
    fecha_ultima_rotacion DATE NOT NULL
);

CREATE TABLE oltp_produccion.campanias (
    campania_id SERIAL PRIMARY KEY,
    nombre_campania VARCHAR(100) NOT NULL,
    fecha_inicio DATE NOT NULL,
    fecha_fin DATE NOT NULL,
    temporada VARCHAR(50) NOT NULL,
    anio_agricola INT NOT NULL,
    observaciones_climaticas VARCHAR(255),
    CHECK (fecha_fin > fecha_inicio)
);

CREATE TABLE oltp_produccion.cultivos (
    cultivo_id SERIAL PRIMARY KEY,
    nombre_producto VARCHAR(100) NOT NULL,
    familia_producto VARCHAR(100) NOT NULL,
    variedad VARCHAR(100) NOT NULL,
    ciclo_dias INT NOT NULL CHECK (ciclo_dias > 0),
    riego_necesario_litros_hectarea NUMERIC(12,2) NOT NULL CHECK (riego_necesario_litros_hectarea > 0)
);

CREATE TABLE oltp_produccion.cosechas (
    cosecha_id SERIAL PRIMARY KEY,
    parcela_id INT NOT NULL REFERENCES oltp_produccion.parcelas(parcela_id),
    cultivo_id INT NOT NULL REFERENCES oltp_produccion.cultivos(cultivo_id),
    campania_id INT NOT NULL REFERENCES oltp_produccion.campanias(campania_id),
    fecha_cosecha DATE NOT NULL,
    kilos_cosechados NUMERIC(10,2) NOT NULL CHECK (kilos_cosechados > 0),
    rendimiento_kg_hectarea NUMERIC(12,2) NOT NULL CHECK (rendimiento_kg_hectarea >= 0),
    coste_produccion NUMERIC(12,2) NOT NULL CHECK (coste_produccion >= 0),
    merma_campo_porcentaje NUMERIC(5,2) NOT NULL CHECK (merma_campo_porcentaje BETWEEN 0 AND 100),
    numero_recolecciones INT NOT NULL CHECK (numero_recolecciones > 0)
);

CREATE TABLE oltp_produccion.lotes_origen (
    lote_id SERIAL PRIMARY KEY,
    cosecha_id INT NOT NULL REFERENCES oltp_produccion.cosechas(cosecha_id),
    codigo_lote VARCHAR(30) NOT NULL UNIQUE,
    kilos_lote NUMERIC(10,2) NOT NULL CHECK (kilos_lote > 0),
    fecha_creacion_lote DATE NOT NULL,
    estado VARCHAR(20) NOT NULL CHECK (estado IN ('en_campo','en_almacen','enviado'))
);

CREATE INDEX idx_cosechas_fecha ON oltp_produccion.cosechas(fecha_cosecha);
CREATE INDEX idx_lotes_codigo ON oltp_produccion.lotes_origen(codigo_lote);

-- OLTP CALIDAD

CREATE TABLE oltp_calidad.lotes (
    lote_id INT PRIMARY KEY REFERENCES oltp_produccion.lotes_origen(lote_id),
    codigo_lote VARCHAR(30) NOT NULL UNIQUE,
    producto VARCHAR(100) NOT NULL,
    kilos_totales NUMERIC(10,2) NOT NULL CHECK (kilos_totales > 0),
    fecha_entrada_almacen DATE NOT NULL
);

CREATE TABLE oltp_calidad.controles_calidad (
    control_id SERIAL PRIMARY KEY,
    lote_id INT NOT NULL REFERENCES oltp_calidad.lotes(lote_id),
    fecha_control DATE NOT NULL,
    inspector VARCHAR(120) NOT NULL,
    kilos_inspeccionados NUMERIC(10,2) NOT NULL CHECK (kilos_inspeccionados > 0),
    kilos_rechazados NUMERIC(10,2) NOT NULL CHECK (kilos_rechazados >= 0),
    porcentaje_merma NUMERIC(5,2) NOT NULL CHECK (porcentaje_merma BETWEEN 0 AND 100),
    porcentaje_primera_categoria NUMERIC(5,2) NOT NULL CHECK (porcentaje_primera_categoria BETWEEN 0 AND 100),
    porcentaje_segunda_categoria NUMERIC(5,2) NOT NULL CHECK (porcentaje_segunda_categoria BETWEEN 0 AND 100),
    puntuacion_calidad NUMERIC(4,2) NOT NULL CHECK (puntuacion_calidad BETWEEN 0 AND 10)
);

CREATE TABLE oltp_calidad.calibres (
    calibre_id SERIAL PRIMARY KEY,
    lote_id INT NOT NULL REFERENCES oltp_calidad.lotes(lote_id),
    calibre VARCHAR(10) NOT NULL CHECK (calibre IN ('S','M','L','XL')),
    kilos_calibre NUMERIC(10,2) NOT NULL CHECK (kilos_calibre >= 0),
    porcentaje_sobre_total NUMERIC(5,2) NOT NULL CHECK (porcentaje_sobre_total BETWEEN 0 AND 100)
);

CREATE TABLE oltp_calidad.certificados (
    certificado_id SERIAL PRIMARY KEY,
    lote_id INT NOT NULL REFERENCES oltp_calidad.lotes(lote_id),
    tipo_certificado VARCHAR(50) NOT NULL CHECK (tipo_certificado IN ('GlobalGAP','ECO','ISO22000','GRASP')),
    fecha_emision DATE NOT NULL,
    fecha_caducidad DATE NOT NULL,
    entidad_certificadora VARCHAR(100) NOT NULL,
    numero_certificado VARCHAR(50) NOT NULL UNIQUE,
    CHECK (fecha_caducidad > fecha_emision)
);

CREATE TABLE oltp_calidad.incidencias_calidad (
    incidencia_id SERIAL PRIMARY KEY,
    lote_id INT NOT NULL REFERENCES oltp_calidad.lotes(lote_id),
    fecha_incidencia DATE NOT NULL,
    tipo_incidencia VARCHAR(50) NOT NULL CHECK (tipo_incidencia IN ('plagas','defectos','contaminacion','rechazo_cliente')),
    descripcion VARCHAR(255) NOT NULL,
    kilos_afectados NUMERIC(10,2) NOT NULL CHECK (kilos_afectados >= 0),
    accion_correctiva VARCHAR(255) NOT NULL
);

CREATE INDEX idx_controles_fecha ON oltp_calidad.controles_calidad(fecha_control);
CREATE INDEX idx_certificados_tipo ON oltp_calidad.certificados(tipo_certificado);

-- OLTP VENTAS

CREATE TABLE oltp_ventas.clientes (
    cliente_id SERIAL PRIMARY KEY,
    nombre_cliente VARCHAR(150) NOT NULL,
    cif VARCHAR(10) NOT NULL UNIQUE,
    tipo_cliente VARCHAR(50) NOT NULL CHECK (tipo_cliente IN ('mayorista','distribuidor','minorista','retail')),
    canal VARCHAR(50) NOT NULL CHECK (canal IN ('directo','intermediario','plataforma')),
    segmento VARCHAR(50) NOT NULL CHECK (segmento IN ('premium','estandar','ocasional')),
    pais VARCHAR(100) NOT NULL,
    ciudad VARCHAR(100) NOT NULL,
    direccion VARCHAR(150) NOT NULL,
    codigo_postal VARCHAR(15) NOT NULL,
    telefono VARCHAR(30) NOT NULL,
    email VARCHAR(150) NOT NULL,
    fecha_alta DATE NOT NULL
);

CREATE TABLE oltp_ventas.contratos (
    contrato_id SERIAL PRIMARY KEY,
    cliente_id INT NOT NULL REFERENCES oltp_ventas.clientes(cliente_id),
    fecha_inicio DATE NOT NULL,
    fecha_fin DATE NOT NULL,
    condiciones_pago VARCHAR(100) NOT NULL,
    descuento_general_porcentaje NUMERIC(5,2) NOT NULL CHECK (descuento_general_porcentaje BETWEEN 0 AND 100),
    volumen_minimo_kg NUMERIC(10,2) NOT NULL CHECK (volumen_minimo_kg >= 0),
    precio_base_kg NUMERIC(10,2) NOT NULL CHECK (precio_base_kg > 0),
    CHECK (fecha_fin >= fecha_inicio)
);

CREATE TABLE oltp_ventas.pedidos (
    pedido_id SERIAL PRIMARY KEY,
    cliente_id INT NOT NULL REFERENCES oltp_ventas.clientes(cliente_id),
    contrato_id INT REFERENCES oltp_ventas.contratos(contrato_id),
    fecha_pedido DATE NOT NULL,
    fecha_entrega_solicitada DATE NOT NULL,
    estado VARCHAR(30) NOT NULL CHECK (estado IN ('pendiente','confirmado','enviado','entregado','cancelado')),
    observaciones VARCHAR(255),
    CHECK (fecha_entrega_solicitada >= fecha_pedido)
);

CREATE TABLE oltp_ventas.detalle_pedido (
    detalle_id SERIAL PRIMARY KEY,
    pedido_id INT NOT NULL REFERENCES oltp_ventas.pedidos(pedido_id),
    lote_id INT NOT NULL REFERENCES oltp_calidad.lotes(lote_id),
    producto VARCHAR(100) NOT NULL,
    variedad VARCHAR(100) NOT NULL,
    kilos_solicitados NUMERIC(10,2) NOT NULL CHECK (kilos_solicitados > 0),
    cajas_solicitadas INT NOT NULL CHECK (cajas_solicitadas > 0),
    precio_unitario_kg NUMERIC(10,2) NOT NULL CHECK (precio_unitario_kg > 0),
    descuento_porcentaje NUMERIC(5,2) NOT NULL CHECK (descuento_porcentaje BETWEEN 0 AND 100),
    importe_bruto NUMERIC(12,2) NOT NULL,
    importe_neto NUMERIC(12,2) NOT NULL,
    CONSTRAINT chk_detalle_importe_neto CHECK (importe_neto = ROUND(importe_bruto * (1 - descuento_porcentaje / 100.0), 2))
);

CREATE TABLE oltp_ventas.tarifas (
    tarifa_id SERIAL PRIMARY KEY,
    producto VARCHAR(100) NOT NULL,
    variedad VARCHAR(100) NOT NULL,
    calibre VARCHAR(10) NOT NULL CHECK (calibre IN ('S','M','L','XL')),
    precio_base_kg NUMERIC(10,2) NOT NULL CHECK (precio_base_kg > 0),
    fecha_vigencia_inicio DATE NOT NULL,
    fecha_vigencia_fin DATE NOT NULL,
    CHECK (fecha_vigencia_fin >= fecha_vigencia_inicio)
);

CREATE INDEX idx_pedidos_fecha ON oltp_ventas.pedidos(fecha_pedido);
CREATE INDEX idx_detalle_producto ON oltp_ventas.detalle_pedido(producto);

-- OLTP LOGÍSTICA

CREATE TABLE oltp_logistica.transitarios (
    transitario_id SERIAL PRIMARY KEY,
    nombre_transitario VARCHAR(150) NOT NULL,
    cif VARCHAR(10) NOT NULL UNIQUE,
    pais VARCHAR(100) NOT NULL,
    telefono VARCHAR(30) NOT NULL,
    email VARCHAR(150) NOT NULL,
    tipo_transporte VARCHAR(30) NOT NULL CHECK (tipo_transporte IN ('maritimo','terrestre','aereo','multimodal')),
    tarifa_base_kg NUMERIC(10,2) NOT NULL CHECK (tarifa_base_kg > 0)
);

CREATE TABLE oltp_logistica.puertos (
    puerto_id SERIAL PRIMARY KEY,
    nombre_puerto VARCHAR(120) NOT NULL,
    ciudad VARCHAR(100) NOT NULL,
    pais VARCHAR(100) NOT NULL,
    tipo VARCHAR(30) NOT NULL CHECK (tipo IN ('maritimo','aereo','terrestre')),
    codigo_un_locode VARCHAR(10) NOT NULL UNIQUE
);

CREATE TABLE oltp_logistica.rutas_logisticas (
    ruta_id SERIAL PRIMARY KEY,
    transitario_id INT NOT NULL REFERENCES oltp_logistica.transitarios(transitario_id),
    puerto_salida_id INT NOT NULL REFERENCES oltp_logistica.puertos(puerto_id),
    puerto_llegada_id INT NOT NULL REFERENCES oltp_logistica.puertos(puerto_id),
    tipo_transporte VARCHAR(30) NOT NULL CHECK (tipo_transporte IN ('maritimo','terrestre','aereo','multimodal')),
    duracion_estimada_dias INT NOT NULL CHECK (duracion_estimada_dias > 0),
    coste_base_kg NUMERIC(10,2) NOT NULL CHECK (coste_base_kg > 0),
    observaciones VARCHAR(255)
);

CREATE TABLE oltp_logistica.envios (
    envio_id SERIAL PRIMARY KEY,
    pedido_id INT NOT NULL REFERENCES oltp_ventas.pedidos(pedido_id),
    ruta_id INT NOT NULL REFERENCES oltp_logistica.rutas_logisticas(ruta_id),
    transitario_id INT NOT NULL REFERENCES oltp_logistica.transitarios(transitario_id),
    fecha_envio DATE NOT NULL,
    fecha_entrega_estimada DATE NOT NULL,
    fecha_entrega_real DATE NOT NULL,
    kilos_totales NUMERIC(10,2) NOT NULL CHECK (kilos_totales > 0),
    numero_contenedor VARCHAR(30) NOT NULL,
    coste_envio_total NUMERIC(12,2) NOT NULL CHECK (coste_envio_total >= 0),
    coste_por_kg NUMERIC(10,4) NOT NULL CHECK (coste_por_kg >= 0),
    estado VARCHAR(30) NOT NULL CHECK (estado IN ('preparado','en_transito','aduana','entregado','incidencia')),
    CHECK (fecha_entrega_estimada >= fecha_envio),
    CHECK (fecha_entrega_real >= fecha_envio)
);

CREATE TABLE oltp_logistica.aduanas (
    aduana_id SERIAL PRIMARY KEY,
    envio_id INT NOT NULL REFERENCES oltp_logistica.envios(envio_id),
    pais_aduana VARCHAR(100) NOT NULL,
    fecha_entrada_aduana DATE NOT NULL,
    fecha_salida_aduana DATE NOT NULL,
    coste_aranceles NUMERIC(12,2) NOT NULL CHECK (coste_aranceles >= 0),
    documentacion_completa BOOLEAN NOT NULL,
    CHECK (fecha_salida_aduana >= fecha_entrada_aduana)
);

CREATE TABLE oltp_logistica.incidencias_envio (
    incidencia_envio_id SERIAL PRIMARY KEY,
    envio_id INT NOT NULL REFERENCES oltp_logistica.envios(envio_id),
    fecha_incidencia DATE NOT NULL,
    tipo_incidencia VARCHAR(50) NOT NULL CHECK (tipo_incidencia IN ('retraso','merma','rechazo_aduana','danos_transporte')),
    descripcion VARCHAR(255) NOT NULL,
    coste_adicional NUMERIC(10,2) NOT NULL CHECK (coste_adicional >= 0),
    responsable VARCHAR(100) NOT NULL
);

CREATE INDEX idx_envios_fecha ON oltp_logistica.envios(fecha_envio);
CREATE INDEX idx_rutas_transitario ON oltp_logistica.rutas_logisticas(transitario_id);

-- OLTP FINANZAS

CREATE TABLE oltp_finanzas.facturas (
    factura_id SERIAL PRIMARY KEY,
    pedido_id INT NOT NULL UNIQUE REFERENCES oltp_ventas.pedidos(pedido_id),
    fecha_emision DATE NOT NULL,
    fecha_vencimiento DATE NOT NULL,
    importe_bruto NUMERIC(12,2) NOT NULL,
    descuento NUMERIC(12,2) NOT NULL,
    impuestos NUMERIC(12,2) NOT NULL,
    importe_total NUMERIC(12,2) NOT NULL,
    divisa VARCHAR(10) NOT NULL,
    estado VARCHAR(20) NOT NULL CHECK (estado IN ('pendiente','pagada','vencida','anulada')),
    CHECK (fecha_vencimiento >= fecha_emision),
    CHECK (importe_total = ROUND(importe_bruto - descuento + impuestos, 2))
);

CREATE TABLE oltp_finanzas.cobros (
    cobro_id SERIAL PRIMARY KEY,
    factura_id INT NOT NULL REFERENCES oltp_finanzas.facturas(factura_id),
    fecha_cobro DATE NOT NULL,
    importe_cobrado NUMERIC(12,2) NOT NULL CHECK (importe_cobrado > 0),
    metodo_pago VARCHAR(30) NOT NULL CHECK (metodo_pago IN ('transferencia','credito','contado')),
    banco VARCHAR(100) NOT NULL,
    observaciones VARCHAR(255)
);

CREATE TABLE oltp_finanzas.costes_lote (
    coste_lote_id SERIAL PRIMARY KEY,
    lote_id INT NOT NULL UNIQUE REFERENCES oltp_produccion.lotes_origen(lote_id),
    coste_semillas NUMERIC(12,2) NOT NULL CHECK (coste_semillas >= 0),
    coste_agua NUMERIC(12,2) NOT NULL CHECK (coste_agua >= 0),
    coste_fertilizantes NUMERIC(12,2) NOT NULL CHECK (coste_fertilizantes >= 0),
    coste_mano_obra NUMERIC(12,2) NOT NULL CHECK (coste_mano_obra >= 0),
    coste_maquinaria NUMERIC(12,2) NOT NULL CHECK (coste_maquinaria >= 0),
    coste_total_produccion NUMERIC(12,2) NOT NULL CHECK (coste_total_produccion >= 0),
    coste_por_kg NUMERIC(10,4) NOT NULL CHECK (coste_por_kg >= 0)
);

CREATE TABLE oltp_finanzas.costes_logisticos (
    coste_logistico_id SERIAL PRIMARY KEY,
    envio_id INT NOT NULL UNIQUE REFERENCES oltp_logistica.envios(envio_id),
    coste_transporte NUMERIC(12,2) NOT NULL CHECK (coste_transporte >= 0),
    coste_aduana NUMERIC(12,2) NOT NULL CHECK (coste_aduana >= 0),
    coste_almacenaje NUMERIC(12,2) NOT NULL CHECK (coste_almacenaje >= 0),
    coste_seguro NUMERIC(12,2) NOT NULL CHECK (coste_seguro >= 0),
    coste_total_logistica NUMERIC(12,2) NOT NULL CHECK (coste_total_logistica >= 0),
    coste_por_kg NUMERIC(10,4) NOT NULL CHECK (coste_por_kg >= 0)
);

CREATE TABLE oltp_finanzas.tipos_cambio (
    tipo_cambio_id SERIAL PRIMARY KEY,
    divisa_origen VARCHAR(10) NOT NULL,
    divisa_destino VARCHAR(10) NOT NULL,
    fecha DATE NOT NULL,
    tasa_cambio NUMERIC(10,4) NOT NULL CHECK (tasa_cambio > 0)
);

CREATE INDEX idx_facturas_fecha ON oltp_finanzas.facturas(fecha_emision);
CREATE INDEX idx_cobros_fecha ON oltp_finanzas.cobros(fecha_cobro);

CREATE OR REPLACE FUNCTION oltp_finanzas.fn_validar_fecha_cobro()
RETURNS TRIGGER AS $$
DECLARE
    v_fecha_emision DATE;
BEGIN
    SELECT fecha_emision
    INTO v_fecha_emision
    FROM oltp_finanzas.facturas
    WHERE factura_id = NEW.factura_id;

    IF NEW.fecha_cobro < v_fecha_emision THEN
        RAISE EXCEPTION 'fecha_cobro no puede ser anterior a fecha_emision';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_validar_fecha_cobro
BEFORE INSERT OR UPDATE ON oltp_finanzas.cobros
FOR EACH ROW
EXECUTE FUNCTION oltp_finanzas.fn_validar_fecha_cobro();

-- POBLACIÓN OLTP

INSERT INTO oltp_produccion.fincas
(nombre_finca, superficie_total_hectareas, municipio, provincia, zona_agricola, tipo_suelo, sistema_riego)
SELECT
    'Finca_' || LPAD(g::text, 3, '0'),
    ROUND((18 + (g % 17) * 1.65)::numeric, 2),
    CASE g % 6
        WHEN 0 THEN 'Motril'
        WHEN 1 THEN 'El Ejido'
        WHEN 2 THEN 'Roquetas de Mar'
        WHEN 3 THEN 'Cartaya'
        WHEN 4 THEN 'Lepe'
        ELSE 'Velez-Malaga'
    END,
    CASE g % 6
        WHEN 0 THEN 'Granada'
        WHEN 1 THEN 'Almeria'
        WHEN 2 THEN 'Almeria'
        WHEN 3 THEN 'Huelva'
        WHEN 4 THEN 'Huelva'
        ELSE 'Malaga'
    END,
    CASE WHEN g % 2 = 0 THEN 'Costa Mediterranea' ELSE 'Costa Atlantica' END,
    CASE g % 4
        WHEN 0 THEN 'Franco-arenoso'
        WHEN 1 THEN 'Franco-arcilloso'
        WHEN 2 THEN 'Limoso'
        ELSE 'Franco'
    END,
    CASE g % 3
        WHEN 0 THEN 'Goteo'
        WHEN 1 THEN 'Aspersion'
        ELSE 'Microaspersion'
    END
FROM generate_series(1, 60) g;

INSERT INTO oltp_produccion.parcelas
(finca_id, nombre_parcela, superficie_hectareas, cultivo_actual, fecha_ultima_rotacion)
SELECT
    ((g - 1) % 60) + 1,
    'Parcela_' || LPAD(g::text, 3, '0'),
    ROUND((2.50 + (g % 8) * 0.70)::numeric, 2),
    CASE g % 10
        WHEN 0 THEN 'Tomate'
        WHEN 1 THEN 'Pimiento'
        WHEN 2 THEN 'Pepino'
        WHEN 3 THEN 'Aguacate'
        WHEN 4 THEN 'Fresa'
        WHEN 5 THEN 'Naranja'
        WHEN 6 THEN 'Limon'
        WHEN 7 THEN 'Calabacin'
        WHEN 8 THEN 'Frambuesa'
        ELSE 'Arandano'
    END,
    DATE '2023-01-01' + (g * 6)
FROM generate_series(1, 120) g;

INSERT INTO oltp_produccion.campanias
(nombre_campania, fecha_inicio, fecha_fin, temporada, anio_agricola, observaciones_climaticas)
SELECT
    'Campania_' || (2000 + g)::text || '_' || (2001 + g)::text,
    make_date(2000 + g, 9, 1),
    make_date(2001 + g, 8, 31),
    'Otono-Primavera',
    2001 + g,
    CASE g % 4
        WHEN 0 THEN 'Campania templada con pluviometria moderada'
        WHEN 1 THEN 'Ligera sequia en primavera'
        WHEN 2 THEN 'Buen comportamiento general'
        ELSE 'Mayor calor estival y buena produccion'
    END
FROM generate_series(1, 60) g;

INSERT INTO oltp_produccion.cultivos
(nombre_producto, familia_producto, variedad, ciclo_dias, riego_necesario_litros_hectarea)
SELECT
    CASE ((g - 1) % 12)
        WHEN 0 THEN 'Tomate'
        WHEN 1 THEN 'Tomate'
        WHEN 2 THEN 'Pimiento'
        WHEN 3 THEN 'Pimiento'
        WHEN 4 THEN 'Pepino'
        WHEN 5 THEN 'Aguacate'
        WHEN 6 THEN 'Fresa'
        WHEN 7 THEN 'Naranja'
        WHEN 8 THEN 'Limon'
        WHEN 9 THEN 'Calabacin'
        WHEN 10 THEN 'Frambuesa'
        ELSE 'Arandano'
    END,
    CASE ((g - 1) % 12)
        WHEN 0 THEN 'Hortalizas'
        WHEN 1 THEN 'Hortalizas'
        WHEN 2 THEN 'Hortalizas'
        WHEN 3 THEN 'Hortalizas'
        WHEN 4 THEN 'Hortalizas'
        WHEN 5 THEN 'Frutas'
        WHEN 6 THEN 'Frutos rojos'
        WHEN 7 THEN 'Citricos'
        WHEN 8 THEN 'Citricos'
        WHEN 9 THEN 'Hortalizas'
        WHEN 10 THEN 'Frutos rojos'
        ELSE 'Frutos rojos'
    END,
    CASE ((g - 1) % 12)
        WHEN 0 THEN 'Cherry'
        WHEN 1 THEN 'Rama'
        WHEN 2 THEN 'Rojo'
        WHEN 3 THEN 'Amarillo'
        WHEN 4 THEN 'Almeria'
        WHEN 5 THEN 'Hass'
        WHEN 6 THEN 'Primoris'
        WHEN 7 THEN 'Navelina'
        WHEN 8 THEN 'Verna'
        WHEN 9 THEN 'Verde'
        WHEN 10 THEN 'Adelita'
        ELSE 'Legacy'
    END || '_V' || (((g - 1) / 12) + 1),
    70 + (g % 180),
    ROUND((2200 + (g % 15) * 180)::numeric, 2)
FROM generate_series(1, 60) g;

INSERT INTO oltp_produccion.cosechas
(parcela_id, cultivo_id, campania_id, fecha_cosecha, kilos_cosechados, rendimiento_kg_hectarea, coste_produccion, merma_campo_porcentaje, numero_recolecciones)
SELECT
    ((g - 1) % 120) + 1,
    ((g - 1) % 60) + 1,
    ((g - 1) % 6) + 20,
    DATE '2024-01-10' + (g * 3),
    ROUND((900 + (g % 30) * 42)::numeric, 2),
    ROUND((3200 + (g % 25) * 135)::numeric, 2),
    ROUND((650 + (g % 35) * 31)::numeric, 2),
    ROUND((1.50 + (g % 10) * 0.65)::numeric, 2),
    1 + (g % 4)
FROM generate_series(1, 180) g;

INSERT INTO oltp_produccion.lotes_origen
(cosecha_id, codigo_lote, kilos_lote, fecha_creacion_lote, estado)
SELECT
    c.cosecha_id,
    'LOT' || LPAD(c.cosecha_id::text, 5, '0'),
    ROUND((c.kilos_cosechados * (1 - c.merma_campo_porcentaje / 100.0))::numeric, 2),
    c.fecha_cosecha + 1,
    CASE c.cosecha_id % 3
        WHEN 0 THEN 'enviado'
        WHEN 1 THEN 'en_almacen'
        ELSE 'en_campo'
    END
FROM oltp_produccion.cosechas c;

INSERT INTO oltp_calidad.lotes
(lote_id, codigo_lote, producto, kilos_totales, fecha_entrada_almacen)
SELECT
    lo.lote_id,
    lo.codigo_lote,
    cu.nombre_producto,
    lo.kilos_lote,
    lo.fecha_creacion_lote + 1
FROM oltp_produccion.lotes_origen lo
JOIN oltp_produccion.cosechas co ON co.cosecha_id = lo.cosecha_id
JOIN oltp_produccion.cultivos cu ON cu.cultivo_id = co.cultivo_id;

INSERT INTO oltp_calidad.controles_calidad
(lote_id, fecha_control, inspector, kilos_inspeccionados, kilos_rechazados, porcentaje_merma, porcentaje_primera_categoria, porcentaje_segunda_categoria, puntuacion_calidad)
SELECT
    l.lote_id,
    l.fecha_entrada_almacen + 1,
    CASE l.lote_id % 5
        WHEN 0 THEN 'Ana Lopez'
        WHEN 1 THEN 'Luis Garcia'
        WHEN 2 THEN 'Marta Ruiz'
        WHEN 3 THEN 'Pablo Gomez'
        ELSE 'Carmen Perez'
    END,
    l.kilos_totales,
    ROUND((l.kilos_totales * (0.02 + (l.lote_id % 6) * 0.006))::numeric, 2),
    ROUND(((0.02 + (l.lote_id % 6) * 0.006) * 100)::numeric, 2),
    ROUND((60 + (l.lote_id % 20))::numeric, 2),
    ROUND((20 + ((l.lote_id + 3) % 15))::numeric, 2),
    ROUND((7 + (l.lote_id % 7) * 0.35)::numeric, 2)
FROM oltp_calidad.lotes l;

INSERT INTO oltp_calidad.calibres
(lote_id, calibre, kilos_calibre, porcentaje_sobre_total)
SELECT
    l.lote_id,
    x.calibre,
    ROUND((l.kilos_totales * x.pct / 100.0)::numeric, 2),
    x.pct
FROM oltp_calidad.lotes l
CROSS JOIN LATERAL (
    VALUES
        ('S',  CASE l.lote_id % 4 WHEN 0 THEN 45 WHEN 1 THEN 20 WHEN 2 THEN 15 ELSE 10 END::numeric),
        ('M',  CASE l.lote_id % 4 WHEN 0 THEN 25 WHEN 1 THEN 45 WHEN 2 THEN 25 ELSE 20 END::numeric),
        ('L',  CASE l.lote_id % 4 WHEN 0 THEN 20 WHEN 1 THEN 25 WHEN 2 THEN 45 ELSE 25 END::numeric),
        ('XL', CASE l.lote_id % 4 WHEN 0 THEN 10 WHEN 1 THEN 10 WHEN 2 THEN 15 ELSE 45 END::numeric)
) AS x(calibre, pct);

INSERT INTO oltp_calidad.certificados
(lote_id, tipo_certificado, fecha_emision, fecha_caducidad, entidad_certificadora, numero_certificado)
SELECT
    l.lote_id,
    CASE l.lote_id % 4
        WHEN 0 THEN 'GlobalGAP'
        WHEN 1 THEN 'ECO'
        WHEN 2 THEN 'ISO22000'
        ELSE 'GRASP'
    END,
    l.fecha_entrada_almacen,
    l.fecha_entrada_almacen + 365,
    CASE l.lote_id % 4
        WHEN 0 THEN 'AENOR'
        WHEN 1 THEN 'CAAE'
        WHEN 2 THEN 'Bureau Veritas'
        ELSE 'SGS'
    END,
    'CERT-' || LPAD(l.lote_id::text, 6, '0')
FROM oltp_calidad.lotes l;

INSERT INTO oltp_calidad.incidencias_calidad
(lote_id, fecha_incidencia, tipo_incidencia, descripcion, kilos_afectados, accion_correctiva)
SELECT
    l.lote_id,
    l.fecha_entrada_almacen + 2,
    CASE l.lote_id % 4
        WHEN 0 THEN 'plagas'
        WHEN 1 THEN 'defectos'
        WHEN 2 THEN 'contaminacion'
        ELSE 'rechazo_cliente'
    END,
    'Incidencia registrada sobre el lote ' || l.codigo_lote,
    ROUND((4 + (l.lote_id % 12))::numeric, 2),
    'Reclasificacion, separacion y seguimiento de trazabilidad'
FROM oltp_calidad.lotes l
WHERE l.lote_id % 5 = 0;

INSERT INTO oltp_ventas.clientes
(nombre_cliente, cif, tipo_cliente, canal, segmento, pais, ciudad, direccion, codigo_postal, telefono, email, fecha_alta)
SELECT
    'Cliente_' || LPAD(g::text, 3, '0'),
    'B' || LPAD(g::text, 8, '0') || 'A',
    CASE g % 4
        WHEN 0 THEN 'mayorista'
        WHEN 1 THEN 'distribuidor'
        WHEN 2 THEN 'minorista'
        ELSE 'retail'
    END,
    CASE g % 3
        WHEN 0 THEN 'directo'
        WHEN 1 THEN 'intermediario'
        ELSE 'plataforma'
    END,
    CASE g % 3
        WHEN 0 THEN 'premium'
        WHEN 1 THEN 'estandar'
        ELSE 'ocasional'
    END,
    CASE g % 6
        WHEN 0 THEN 'Espana'
        WHEN 1 THEN 'Francia'
        WHEN 2 THEN 'Portugal'
        WHEN 3 THEN 'Alemania'
        WHEN 4 THEN 'Paises Bajos'
        ELSE 'Italia'
    END,
    CASE g % 6
        WHEN 0 THEN 'Madrid'
        WHEN 1 THEN 'Marsella'
        WHEN 2 THEN 'Lisboa'
        WHEN 3 THEN 'Berlin'
        WHEN 4 THEN 'Rotterdam'
        ELSE 'Milan'
    END,
    'Calle Comercial ' || g,
    '28' || LPAD((100 + g)::text, 3, '0'),
    '+34-600-' || LPAD((100000 + g)::text, 6, '0'),
    'cliente' || g || '@agroceuta-demo.com',
    DATE '2023-01-01' + (g * 5)
FROM generate_series(1, 60) g;

INSERT INTO oltp_ventas.contratos
(cliente_id, fecha_inicio, fecha_fin, condiciones_pago, descuento_general_porcentaje, volumen_minimo_kg, precio_base_kg)
SELECT
    c.cliente_id,
    DATE '2024-01-01',
    DATE '2026-12-31',
    CASE c.cliente_id % 3
        WHEN 0 THEN '30 dias transferencia'
        WHEN 1 THEN '45 dias transferencia'
        ELSE 'Contado'
    END,
    ROUND((3 + (c.cliente_id % 6))::numeric, 2),
    ROUND((500 + (c.cliente_id % 10) * 150)::numeric, 2),
    ROUND((1.80 + (c.cliente_id % 7) * 0.20)::numeric, 2)
FROM oltp_ventas.clientes c;

INSERT INTO oltp_ventas.pedidos
(cliente_id, contrato_id, fecha_pedido, fecha_entrega_solicitada, estado, observaciones)
SELECT
    ((g - 1) % 60) + 1,
    ((g - 1) % 60) + 1,
    DATE '2024-02-01' + (g * 2),
    DATE '2024-02-05' + (g * 2),
    CASE g % 5
        WHEN 0 THEN 'entregado'
        WHEN 1 THEN 'confirmado'
        WHEN 2 THEN 'enviado'
        WHEN 3 THEN 'pendiente'
        ELSE 'entregado'
    END,
    'Pedido de exportacion asociado a canal comercial'
FROM generate_series(1, 180) g;

INSERT INTO oltp_ventas.detalle_pedido
(pedido_id, lote_id, producto, variedad, kilos_solicitados, cajas_solicitadas, precio_unitario_kg, descuento_porcentaje, importe_bruto, importe_neto)
SELECT
    p.pedido_id,
    lo.lote_id,
    cu.nombre_producto,
    cu.variedad,
    ROUND((120 + (p.pedido_id % 60) * 4)::numeric, 2),
    10 + (p.pedido_id % 8),
    ROUND((
        CASE cu.nombre_producto
            WHEN 'Tomate' THEN 2.20
            WHEN 'Pimiento' THEN 2.50
            WHEN 'Pepino' THEN 1.95
            WHEN 'Aguacate' THEN 3.80
            WHEN 'Fresa' THEN 3.20
            WHEN 'Naranja' THEN 1.85
            WHEN 'Limon' THEN 2.05
            WHEN 'Calabacin' THEN 1.90
            WHEN 'Frambuesa' THEN 4.10
            WHEN 'Arandano' THEN 4.40
            ELSE 2.10
        END + (p.pedido_id % 5) * 0.10
    )::numeric, 2),
    con.descuento_general_porcentaje,
    ROUND((
        (120 + (p.pedido_id % 60) * 4) *
        (
            CASE cu.nombre_producto
                WHEN 'Tomate' THEN 2.20
                WHEN 'Pimiento' THEN 2.50
                WHEN 'Pepino' THEN 1.95
                WHEN 'Aguacate' THEN 3.80
                WHEN 'Fresa' THEN 3.20
                WHEN 'Naranja' THEN 1.85
                WHEN 'Limon' THEN 2.05
                WHEN 'Calabacin' THEN 1.90
                WHEN 'Frambuesa' THEN 4.10
                WHEN 'Arandano' THEN 4.40
                ELSE 2.10
            END + (p.pedido_id % 5) * 0.10
        )
    )::numeric, 2),
    ROUND((
        ((120 + (p.pedido_id % 60) * 4) *
        (
            CASE cu.nombre_producto
                WHEN 'Tomate' THEN 2.20
                WHEN 'Pimiento' THEN 2.50
                WHEN 'Pepino' THEN 1.95
                WHEN 'Aguacate' THEN 3.80
                WHEN 'Fresa' THEN 3.20
                WHEN 'Naranja' THEN 1.85
                WHEN 'Limon' THEN 2.05
                WHEN 'Calabacin' THEN 1.90
                WHEN 'Frambuesa' THEN 4.10
                WHEN 'Arandano' THEN 4.40
                ELSE 2.10
            END + (p.pedido_id % 5) * 0.10
        )) * (1 - con.descuento_general_porcentaje / 100.0)
    )::numeric, 2)
FROM oltp_ventas.pedidos p
JOIN oltp_ventas.contratos con ON con.contrato_id = p.contrato_id
JOIN oltp_produccion.lotes_origen lo ON lo.lote_id = p.pedido_id
JOIN oltp_produccion.cosechas co ON co.cosecha_id = lo.cosecha_id
JOIN oltp_produccion.cultivos cu ON cu.cultivo_id = co.cultivo_id;

INSERT INTO oltp_ventas.tarifas
(producto, variedad, calibre, precio_base_kg, fecha_vigencia_inicio, fecha_vigencia_fin)
SELECT
    c.nombre_producto,
    c.variedad,
    cal.calibre,
    ROUND((
        CASE c.nombre_producto
            WHEN 'Tomate' THEN 2.20
            WHEN 'Pimiento' THEN 2.50
            WHEN 'Pepino' THEN 1.95
            WHEN 'Aguacate' THEN 3.80
            WHEN 'Fresa' THEN 3.20
            WHEN 'Naranja' THEN 1.85
            WHEN 'Limon' THEN 2.05
            WHEN 'Calabacin' THEN 1.90
            WHEN 'Frambuesa' THEN 4.10
            WHEN 'Arandano' THEN 4.40
            ELSE 2.10
        END +
        CASE cal.calibre
            WHEN 'S' THEN -0.10
            WHEN 'M' THEN 0.00
            WHEN 'L' THEN 0.10
            ELSE 0.20
        END
    )::numeric, 2),
    DATE '2024-01-01',
    DATE '2026-12-31'
FROM oltp_produccion.cultivos c
CROSS JOIN (VALUES ('S'), ('M'), ('L'), ('XL')) cal(calibre);

INSERT INTO oltp_logistica.transitarios
(nombre_transitario, cif, pais, telefono, email, tipo_transporte, tarifa_base_kg)
SELECT
    'Transitario_' || LPAD(g::text, 3, '0'),
    'A' || LPAD((50000000 + g)::text, 8, '0') || 'Z',
    CASE g % 5
        WHEN 0 THEN 'Espana'
        WHEN 1 THEN 'Francia'
        WHEN 2 THEN 'Portugal'
        WHEN 3 THEN 'Alemania'
        ELSE 'Paises Bajos'
    END,
    '+34-700-' || LPAD((100000 + g)::text, 6, '0'),
    'ops' || g || '@transitario-demo.com',
    CASE g % 4
        WHEN 0 THEN 'maritimo'
        WHEN 1 THEN 'terrestre'
        WHEN 2 THEN 'aereo'
        ELSE 'multimodal'
    END,
    ROUND((0.22 + (g % 5) * 0.04)::numeric, 2)
FROM generate_series(1, 60) g;

INSERT INTO oltp_logistica.puertos
(nombre_puerto, ciudad, pais, tipo, codigo_un_locode)
SELECT
    CASE g % 12
        WHEN 0 THEN 'Algeciras'
        WHEN 1 THEN 'Motril'
        WHEN 2 THEN 'Valencia'
        WHEN 3 THEN 'Barcelona'
        WHEN 4 THEN 'Marsella'
        WHEN 5 THEN 'Rotterdam'
        WHEN 6 THEN 'Lisboa'
        WHEN 7 THEN 'Hamburgo'
        WHEN 8 THEN 'Milan Hub'
        WHEN 9 THEN 'Lyon Hub'
        WHEN 10 THEN 'Madrid Air'
        ELSE 'Frankfurt Air'
    END || '_' || LPAD(g::text, 2, '0'),
    CASE g % 12
        WHEN 0 THEN 'Algeciras'
        WHEN 1 THEN 'Motril'
        WHEN 2 THEN 'Valencia'
        WHEN 3 THEN 'Barcelona'
        WHEN 4 THEN 'Marsella'
        WHEN 5 THEN 'Rotterdam'
        WHEN 6 THEN 'Lisboa'
        WHEN 7 THEN 'Hamburgo'
        WHEN 8 THEN 'Milan'
        WHEN 9 THEN 'Lyon'
        WHEN 10 THEN 'Madrid'
        ELSE 'Frankfurt'
    END,
    CASE g % 12
        WHEN 0 THEN 'Espana'
        WHEN 1 THEN 'Espana'
        WHEN 2 THEN 'Espana'
        WHEN 3 THEN 'Espana'
        WHEN 4 THEN 'Francia'
        WHEN 5 THEN 'Paises Bajos'
        WHEN 6 THEN 'Portugal'
        WHEN 7 THEN 'Alemania'
        WHEN 8 THEN 'Italia'
        WHEN 9 THEN 'Francia'
        WHEN 10 THEN 'Espana'
        ELSE 'Alemania'
    END,
    CASE
        WHEN g % 12 IN (10,11) THEN 'aereo'
        WHEN g % 12 IN (8,9) THEN 'terrestre'
        ELSE 'maritimo'
    END,
    'LOC' || LPAD(g::text, 6, '0')
FROM generate_series(1, 60) g;

INSERT INTO oltp_logistica.rutas_logisticas
(transitario_id, puerto_salida_id, puerto_llegada_id, tipo_transporte, duracion_estimada_dias, coste_base_kg, observaciones)
SELECT
    ((g - 1) % 60) + 1,
    ((g - 1) % 20) + 1,
    ((g + 9) % 60) + 1,
    CASE (((g - 1) % 60) % 4)
        WHEN 0 THEN 'maritimo'
        WHEN 1 THEN 'terrestre'
        WHEN 2 THEN 'aereo'
        ELSE 'multimodal'
    END,
    2 + (g % 7),
    ROUND((0.24 + (g % 6) * 0.05)::numeric, 2),
    'Ruta operativa de exportacion ' || g
FROM generate_series(1, 120) g;

INSERT INTO oltp_logistica.envios
(pedido_id, ruta_id, transitario_id, fecha_envio, fecha_entrega_estimada, fecha_entrega_real, kilos_totales, numero_contenedor, coste_envio_total, coste_por_kg, estado)
SELECT
    p.pedido_id,
    r.ruta_id,
    r.transitario_id,
    p.fecha_pedido + 1,
    p.fecha_pedido + 1 + r.duracion_estimada_dias,
    p.fecha_pedido + 1 + r.duracion_estimada_dias + (p.pedido_id % 2),
    d.kilos_solicitados,
    'CONT-' || LPAD(p.pedido_id::text, 6, '0'),
    ROUND((d.kilos_solicitados * r.coste_base_kg)::numeric, 2),
    ROUND(r.coste_base_kg::numeric, 4),
    CASE
        WHEN p.pedido_id % 7 = 0 THEN 'incidencia'
        WHEN p.pedido_id % 5 = 0 THEN 'aduana'
        ELSE 'entregado'
    END
FROM oltp_ventas.pedidos p
JOIN oltp_ventas.detalle_pedido d ON d.pedido_id = p.pedido_id
JOIN oltp_logistica.rutas_logisticas r ON r.ruta_id = ((p.pedido_id - 1) % 120) + 1;

INSERT INTO oltp_logistica.aduanas
(envio_id, pais_aduana, fecha_entrada_aduana, fecha_salida_aduana, coste_aranceles, documentacion_completa)
SELECT
    e.envio_id,
    pl.pais,
    e.fecha_envio + 1,
    e.fecha_envio + 2,
    ROUND((15 + (e.envio_id % 10) * 3)::numeric, 2),
    CASE WHEN e.envio_id % 6 = 0 THEN FALSE ELSE TRUE END
FROM oltp_logistica.envios e
JOIN oltp_logistica.rutas_logisticas r ON r.ruta_id = e.ruta_id
JOIN oltp_logistica.puertos pl ON pl.puerto_id = r.puerto_llegada_id;

INSERT INTO oltp_logistica.incidencias_envio
(envio_id, fecha_incidencia, tipo_incidencia, descripcion, coste_adicional, responsable)
SELECT
    e.envio_id,
    e.fecha_envio + 2,
    CASE e.envio_id % 4
        WHEN 0 THEN 'retraso'
        WHEN 1 THEN 'merma'
        WHEN 2 THEN 'rechazo_aduana'
        ELSE 'danos_transporte'
    END,
    'Incidencia registrada en el envio ' || e.numero_contenedor,
    ROUND((20 + e.envio_id % 30)::numeric, 2),
    CASE e.envio_id % 3
        WHEN 0 THEN 'transitario'
        WHEN 1 THEN 'aduana'
        ELSE 'almacen'
    END
FROM oltp_logistica.envios e
WHERE e.envio_id % 4 = 0;

INSERT INTO oltp_finanzas.facturas
(pedido_id, fecha_emision, fecha_vencimiento, importe_bruto, descuento, impuestos, importe_total, divisa, estado)
SELECT
    d.pedido_id,
    p.fecha_pedido + 1,
    p.fecha_pedido + 31,
    d.importe_bruto,
    ROUND((d.importe_bruto - d.importe_neto)::numeric, 2),
    ROUND((d.importe_neto * 0.04)::numeric, 2),
    ROUND((d.importe_neto * 1.04)::numeric, 2),
    CASE WHEN d.pedido_id % 5 = 0 THEN 'USD' ELSE 'EUR' END,
    CASE WHEN d.pedido_id % 6 = 0 THEN 'pagada' ELSE 'pendiente' END
FROM oltp_ventas.detalle_pedido d
JOIN oltp_ventas.pedidos p ON p.pedido_id = d.pedido_id;

INSERT INTO oltp_finanzas.cobros
(factura_id, fecha_cobro, importe_cobrado, metodo_pago, banco, observaciones)
SELECT
    f.factura_id,
    f.fecha_emision + 15,
    f.importe_total,
    CASE f.factura_id % 3
        WHEN 0 THEN 'transferencia'
        WHEN 1 THEN 'credito'
        ELSE 'contado'
    END,
    CASE f.factura_id % 4
        WHEN 0 THEN 'Santander'
        WHEN 1 THEN 'CaixaBank'
        WHEN 2 THEN 'BBVA'
        ELSE 'Sabadell'
    END,
    'Cobro registrado sobre factura ' || f.factura_id
FROM oltp_finanzas.facturas f;

INSERT INTO oltp_finanzas.costes_lote
(lote_id, coste_semillas, coste_agua, coste_fertilizantes, coste_mano_obra, coste_maquinaria, coste_total_produccion, coste_por_kg)
SELECT
    lo.lote_id,
    ROUND((40 + (lo.lote_id % 12) * 3)::numeric, 2),
    ROUND((35 + (lo.lote_id % 10) * 2.5)::numeric, 2),
    ROUND((30 + (lo.lote_id % 9) * 2.8)::numeric, 2),
    ROUND((80 + (lo.lote_id % 15) * 4)::numeric, 2),
    ROUND((45 + (lo.lote_id % 11) * 2.2)::numeric, 2),
    ROUND((
        (40 + (lo.lote_id % 12) * 3) +
        (35 + (lo.lote_id % 10) * 2.5) +
        (30 + (lo.lote_id % 9) * 2.8) +
        (80 + (lo.lote_id % 15) * 4) +
        (45 + (lo.lote_id % 11) * 2.2)
    )::numeric, 2),
    ROUND((
        (
            (40 + (lo.lote_id % 12) * 3) +
            (35 + (lo.lote_id % 10) * 2.5) +
            (30 + (lo.lote_id % 9) * 2.8) +
            (80 + (lo.lote_id % 15) * 4) +
            (45 + (lo.lote_id % 11) * 2.2)
        ) / lo.kilos_lote
    )::numeric, 4)
FROM oltp_produccion.lotes_origen lo;

INSERT INTO oltp_finanzas.costes_logisticos
(envio_id, coste_transporte, coste_aduana, coste_almacenaje, coste_seguro, coste_total_logistica, coste_por_kg)
SELECT
    e.envio_id,
    ROUND((e.kilos_totales * e.coste_por_kg * 0.70)::numeric, 2),
    ROUND((10 + (e.envio_id % 8) * 2)::numeric, 2),
    ROUND((8 + (e.envio_id % 6) * 1.5)::numeric, 2),
    ROUND((5 + (e.envio_id % 5) * 1.2)::numeric, 2),
    ROUND((
        (e.kilos_totales * e.coste_por_kg * 0.70) +
        (10 + (e.envio_id % 8) * 2) +
        (8 + (e.envio_id % 6) * 1.5) +
        (5 + (e.envio_id % 5) * 1.2)
    )::numeric, 2),
    ROUND((
        (
            (e.kilos_totales * e.coste_por_kg * 0.70) +
            (10 + (e.envio_id % 8) * 2) +
            (8 + (e.envio_id % 6) * 1.5) +
            (5 + (e.envio_id % 5) * 1.2)
        ) / e.kilos_totales
    )::numeric, 4)
FROM oltp_logistica.envios e;

INSERT INTO oltp_finanzas.tipos_cambio
(divisa_origen, divisa_destino, fecha, tasa_cambio)
SELECT
    CASE WHEN g % 2 = 0 THEN 'EUR' ELSE 'USD' END,
    CASE WHEN g % 2 = 0 THEN 'USD' ELSE 'EUR' END,
    DATE '2024-01-01' + g,
    ROUND((1.05 + g * 0.002)::numeric, 4)
FROM generate_series(1, 60) g;

-- OLAP ESTRELLA

CREATE SCHEMA olap_star;

CREATE TABLE olap_star.dim_tiempo (
    tiempo_sk SERIAL PRIMARY KEY,
    fecha DATE UNIQUE NOT NULL,
    dia INT NOT NULL,
    mes INT NOT NULL,
    trimestre INT NOT NULL,
    anio INT NOT NULL,
    semana INT,
    dia_semana VARCHAR(20),
    es_festivo BOOLEAN DEFAULT FALSE
);

CREATE TABLE olap_star.dim_campania (
    campania_sk SERIAL PRIMARY KEY,
    campania_id INT NOT NULL,
    nombre_campania VARCHAR(100) NOT NULL,
    fecha_inicio DATE,
    fecha_fin DATE,
    temporada VARCHAR(50),
    anio_agricola INT,
    observaciones_climaticas VARCHAR(255)
);

CREATE TABLE olap_star.dim_cliente (
    cliente_sk SERIAL PRIMARY KEY,
    cliente_id INT NOT NULL,
    nombre_cliente VARCHAR(150) NOT NULL,
    tipo_cliente VARCHAR(50),
    canal VARCHAR(50),
    segmento VARCHAR(50),
    pais_cliente VARCHAR(100),
    ciudad_cliente VARCHAR(100),
    email VARCHAR(150),
    telefono VARCHAR(30)
);

CREATE TABLE olap_star.dim_geografia (
    geografia_sk SERIAL PRIMARY KEY,
    ciudad VARCHAR(100),
    provincia VARCHAR(100),
    region VARCHAR(100),
    pais VARCHAR(100),
    zona_comercial VARCHAR(100),
    continente VARCHAR(100)
);

CREATE TABLE olap_star.dim_finca (
    finca_sk SERIAL PRIMARY KEY,
    finca_id INT NOT NULL,
    nombre_finca VARCHAR(150),
    parcela VARCHAR(100),
    superficie_hectareas NUMERIC(10,2),
    municipio VARCHAR(100),
    provincia VARCHAR(100),
    zona_agricola VARCHAR(100),
    tipo_suelo VARCHAR(100),
    sistema_riego VARCHAR(100)
);

CREATE TABLE olap_star.dim_producto (
    producto_sk SERIAL PRIMARY KEY,
    cultivo_id INT NOT NULL,
    nombre_producto VARCHAR(100),
    variedad VARCHAR(100),
    familia_producto VARCHAR(100),
    tipo_envase VARCHAR(100),
    formato_comercial VARCHAR(100),
    peso_unidad NUMERIC(10,2)
);

CREATE TABLE olap_star.dim_calidad (
    calidad_sk SERIAL PRIMARY KEY,
    lote_id INT NOT NULL,
    categoria_comercial VARCHAR(50),
    calibre VARCHAR(20),
    tipo_certificado VARCHAR(100),
    estado_lote VARCHAR(50),
    tiene_certificacion_eco BOOLEAN,
    tiene_certificacion_global_gap BOOLEAN
);

CREATE TABLE olap_star.dim_logistica (
    logistica_sk SERIAL PRIMARY KEY,
    ruta_id INT NOT NULL,
    nombre_transitario VARCHAR(150),
    puerto_salida VARCHAR(100),
    puerto_llegada VARCHAR(100),
    tipo_transporte VARCHAR(50),
    ruta_logistica VARCHAR(150),
    duracion_estimada_dias INT
);

CREATE TABLE olap_star.fact_exportaciones (
    exportacion_sk SERIAL PRIMARY KEY,
    tiempo_sk INT NOT NULL REFERENCES olap_star.dim_tiempo(tiempo_sk),
    producto_sk INT NOT NULL REFERENCES olap_star.dim_producto(producto_sk),
    cliente_sk INT NOT NULL REFERENCES olap_star.dim_cliente(cliente_sk),
    geografia_sk INT NOT NULL REFERENCES olap_star.dim_geografia(geografia_sk),
    finca_sk INT NOT NULL REFERENCES olap_star.dim_finca(finca_sk),
    calidad_sk INT NOT NULL REFERENCES olap_star.dim_calidad(calidad_sk),
    logistica_sk INT NOT NULL REFERENCES olap_star.dim_logistica(logistica_sk),
    campania_sk INT NOT NULL REFERENCES olap_star.dim_campania(campania_sk),
    kilos_vendidos NUMERIC(12,2) NOT NULL,
    cajas_vendidas INT NOT NULL,
    importe_bruto NUMERIC(12,2) NOT NULL,
    descuento NUMERIC(12,2) NOT NULL,
    importe_neto NUMERIC(12,2) NOT NULL,
    coste_producto NUMERIC(12,2) NOT NULL,
    coste_logistico NUMERIC(12,2) NOT NULL,
    coste_total NUMERIC(12,2) NOT NULL,
    margen NUMERIC(12,2) NOT NULL,
    precio_medio_kg NUMERIC(12,4) NOT NULL,
    tiempo_entrega_dias INT NOT NULL
);

INSERT INTO olap_star.dim_tiempo
(fecha, dia, mes, trimestre, anio, semana, dia_semana, es_festivo)
SELECT DISTINCT
    p.fecha_pedido,
    EXTRACT(DAY FROM p.fecha_pedido)::INT,
    EXTRACT(MONTH FROM p.fecha_pedido)::INT,
    EXTRACT(QUARTER FROM p.fecha_pedido)::INT,
    EXTRACT(YEAR FROM p.fecha_pedido)::INT,
    EXTRACT(WEEK FROM p.fecha_pedido)::INT,
    TO_CHAR(p.fecha_pedido, 'Day'),
    CASE WHEN EXTRACT(ISODOW FROM p.fecha_pedido) IN (6,7) THEN TRUE ELSE FALSE END
FROM oltp_ventas.pedidos p
ORDER BY p.fecha_pedido;

INSERT INTO olap_star.dim_campania
(campania_id, nombre_campania, fecha_inicio, fecha_fin, temporada, anio_agricola, observaciones_climaticas)
SELECT
    campania_id,
    nombre_campania,
    fecha_inicio,
    fecha_fin,
    temporada,
    anio_agricola,
    observaciones_climaticas
FROM oltp_produccion.campanias
ORDER BY campania_id;

INSERT INTO olap_star.dim_cliente
(cliente_id, nombre_cliente, tipo_cliente, canal, segmento, pais_cliente, ciudad_cliente, email, telefono)
SELECT
    cliente_id,
    nombre_cliente,
    tipo_cliente,
    canal,
    segmento,
    pais,
    ciudad,
    email,
    telefono
FROM oltp_ventas.clientes
ORDER BY cliente_id;

INSERT INTO olap_star.dim_geografia
(ciudad, provincia, region, pais, zona_comercial, continente)
SELECT DISTINCT
    c.ciudad,
    CASE
        WHEN c.pais = 'Espana' THEN c.ciudad
        ELSE c.ciudad
    END AS provincia,
    CASE
        WHEN c.pais IN ('Espana','Francia','Portugal','Alemania','Italia','Paises Bajos') THEN 'Europa Occidental'
        ELSE 'Otras regiones'
    END AS region,
    c.pais,
    CASE
        WHEN c.pais IN ('Espana','Portugal','Italia') THEN 'Sur de Europa'
        WHEN c.pais IN ('Francia','Alemania','Paises Bajos') THEN 'Centro de Europa'
        ELSE 'Mercado Internacional'
    END AS zona_comercial,
    CASE
        WHEN c.pais IN ('Espana','Francia','Portugal','Alemania','Italia','Paises Bajos') THEN 'Europa'
        ELSE 'Otro'
    END AS continente
FROM oltp_ventas.clientes c
ORDER BY c.pais, c.ciudad;

INSERT INTO olap_star.dim_finca
(finca_id, nombre_finca, parcela, superficie_hectareas, municipio, provincia, zona_agricola, tipo_suelo, sistema_riego)
SELECT
    f.finca_id,
    f.nombre_finca,
    p.nombre_parcela,
    p.superficie_hectareas,
    f.municipio,
    f.provincia,
    f.zona_agricola,
    f.tipo_suelo,
    f.sistema_riego
FROM oltp_produccion.parcelas p
JOIN oltp_produccion.fincas f ON f.finca_id = p.finca_id
ORDER BY f.finca_id, p.parcela_id;

INSERT INTO olap_star.dim_producto
(cultivo_id, nombre_producto, variedad, familia_producto, tipo_envase, formato_comercial, peso_unidad)
SELECT
    c.cultivo_id,
    c.nombre_producto,
    c.variedad,
    c.familia_producto,
    CASE
        WHEN c.nombre_producto IN ('Fresa','Frambuesa','Arandano') THEN 'Tarrina'
        WHEN c.nombre_producto IN ('Aguacate','Naranja','Limon') THEN 'Caja'
        ELSE 'Caja'
    END AS tipo_envase,
    CASE
        WHEN c.nombre_producto IN ('Tomate','Pimiento','Pepino','Calabacin') THEN 'Granel'
        WHEN c.nombre_producto IN ('Aguacate','Naranja','Limon') THEN 'Caja 4kg'
        ELSE 'Tarrina 500g'
    END AS formato_comercial,
    CASE
        WHEN c.nombre_producto IN ('Fresa','Frambuesa','Arandano') THEN 0.50
        WHEN c.nombre_producto IN ('Aguacate','Naranja','Limon') THEN 4.00
        ELSE 5.00
    END AS peso_unidad
FROM oltp_produccion.cultivos c
ORDER BY c.cultivo_id;

WITH calibre_dominante AS (
    SELECT lote_id, calibre
    FROM (
        SELECT
            lote_id,
            calibre,
            kilos_calibre,
            ROW_NUMBER() OVER (PARTITION BY lote_id ORDER BY kilos_calibre DESC, calibre) AS rn
        FROM oltp_calidad.calibres
    ) q
    WHERE rn = 1
)
INSERT INTO olap_star.dim_calidad
(lote_id, categoria_comercial, calibre, tipo_certificado, estado_lote, tiene_certificacion_eco, tiene_certificacion_global_gap)
SELECT
    lo.lote_id,
    CASE
        WHEN cc.porcentaje_primera_categoria >= 70 THEN 'Primera'
        ELSE 'Segunda'
    END AS categoria_comercial,
    cd.calibre,
    ce.tipo_certificado,
    lo.estado,
    CASE WHEN ce.tipo_certificado = 'ECO' THEN TRUE ELSE FALSE END,
    CASE WHEN ce.tipo_certificado = 'GlobalGAP' THEN TRUE ELSE FALSE END
FROM oltp_produccion.lotes_origen lo
JOIN oltp_calidad.controles_calidad cc ON cc.lote_id = lo.lote_id
JOIN calibre_dominante cd ON cd.lote_id = lo.lote_id
JOIN oltp_calidad.certificados ce ON ce.lote_id = lo.lote_id
ORDER BY lo.lote_id;

INSERT INTO olap_star.dim_logistica
(ruta_id, nombre_transitario, puerto_salida, puerto_llegada, tipo_transporte, ruta_logistica, duracion_estimada_dias)
SELECT
    r.ruta_id,
    t.nombre_transitario,
    ps.nombre_puerto,
    pl.nombre_puerto,
    r.tipo_transporte,
    ps.nombre_puerto || ' -> ' || pl.nombre_puerto,
    r.duracion_estimada_dias
FROM oltp_logistica.rutas_logisticas r
JOIN oltp_logistica.transitarios t ON t.transitario_id = r.transitario_id
JOIN oltp_logistica.puertos ps ON ps.puerto_id = r.puerto_salida_id
JOIN oltp_logistica.puertos pl ON pl.puerto_id = r.puerto_llegada_id
ORDER BY r.ruta_id;

INSERT INTO olap_star.fact_exportaciones
(tiempo_sk, producto_sk, cliente_sk, geografia_sk, finca_sk, calidad_sk, logistica_sk, campania_sk,
 kilos_vendidos, cajas_vendidas, importe_bruto, descuento, importe_neto,
 coste_producto, coste_logistico, coste_total, margen, precio_medio_kg, tiempo_entrega_dias)
SELECT
    dt.tiempo_sk,
    dp.producto_sk,
    dc.cliente_sk,
    dg.geografia_sk,
    df.finca_sk,
    dq.calidad_sk,
    dl.logistica_sk,
    dca.campania_sk,
    d.kilos_solicitados,
    d.cajas_solicitadas,
    d.importe_bruto,
    ROUND((d.importe_bruto - d.importe_neto)::numeric, 2) AS descuento,
    d.importe_neto,
    ROUND((d.kilos_solicitados * cl.coste_por_kg)::numeric, 2) AS coste_producto,
    ROUND((d.kilos_solicitados * clog.coste_por_kg)::numeric, 2) AS coste_logistico,
    ROUND(((d.kilos_solicitados * cl.coste_por_kg) + (d.kilos_solicitados * clog.coste_por_kg))::numeric, 2) AS coste_total,
    ROUND((d.importe_neto - ((d.kilos_solicitados * cl.coste_por_kg) + (d.kilos_solicitados * clog.coste_por_kg)))::numeric, 2) AS margen,
    ROUND((d.importe_neto / d.kilos_solicitados)::numeric, 4) AS precio_medio_kg,
    (e.fecha_entrega_real - e.fecha_envio) AS tiempo_entrega_dias
FROM oltp_ventas.detalle_pedido d
JOIN oltp_ventas.pedidos p ON p.pedido_id = d.pedido_id
JOIN oltp_ventas.clientes c ON c.cliente_id = p.cliente_id
JOIN oltp_logistica.envios e ON e.pedido_id = p.pedido_id
JOIN oltp_finanzas.costes_logisticos clog ON clog.envio_id = e.envio_id
JOIN oltp_finanzas.costes_lote cl ON cl.lote_id = d.lote_id
JOIN oltp_produccion.lotes_origen lo ON lo.lote_id = d.lote_id
JOIN oltp_produccion.cosechas co ON co.cosecha_id = lo.cosecha_id
JOIN oltp_produccion.cultivos cu ON cu.cultivo_id = co.cultivo_id
JOIN oltp_produccion.parcelas pa ON pa.parcela_id = co.parcela_id
JOIN oltp_produccion.fincas fi ON fi.finca_id = pa.finca_id
JOIN oltp_produccion.campanias ca ON ca.campania_id = co.campania_id
JOIN olap_star.dim_tiempo dt ON dt.fecha = p.fecha_pedido
JOIN olap_star.dim_producto dp ON dp.cultivo_id = cu.cultivo_id
JOIN olap_star.dim_cliente dc ON dc.cliente_id = c.cliente_id
JOIN olap_star.dim_geografia dg ON dg.pais = c.pais AND dg.ciudad = c.ciudad
JOIN olap_star.dim_finca df ON df.finca_id = fi.finca_id AND df.parcela = pa.nombre_parcela
JOIN olap_star.dim_calidad dq ON dq.lote_id = d.lote_id
JOIN olap_star.dim_logistica dl ON dl.ruta_id = e.ruta_id
JOIN olap_star.dim_campania dca ON dca.campania_id = ca.campania_id;

CREATE INDEX idx_star_fact_tiempo ON olap_star.fact_exportaciones(tiempo_sk);
CREATE INDEX idx_star_fact_producto ON olap_star.fact_exportaciones(producto_sk);
CREATE INDEX idx_star_fact_cliente ON olap_star.fact_exportaciones(cliente_sk);
CREATE INDEX idx_star_fact_campania ON olap_star.fact_exportaciones(campania_sk);

-- OLAP SNOWFLAKE

CREATE SCHEMA olap_snow;

CREATE TABLE olap_snow.dim_tiempo (
    tiempo_sk SERIAL PRIMARY KEY,
    fecha DATE UNIQUE NOT NULL,
    dia INT NOT NULL,
    mes INT NOT NULL,
    trimestre INT NOT NULL,
    anio INT NOT NULL,
    semana INT,
    dia_semana VARCHAR(20),
    es_festivo BOOLEAN DEFAULT FALSE
);

CREATE TABLE olap_snow.dim_campania (
    campania_sk SERIAL PRIMARY KEY,
    campania_id INT NOT NULL,
    nombre_campania VARCHAR(100) NOT NULL,
    fecha_inicio DATE,
    fecha_fin DATE,
    temporada VARCHAR(50),
    anio_agricola INT,
    observaciones_climaticas VARCHAR(255)
);

CREATE TABLE olap_snow.dim_cliente (
    cliente_sk SERIAL PRIMARY KEY,
    cliente_id INT NOT NULL,
    nombre_cliente VARCHAR(150) NOT NULL,
    tipo_cliente VARCHAR(50),
    canal VARCHAR(50),
    segmento VARCHAR(50),
    pais_cliente VARCHAR(100),
    ciudad_cliente VARCHAR(100),
    email VARCHAR(150),
    telefono VARCHAR(30)
);

CREATE TABLE olap_snow.dim_geografia (
    geografia_sk SERIAL PRIMARY KEY,
    ciudad VARCHAR(100),
    provincia VARCHAR(100),
    region VARCHAR(100),
    pais VARCHAR(100),
    zona_comercial VARCHAR(100),
    continente VARCHAR(100)
);

CREATE TABLE olap_snow.dim_finca (
    finca_sk SERIAL PRIMARY KEY,
    finca_id INT NOT NULL,
    nombre_finca VARCHAR(150),
    parcela VARCHAR(100),
    superficie_hectareas NUMERIC(10,2),
    municipio VARCHAR(100),
    provincia VARCHAR(100),
    zona_agricola VARCHAR(100),
    tipo_suelo VARCHAR(100),
    sistema_riego VARCHAR(100)
);

CREATE TABLE olap_snow.dim_familia_producto (
    familia_sk SERIAL PRIMARY KEY,
    familia_producto VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE olap_snow.dim_producto (
    producto_sk SERIAL PRIMARY KEY,
    cultivo_id INT NOT NULL,
    nombre_producto VARCHAR(100),
    variedad VARCHAR(100),
    familia_sk INT NOT NULL REFERENCES olap_snow.dim_familia_producto(familia_sk),
    tipo_envase VARCHAR(100),
    formato_comercial VARCHAR(100),
    peso_unidad NUMERIC(10,2)
);

CREATE TABLE olap_snow.dim_calidad (
    calidad_sk SERIAL PRIMARY KEY,
    lote_id INT NOT NULL,
    categoria_comercial VARCHAR(50),
    calibre VARCHAR(20),
    tipo_certificado VARCHAR(100),
    estado_lote VARCHAR(50),
    tiene_certificacion_eco BOOLEAN,
    tiene_certificacion_global_gap BOOLEAN
);

CREATE TABLE olap_snow.dim_logistica (
    logistica_sk SERIAL PRIMARY KEY,
    ruta_id INT NOT NULL,
    nombre_transitario VARCHAR(150),
    puerto_salida VARCHAR(100),
    puerto_llegada VARCHAR(100),
    tipo_transporte VARCHAR(50),
    ruta_logistica VARCHAR(150),
    duracion_estimada_dias INT
);

CREATE TABLE olap_snow.fact_exportaciones (
    exportacion_sk SERIAL PRIMARY KEY,
    tiempo_sk INT NOT NULL REFERENCES olap_snow.dim_tiempo(tiempo_sk),
    producto_sk INT NOT NULL REFERENCES olap_snow.dim_producto(producto_sk),
    cliente_sk INT NOT NULL REFERENCES olap_snow.dim_cliente(cliente_sk),
    geografia_sk INT NOT NULL REFERENCES olap_snow.dim_geografia(geografia_sk),
    finca_sk INT NOT NULL REFERENCES olap_snow.dim_finca(finca_sk),
    calidad_sk INT NOT NULL REFERENCES olap_snow.dim_calidad(calidad_sk),
    logistica_sk INT NOT NULL REFERENCES olap_snow.dim_logistica(logistica_sk),
    campania_sk INT NOT NULL REFERENCES olap_snow.dim_campania(campania_sk),
    kilos_vendidos NUMERIC(12,2) NOT NULL,
    cajas_vendidas INT NOT NULL,
    importe_bruto NUMERIC(12,2) NOT NULL,
    descuento NUMERIC(12,2) NOT NULL,
    importe_neto NUMERIC(12,2) NOT NULL,
    coste_producto NUMERIC(12,2) NOT NULL,
    coste_logistico NUMERIC(12,2) NOT NULL,
    coste_total NUMERIC(12,2) NOT NULL,
    margen NUMERIC(12,2) NOT NULL,
    precio_medio_kg NUMERIC(12,4) NOT NULL,
    tiempo_entrega_dias INT NOT NULL
);

INSERT INTO olap_snow.dim_tiempo
(fecha, dia, mes, trimestre, anio, semana, dia_semana, es_festivo)
SELECT fecha, dia, mes, trimestre, anio, semana, dia_semana, es_festivo
FROM olap_star.dim_tiempo;

INSERT INTO olap_snow.dim_campania
(campania_id, nombre_campania, fecha_inicio, fecha_fin, temporada, anio_agricola, observaciones_climaticas)
SELECT campania_id, nombre_campania, fecha_inicio, fecha_fin, temporada, anio_agricola, observaciones_climaticas
FROM olap_star.dim_campania;

INSERT INTO olap_snow.dim_cliente
(cliente_id, nombre_cliente, tipo_cliente, canal, segmento, pais_cliente, ciudad_cliente, email, telefono)
SELECT cliente_id, nombre_cliente, tipo_cliente, canal, segmento, pais_cliente, ciudad_cliente, email, telefono
FROM olap_star.dim_cliente;

INSERT INTO olap_snow.dim_geografia
(ciudad, provincia, region, pais, zona_comercial, continente)
SELECT ciudad, provincia, region, pais, zona_comercial, continente
FROM olap_star.dim_geografia;

INSERT INTO olap_snow.dim_finca
(finca_id, nombre_finca, parcela, superficie_hectareas, municipio, provincia, zona_agricola, tipo_suelo, sistema_riego)
SELECT finca_id, nombre_finca, parcela, superficie_hectareas, municipio, provincia, zona_agricola, tipo_suelo, sistema_riego
FROM olap_star.dim_finca;

INSERT INTO olap_snow.dim_familia_producto (familia_producto)
SELECT DISTINCT familia_producto
FROM olap_star.dim_producto
ORDER BY familia_producto;

INSERT INTO olap_snow.dim_producto
(cultivo_id, nombre_producto, variedad, familia_sk, tipo_envase, formato_comercial, peso_unidad)
SELECT
    p.cultivo_id,
    p.nombre_producto,
    p.variedad,
    fp.familia_sk,
    p.tipo_envase,
    p.formato_comercial,
    p.peso_unidad
FROM olap_star.dim_producto p
JOIN olap_snow.dim_familia_producto fp
    ON fp.familia_producto = p.familia_producto;

INSERT INTO olap_snow.dim_calidad
(lote_id, categoria_comercial, calibre, tipo_certificado, estado_lote, tiene_certificacion_eco, tiene_certificacion_global_gap)
SELECT lote_id, categoria_comercial, calibre, tipo_certificado, estado_lote, tiene_certificacion_eco, tiene_certificacion_global_gap
FROM olap_star.dim_calidad;

INSERT INTO olap_snow.dim_logistica
(ruta_id, nombre_transitario, puerto_salida, puerto_llegada, tipo_transporte, ruta_logistica, duracion_estimada_dias)
SELECT ruta_id, nombre_transitario, puerto_salida, puerto_llegada, tipo_transporte, ruta_logistica, duracion_estimada_dias
FROM olap_star.dim_logistica;

INSERT INTO olap_snow.fact_exportaciones
(tiempo_sk, producto_sk, cliente_sk, geografia_sk, finca_sk, calidad_sk, logistica_sk, campania_sk,
 kilos_vendidos, cajas_vendidas, importe_bruto, descuento, importe_neto,
 coste_producto, coste_logistico, coste_total, margen, precio_medio_kg, tiempo_entrega_dias)
SELECT
    st.tiempo_sk,
    sp.producto_sk,
    sc.cliente_sk,
    sg.geografia_sk,
    sf.finca_sk,
    sq.calidad_sk,
    sl.logistica_sk,
    sca.campania_sk,
    f.kilos_vendidos,
    f.cajas_vendidas,
    f.importe_bruto,
    f.descuento,
    f.importe_neto,
    f.coste_producto,
    f.coste_logistico,
    f.coste_total,
    f.margen,
    f.precio_medio_kg,
    f.tiempo_entrega_dias
FROM olap_star.fact_exportaciones f
JOIN olap_star.dim_tiempo t ON t.tiempo_sk = f.tiempo_sk
JOIN olap_star.dim_producto p ON p.producto_sk = f.producto_sk
JOIN olap_star.dim_cliente c ON c.cliente_sk = f.cliente_sk
JOIN olap_star.dim_geografia g ON g.geografia_sk = f.geografia_sk
JOIN olap_star.dim_finca fi ON fi.finca_sk = f.finca_sk
JOIN olap_star.dim_calidad q ON q.calidad_sk = f.calidad_sk
JOIN olap_star.dim_logistica l ON l.logistica_sk = f.logistica_sk
JOIN olap_star.dim_campania ca ON ca.campania_sk = f.campania_sk
JOIN olap_snow.dim_tiempo st ON st.fecha = t.fecha
JOIN olap_snow.dim_familia_producto sfp ON sfp.familia_producto = p.familia_producto
JOIN olap_snow.dim_producto sp
    ON sp.cultivo_id = p.cultivo_id
   AND sp.nombre_producto = p.nombre_producto
   AND sp.variedad = p.variedad
   AND sp.familia_sk = sfp.familia_sk
JOIN olap_snow.dim_cliente sc ON sc.cliente_id = c.cliente_id
JOIN olap_snow.dim_geografia sg ON sg.pais = g.pais AND sg.ciudad = g.ciudad
JOIN olap_snow.dim_finca sf ON sf.finca_id = fi.finca_id AND sf.parcela = fi.parcela
JOIN olap_snow.dim_calidad sq ON sq.lote_id = q.lote_id
JOIN olap_snow.dim_logistica sl ON sl.ruta_id = l.ruta_id
JOIN olap_snow.dim_campania sca ON sca.campania_id = ca.campania_id;

CREATE INDEX idx_snow_fact_tiempo ON olap_snow.fact_exportaciones(tiempo_sk);
CREATE INDEX idx_snow_fact_producto ON olap_snow.fact_exportaciones(producto_sk);
CREATE INDEX idx_snow_fact_cliente ON olap_snow.fact_exportaciones(cliente_sk);
CREATE INDEX idx_snow_fact_campania ON olap_snow.fact_exportaciones(campania_sk);

COMMIT;