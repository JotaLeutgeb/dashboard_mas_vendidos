import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any

# Configuración básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuración de la página ---
st.set_page_config(
    page_title="Radar de Oportunidad",
    page_icon="📡",
    layout="wide",
)

st.markdown("""
    <style>
    /* Elimina el padding superior del contenedor principal */
    .block-container {
        padding-top: 1.1rem;
    }
    /* Elimina el padding superior del contenido de la barra lateral */
    [data-testid="stSidebarUserContent"] {
        padding-top: 0rem;
    }
    </style>
""", unsafe_allow_html=True)


# --- Conexión a la Base de Datos (usando st.secrets) ---

@st.cache_resource
def get_engine():
    """Crea y cachea la conexión a la base de datos."""
    try:
        db_user = st.secrets["db_user"]
        db_password_raw = st.secrets["db_password"]
        db_host = st.secrets["db_host"]
        db_port = st.secrets["db_port"]
        db_name = st.secrets["db_name"]
        db_password_encoded = quote_plus(db_password_raw)
        conn_string = f"postgresql+psycopg://{db_user}:{db_password_encoded}@{db_host}:{db_port}/{db_name}"
        return create_engine(conn_string)
    except Exception as e:
        st.error(f"Error al configurar la conexión con la base de datos: {e}")
        st.stop()

engine = get_engine()

# --- Funciones de Carga de Datos ---

@st.cache_data(ttl=600) # Cache por 10 minutos
def load_data(_engine, query, params=None):
    """Ejecuta una consulta SQL y devuelve un DataFrame de pandas."""
    if _engine is None:
        return pd.DataFrame()
    try:
        with _engine.connect() as connection:
            df = pd.read_sql(text(query), connection, params=params)
            logging.info(f"Consulta ejecutada exitosamente, {len(df)} filas obtenidas.")
            return df
    except Exception as e:
        st.error(f"Error al ejecutar la consulta: {e}")
        logging.error(f"Error en la consulta: {query} - {e}")
        return pd.DataFrame()

def format_price(value: float) -> str:
    """Formatea un número como precio en ARS."""
    return f"{value:,.0f}".replace(",", ".")

# --- MANTENEMOS ESTA FUNCIÓN PARA LA VISTA DIARIA ---
def calcular_variaciones(productos_hoy: List[Dict[str, Any]], productos_ayer: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Analiza los productos de hoy y ayer para calcular la variación de ranking y precio."""
    if not productos_ayer:
        for p in productos_hoy:
            p['variacion_ranking'] = None
            p['variacion_precio'] = 0
            p['ranking_anterior'] = None
        return productos_hoy

    df_ayer = pd.DataFrame(productos_ayer)
    df_ayer.set_index('link_publicacion', inplace=True)

    productos_enriquecidos = []
    for producto_hoy in productos_hoy:
        link = producto_hoy['link_publicacion']
        ranking_actual = producto_hoy['posicion']
        precio_actual = producto_hoy['precio']

        if link in df_ayer.index:
            producto_anterior = df_ayer.loc[link]
            ranking_anterior = int(producto_anterior['posicion'])
            precio_anterior = float(producto_anterior['precio'])
            
            variacion_ranking = ranking_anterior - ranking_actual
            variacion_precio = precio_actual - precio_anterior
            
            producto_hoy['variacion_ranking'] = variacion_ranking
            producto_hoy['variacion_precio'] = variacion_precio
            producto_hoy['ranking_anterior'] = ranking_anterior
        else:
            producto_hoy['variacion_ranking'] = None
            producto_hoy['variacion_precio'] = 0
            producto_hoy['ranking_anterior'] = None
        
        productos_enriquecidos.append(producto_hoy)
    
    return productos_enriquecidos


# --- Sidebar de Filtros (Lógica de fechas sin cambios) ---

st.sidebar.title("📡 Radar de Oportunidad")
st.sidebar.header("Filtros")

today = datetime.now().date()
if 'start_date' not in st.session_state:
    st.session_state.start_date = today
if 'end_date' not in st.session_state:
    st.session_state.end_date = today

def set_date_range(days):
    st.session_state.end_date = today
    st.session_state.start_date = today - timedelta(days=days-1)

st.sidebar.markdown("##### Períodos rápidos")
b_cols = st.sidebar.columns(2)
with b_cols[0]:
    if st.button("Última semana", on_click=set_date_range, args=(7,), use_container_width=True): pass
    if st.button("Último mes", on_click=set_date_range, args=(30,), use_container_width=True): pass
with b_cols[1]:
    if st.button("Últ. 2 semanas", on_click=set_date_range, args=(14,), use_container_width=True): pass
    if st.button("Hoy", on_click=set_date_range, args=(1,), use_container_width=True): pass

fechas_disponibles = load_data(engine, "SELECT MIN(fecha_extraccion) as min_fecha, MAX(fecha_extraccion) as max_fecha FROM public.productos_mas_vendidos;")
fecha_minima_db = fechas_disponibles['min_fecha'][0] if not fechas_disponibles.empty else today - timedelta(days=90)
fecha_maxima_db = fechas_disponibles['max_fecha'][0] if not fechas_disponibles.empty else today

selected_range = st.sidebar.date_input(
    "Seleccione un Período",
    value=(st.session_state.start_date, st.session_state.end_date),
    min_value=fecha_minima_db,
    max_value=fecha_maxima_db,
    format="DD/MM/YYYY"
)

if isinstance(selected_range, tuple) and len(selected_range) == 2:
    st.session_state.start_date, st.session_state.end_date = selected_range
else:
    st.session_state.start_date = st.session_state.end_date = selected_range

# Filtros de categoría
selected_cat_principal = None
selected_cat_secundaria = None
if engine:
    query_cat_principal = "SELECT DISTINCT categoria_principal FROM public.productos_mas_vendidos WHERE categoria_principal IS NOT NULL ORDER BY categoria_principal;"
    df_cat_principal = load_data(engine, query_cat_principal)
    categorias_principales = df_cat_principal["categoria_principal"].tolist()
    
    if categorias_principales:
        selected_cat_principal = st.sidebar.selectbox("Categoría Principal", options=categorias_principales, index=0)

    if selected_cat_principal:
        query_cat_secundaria = "SELECT DISTINCT categoria_secundaria FROM public.productos_mas_vendidos WHERE categoria_principal = :cat_principal AND categoria_secundaria IS NOT NULL ORDER BY categoria_secundaria;"
        params_sec = {"cat_principal": selected_cat_principal}
        df_cat_secundaria = load_data(engine, query_cat_secundaria, params=params_sec)
        categorias_secundarias = df_cat_secundaria["categoria_secundaria"].tolist()
        
        if categorias_secundarias:
            selected_cat_secundaria = st.sidebar.selectbox("Categoría Secundaria", options=["Todas"] + categorias_secundarias, index=0)


# --- >>> INICIO DE LA LÓGICA CONDICIONAL PRINCIPAL <<< ---

# --- VISTA 1: ANÁLISIS DE UN SOLO DÍA ---
if st.session_state.start_date == st.session_state.end_date:
    
    fecha_seleccionada = st.session_state.start_date
    st.title("Productos más vendidos")
    st.markdown(f"Mostrando resultados para la fecha: **{fecha_seleccionada.strftime('%d/%m/%Y')}**")

    # --- Lógica de consulta para un solo día (código original) ---
    df_productos = pd.DataFrame()
    if engine and selected_cat_principal:
        query_base = "SELECT posicion, titulo, precio, imagen, link_publicacion FROM public.productos_mas_vendidos WHERE fecha_extraccion = :fecha AND categoria_principal = :cat_p"
        params = {"fecha": fecha_seleccionada, "cat_p": selected_cat_principal}

        if selected_cat_secundaria and selected_cat_secundaria != "Todas":
            query_base += " AND categoria_secundaria = :cat_s"
            params["cat_s"] = selected_cat_secundaria

        query_base += " ORDER BY posicion ASC;"
        df_productos = load_data(engine, query_base, params=params)

    # --- Consulta del día anterior ---
    df_anterior = pd.DataFrame()
    if engine and selected_cat_principal:
        fecha_anterior = fecha_seleccionada - timedelta(days=1)
        query_anterior = "SELECT posicion, titulo, precio, link_publicacion FROM public.productos_mas_vendidos WHERE fecha_extraccion = :fecha AND categoria_principal = :cat_p"
        params_anterior = {"fecha": fecha_anterior, "cat_p": selected_cat_principal}

        if selected_cat_secundaria and selected_cat_secundaria != "Todas":
            query_anterior += " AND categoria_secundaria = :cat_s"
            params_anterior["cat_s"] = selected_cat_secundaria
        
        df_anterior = load_data(engine, query_anterior, params=params_anterior)

    if df_productos.empty:
        st.warning("No se encontraron productos con los filtros seleccionados.")
    else:
        # --- Llamada a la lógica de cálculo diario ---
        productos_hoy = df_productos.to_dict(orient='records')
        productos_ayer = df_anterior.to_dict(orient='records')
        productos_analizados = calcular_variaciones(productos_hoy, productos_ayer)
        
        # --- Visualización en Grilla (código original) ---
        num_columnas = 5
        cols = st.columns(num_columnas)
        for i, producto in enumerate(productos_analizados):
            col_actual = cols[i % num_columnas]
            with col_actual:
                with st.container(border=True, height=450):
                    st.image(producto.get("imagen", "https://placehold.co/300x300/F0F2F6/31333F?text=Sin+Imagen"), use_container_width=True)
                    c1, c2 = st.columns([7, 3])
                    with c1:
                        delta_precio = round(producto['variacion_precio'], 2) if producto['variacion_precio'] != 0 else None
                        st.metric(label="Precio", value=f"${format_price(producto['precio'])}", delta=delta_precio)
                    with c2:
                        delta_ranking_texto = ""
                        if producto['variacion_ranking'] is None: delta_ranking_texto = "Nuevo"
                        elif producto['variacion_ranking'] == 0: delta_ranking_texto = None
                        else: delta_ranking_texto = f"{producto['variacion_ranking']:#,}"
                        st.metric(label="Top", value=f"{producto['posicion']}", delta=delta_ranking_texto)
                    
                    titulo_completo = producto['titulo']
                    titulo_mostrado = (titulo_completo[:60] + '...') if len(titulo_completo) > 60 else titulo_completo
                    st.markdown(f'<h5><a href="{producto["link_publicacion"]}" target="_blank" title="{titulo_completo}" style="text-decoration: none; color: inherit;">{titulo_mostrado}</a></h5>', unsafe_allow_html=True)

# --- VISTA 2: ANÁLISIS DE TENDENCIAS EN UN RANGO DE FECHAS ---
else:
    st.title("Productos en Tendencia")
    st.markdown(f"Mostrando tendencias para el período: **{st.session_state.start_date.strftime('%d/%m/%Y')}** al **{st.session_state.end_date.strftime('%d/%m/%Y')}**")

    df_productos = pd.DataFrame()
    if engine and selected_cat_principal:
        query_tendencia = """
        WITH RankedProducts AS (
            SELECT link_publicacion, titulo, precio, imagen, posicion, fecha_extraccion, ROW_NUMBER() OVER(PARTITION BY link_publicacion ORDER BY fecha_extraccion DESC) as rn
            FROM public.productos_mas_vendidos
            WHERE fecha_extraccion BETWEEN :start_date AND :end_date AND categoria_principal = :cat_p
            AND (:cat_s IS NULL OR categoria_secundaria = :cat_s)
        ), AggregatedProducts AS (
            SELECT link_publicacion, COUNT(DISTINCT fecha_extraccion) AS dias_en_top, AVG(posicion) AS posicion_promedio, MIN(posicion) AS mejor_posicion
            FROM public.productos_mas_vendidos
            WHERE fecha_extraccion BETWEEN :start_date AND :end_date AND categoria_principal = :cat_p AND (:cat_s IS NULL OR categoria_secundaria = :cat_s)
            GROUP BY link_publicacion
        )
        SELECT rp.titulo, rp.precio, rp.imagen, rp.link_publicacion, ap.posicion_promedio, ap.dias_en_top, ap.mejor_posicion
        FROM RankedProducts rp JOIN AggregatedProducts ap ON rp.link_publicacion = ap.link_publicacion
        WHERE rp.rn = 1
        ORDER BY ap.posicion_promedio ASC, ap.dias_en_top DESC;
        """
        params = {
            "start_date": st.session_state.start_date, 
            "end_date": st.session_state.end_date, 
            "cat_p": selected_cat_principal,
            "cat_s": selected_cat_secundaria if selected_cat_secundaria and selected_cat_secundaria != "Todas" else None
        }
        df_productos = load_data(engine, query_tendencia, params=params)

    if df_productos.empty:
        st.warning("No se encontraron productos en tendencia con los filtros seleccionados.")
    else:
        productos_analizados = df_productos.to_dict(orient='records')
        num_columnas = 5
        cols = st.columns(num_columnas)
        for i, producto in enumerate(productos_analizados):
            col_actual = cols[i % num_columnas]
            with col_actual:
                with st.container(border=True, height=450):
                    st.image(producto.get("imagen", "https://placehold.co/300x300/F0F2F6/31333F?text=Sin+Imagen"), use_container_width=True)
                    c1, c2 = st.columns([3, 2])
                    with c1: st.metric(label="Posición Prom.", value=f"#{producto['posicion_promedio']:.1f}")
                    with c2: st.metric(label="Días en Top", value=f"{producto['dias_en_top']}")
                    st.metric(label="Precio Actual", value=f"${format_price(producto['precio'])}")
                    
                    titulo_completo = producto['titulo']
                    titulo_mostrado = (titulo_completo[:60] + '...') if len(titulo_completo) > 60 else titulo_completo
                    st.markdown(f'<h5><a href="{producto["link_publicacion"]}" target="_blank" title="{titulo_completo}" style="text-decoration: none; color: inherit;">{titulo_mostrado}</a></h5>', unsafe_allow_html=True)