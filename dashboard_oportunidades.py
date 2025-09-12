import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from datetime import datetime
import logging

# Configuración básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuración de la página ---
st.set_page_config(
    page_title="Radar de Oportunidad",
    page_icon="📡",
    layout="wide",
)

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
    """
    Ejecuta una consulta SQL y devuelve un DataFrame de pandas.
    Los resultados se cachean.
    """
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
    """
    Formatea un número como precio en ARS:
    miles con punto y decimales con coma.
    Ejemplo: 1234567.89 -> '1.234.568'
    """
    return f"{value:,.0f}".replace(",", ".")

# --- Sidebar de Filtros ---

st.sidebar.title("📡 Radar de Oportunidad")
st.sidebar.header("Filtros")

# Filtro de fecha
selected_date = st.sidebar.date_input(
    "Fecha de extracción",
    value=datetime.today()
)

# Inicializar variables de selección
selected_cat_principal = None
selected_cat_secundaria = None

# Filtros de categoría (se cargan solo si la conexión a la BD es exitosa)
if engine:
    # 1. Filtro de Categoría Principal
    query_cat_principal = "SELECT DISTINCT categoria_principal FROM public.productos_mas_vendidos WHERE categoria_principal IS NOT NULL ORDER BY categoria_principal;"
    df_cat_principal = load_data(engine, query_cat_principal)
    categorias_principales = df_cat_principal["categoria_principal"].tolist()
    
    if categorias_principales:
        selected_cat_principal = st.sidebar.selectbox(
            "Categoría Principal",
            options=categorias_principales,
            index=0 # Por defecto, la primera de la lista
        )
    else:
        st.sidebar.warning("No se encontraron categorías principales.")

    # 2. Filtro de Categoría Secundaria (dependiente de la principal)
    if selected_cat_principal:
        query_cat_secundaria = "SELECT DISTINCT categoria_secundaria FROM public.productos_mas_vendidos WHERE categoria_principal = :cat_principal AND categoria_secundaria IS NOT NULL ORDER BY categoria_secundaria;"
        params_sec = {"cat_principal": selected_cat_principal}

        df_cat_secundaria = load_data(engine, query_cat_secundaria, params=params_sec)
        categorias_secundarias = df_cat_secundaria["categoria_secundaria"].tolist()

        if categorias_secundarias:
            selected_cat_secundaria = st.sidebar.selectbox(
                "Categoría Secundaria",
                options=categorias_secundarias,
                index=0 # Por defecto, la primera de la lista
            )
        else:
            st.sidebar.info(f"No hay sub-categorías para '{selected_cat_principal}'.")
else:
    st.sidebar.warning("No se pueden cargar los filtros de categoría sin conexión a la base de datos.")


# --- Lógica de la consulta principal ---
if engine:
    # Solo ejecutar la consulta si hay al menos una categoría principal seleccionada
    if selected_cat_principal:
        query_base = "SELECT titulo, precio, imagen, link_publicacion FROM public.productos_mas_vendidos WHERE fecha_extraccion = :fecha"
        params = {"fecha": selected_date}

        query_base += " AND categoria_principal = :cat_p"
        params["cat_p"] = selected_cat_principal

        # Si hay una categoría secundaria seleccionada, también se filtra por ella
        if selected_cat_secundaria:
            query_base += " AND categoria_secundaria = :cat_s"
            params["cat_s"] = selected_cat_secundaria

        query_base += " ORDER BY precio ASC;"

        df_productos = load_data(engine, query_base, params=params)
    else:
        df_productos = pd.DataFrame() # DataFrame vacío si no hay categorías
else:
    df_productos = pd.DataFrame()

# --- Consulta del día anterior (para comparación de métricas) ---
fecha_anterior = selected_date - pd.Timedelta(days=1)

query_anterior = """
SELECT titulo, precio, categoria_principal, categoria_secundaria
FROM public.productos_mas_vendidos
WHERE fecha_extraccion = :fecha
AND categoria_principal = :cat_p
"""

params_anterior = {"fecha": fecha_anterior, "cat_p": selected_cat_principal}
if selected_cat_secundaria:
    query_anterior += " AND categoria_secundaria = :cat_s"
    params_anterior["cat_s"] = selected_cat_secundaria

df_anterior = load_data(engine, query_anterior, params=params_anterior)


# --- Página Principal ---
st.title("Productos más vendidos")
st.markdown(f"Mostrando resultados para la fecha: **{selected_date.strftime('%d/%m/%Y')}**")

if df_productos.empty:
    st.warning("No se encontraron productos con los filtros seleccionados. Intenta con otra fecha o categoría.")
else:
    # --- Visualización en Grilla ---
    num_columnas = 5
    
    # Crea las columnas una sola vez
    cols = st.columns(num_columnas)
    
    for i, (index, producto) in enumerate(df_productos.iterrows()):
        col_actual = cols[i % num_columnas]
        with col_actual:
            with st.container(border=True, height=450):
                # Imagen
                if producto["imagen"] and isinstance(producto["imagen"], str):
                    st.image(producto["imagen"], use_container_width=True)
                else:
                    st.image("https://placehold.co/300x300/F0F2F6/31333F?text=Sin+Imagen", use_container_width=True)

                # --- Calcular métricas ---
                # Ranking actual
                ranking_actual = i + 1

                # Buscar producto en día anterior
                prod_ayer = df_anterior[df_anterior["titulo"] == producto["titulo"]]

                if not prod_ayer.empty:
                    precio_ayer = prod_ayer.iloc[0]["precio"]
                    ranking_ayer = df_anterior[df_anterior["titulo"] == producto["titulo"]].index[0] + 1

                    variacion_precio = producto["precio"] - precio_ayer
                    variacion_ranking = ranking_ayer - ranking_actual
                else:
                    variacion_precio = None
                    variacion_ranking = None

                # --- Métricas rápidas ---
                c1, c2 = st.columns(2)
                with c1:
                    if variacion_precio is None:
                        st.markdown("🟧 **Nuevo**") 
                    else:
                        precio_formateado = format_price(producto['precio'])
                        if len(precio_formateado) > 10:
                            precio_display = f"<span style='font-size:0.8em;'>${precio_formateado}</span>"
                            st.markdown(precio_display, unsafe_allow_html=True)
                        else:
                            st.metric("Precio", f"${precio_formateado}", f"{format_price(variacion_precio)}")

                if variacion_ranking is None:
                    st.markdown("🟧 **Nuevo**")
                else:
                    st.metric("Ranking", f"#{ranking_actual}", f"{variacion_ranking:+}".rjust(3))

                # --- Título con link ---
                st.markdown(f"**{producto['titulo']}**")
                
                # --- Botón CTA ---
                st.link_button("🔗 Ver en MercadoLibre", producto["link_publicacion"])

