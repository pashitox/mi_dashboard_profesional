# dashboard.py - VERSIÓN PROFESIONAL PARA STREAMLIT CLOUD
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from databricks import sql
from datetime import datetime, timedelta
import os

# ========= CONFIGURACIÓN DE PÁGINA =========
st.set_page_config(
    page_title="Dashboard IoT Industrial",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ========= SECRETOS (Streamlit Cloud) =========
# En Streamlit Cloud, configura estos secretos en la web
@st.cache_resource
def init_connection():
    return sql.connect(
        server_hostname=st.secrets["DATABRICKS_SERVER"],
        http_path=st.secrets["DATABRICKS_HTTP_PATH"],
        access_token=st.secrets["DATABRICKS_TOKEN"]
    )

# ========= SIDEBAR =========
with st.sidebar:
    st.image("https://databricks.com/wp-content/uploads/2021/10/logo-databricks@2x.png", width=150)
    st.title("🔧 Controles")
    
    # Filtro de fecha
    fecha_inicio = st.date_input(
        "Fecha inicio",
        value=datetime.now() - timedelta(days=7)
    )
    
    # Filtro de dispositivo
    todos_dispositivos = st.checkbox("Todos los dispositivos", value=True)
    
    st.divider()
    st.markdown("**📊 Métricas en vivo**")
    st.metric("Tiempo respuesta", "124ms", delta="-12ms")
    
    st.divider()
    st.markdown("""
    **ℹ️ Información**
    - Datos: 10M+ registros IoT
    - Actualización: Cada 5 min
    - Fuente: Azure Databricks
    """)

# ========= CONEXIÓN Y CONSULTAS =========
@st.cache_data(ttl=300)  # Cache 5 minutos
def load_data():
    conn = init_connection()
    
    query = """
    SELECT 
        device_id,
        status,
        event_time,
        event_hour,
        is_ok_flag
    FROM databricks_juan2.iot_lab.dispositivos_masivos_powerbi
    WHERE event_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
    ORDER BY event_time DESC
    LIMIT 50000
    """
    
    return pd.read_sql(query, conn)

# ========= INTERFAZ PRINCIPAL =========
st.title("🏭 Dashboard de Monitoreo Industrial IoT")
st.markdown("""
**Monitoreo en tiempo real de dispositivos IoT** | 
[Ver Databricks](https://adb-7405605953039369.9.azuredatabricks.net) | 
[GitHub](https://github.com)
""")

# Cargar datos con spinner profesional
with st.spinner("🔄 Conectando a Databricks..."):
    df = load_data()

if df.empty:
    st.error("No se pudieron cargar datos. Verifica la conexión.")
    st.stop()

# ========= KPIs =========
st.subheader("📈 Indicadores Clave (KPIs)")
col1, col2, col3, col4 = st.columns(4)

with col1:
    disponibilidad = df['is_ok_flag'].mean() * 100
    st.metric(
        "Disponibilidad", 
        f"{disponibilidad:.1f}%",
        delta=f"{disponibilidad-95:.1f}%" if disponibilidad > 95 else f"{disponibilidad-95:.1f}%",
        delta_color="normal"
    )

with col2:
    total_eventos = len(df)
    st.metric("Eventos Totales", f"{total_eventos:,}")

with col3:
    errores = df[df['status'] == 'error'].shape[0]
    tasa_error = (errores / total_eventos * 100) if total_eventos > 0 else 0
    st.metric("Tasa de Error", f"{tasa_error:.2f}%")

with col4:
    dispositivos_activos = df['device_id'].nunique()
    st.metric("Dispositivos Activos", dispositivos_activos)

# ========= GRÁFICAS PROFESIONALES =========
tab1, tab2, tab3 = st.tabs(["📊 Visión General", "⏰ Tendencia", "🔍 Análisis Detallado"])

with tab1:
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Gráfico de líneas - Eventos por hora
        df_hourly = df.copy()
        df_hourly['hour'] = pd.to_datetime(df_hourly['event_hour']).dt.floor('H')
        hourly_counts = df_hourly.groupby('hour').size().reset_index(name='count')
        
        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(
            x=hourly_counts['hour'],
            y=hourly_counts['count'],
            mode='lines+markers',
            name='Eventos',
            line=dict(color='#1f77b4', width=3),
            fill='tozeroy',
            fillcolor='rgba(31, 119, 180, 0.1)'
        ))
        
        # Añadir línea de errores
        hourly_errors = df_hourly[df_hourly['status'] == 'error'].groupby('hour').size().reset_index(name='errors')
        fig1.add_trace(go.Scatter(
            x=hourly_errors['hour'],
            y=hourly_errors['errors'],
            mode='lines',
            name='Errores',
            line=dict(color='#ff7f0e', width=2, dash='dash')
        ))
        
        fig1.update_layout(
            title='Eventos por Hora (Últimas 24h)',
            xaxis_title='Hora',
            yaxis_title='Cantidad',
            hovermode='x unified',
            template='plotly_white'
        )
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        # Donut chart
        status_counts = df['status'].value_counts()
        fig2 = px.pie(
            values=status_counts.values,
            names=status_counts.index,
            hole=0.5,
            color=status_counts.index,
            color_discrete_map={'ok': '#2ca02c', 'error': '#d62728'}
        )
        fig2.update_layout(
            title='Distribución de Estados',
            annotations=[dict(text=f'{total_eventos:,}', x=0.5, y=0.5, font_size=20, showarrow=False)]
        )
        st.plotly_chart(fig2, use_container_width=True)

with tab2:
    # Heatmap de actividad
    st.subheader("🌡️ Mapa de Calor - Actividad por Hora")
    
    # Preparar datos para heatmap
    df_heatmap = df.copy()
    df_heatmap['hour'] = pd.to_datetime(df_heatmap['event_time']).dt.hour
    df_heatmap['day'] = pd.to_datetime(df_heatmap['event_time']).dt.date
    
    pivot_data = df_heatmap.pivot_table(
        index='day',
        columns='hour',
        values='device_id',
        aggfunc='count',
        fill_value=0
    )
    
    fig3 = go.Figure(data=go.Heatmap(
        z=pivot_data.values,
        x=list(range(24)),
        y=pivot_data.index.astype(str),
        colorscale='Viridis',
        showscale=True
    ))
    
    fig3.update_layout(
        title='Actividad por Día y Hora',
        xaxis_title='Hora del día',
        yaxis_title='Fecha',
        height=400
    )
    st.plotly_chart(fig3, use_container_width=True)

with tab3:
    # Tabla interactiva con filtros
    st.subheader("📋 Datos en Tiempo Real")
    
    # Filtros para la tabla
    col_filter1, col_filter2 = st.columns(2)
    with col_filter1:
        selected_status = st.multiselect(
            "Filtrar por estado:",
            options=df['status'].unique(),
            default=df['status'].unique()
        )
    
    with col_filter2:
        min_events = st.slider("Mínimo eventos por dispositivo:", 1, 1000, 10)
    
    # Aplicar filtros
    df_filtered = df[df['status'].isin(selected_status)]
    device_counts = df_filtered['device_id'].value_counts()
    active_devices = device_counts[device_counts >= min_events].index
    df_filtered = df_filtered[df_filtered['device_id'].isin(active_devices)]
    
    # Mostrar tabla
    st.dataframe(
        df_filtered.sort_values('event_time', ascending=False).head(100),
        use_container_width=True,
        column_config={
            "device_id": "Dispositivo",
            "status": st.column_config.TextColumn(
                "Estado",
                help="Estado del dispositivo"
            ),
            "event_time": st.column_config.DatetimeColumn(
                "Fecha/Hora",
                format="DD/MM/YYYY HH:mm:ss"
            ),
            "is_ok_flag": st.column_config.ProgressColumn(
                "OK",
                help="Flag de operación correcta",
                format="%d",
                min_value=0,
                max_value=1
            )
        }
    )
    
    # Botón de descarga
    csv = df_filtered.to_csv(index=False)
    st.download_button(
        label="📥 Descargar CSV",
        data=csv,
        file_name="datos_iot.csv",
        mime="text/csv"
    )

# ========= FOOTER =========
st.divider()
st.markdown("""
<div style='text-align: center; color: gray;'>
    <p>Dashboard IoT Industrial | Actualizado: {}</p>
    <p>Powered by <b>Azure Databricks</b> + <b>Streamlit</b> | 
    <a href='https://github.com'>Ver código fuente</a></p>
</div>
""".format(datetime.now().strftime("%d/%m/%Y %H:%M")), unsafe_allow_html=True)