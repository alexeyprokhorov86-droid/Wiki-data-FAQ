
import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta


# ============================================================
# НАСТРОЙКИ СТРАНИЦЫ
# ============================================================

st.set_page_config(
    page_title="История цен | Аналитика",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ============================================================
# ПОДКЛЮЧЕНИЕ К БАЗЕ ДАННЫХ
# ============================================================

@st.cache_resource
def get_connection():
    """Создаёт подключение к PostgreSQL"""
    return psycopg2.connect(
        host=st.secrets["postgres"]["host"],
        port=st.secrets["postgres"]["port"],
        database=st.secrets["postgres"]["database"],
        user=st.secrets["postgres"]["user"],
        password=st.secrets["postgres"]["password"],
    )


@st.cache_data(ttl=300)  # Кэш на 5 минут
def load_data(date_from: str, date_to: str) -> pd.DataFrame:
    """Загружает данные о ценах за период"""
    conn = get_connection()
    
    query = """
        SELECT 
            doc_date as "Дата",
            doc_number as "Номер",
            contractor_name as "Поставщик",
            nomenclature_name as "Номенклатура",
            quantity as "Количество",
            price as "Цена",
            sum_total as "Сумма"
        FROM purchase_prices
        WHERE doc_date BETWEEN %s AND %s
        ORDER BY doc_date DESC, nomenclature_name
    """
    
    df = pd.read_sql(query, conn, params=[date_from, date_to])
    return df


@st.cache_data(ttl=300)
def get_stats() -> dict:
    """Получает общую статистику"""
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM purchase_prices")
    total_records = cur.fetchone()[0]
    
    cur.execute("SELECT MIN(doc_date), MAX(doc_date) FROM purchase_prices")
    min_date, max_date = cur.fetchone()
    
    cur.execute("SELECT COUNT(DISTINCT contractor_name) FROM purchase_prices")
    suppliers_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT nomenclature_name) FROM purchase_prices")
    nomenclature_count = cur.fetchone()[0]
    
    cur.close()
    
    return {
        "total_records": total_records,
        "min_date": min_date,
        "max_date": max_date,
        "suppliers_count": suppliers_count,
        "nomenclature_count": nomenclature_count,
    }


@st.cache_data(ttl=300)
def get_analysis(date_from: str, date_to: str) -> pd.DataFrame:
    """Анализ цен по номенклатуре и поставщикам"""
    conn = get_connection()
    
    query = """
        SELECT 
            nomenclature_name as "Номенклатура",
            contractor_name as "Поставщик",
            MIN(price) as "Цена_мин",
            MAX(price) as "Цена_макс",
            ROUND(AVG(price)::numeric, 2) as "Цена_средняя",
            (array_agg(price ORDER BY doc_date ASC))[1] as "Цена_первая",
            (array_agg(price ORDER BY doc_date DESC))[1] as "Цена_последняя",
            COUNT(*) as "Поставок",
            SUM(quantity) as "Всего_кол_во",
            SUM(sum_total) as "Всего_сумма",
            MIN(doc_date) as "Первая_дата",
            MAX(doc_date) as "Последняя_дата"
        FROM purchase_prices
        WHERE doc_date BETWEEN %s AND %s
        GROUP BY nomenclature_name, contractor_name
        ORDER BY "Всего_сумма" DESC
    """
    
    df = pd.read_sql(query, conn, params=[date_from, date_to])
    
    # Вычисляем изменение цены
    if not df.empty:
        df["Изменение_%"] = ((df["Цена_последняя"] - df["Цена_первая"]) / df["Цена_первая"] * 100).round(1)
        df["Изменение_%"] = df["Изменение_%"].replace([float('inf'), float('-inf')], 0)
    
    return df


# ============================================================
# ИНТЕРФЕЙС
# ============================================================

def main():
    st.title("📊 История закупочных цен")
    st.caption("Данные из 1С:Комплексная автоматизация 2.5")
    
    # Проверяем подключение
    try:
        stats = get_stats()
    except Exception as e:
        st.error(f"❌ Ошибка подключения к базе данных: {e}")
        st.info("Проверьте настройки в .streamlit/secrets.toml")
        return
    
    # ========== БОКОВАЯ ПАНЕЛЬ ==========
    with st.sidebar:
        st.header("📅 Период")
        
        # Безопасная обработка дат
        today = datetime.now().date()
        min_date = stats["min_date"] if stats["min_date"] else today - timedelta(days=365)
        max_date = stats["max_date"] if stats["max_date"] else today
        
        # Убедимся что min_date <= max_date
        if min_date > max_date:
            min_date, max_date = max_date, min_date
        
        # Значение по умолчанию для date_from
        default_from = max(min_date, max_date - timedelta(days=365))
        
        col1, col2 = st.columns(2)
        with col1:
            date_from = st.date_input(
                "С",
                value=default_from,
                min_value=min_date,
                max_value=max_date,
                format="DD.MM.YYYY"
            )
        with col2:
            date_to = st.date_input(
                "По",
                value=max_date,
                min_value=min_date,
                max_value=max_date,
                format="DD.MM.YYYY"
            )
        
        st.divider()
        
        st.header("ℹ️ О данных")
        st.metric("Всего записей в базе", f"{stats['total_records']:,}")
        st.caption(f"Период: {stats['min_date']} — {stats['max_date']}")
        st.caption(f"Поставщиков: {stats['suppliers_count']}")
        st.caption(f"Позиций: {stats['nomenclature_count']}")
        
        st.divider()
        
        if st.button("🔄 Обновить данные", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    
    # ========== ЗАГРУЗКА ДАННЫХ ==========
    with st.spinner("Загрузка данных..."):
        df = load_data(str(date_from), str(date_to))
        analysis_df = get_analysis(str(date_from), str(date_to))
    
    if df.empty:
        st.warning("Нет данных за выбранный период")
        return
    
    # ========== МЕТРИКИ ==========
    st.header("📈 Статистика за период")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Записей", f"{len(df):,}")
    with col2:
        st.metric("Поставщиков", df['Поставщик'].nunique())
    with col3:
        st.metric("Позиций", df['Номенклатура'].nunique())
    with col4:
        st.metric("Сумма закупок", f"{df['Сумма'].sum():,.0f} ₽")
    
    # ========== ФИЛЬТРЫ ==========
    st.header("🔍 Фильтры")
    
    col1, col2 = st.columns(2)
    
    with col1:
        suppliers = ['Все'] + sorted(df['Поставщик'].unique().tolist())
        selected_supplier = st.selectbox("Поставщик", suppliers)
    
    with col2:
        search_text = st.text_input("Поиск по номенклатуре", "")
    
    # Применяем фильтры
    filtered_df = df.copy()
    filtered_analysis = analysis_df.copy()
    
    if selected_supplier != 'Все':
        filtered_df = filtered_df[filtered_df['Поставщик'] == selected_supplier]
        filtered_analysis = filtered_analysis[filtered_analysis['Поставщик'] == selected_supplier]
    
    if search_text:
        mask = filtered_df['Номенклатура'].str.lower().str.contains(search_text.lower(), na=False)
        filtered_df = filtered_df[mask]
        mask_analysis = filtered_analysis['Номенклатура'].str.lower().str.contains(search_text.lower(), na=False)
        filtered_analysis = filtered_analysis[mask_analysis]
    
    # ========== ВКЛАДКИ ==========
    tab1, tab2, tab3, tab4 = st.tabs([
        "📋 Все данные",
        "📊 Анализ цен",
        "📈 Динамика",
        "🏆 Топ изменений"
    ])
    
    # Вкладка 1: Все данные
    with tab1:
        st.subheader(f"Данные ({len(filtered_df)} записей)")
        st.dataframe(
            filtered_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Дата": st.column_config.DateColumn("Дата", format="DD.MM.YYYY"),
                "Цена": st.column_config.NumberColumn("Цена", format="%.2f ₽"),
                "Сумма": st.column_config.NumberColumn("Сумма", format="%.2f ₽"),
            }
        )
        
        # Кнопка скачивания
        csv = filtered_df.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            "📥 Скачать CSV",
            csv,
            "история_цен.csv",
            "text/csv",
        )
    
    # Вкладка 2: Анализ
    with tab2:
        st.subheader("Анализ по позициям")
        
        if not filtered_analysis.empty:
            st.dataframe(
                filtered_analysis,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Цена_мин": st.column_config.NumberColumn("Мин", format="%.2f ₽"),
                    "Цена_макс": st.column_config.NumberColumn("Макс", format="%.2f ₽"),
                    "Цена_средняя": st.column_config.NumberColumn("Средняя", format="%.2f ₽"),
                    "Цена_первая": st.column_config.NumberColumn("Первая", format="%.2f ₽"),
                    "Цена_последняя": st.column_config.NumberColumn("Последняя", format="%.2f ₽"),
                    "Изменение_%": st.column_config.NumberColumn("Изм. %", format="%.1f%%"),
                    "Всего_сумма": st.column_config.NumberColumn("Сумма", format="%.0f ₽"),
                }
            )
    
    # Вкладка 3: Динамика
    with tab3:
        st.subheader("Динамика цены")
        
        nomenclatures = sorted(filtered_df['Номенклатура'].unique().tolist())
        
        if nomenclatures:
            selected_nom = st.selectbox("Выберите позицию", nomenclatures)
            
            if selected_nom:
                nom_df = filtered_df[filtered_df['Номенклатура'] == selected_nom].copy()
                
                if len(nom_df) > 1:
                    # График
                    fig = px.line(
                        nom_df,
                        x='Дата',
                        y='Цена',
                        color='Поставщик',
                        markers=True,
                        title=f"Динамика цены: {selected_nom}"
                    )
                    fig.update_layout(
                        xaxis_title="Дата",
                        yaxis_title="Цена, ₽",
                        hovermode='x unified'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Статистика
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Мин. цена", f"{nom_df['Цена'].min():.2f} ₽")
                    with col2:
                        st.metric("Макс. цена", f"{nom_df['Цена'].max():.2f} ₽")
                    with col3:
                        first_price = nom_df.sort_values('Дата')['Цена'].iloc[0]
                        last_price = nom_df.sort_values('Дата')['Цена'].iloc[-1]
                        change = last_price - first_price
                        st.metric(
                            "Изменение",
                            f"{last_price:.2f} ₽",
                            f"{change:+.2f} ₽"
                        )
                else:
                    st.info("Недостаточно данных для графика (нужно минимум 2 записи)")
    
    # Вкладка 4: Топ изменений
    with tab4:
        if not filtered_analysis.empty:
            multi = filtered_analysis[filtered_analysis['Поставок'] > 1].copy()
            
            if not multi.empty:
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("📈 Топ по росту цены")
                    top_growth = multi.nlargest(10, 'Изменение_%')[
                        ['Номенклатура', 'Поставщик', 'Цена_первая', 'Цена_последняя', 'Изменение_%']
                    ]
                    st.dataframe(top_growth, hide_index=True, use_container_width=True)
                
                with col2:
                    st.subheader("📉 Топ по снижению цены")
                    top_decline = multi.nsmallest(10, 'Изменение_%')[
                        ['Номенклатура', 'Поставщик', 'Цена_первая', 'Цена_последняя', 'Изменение_%']
                    ]
                    st.dataframe(top_decline, hide_index=True, use_container_width=True)
                
                # График распределения
                st.subheader("Распределение изменений цен")
                fig = px.histogram(
                    multi,
                    x='Изменение_%',
                    nbins=30,
                    title="Распределение изменений цен (%)"
                )
                fig.add_vline(x=0, line_dash="dash", line_color="red")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Недостаточно данных для анализа (нужны позиции с 2+ поставками)")


if __name__ == "__main__":
    main()
