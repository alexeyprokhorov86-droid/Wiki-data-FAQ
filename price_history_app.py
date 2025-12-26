
import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta


# ============================================================
# НАСТРОЙКИ
# ============================================================

st.set_page_config(
    page_title="Аналитика | Кондитерская Прохорова",
    page_icon="🍪",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ============================================================
# ПОДКЛЮЧЕНИЕ К БД
# ============================================================

@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=st.secrets["postgres"]["host"],
        port=st.secrets["postgres"]["port"],
        database=st.secrets["postgres"]["database"],
        user=st.secrets["postgres"]["user"],
        password=st.secrets["postgres"]["password"],
    )


# ============================================================
# ЗАПРОСЫ К БД
# ============================================================

@st.cache_data(ttl=300)
def get_db_stats():
    """Общая статистика по базе"""
    conn = get_connection()
    cur = conn.cursor()
    
    stats = {}
    
    # Закупки
    cur.execute("SELECT COUNT(*), MIN(doc_date), MAX(doc_date), SUM(sum_total) FROM purchase_prices")
    row = cur.fetchone()
    stats['purchases'] = {
        'count': row[0] or 0,
        'min_date': row[1],
        'max_date': row[2],
        'total_sum': row[3] or 0
    }
    
    # Продажи
    cur.execute("SELECT COUNT(*), MIN(doc_date), MAX(doc_date), SUM(sum_with_vat) FROM sales")
    row = cur.fetchone()
    stats['sales'] = {
        'count': row[0] or 0,
        'min_date': row[1],
        'max_date': row[2],
        'total_sum': row[3] or 0
    }
    
    # Номенклатура
    cur.execute("SELECT COUNT(*) FROM nomenclature WHERE is_folder = false")
    stats['nomenclature_count'] = cur.fetchone()[0] or 0
    
    # Клиенты
    cur.execute("SELECT COUNT(*) FROM clients")
    stats['clients_count'] = cur.fetchone()[0] or 0
    
    cur.close()
    return stats


@st.cache_data(ttl=300)
def load_purchases(date_from: str, date_to: str):
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
        ORDER BY doc_date DESC
    """
    return pd.read_sql(query, conn, params=[date_from, date_to])


@st.cache_data(ttl=300)
def load_sales(date_from: str, date_to: str):
    conn = get_connection()
    query = """
        SELECT 
            doc_type as "Тип",
            doc_date as "Дата",
            doc_number as "Номер",
            client_name as "Клиент",
            consignee_name as "Грузополучатель",
            nomenclature_name as "Номенклатура",
            quantity as "Количество",
            price as "Цена",
            sum_with_vat as "Сумма",
            pallets_count as "Паллеты",
            logistics_cost_fact as "Логистика_факт"
        FROM sales
        WHERE doc_date BETWEEN %s AND %s
        ORDER BY doc_date DESC
    """
    return pd.read_sql(query, conn, params=[date_from, date_to])


@st.cache_data(ttl=300)
def load_nomenclature():
    conn = get_connection()
    query = """
        SELECT 
            n.name as "Наименование",
            n.article as "Артикул",
            n.code as "Код",
            nt.name as "Вид номенклатуры",
            n.weight as "Вес"
        FROM nomenclature n
        LEFT JOIN nomenclature_types nt ON n.type_id = nt.id
        WHERE n.is_folder = false
        ORDER BY nt.name, n.name
    """
    return pd.read_sql(query, conn)


@st.cache_data(ttl=300)
def get_sales_by_client(date_from: str, date_to: str):
    conn = get_connection()
    query = """
        SELECT 
            client_name as "Клиент",
            SUM(quantity) as "Количество",
            SUM(sum_with_vat) as "Сумма",
            COUNT(DISTINCT doc_number) as "Документов"
        FROM sales
        WHERE doc_date BETWEEN %s AND %s
        GROUP BY client_name
        ORDER BY "Сумма" DESC
    """
    return pd.read_sql(query, conn, params=[date_from, date_to])


@st.cache_data(ttl=300)
def get_sales_by_nomenclature(date_from: str, date_to: str):
    conn = get_connection()
    query = """
        SELECT 
            nomenclature_name as "Номенклатура",
            SUM(quantity) as "Количество",
            SUM(sum_with_vat) as "Сумма",
            COUNT(DISTINCT client_name) as "Клиентов"
        FROM sales
        WHERE doc_date BETWEEN %s AND %s
        GROUP BY nomenclature_name
        ORDER BY "Сумма" DESC
    """
    return pd.read_sql(query, conn, params=[date_from, date_to])


@st.cache_data(ttl=300)
def get_purchases_analysis(date_from: str, date_to: str):
    conn = get_connection()
    query = """
        SELECT 
            nomenclature_name as "Номенклатура",
            contractor_name as "Поставщик",
            MIN(price) as "Цена_мин",
            MAX(price) as "Цена_макс",
            ROUND(AVG(price)::numeric, 2) as "Цена_средняя",
            COUNT(*) as "Поставок",
            SUM(sum_total) as "Сумма"
        FROM purchase_prices
        WHERE doc_date BETWEEN %s AND %s
        GROUP BY nomenclature_name, contractor_name
        ORDER BY "Сумма" DESC
    """
    return pd.read_sql(query, conn, params=[date_from, date_to])


# ============================================================
# СТРАНИЦЫ
# ============================================================

def page_purchases(date_from, date_to):
    """Страница закупок"""
    st.header("🛒 Закупки")
    
    df = load_purchases(str(date_from), str(date_to))
    
    if df.empty:
        st.warning("Нет данных о закупках за выбранный период")
        return
    
    # Метрики
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Записей", f"{len(df):,}")
    col2.metric("Поставщиков", df['Поставщик'].nunique())
    col3.metric("Позиций", df['Номенклатура'].nunique())
    col4.metric("Сумма", f"{df['Сумма'].sum():,.0f} ₽")
    
    # Вкладки
    tab1, tab2, tab3 = st.tabs(["📋 Данные", "📊 Анализ цен", "📈 Топ изменений"])
    
    with tab1:
        # Фильтры
        col1, col2 = st.columns(2)
        with col1:
            suppliers = ['Все'] + sorted(df['Поставщик'].dropna().unique().tolist())
            supplier = st.selectbox("Поставщик", suppliers, key="purch_supplier")
        with col2:
            search = st.text_input("Поиск по номенклатуре", key="purch_search")
        
        filtered = df.copy()
        if supplier != 'Все':
            filtered = filtered[filtered['Поставщик'] == supplier]
        if search:
            filtered = filtered[filtered['Номенклатура'].str.lower().str.contains(search.lower(), na=False)]
        
        st.dataframe(filtered, use_container_width=True, hide_index=True)
        
        csv = filtered.to_csv(index=False).encode('utf-8-sig')
        st.download_button("📥 Скачать CSV", csv, "закупки.csv", "text/csv")
    
    with tab2:
        analysis = get_purchases_analysis(str(date_from), str(date_to))
        if not analysis.empty:
            st.dataframe(analysis, use_container_width=True, hide_index=True)
    
    with tab3:
        analysis = get_purchases_analysis(str(date_from), str(date_to))
        if not analysis.empty:
            # Считаем изменение (для позиций с несколькими поставками)
            multi = analysis[analysis['Поставок'] > 1].copy()
            if not multi.empty:
                multi['Изменение_%'] = ((multi['Цена_макс'] - multi['Цена_мин']) / multi['Цена_мин'] * 100).round(1)
                
                col1, col2 = st.columns(2)
                with col1:
                    st.subheader("📈 Наибольший разброс цен")
                    top = multi.nlargest(10, 'Изменение_%')[['Номенклатура', 'Поставщик', 'Цена_мин', 'Цена_макс', 'Изменение_%']]
                    st.dataframe(top, hide_index=True)
                
                with col2:
                    st.subheader("📊 Распределение")
                    fig = px.histogram(multi, x='Изменение_%', nbins=20, title="Разброс цен (%)")
                    st.plotly_chart(fig, use_container_width=True)


def page_sales(date_from, date_to):
    """Страница продаж"""
    st.header("💰 Продажи")
    
    df = load_sales(str(date_from), str(date_to))
    
    if df.empty:
        st.warning("Нет данных о продажах за выбранный период")
        return
    
    # Метрики
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Записей", f"{len(df):,}")
    col2.metric("Клиентов", df['Клиент'].nunique())
    col3.metric("Позиций", df['Номенклатура'].nunique())
    col4.metric("Выручка", f"{df['Сумма'].sum():,.0f} ₽")
    
    # Дополнительные метрики
    col1, col2, col3 = st.columns(3)
    pallets = df['Паллеты'].sum()
    logistics = df['Логистика_факт'].sum()
    corrections = df[df['Тип'] == 'Корректировка']['Сумма'].sum()
    
    col1.metric("Паллет", f"{pallets:,.0f}")
    col2.metric("Логистика (факт)", f"{logistics:,.0f} ₽")
    col3.metric("Корректировки", f"{corrections:,.0f} ₽")
    
    # Вкладки
    tab1, tab2, tab3, tab4 = st.tabs(["📋 Данные", "👥 По клиентам", "📦 По номенклатуре", "🚚 По грузополучателям"])
    
    with tab1:
        col1, col2 = st.columns(2)
        with col1:
            clients = ['Все'] + sorted(df['Клиент'].dropna().unique().tolist())
            client = st.selectbox("Клиент", clients, key="sales_client")
        with col2:
            search = st.text_input("Поиск по номенклатуре", key="sales_search")
        
        filtered = df.copy()
        if client != 'Все':
            filtered = filtered[filtered['Клиент'] == client]
        if search:
            filtered = filtered[filtered['Номенклатура'].str.lower().str.contains(search.lower(), na=False)]
        
        st.dataframe(filtered, use_container_width=True, hide_index=True)
        
        csv = filtered.to_csv(index=False).encode('utf-8-sig')
        st.download_button("📥 Скачать CSV", csv, "продажи.csv", "text/csv")
    
    with tab2:
        by_client = get_sales_by_client(str(date_from), str(date_to))
        if not by_client.empty:
            col1, col2 = st.columns([2, 1])
            with col1:
                st.dataframe(by_client, use_container_width=True, hide_index=True)
            with col2:
                top10 = by_client.head(10)
                fig = px.pie(top10, values='Сумма', names='Клиент', title='Топ-10 клиентов')
                st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        by_nom = get_sales_by_nomenclature(str(date_from), str(date_to))
        if not by_nom.empty:
            col1, col2 = st.columns([2, 1])
            with col1:
                st.dataframe(by_nom, use_container_width=True, hide_index=True)
            with col2:
                top10 = by_nom.head(10)
                fig = px.bar(top10, x='Сумма', y='Номенклатура', orientation='h', title='Топ-10 товаров')
                fig.update_layout(yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        by_consignee = df.groupby('Грузополучатель').agg({
            'Количество': 'sum',
            'Сумма': 'sum',
            'Паллеты': 'sum',
            'Логистика_факт': 'sum'
        }).reset_index().sort_values('Сумма', ascending=False)
        
        st.dataframe(by_consignee, use_container_width=True, hide_index=True)


def page_nomenclature():
    """Страница номенклатуры"""
    st.header("📦 Номенклатура")
    
    df = load_nomenclature()
    
    if df.empty:
        st.warning("Справочник номенклатуры пуст")
        return
    
    # Метрики
    col1, col2 = st.columns(2)
    col1.metric("Всего позиций", f"{len(df):,}")
    col2.metric("Видов номенклатуры", df['Вид номенклатуры'].nunique())
    
    # Фильтры
    col1, col2 = st.columns(2)
    with col1:
        types = ['Все'] + sorted(df['Вид номенклатуры'].dropna().unique().tolist())
        nom_type = st.selectbox("Вид номенклатуры", types)
    with col2:
        search = st.text_input("Поиск", key="nom_search")
    
    filtered = df.copy()
    if nom_type != 'Все':
        filtered = filtered[filtered['Вид номенклатуры'] == nom_type]
    if search:
        mask = (
            filtered['Наименование'].str.lower().str.contains(search.lower(), na=False) |
            filtered['Артикул'].str.lower().str.contains(search.lower(), na=False)
        )
        filtered = filtered[mask]
    
    st.dataframe(filtered, use_container_width=True, hide_index=True)
    
    # Статистика по видам
    st.subheader("Распределение по видам номенклатуры")
    by_type = df.groupby('Вид номенклатуры').size().reset_index(name='Количество')
    by_type = by_type.sort_values('Количество', ascending=False)
    
    fig = px.bar(by_type.head(15), x='Вид номенклатуры', y='Количество', title='Топ-15 видов номенклатуры')
    st.plotly_chart(fig, use_container_width=True)


def page_summary(date_from, date_to):
    """Сводная страница"""
    st.header("📈 Сводка")
    
    stats = get_db_stats()
    
    # Общие метрики
    col1, col2, col3, col4 = st.columns(4)
    
    col1.metric(
        "Закупки", 
        f"{stats['purchases']['count']:,} записей",
        f"{stats['purchases']['total_sum']:,.0f} ₽"
    )
    col2.metric(
        "Продажи",
        f"{stats['sales']['count']:,} записей", 
        f"{stats['sales']['total_sum']:,.0f} ₽"
    )
    col3.metric("Номенклатура", f"{stats['nomenclature_count']:,}")
    col4.metric("Клиенты", f"{stats['clients_count']:,}")
    
    st.divider()
    
    # Графики
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Закупки по месяцам")
        purchases = load_purchases(str(date_from), str(date_to))
        if not purchases.empty:
            purchases['Месяц'] = pd.to_datetime(purchases['Дата']).dt.to_period('M').astype(str)
            by_month = purchases.groupby('Месяц')['Сумма'].sum().reset_index()
            fig = px.bar(by_month, x='Месяц', y='Сумма', title='Закупки по месяцам')
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("💰 Продажи по месяцам")
        sales = load_sales(str(date_from), str(date_to))
        if not sales.empty:
            sales['Месяц'] = pd.to_datetime(sales['Дата']).dt.to_period('M').astype(str)
            by_month = sales.groupby('Месяц')['Сумма'].sum().reset_index()
            fig = px.bar(by_month, x='Месяц', y='Сумма', title='Продажи по месяцам')
            st.plotly_chart(fig, use_container_width=True)


# ============================================================
# MAIN
# ============================================================

def main():
    # Заголовок
    st.title("🍪 Аналитика | Кондитерская Прохорова")
    st.caption("Данные из 1С:Комплексная автоматизация 2.5")
    
    # Проверка подключения
    try:
        stats = get_db_stats()
    except Exception as e:
        st.error(f"❌ Ошибка подключения к БД: {e}")
        st.info("Проверьте настройки в secrets.toml")
        return
    
    # Боковая панель
    with st.sidebar:
        st.header("📅 Период")
        
        # Определяем границы дат
        today = datetime.now().date()
        
        # Берём минимальную и максимальную даты из статистики
        min_dates = [
            stats['purchases']['min_date'],
            stats['sales']['min_date']
        ]
        max_dates = [
            stats['purchases']['max_date'],
            stats['sales']['max_date']
        ]
        
        min_date = min([d for d in min_dates if d], default=today - timedelta(days=365))
        max_date = max([d for d in max_dates if d], default=today)
        
        if min_date > max_date:
            min_date, max_date = max_date, min_date
        
        default_from = max(min_date, max_date - timedelta(days=365))
        
        col1, col2 = st.columns(2)
        with col1:
            date_from = st.date_input("С", value=default_from, min_value=min_date, max_value=max_date)
        with col2:
            date_to = st.date_input("По", value=max_date, min_value=min_date, max_value=max_date)
        
        st.divider()
        
        if st.button("🔄 Обновить", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    
    # Навигация
    page = st.radio(
        "Раздел",
        ["📈 Сводка", "🛒 Закупки", "💰 Продажи", "📦 Номенклатура"],
        horizontal=True,
        label_visibility="collapsed"
    )
    
    st.divider()
    
    # Отображение страницы
    if page == "📈 Сводка":
        page_summary(date_from, date_to)
    elif page == "🛒 Закупки":
        page_purchases(date_from, date_to)
    elif page == "💰 Продажи":
        page_sales(date_from, date_to)
    elif page == "📦 Номенклатура":
        page_nomenclature()


if __name__ == "__main__":
    main()
