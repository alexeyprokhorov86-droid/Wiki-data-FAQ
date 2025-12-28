"""
📊 Аналитика | Кондитерская Прохорова
Данные из 1С:Комплексная автоматизация 2.5
"""

import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
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
    
    cur.execute("SELECT COUNT(*), MIN(doc_date), MAX(doc_date), COALESCE(SUM(sum_total), 0) FROM purchase_prices")
    row = cur.fetchone()
    stats['purchases'] = {
        'count': row[0] or 0,
        'min_date': row[1],
        'max_date': row[2],
        'total_sum': float(row[3] or 0)
    }
    
    cur.execute("SELECT COUNT(*), MIN(doc_date), MAX(doc_date), COALESCE(SUM(sum_with_vat), 0) FROM sales")
    row = cur.fetchone()
    stats['sales'] = {
        'count': row[0] or 0,
        'min_date': row[1],
        'max_date': row[2],
        'total_sum': float(row[3] or 0)
    }
    
    cur.execute("SELECT COUNT(*) FROM nomenclature WHERE is_folder = false")
    stats['nomenclature_count'] = cur.fetchone()[0] or 0
    
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
        ORDER BY doc_date DESC, nomenclature_name
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
    
    if not df.empty:
        df["Изменение_%"] = ((df["Цена_последняя"] - df["Цена_первая"]) / df["Цена_первая"] * 100).round(1)
        df["Изменение_%"] = df["Изменение_%"].replace([float('inf'), float('-inf')], 0).fillna(0)
    
    return df


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
def get_sales_by_client(date_from: str, date_to: str):
    conn = get_connection()
    query = """
        SELECT 
            client_name as "Клиент",
            SUM(quantity) as "Количество",
            SUM(sum_with_vat) as "Сумма",
            COUNT(DISTINCT doc_number) as "Документов"
        FROM sales
        WHERE doc_date BETWEEN %s AND %s AND doc_type = 'Реализация'
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
        WHERE doc_date BETWEEN %s AND %s AND doc_type = 'Реализация'
        GROUP BY nomenclature_name
        ORDER BY "Сумма" DESC
    """
    return pd.read_sql(query, conn, params=[date_from, date_to])


@st.cache_data(ttl=300)
def load_nomenclature_hierarchy():
    conn = get_connection()
    
    query_types = """
        WITH RECURSIVE type_tree AS (
            SELECT id, parent_id, name, is_folder, 0 as level, name as full_path
            FROM nomenclature_types WHERE parent_id IS NULL
            UNION ALL
            SELECT nt.id, nt.parent_id, nt.name, nt.is_folder, tt.level + 1,
                   tt.full_path || ' → ' || nt.name
            FROM nomenclature_types nt
            JOIN type_tree tt ON nt.parent_id = tt.id
        )
        SELECT id, name, full_path, level, is_folder FROM type_tree ORDER BY full_path
    """
    types_df = pd.read_sql(query_types, conn)
    
    query_nom = """
        SELECT n.id, n.name as "Наименование", n.article as "Артикул",
               n.code as "Код", n.type_id, n.weight as "Вес"
        FROM nomenclature n WHERE n.is_folder = false ORDER BY n.name
    """
    nom_df = pd.read_sql(query_nom, conn)
    
    if not nom_df.empty and not types_df.empty:
        merged = nom_df.merge(types_df[['id', 'name', 'full_path']], 
                              left_on='type_id', right_on='id', how='left', suffixes=('', '_type'))
        merged = merged.rename(columns={'name': 'Вид номенклатуры', 'full_path': 'Иерархия'})
        merged = merged[['Иерархия', 'Вид номенклатуры', 'Наименование', 'Артикул', 'Код', 'Вес']]
        return merged.sort_values(['Иерархия', 'Наименование'])
    return nom_df


@st.cache_data(ttl=300)
def get_nomenclature_types_tree():
    conn = get_connection()
    query = """
        WITH RECURSIVE type_tree AS (
            SELECT id, parent_id, name, is_folder, 0 as level, name as path
            FROM nomenclature_types WHERE parent_id IS NULL
            UNION ALL
            SELECT nt.id, nt.parent_id, nt.name, nt.is_folder, tt.level + 1,
                   tt.path || ' → ' || nt.name
            FROM nomenclature_types nt JOIN type_tree tt ON nt.parent_id = tt.id
        )
        SELECT path as "Иерархия", name as "Название", 
               CASE WHEN is_folder THEN 'Группа' ELSE 'Вид' END as "Тип", level as "Уровень"
        FROM type_tree ORDER BY path
    """
    return pd.read_sql(query, conn)


# ============================================================
# СТРАНИЦЫ
# ============================================================

def page_purchases(date_from, date_to):
    st.header("🛒 Закупки")
    st.caption("Документы: Приобретение товаров и услуг (Проведённые)")
    
    df = load_purchases(str(date_from), str(date_to))
    analysis_df = get_purchases_analysis(str(date_from), str(date_to))
    
    if df.empty:
        st.warning("Нет данных о закупках за выбранный период")
        return
    
    # ========== МЕТРИКИ ==========
    st.subheader("📈 Статистика за период")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Записей", f"{len(df):,}")
    col2.metric("Поставщиков", df['Поставщик'].nunique())
    col3.metric("Позиций", df['Номенклатура'].nunique())
    col4.metric("Сумма закупок", f"{df['Сумма'].sum():,.0f} ₽")
    
    # ========== ФИЛЬТРЫ ==========
    st.subheader("🔍 Фильтры")
    col1, col2 = st.columns(2)
    with col1:
        suppliers = ['Все'] + sorted(df['Поставщик'].dropna().unique().tolist())
        selected_supplier = st.selectbox("Поставщик", suppliers, key="purch_supplier")
    with col2:
        search_text = st.text_input("Поиск по номенклатуре", "", key="purch_search")
    
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
    tab1, tab2, tab3, tab4 = st.tabs(["📋 Все данные", "📊 Анализ цен", "📈 Динамика", "🏆 Топ изменений"])
    
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
        csv = filtered_df.to_csv(index=False).encode('utf-8-sig')
        st.download_button("📥 Скачать CSV", csv, "история_цен.csv", "text/csv")
    
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
    
    with tab3:
        st.subheader("Динамика цены")
        nomenclatures = sorted(filtered_df['Номенклатура'].unique().tolist())
        
        if nomenclatures:
            selected_nom = st.selectbox("Выберите позицию", nomenclatures, key="purch_nom")
            
            if selected_nom:
                nom_df = filtered_df[filtered_df['Номенклатура'] == selected_nom].copy()
                
                if len(nom_df) > 1:
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
                    
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Мин. цена", f"{nom_df['Цена'].min():.2f} ₽")
                    col2.metric("Макс. цена", f"{nom_df['Цена'].max():.2f} ₽")
                    first_price = nom_df.sort_values('Дата')['Цена'].iloc[0]
                    last_price = nom_df.sort_values('Дата')['Цена'].iloc[-1]
                    change = last_price - first_price
                    col3.metric("Изменение", f"{last_price:.2f} ₽", f"{change:+.2f} ₽")
                else:
                    st.info("Недостаточно данных для графика (нужно минимум 2 записи)")
    
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
                
                st.subheader("Распределение изменений цен")
                fig = px.histogram(multi, x='Изменение_%', nbins=30, title="Распределение изменений цен (%)")
                fig.add_vline(x=0, line_dash="dash", line_color="red")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Недостаточно данных для анализа (нужны позиции с 2+ поставками)")


def page_sales(date_from, date_to):
    st.header("💰 Продажи")
    st.caption("Документы: Реализация товаров и услуг + Корректировки (Проведённые)")
    
    df = load_sales(str(date_from), str(date_to))
    
    if df.empty:
        st.warning("Нет данных о продажах за выбранный период")
        return
    
    realizations = df[df['Тип'] == 'Реализация']
    corrections = df[df['Тип'] == 'Корректировка']
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Реализаций", f"{len(realizations):,}")
    col2.metric("Корректировок", f"{len(corrections):,}")
    col3.metric("Клиентов", df['Клиент'].nunique())
    col4.metric("Выручка", f"{realizations['Сумма'].sum():,.0f} ₽")
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Паллет", f"{realizations['Паллеты'].sum():,.0f}")
    col2.metric("Логистика (факт)", f"{realizations['Логистика_факт'].sum():,.0f} ₽")
    col3.metric("Сумма корректировок", f"{corrections['Сумма'].sum():,.0f} ₽")
    
    tab1, tab2, tab3, tab4 = st.tabs(["📋 Все данные", "👥 По клиентам", "📦 По номенклатуре", "🚚 По грузополучателям"])
    
    with tab1:
        col1, col2, col3 = st.columns(3)
        with col1:
            doc_type = st.selectbox("Тип документа", ['Все', 'Реализация', 'Корректировка'], key="sales_type")
        with col2:
            clients = ['Все'] + sorted(df['Клиент'].dropna().unique().tolist())
            client = st.selectbox("Клиент", clients, key="sales_client")
        with col3:
            search = st.text_input("Поиск по номенклатуре", key="sales_search")
        
        filtered = df.copy()
        if doc_type != 'Все':
            filtered = filtered[filtered['Тип'] == doc_type]
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
                if not top10.empty:
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
                if not top10.empty:
                    fig = px.bar(top10, x='Сумма', y='Номенклатура', orientation='h', title='Топ-10 товаров')
                    fig.update_layout(yaxis={'categoryorder':'total ascending'})
                    st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        by_consignee = realizations.groupby('Грузополучатель').agg({
            'Количество': 'sum', 'Сумма': 'sum', 'Паллеты': 'sum', 'Логистика_факт': 'sum'
        }).reset_index().sort_values('Сумма', ascending=False)
        st.dataframe(by_consignee, use_container_width=True, hide_index=True)


def page_nomenclature():
    st.header("📦 Номенклатура")
    
    tab1, tab2 = st.tabs(["📋 Справочник", "🌳 Структура видов"])
    
    with tab1:
        df = load_nomenclature_hierarchy()
        if df.empty:
            st.warning("Справочник номенклатуры пуст")
            return
        
        col1, col2 = st.columns(2)
        col1.metric("Всего позиций", f"{len(df):,}")
        col2.metric("Видов номенклатуры", df['Вид номенклатуры'].nunique())
        
        col1, col2 = st.columns(2)
        with col1:
            hierarchies = ['Все'] + sorted(df['Иерархия'].dropna().unique().tolist())
            hierarchy = st.selectbox("Группа / Вид номенклатуры", hierarchies)
        with col2:
            search = st.text_input("Поиск", key="nom_search")
        
        filtered = df.copy()
        if hierarchy != 'Все':
            filtered = filtered[filtered['Иерархия'] == hierarchy]
        if search:
            mask = (filtered['Наименование'].str.lower().str.contains(search.lower(), na=False) |
                    filtered['Артикул'].fillna('').str.lower().str.contains(search.lower(), na=False))
            filtered = filtered[mask]
        
        st.dataframe(filtered, use_container_width=True, hide_index=True)
        
        st.subheader("Распределение по видам")
        by_type = df.groupby('Вид номенклатуры').size().reset_index(name='Количество').sort_values('Количество', ascending=False)
        fig = px.bar(by_type.head(15), x='Вид номенклатуры', y='Количество', title='Топ-15 видов')
        st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader("Структура видов номенклатуры")
        tree = get_nomenclature_types_tree()
        if not tree.empty:
            st.dataframe(tree, use_container_width=True, hide_index=True)


def page_summary(date_from, date_to):
    st.header("📈 Сводка")
    
    stats = get_db_stats()
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Закупки", f"{stats['purchases']['count']:,}", f"{stats['purchases']['total_sum']:,.0f} ₽")
    col2.metric("Продажи", f"{stats['sales']['count']:,}", f"{stats['sales']['total_sum']:,.0f} ₽")
    col3.metric("Номенклатура", f"{stats['nomenclature_count']:,}")
    col4.metric("Клиенты", f"{stats['clients_count']:,}")
    
    st.divider()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Закупки по месяцам")
        purchases = load_purchases(str(date_from), str(date_to))
        if not purchases.empty:
            purchases['Месяц'] = pd.to_datetime(purchases['Дата']).dt.to_period('M').astype(str)
            by_month = purchases.groupby('Месяц')['Сумма'].sum().reset_index()
            fig = px.bar(by_month, x='Месяц', y='Сумма')
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("💰 Продажи по месяцам")
        sales = load_sales(str(date_from), str(date_to))
        if not sales.empty:
            real = sales[sales['Тип'] == 'Реализация']
            if not real.empty:
                real = real.copy()
                real['Месяц'] = pd.to_datetime(real['Дата']).dt.to_period('M').astype(str)
                by_month = real.groupby('Месяц')['Сумма'].sum().reset_index()
                fig = px.bar(by_month, x='Месяц', y='Сумма')
                st.plotly_chart(fig, use_container_width=True)


# ============================================================
# MAIN
# ============================================================

def main():
    st.title("🍪 Аналитика | Кондитерская Прохорова")
    st.caption("Данные из 1С:Комплексная автоматизация 2.5")
    
    try:
        stats = get_db_stats()
    except Exception as e:
        st.error(f"❌ Ошибка подключения к БД: {e}")
        return
    
    with st.sidebar:
        st.header("📅 Период")
        
        today = datetime.now().date()
        min_dates = [stats['purchases']['min_date'], stats['sales']['min_date']]
        max_dates = [stats['purchases']['max_date'], stats['sales']['max_date']]
        
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
    
    page = st.radio("Раздел", ["📈 Сводка", "🛒 Закупки", "💰 Продажи", "📦 Номенклатура"],
                    horizontal=True, label_visibility="collapsed")
    
    st.divider()
    
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






















