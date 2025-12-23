"""
Анализ истории закупочных цен из 1С:КА 2.5
Streamlit-приложение

Запуск: streamlit run price_history_app.py
"""

import streamlit as st
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Optional
import time


# ============================================================
# НАСТРОЙКИ СТРАНИЦЫ
# ============================================================

st.set_page_config(
    page_title="История цен | 1С",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ============================================================
# КЛАСС ДЛЯ РАБОТЫ С 1С
# ============================================================

class PriceHistoryExtractor:
    """Извлекает историю закупочных цен из 1С через OData"""
    
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.auth = HTTPBasicAuth(username, password)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update({
            'Accept': 'application/json',
            'Content-Type': 'application/json; charset=utf-8'
        })
        self._contractors_cache: dict[str, str] = {}
        self._nomenclature_cache: dict[str, str] = {}
    
    def test_connection(self) -> tuple[bool, str]:
        """Проверка подключения"""
        try:
            response = self.session.get(
                f"{self.base_url}/Catalog_Контрагенты?$top=1&$format=json",
                timeout=30
            )
            if response.status_code == 200:
                return True, "Подключение успешно"
            elif response.status_code == 401:
                return False, "Ошибка авторизации. Проверьте логин/пароль"
            else:
                return False, f"Ошибка HTTP {response.status_code}"
        except requests.exceptions.ConnectionError:
            return False, f"Не удалось подключиться к {self.base_url}"
        except requests.exceptions.Timeout:
            return False, "Таймаут подключения"
    
    def _get_contractor_name(self, contractor_key: str) -> str:
        if not contractor_key or contractor_key == "00000000-0000-0000-0000-000000000000":
            return "Не указан"
        
        if contractor_key in self._contractors_cache:
            return self._contractors_cache[contractor_key]
        
        try:
            response = self.session.get(
                f"{self.base_url}/Catalog_Контрагенты(guid'{contractor_key}')?$format=json",
                timeout=30
            )
            if response.status_code == 200:
                data = response.json()
                name = data.get('Description', '') or data.get('НаименованиеПолное', '') or 'Без названия'
                self._contractors_cache[contractor_key] = name
                return name
        except:
            pass
        return "Неизвестный"
    
    def _get_nomenclature_name(self, nomenclature_key: str) -> str:
        if not nomenclature_key or nomenclature_key == "00000000-0000-0000-0000-000000000000":
            return "Не указана"
        
        if nomenclature_key in self._nomenclature_cache:
            return self._nomenclature_cache[nomenclature_key]
        
        try:
            response = self.session.get(
                f"{self.base_url}/Catalog_Номенклатура(guid'{nomenclature_key}')?$format=json",
                timeout=30
            )
            if response.status_code == 200:
                data = response.json()
                name = data.get('Description', '') or data.get('НаименованиеПолное', '') or 'Без названия'
                self._nomenclature_cache[nomenclature_key] = name
                return name
        except:
            pass
        return "Неизвестная"
    
    def get_purchases(
        self,
        date_from: datetime,
        date_to: datetime,
        progress_callback=None
    ) -> list[dict]:
        """Получает документы приобретения за период"""
        
        date_from_str = date_from.strftime("%Y-%m-%dT00:00:00")
        date_to_str = date_to.strftime("%Y-%m-%dT23:59:59")
        
        filter_query = f"Date ge datetime'{date_from_str}' and Date le datetime'{date_to_str}' and Posted eq true"
        
        params = {
            "$filter": filter_query,
            "$orderby": "Date desc",
            "$format": "json"
        }
        
        url = f"{self.base_url}/Document_ПриобретениеТоваровУслуг"
        
        all_documents = []
        page = 1
        
        while url:
            try:
                response = self.session.get(url, params=params, timeout=60)
                
                if response.status_code != 200:
                    break
                
                data = response.json()
                documents = data.get('value', [])
                all_documents.extend(documents)
                
                if progress_callback:
                    progress_callback(f"Загружено {len(all_documents)} документов...")
                
                next_link = data.get('odata.nextLink') or data.get('@odata.nextLink')
                if next_link:
                    url = next_link
                    params = {}
                    page += 1
                    time.sleep(0.3)
                else:
                    url = None
                    
            except Exception as e:
                if progress_callback:
                    progress_callback(f"Ошибка: {e}")
                break
        
        return all_documents
    
    def extract_price_history(
        self,
        date_from: datetime,
        date_to: datetime,
        progress_callback=None
    ) -> pd.DataFrame:
        """Извлекает историю цен"""
        
        documents = self.get_purchases(date_from, date_to, progress_callback)
        
        if not documents:
            return pd.DataFrame()
        
        price_records = []
        total = len(documents)
        
        for i, doc in enumerate(documents):
            doc_date = doc.get('Date', '')[:10]
            doc_number = doc.get('Number', '').strip()
            contractor_key = doc.get('Контрагент_Key', '')
            contractor_name = self._get_contractor_name(contractor_key)
            
            items = doc.get('Товары', [])
            
            for item in items:
                nomenclature_key = item.get('Номенклатура_Key', '')
                nomenclature_name = self._get_nomenclature_name(nomenclature_key)
                
                quantity = item.get('Количество', 0) or 0
                price = item.get('Цена', 0) or 0
                summa = item.get('Сумма', 0) or 0
                summa_nds = item.get('СуммаНДС', 0) or 0
                summa_s_nds = item.get('СуммаСНДС', 0) or 0
                
                if price == 0 and quantity > 0:
                    price = summa / quantity
                
                price_with_nds = summa_s_nds / quantity if quantity > 0 else price
                
                price_records.append({
                    'Дата': doc_date,
                    'Номер': doc_number,
                    'Поставщик': contractor_name,
                    'Номенклатура': nomenclature_name,
                    'Количество': round(quantity, 3),
                    'Цена': round(price_with_nds, 2),
                    'Сумма': round(summa_s_nds, 2),
                })
            
            if progress_callback and (i + 1) % 20 == 0:
                progress_callback(f"Обработано {i + 1}/{total} документов...")
        
        df = pd.DataFrame(price_records)
        
        if not df.empty:
            df['Дата'] = pd.to_datetime(df['Дата'])
            df = df.sort_values(['Номенклатура', 'Дата'])
        
        return df


# ============================================================
# ФУНКЦИИ АНАЛИЗА
# ============================================================

def analyze_prices(df: pd.DataFrame) -> pd.DataFrame:
    """Анализ цен по номенклатуре и поставщикам"""
    if df.empty:
        return pd.DataFrame()
    
    analysis = df.groupby(['Номенклатура', 'Поставщик']).agg({
        'Цена': ['min', 'max', 'mean', 'first', 'last', 'count'],
        'Количество': 'sum',
        'Сумма': 'sum',
        'Дата': ['min', 'max']
    }).round(2)
    
    analysis.columns = [
        'Цена_мин', 'Цена_макс', 'Цена_средняя',
        'Цена_первая', 'Цена_последняя', 'Поставок',
        'Всего_кол_во', 'Всего_сумма',
        'Первая_дата', 'Последняя_дата'
    ]
    
    analysis['Изменение_%'] = (
        (analysis['Цена_последняя'] - analysis['Цена_первая'])
        / analysis['Цена_первая'] * 100
    ).round(1)
    
    analysis['Изменение_%'] = analysis['Изменение_%'].replace([float('inf'), float('-inf')], 0)
    
    return analysis.reset_index()


def get_price_dynamics(df: pd.DataFrame, nomenclature: str) -> pd.DataFrame:
    """Динамика цены для конкретной номенклатуры"""
    filtered = df[df['Номенклатура'] == nomenclature].copy()
    return filtered.sort_values('Дата')


# ============================================================
# ИНТЕРФЕЙС STREAMLIT
# ============================================================

def main():
    st.title("📊 История закупочных цен")
    st.caption("Анализ данных из 1С:Комплексная автоматизация 2.5")
    
    # ========== БОКОВАЯ ПАНЕЛЬ ==========
    with st.sidebar:
        st.header("⚙️ Подключение к 1С")
        
        # Настройки подключения
        base_url = st.text_input(
            "URL OData",
            value="http://185.126.95.33:81/NB_KA/odata/standard.odata",
            help="URL публикации OData вашей базы 1С"
        )
        
        col1, col2 = st.columns(2)
        with col1:
            username = st.text_input("Логин", value="")
        with col2:
            password = st.text_input("Пароль", type="password", value="")
        
        st.divider()
        
        st.header("📅 Период")
        
        col1, col2 = st.columns(2)
        with col1:
            date_from = st.date_input(
                "С",
                value=datetime.now() - timedelta(days=365),
                format="DD.MM.YYYY"
            )
        with col2:
            date_to = st.date_input(
                "По",
                value=datetime.now(),
                format="DD.MM.YYYY"
            )
        
        st.divider()
        
        # Кнопка загрузки
        load_button = st.button("🔄 Загрузить данные", type="primary", use_container_width=True)
    
    # ========== ОСНОВНАЯ ОБЛАСТЬ ==========
    
    # Проверяем, есть ли данные в сессии
    if 'prices_df' not in st.session_state:
        st.session_state.prices_df = None
        st.session_state.analysis_df = None
    
    # Загрузка данных
    if load_button:
        if not username or not password:
            st.error("Введите логин и пароль")
            return
        
        # Создаём экстрактор
        extractor = PriceHistoryExtractor(base_url, username, password)
        
        # Проверяем подключение
        with st.spinner("Проверка подключения..."):
            success, message = extractor.test_connection()
        
        if not success:
            st.error(f"❌ {message}")
            return
        
        st.success("✅ Подключение установлено")
        
        # Загружаем данные
        progress_text = st.empty()
        progress_bar = st.progress(0)
        
        def update_progress(text):
            progress_text.text(text)
        
        with st.spinner("Загрузка данных из 1С..."):
            df = extractor.extract_price_history(
                date_from=datetime.combine(date_from, datetime.min.time()),
                date_to=datetime.combine(date_to, datetime.max.time()),
                progress_callback=update_progress
            )
        
        progress_bar.empty()
        progress_text.empty()
        
        if df.empty:
            st.warning("Нет данных за выбранный период")
            return
        
        st.session_state.prices_df = df
        st.session_state.analysis_df = analyze_prices(df)
        st.success(f"✅ Загружено {len(df)} записей")
    
    # Отображаем данные если есть
    if st.session_state.prices_df is not None:
        df = st.session_state.prices_df
        analysis_df = st.session_state.analysis_df
        
        # ========== МЕТРИКИ ==========
        st.header("📈 Общая статистика")
        
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
                            change = nom_df['Цена'].iloc[-1] - nom_df['Цена'].iloc[0]
                            st.metric(
                                "Изменение",
                                f"{nom_df['Цена'].iloc[-1]:.2f} ₽",
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
    
    else:
        # Стартовый экран
        st.info("👈 Введите данные для подключения и нажмите **Загрузить данные**")
        
        with st.expander("ℹ️ Как настроить"):
            st.markdown("""
            ### Настройка OData в 1С
            
            1. Откройте 1С:Предприятие
            2. Перейдите: **Администрирование → Публикация на веб-сервере**
            3. Включите **Стандартный интерфейс OData**
            4. Добавьте в состав OData:
               - `Document_ПриобретениеТоваровУслуг`
               - `Catalog_Контрагенты`
               - `Catalog_Номенклатура`
            
            ### Создание пользователя для API
            
            Рекомендуется создать отдельного пользователя с правами только на чтение.
            """)


if __name__ == "__main__":
    main()
