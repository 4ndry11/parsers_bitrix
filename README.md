# 📊 Income Statement Parser for Bitrix24

**Автоматичний парсер довідок про доходи з Azure Document Intelligence та інтеграцією в Bitrix24 CRM**

![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-Document%20Intelligence-0078D4?logo=microsoftazure&logoColor=white)
![Bitrix24](https://img.shields.io/badge/Bitrix24-CRM%20Integration-00AEEF)
![Flask](https://img.shields.io/badge/Flask-REST%20API-000000?logo=flask&logoColor=white)

---

## 🎯 Про проєкт

Розробив сервіс для автоматичного парсингу українських довідок про доходи (з податкової), який інтегрується в Bitrix24 CRM через webhook. Юристи отримують агреговані дані **в два кліки** замість 10 хвилин роботи з калькулятором.

**Бізнес-проблема:** Юристи витрачали ~10 хвилин на кожну довідку — вручну виписували суми по роках і кодах доходів, рахували підсумки на калькуляторі. При 20+ довідках на день це 3+ години рутинної роботи.

---

## 🏗️ Архітектура

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   BITRIX24      │     │   FLASK API     │     │     AZURE       │
│                 │────▶│                 │────▶│   DOCUMENT      │
│  Webhook call   │     │  /webhook/...   │     │  INTELLIGENCE   │
│  (deal_id)      │     │                 │     │  (OCR + Tables) │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         │                       │◀──────────────────────┘
         │                       │    Structured tables
         │                       ▼
         │              ┌─────────────────┐
         │              │  INCOME PARSER  │
         │              │                 │
         │              │  • Parse tables │
         │              │  • Group by year│
         │              │  • Sum amounts  │
         │              │  • Verify totals│
         │              └────────┬────────┘
         │                       │
         │◀──────────────────────┘
         │    JSON + HTML summary
         ▼
┌─────────────────┐
│   BITRIX24      │
│                 │
│  • Save JSON    │
│  • Timeline     │
│    comment      │
└─────────────────┘
```

---

## ✨ Що було реалізовано

### 📄 Парсинг довідок про доходи

- **Azure Document Intelligence** для OCR та розпізнавання таблиць
- **Інтелектуальний парсер таблиць** — автоматичне визначення структури (багаторівневі заголовки)
- **Групування по роках та кодах доходів** (101 - зарплата, 102 - премії, тощо)
- **Автоматична звірка** з рядками "Всього" для верифікації результату
- **Підтримка багатосторінкових документів** — об'єднання таблиць з різних сторінок

### 🔗 Інтеграція з Bitrix24

- **Webhook endpoint** — виклик одним кліком з картки угоди
- **OAuth 2.0 авторизація** з автоматичним оновленням токенів
- **Завантаження файлів** напряму з полів угоди
- **Збереження результатів** в JSON-поле + HTML-коментар в таймлайн

### 📊 Вивід результатів

Результат зберігається в двох форматах:

**JSON (для автоматизації):**
```json
{
  "2023": {
    "101": {"name": "Заробітна плата", "amount": 156000.00},
    "126": {"name": "Дохід від продажу", "amount": 45000.00},
    "_total": 201000.00
  },
  "2024": {
    "101": {"name": "Заробітна плата", "amount": 180000.00},
    "_total": 180000.00
  }
}
```

**HTML (в таймлайн Bitrix24):**
```
📊 АНАЛІЗ ДОВІДКИ ПРО ДОХОДИ

💰 Загальна сума: 381000.00 грн
📅 Періоди: 2023, 2024
✅ Звірка з 'Всього'

─────────
📆 2023 рік • Всього: 201000.00 грн
  ✅ Звірка: 201000.00 грн

🔹 Код 101: Заробітна плата
   Сума: 156000.00 грн

🔹 Код 126: Дохід від продажу
   Сума: 45000.00 грн
```

---

## 📈 Бізнес-результати

| Метрика | До | Після |
|---------|-----|-------|
| Час обробки 1 довідки | ~10 хвилин | ~5 секунд |
| Людські помилки | ~5% документів | 0% (автоверифікація) |
| Обробка за день | 20-30 довідок | Без обмежень |
| Час юриста на рутину | 3+ години/день | ~0 |

**ROI:** Економія ~60 годин/місяць робочого часу юристів

---

## 🛠️ Технологічний стек

| Компонент | Технологія | Призначення |
|-----------|------------|-------------|
| **OCR Engine** | Azure Document Intelligence | Розпізнавання тексту та таблиць |
| **API Framework** | Flask + CORS | REST API для webhooks |
| **CRM** | Bitrix24 REST API | Інтеграція з робочим процесом |
| **Auth** | OAuth 2.0 | Авторизація Bitrix24 |
| **Hosting** | Render.com | Cloud deployment |

---

## 📁 Структура проєкту

```
parsers_bitrix/
├── app.py                      # Flask app, webhook endpoints
├── core/
│   ├── azure_client.py         # Azure Document Intelligence client
│   └── bitrix_client.py        # Bitrix24 API client (OAuth, files, fields)
├── parsers/
│   ├── base_parser.py          # Base parser class
│   └── income_statement_parser.py  # Income statement specific logic
├── utils/
│   └── logger.py               # Logging configuration
├── requirements.txt
├── runtime.txt
└── Procfile                    # Render deployment
```

---

## 🔄 Як це працює

### 1. Виклик з Bitrix24
Юрист натискає кнопку в картці угоди → webhook `POST /webhook/process-income-statement?deal_id=123`

### 2. Завантаження документа
```python
# Bitrix24 OAuth + download
file_content = bitrix_client.download_file_from_field(deal_id, "UF_CRM_1765540040027")
```

### 3. OCR через Azure
```python
# Azure Document Intelligence
azure_result = azure_client.analyze_document(file_content, model_id="prebuilt-layout")
```

### 4. Парсинг таблиць
```python
# Інтелектуальний парсер
parser = IncomeStatementParser()
parsed_data = parser.parse(azure_result)

# Результат включає:
# - Групування по роках
# - Суми по кодах доходів
# - Верифікацію з "Всього"
```

### 5. Збереження в Bitrix24
```python
# JSON в поле угоди
bitrix_client.update_deal_field(deal_id, result_field, json_output)

# HTML в таймлайн
bitrix_client.add_timeline_comment(deal_id, html_output)
```

---

## 🧠 Особливості парсера

### Автоматичне визначення структури таблиці

Довідки про доходи мають складну структуру з багаторівневими заголовками. Парсер автоматично:

1. **Знаходить рядок-індикатор** (де col[0]='1', col[4]='4', col[7]='7', col[13]='13')
2. **Відкидає всі рядки вище** (заголовки)
3. **Застосовує ті ж колонки** до наступних таблиць (багатосторінковий документ)

```python
def _find_index_row_and_cols(self, rows):
    """Знайти рядок з індексами колонок"""
    for row_idx in sorted(rows.keys()):
        if rows[row_idx].get(0, "").strip() != "1":
            continue
        # col[4] = рік, col[7] = сума, col[13] = код
        ...
```

### Верифікація з "Всього"

Парсер порівнює розраховані суми з рядками "Всього" в документі:

```python
verification = {
    "matches": [{"year": "2023", "our_total": 201000.0, "expected": 201000.0}],
    "mismatches": [],
    "total_match": True
}
```

---

## 👤 Автор

**Andrii** — Data Analyst / Automation Engineer

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?logo=linkedin)](https://linkedin.com/in/prodby4ndry)
[![Telegram](https://img.shields.io/badge/Telegram-Contact-26A5E4?logo=telegram)](https://t.me/prodby4ndry)

---

*Проєкт демонструє інтеграцію Azure AI сервісів з CRM-системою для автоматизації рутинних задач з вимірюваним бізнес-ефектом*
