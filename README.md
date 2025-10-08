# Retail-ETL-Pipeline

## 🚨 Business Problem
Our retail company's sales data is in crisis, preventing accurate business reporting and analytics.

### Critical Data Quality Issues Discovered:
- **84,676 duplicate order records** (29.6% of total data)
- **Calculation errors** where quantity × price ≠ reported value
- **Unnormalized schema** with 30+ redundant columns
- **Complex data structure** hindering analyst productivity

### Business Impact:
- 💰 **Revenue Miscalculations**: Inaccurate financial reports
- 📊 **Inflated Metrics**: 30% duplicate data skewing KPIs  
- ⏱️ **Slow Analytics**: Complex schema causing query delays
- 🚫 **Lost Trust**: Management can't rely on data for decisions

## 💡 Solution
Built an automated ETL pipeline to:
1. **Extract** raw data from Kaggle retail dataset (286,392 records)
2. **Transform** by removing duplicates, fixing calculations, normalizing schema
3. **Load** clean data into PostgreSQL for reliable analytics

## 📈 Expected Results
- ✅ **29.6% data reduction** by eliminating duplicates  
- ✅ **100% calculation accuracy** with automated validation
- ✅ **3-table normalized schema** for efficient querying
- ✅ **Fast, trustworthy reporting** enabling data-driven decisions
