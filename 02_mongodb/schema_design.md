# MongoDB Schema Design (Part 1)

## Why MongoDB for these entities?

MongoDB’s document model fits **nested, frequently read-together** data:

| Collection | Why MongoDB |
|---|---|
| `products` | Catalog docs with embedded `price_history` and category keys |
| `users` | Profiles + optional embedded purchase summary for fast segmentation |
| `transactions` | Orders with embedded `items[]` — one round-trip for an order |
| `categories` | Hierarchy with embedded `subcategories[]` |
| `sessions` *(optional sample)* | Nested `page_views[]` for journey analysis (bulk sessions go to HBase) |

## Collections

### 1. `categories`
```json
{
  "category_id": "cat_007",
  "name": "Johnson-Williams",
  "subcategories": [
    {"subcategory_id": "sub_007_00", "name": "...", "profit_margin": 0.27}
  ]
}
```
**Indexes:** `category_id` (unique)

### 2. `products`
```json
{
  "product_id": "prod_00123",
  "name": "...",
  "category_id": "cat_007",
  "subcategory_id": "sub_007_01",
  "base_price": 129.99,
  "current_stock": 47,
  "is_active": true,
  "price_history": [{"price": 149.99, "date": "..."}],
  "creation_date": "..."
}
```
**Indexes:** `product_id` (unique), `category_id`, `is_active`

### 3. `users`
```json
{
  "user_id": "user_000042",
  "geo_data": {"city": "...", "state": "WY", "country": "US"},
  "registration_date": "...",
  "last_active": "...",
  "purchase_summary": {
    "order_count": 3,
    "lifetime_spend": 412.50,
    "avg_order_value": 137.50,
    "last_purchase": "..."
  }
}
```
`purchase_summary` is denormalized at load time for segmentation queries.

**Indexes:** `user_id` (unique), `geo_data.country`, `purchase_summary.order_count`

### 4. `transactions`
```json
{
  "transaction_id": "txn_...",
  "session_id": "sess_...",
  "user_id": "user_...",
  "timestamp": "...",
  "items": [{"product_id": "...", "quantity": 2, "unit_price": 129.99, "subtotal": 259.98}],
  "subtotal": 259.98,
  "discount": 25.99,
  "total": 233.99,
  "payment_method": "credit_card",
  "status": "completed"
}
```
**Indexes:** `transaction_id` (unique), `user_id`, `timestamp`, `items.product_id`

## MongoDB vs HBase placement

| Data | Store | Reason |
|---|---|---|
| Products, users, transactions, categories | **MongoDB** | Nested docs, aggregations, flexible filters |
| High-volume page-view / session streams | **HBase** | Time-series row keys, sparse columns, range scans |
| Cross-entity heavy joins / ML-style analytics | **Spark** | Distributed transforms over all JSON files |

## Aggregation pipelines implemented

1. **Revenue by category** — unwind transaction items → join products → group by category  
2. **User segmentation by purchase frequency** — bucket users using `purchase_summary`  
3. **Top-selling products** — unwind items → group by product → sort
