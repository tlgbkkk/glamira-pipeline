import re
import json

def parse_react_data(html_text, pid, original_url):
    pattern = re.compile(r'var\s+react_data\s*=\s*(\{.*?\});', re.DOTALL)
    match = pattern.search(html_text)

    if match:
        try:
            data = json.loads(match.group(1))
            product_price_info = data.get("product_price", {})
            return {
                "product_id": pid,
                "name": data.get("name"),
                "sku": data.get("sku"),
                "category": data.get("category_name"),
                "collection": data.get("collection"),
                "type": data.get("product_type"),
                "base_price": data.get("price"),
                "full_price": product_price_info.get("full_price"),
                "sale_price": product_price_info.get("sale_price"),
                "currency": product_price_info.get("currencyCode", "USD"),
                "gold_weight": data.get("gold_weight"),
                "url": original_url,
                "status": "Success"
            }
        except json.JSONDecodeError:
            return {"product_id": pid, "status": "JSON Decode Error", "url": original_url}
    return {"product_id": pid, "status": "Not found", "url": original_url}