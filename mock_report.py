import csv
import json

data = []
with open('data/raw/ecommerce_sales.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        data.append(row)

# Clean data
valid_data = [r for r in data if r['date'] and r['quantity'] and r['unit_price']]

total_count = len(valid_data)
total_revenue = sum(float(r['quantity']) * float(r['unit_price']) * (1.0 - float(r.get('discount') or 0.0)) for r in valid_data)

# Categories
rev_by_cat = {}
for r in valid_data:
    rev = float(r['quantity']) * float(r['unit_price']) * (1.0 - float(r.get('discount') or 0.0))
    rev_by_cat[r['category']] = rev_by_cat.get(r['category'], 0.0) + rev

top_categories = sorted(rev_by_cat.items(), key=lambda x: x[1], reverse=True)[:5]

# State
rev_by_state = {}
for r in valid_data:
    rev = float(r['quantity']) * float(r['unit_price']) * (1.0 - float(r.get('discount') or 0.0))
    rev_by_state[r['state']] = rev_by_state.get(r['state'], 0.0) + rev

top_states = sorted(rev_by_state.items(), key=lambda x: x[1], reverse=True)[:5]

report_content = f"""# E-Commerce Sales Analysis Report

**Generated on:** 2026-10-06

## 1. Executive Summary
- **Total Valid Transactions:** {total_count:,}
- **Total Revenue:** ${total_revenue:,.2f}

## 2. Top Performing Categories
"""
for cat, rev in top_categories:
    report_content += f"- **{cat}**: ${rev:,.2f}\n"

report_content += "\n## 3. Top Performing States\n"
for state, rev in top_states:
    report_content += f"- **{state}**: ${rev:,.2f}\n"

with open('reports/analysis_report.md', 'w') as f:
    f.write(report_content)

print('Report generated successfully!')
