import pandas as pd
from pathlib import Path
import warnings

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore")

def html_escape(text):
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def generate_html_eda_summary(data_dir, output_file):
    data_dir = Path(data_dir)
    if not data_dir.exists():
        print(f"Directory {data_dir} does not exist.")
        return

    # only grab raw datasets for brevity, or we can grab all of them
    # Given the feedback for compact, let's grab all but we might want to let the user know
    csv_files = list(data_dir.glob("*.csv"))
    if not csv_files:
        print(f"No CSV files found in {data_dir}.")
        return

    print(f"Found {len(csv_files)} CSV files. Generating HTML EDA summary...")
    
    html = []
    html.append("<!DOCTYPE html><html><head><meta charset='utf-8'><title>Data EDA Summary</title>")
    html.append("<style>")
    html.append("body { font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Helvetica, Arial, sans-serif; margin: 40px; color: #333; line-height: 1.6; background-color: #f8f9fa; }")
    html.append(".container { max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }")
    html.append("h1, h2, h3 { color: #2c3e50; }")
    html.append("table { border-collapse: collapse; width: 100%; margin-bottom: 20px; font-size: 14px; }")
    html.append("th, td { border: 1px solid #e1e4e8; padding: 10px; text-align: left; }")
    html.append("th { background-color: #f1f8ff; font-weight: 600; color: #0366d6; }")
    html.append("tr:nth-child(even) { background-color: #fafbfc; }")
    html.append("tr:hover { background-color: #f1f8ff; }")
    html.append("summary { font-weight: bold; font-size: 1.1em; cursor: pointer; padding: 12px; background: #f6f8fa; border: 1px solid #e1e4e8; border-radius: 4px; margin-bottom: 10px; list-style: none; }")
    html.append("summary::-webkit-details-marker { display: none; }")
    html.append("summary::before { content: '▶ '; margin-right: 5px; color: #586069; }")
    html.append("details[open] > summary::before { content: '▼ '; }")
    html.append(".content { border: 1px solid #e1e4e8; border-top: none; padding: 20px; margin-top: -10px; margin-bottom: 20px; border-radius: 0 0 4px 4px; background: #fff; }")
    html.append("code { background-color: #rgba(27,31,35,0.05); padding: 0.2em 0.4em; border-radius: 3px; font-family: ui-monospace, SFMono-Regular, Consolas, 'Liberation Mono', Menlo, monospace; font-size: 85%; }")
    html.append("</style></head><body>")
    html.append("<div class='container'>")
    html.append("<h1>Exploratory Data Analysis Summary</h1>")
    html.append("<p>Interactive EDA report containing structural and statistical summaries of the scraped datasets.</p>")
    
    dataset_summaries = []
    total_rows = 0
    total_cols = 0
    
    for file_path in sorted(csv_files):
        print(f"Processing {file_path.name}...")
        try:
            df = pd.read_csv(file_path, low_memory=False)
            rows, cols = df.shape
            total_rows += rows
            total_cols += cols
            mem_usage = df.memory_usage(deep=True).sum() / 1024**2 # MB
            
            dataset_summaries.append({
                "name": file_path.name,
                "rows": rows,
                "cols": cols,
                "mem": mem_usage,
                "df": df
            })
        except Exception as e:
            print(f"Error reading {file_path.name}: {e}")
            
    html.append("<h2>Dataset Overview Overview</h2>")
    html.append(f"<p><strong>Total Datasets:</strong> {len(dataset_summaries)}<br>")
    html.append(f"<strong>Total Rows Across All Datasets:</strong> {total_rows:,}</p>")
    
    html.append("<table><tr><th>Dataset ID / Name</th><th>Rows</th><th>Columns</th><th>Memory (MB)</th><th>Duplicates</th></tr>")
    for ds in dataset_summaries:
        dupes = ds["df"].duplicated().sum() if ds["rows"] > 0 else 0
        html.append(f"<tr><td><code>{html_escape(ds['name'])}</code></td><td>{ds['rows']:,}</td><td>{ds['cols']}</td><td>{ds['mem']:.2f}</td><td>{dupes:,}</td></tr>")
    html.append("</table>")
    
    html.append("<h2>Detailed File Analysis</h2>")
    html.append("<p>Click on each dataset below to expand and view the column-level summary and statistics.</p>")
    
    for ds in dataset_summaries:
        df = ds["df"]
        html.append(f"<details><summary>{html_escape(ds['name'])} <span style='font-weight:normal; color:#586069;'>({ds['rows']:,} rows, {ds['cols']} cols)</span></summary>")
        html.append("<div class='content'>")
        
        if df.empty:
            html.append("<p><em>Dataset is empty.</em></p></div></details>")
            continue
            
        num_cols = df.select_dtypes(include=['number']).columns
        cat_cols = df.select_dtypes(exclude=['number']).columns
        
        if len(num_cols) > 0:
            html.append("<h3>Numerical Columns</h3>")
            html.append("<table><tr><th>Column</th><th>Miss %</th><th>Min</th><th>Max</th><th>Mean</th><th>Std</th><th>Zeros</th></tr>")
            for col in num_cols:
                miss_pct = df[col].isna().mean() * 100
                mini = df[col].min()
                maxi = df[col].max()
                mean = df[col].mean()
                std = df[col].std()
                zeros = (df[col] == 0).sum()
                html.append(f"<tr><td><code>{html_escape(col)}</code></td><td>{miss_pct:.1f}%</td><td>{mini:.2f}</td><td>{maxi:.2f}</td><td>{mean:.2f}</td><td>{std:.2f}</td><td>{zeros:,}</td></tr>")
            html.append("</table>")
            
        if len(cat_cols) > 0:
            html.append("<h3>Categorical Columns</h3>")
            html.append("<table><tr><th>Column</th><th>Miss %</th><th>Unique</th><th>Top Value</th><th>Top Freq</th><th>Samples</th></tr>")
            for col in cat_cols:
                miss_pct = df[col].isna().mean() * 100
                nunique = df[col].nunique()
                
                if nunique > 0:
                    top_val = str(df[col].mode()[0])
                    if len(top_val) > 25: top_val = top_val[:22] + "..."
                    top_freq = df[col].value_counts().iloc[0]
                else:
                    top_val = "N/A"
                    top_freq = 0
                    
                samples = df[col].dropna().unique()
                sample_str = ", ".join(map(str, samples[:3]))
                if len(sample_str) > 35:
                    sample_str = sample_str[:32] + "..."
                    
                html.append(f"<tr><td><code>{html_escape(col)}</code></td><td>{miss_pct:.1f}%</td><td>{nunique:,}</td><td><code>{html_escape(top_val)}</code></td><td>{top_freq:,}</td><td>{html_escape(sample_str)}</td></tr>")
            html.append("</table>")
            
        html.append("</div></details>")
        
    html.append("</div></body></html>")
    
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(html))
        
    print(f"\nDone! HTML EDA summary written to {output_file}")

if __name__ == "__main__":
    generate_html_eda_summary("data/raw", "eda_report.html")
