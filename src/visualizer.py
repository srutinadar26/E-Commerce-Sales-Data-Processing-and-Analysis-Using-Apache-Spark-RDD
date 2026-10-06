"""
src/visualizer.py
-----------------
Generate analytical charts using Pandas + Matplotlib.
Charts are saved to data/output/ as PNG files.

NOTE: This module does NOT use Spark – it reads the CSV output
files produced by rdd_analytics.py and visualises them with
standard Python data-science libraries.
"""

from __future__ import annotations
import os
import pandas as pd
import matplotlib
matplotlib.use("Agg")   # headless backend – no display required
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

from src.utils.logger import get_logger

logger = get_logger(__name__)

# -- Consistent visual style ---------------------------------------------------
plt.rcParams.update({
    "figure.facecolor": "#1e1e2e",
    "axes.facecolor":   "#1e1e2e",
    "axes.edgecolor":   "#4e4e7e",
    "axes.labelcolor":  "#cdd6f4",
    "xtick.color":      "#cdd6f4",
    "ytick.color":      "#cdd6f4",
    "text.color":       "#cdd6f4",
    "grid.color":       "#313244",
    "grid.linestyle":   "--",
    "grid.alpha":       0.6,
    "font.family":      "DejaVu Sans",
    "font.size":        11,
})

ACCENT_COLORS = [
    "#89b4fa", "#a6e3a1", "#fab387", "#f38ba8",
    "#94e2d5", "#cba6f7", "#f9e2af", "#89dceb",
]


def _savefig(fig, path: str) -> None:
    """Save figure and close it."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Chart saved --> %s", path)


# -----------------------------------------------------------------------------
# Chart 1 – Revenue by category (horizontal bar)
# -----------------------------------------------------------------------------

def plot_revenue_by_category(output_dir: str) -> None:
    csv_path = os.path.join(output_dir, "revenue_by_category.csv")
    if not os.path.exists(csv_path):
        logger.warning("revenue_by_category.csv not found; skipping chart.")
        return

    df = pd.read_csv(csv_path).sort_values("total_revenue", ascending=True)

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(df["category"], df["total_revenue"],
                   color=ACCENT_COLORS[:len(df)])
    ax.set_title("Revenue by Category", fontsize=16, fontweight="bold", pad=14)
    ax.set_xlabel("Total Revenue ($)")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.grid(axis="x")
    for bar in bars:
        w = bar.get_width()
        ax.text(w + 500, bar.get_y() + bar.get_height() / 2,
                f"${w:,.0f}", va="center", fontsize=9)
    fig.tight_layout()
    _savefig(fig, os.path.join(output_dir, "chart_revenue_by_category.png"))


# -----------------------------------------------------------------------------
# Chart 2 – Top 10 products (vertical bar)
# -----------------------------------------------------------------------------

def plot_top_products(output_dir: str) -> None:
    csv_path = os.path.join(output_dir, "top_10_products.csv")
    if not os.path.exists(csv_path):
        logger.warning("top_10_products.csv not found; skipping chart.")
        return

    df = pd.read_csv(csv_path)

    fig, ax = plt.subplots(figsize=(12, 6))
    bars = ax.bar(df["product_name"], df["total_revenue"],
                  color=ACCENT_COLORS[0], edgecolor="#313244")
    ax.set_title("Top 10 Products by Revenue", fontsize=16, fontweight="bold", pad=14)
    ax.set_ylabel("Total Revenue ($)")
    ax.set_xticklabels(df["product_name"], rotation=40, ha="right", fontsize=9)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda y, _: f"${y:,.0f}"))
    ax.grid(axis="y")
    for bar in bars:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, h + 100,
                f"${h:,.0f}", ha="center", va="bottom", fontsize=8)
    fig.tight_layout()
    _savefig(fig, os.path.join(output_dir, "chart_top_10_products.png"))


# -----------------------------------------------------------------------------
# Chart 3 – Monthly revenue (line chart)
# -----------------------------------------------------------------------------

def plot_monthly_revenue(output_dir: str) -> None:
    csv_path = os.path.join(output_dir, "monthly_revenue.csv")
    if not os.path.exists(csv_path):
        logger.warning("monthly_revenue.csv not found; skipping chart.")
        return

    df = pd.read_csv(csv_path).sort_values("year_month")

    fig, ax1 = plt.subplots(figsize=(14, 6))
    ax2 = ax1.twinx()

    ax1.plot(df["year_month"], df["total_revenue"],
             color=ACCENT_COLORS[0], marker="o", linewidth=2.5, label="Revenue")
    ax2.bar(df["year_month"], df["transaction_count"],
            color=ACCENT_COLORS[1], alpha=0.4, label="Transactions")

    ax1.set_title("Monthly Revenue & Transaction Count", fontsize=16, fontweight="bold", pad=14)
    ax1.set_xlabel("Month")
    ax1.set_ylabel("Revenue ($)", color=ACCENT_COLORS[0])
    ax2.set_ylabel("Transactions", color=ACCENT_COLORS[1])
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda y, _: f"${y:,.0f}"))
    ax1.set_xticklabels(df["year_month"], rotation=45, ha="right", fontsize=9)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")
    ax1.grid(axis="y")
    fig.tight_layout()
    _savefig(fig, os.path.join(output_dir, "chart_monthly_revenue.png"))


# -----------------------------------------------------------------------------
# Chart 4 – Payment method distribution (pie chart)
# -----------------------------------------------------------------------------

def plot_payment_distribution(output_dir: str) -> None:
    csv_path = os.path.join(output_dir, "payment_method_analysis.csv")
    if not os.path.exists(csv_path):
        logger.warning("payment_method_analysis.csv not found; skipping chart.")
        return

    df = pd.read_csv(csv_path)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 7))

    # Pie: transaction count
    ax1.pie(df["transaction_count"], labels=df["payment_method"],
            colors=ACCENT_COLORS[:len(df)], autopct="%1.1f%%",
            startangle=140, wedgeprops={"edgecolor": "#313244"})
    ax1.set_title("Transaction Count by Payment Method", fontsize=13, fontweight="bold")

    # Bar: revenue
    bars = ax2.bar(df["payment_method"], df["total_revenue"],
                   color=ACCENT_COLORS[:len(df)], edgecolor="#313244")
    ax2.set_title("Revenue by Payment Method", fontsize=13, fontweight="bold")
    ax2.set_ylabel("Total Revenue ($)")
    ax2.set_xticklabels(df["payment_method"], rotation=35, ha="right")
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda y, _: f"${y:,.0f}"))
    ax2.grid(axis="y")

    fig.suptitle("Payment Method Analysis", fontsize=16, fontweight="bold", y=1.01)
    fig.tight_layout()
    _savefig(fig, os.path.join(output_dir, "chart_payment_analysis.png"))


# -----------------------------------------------------------------------------
# Chart 5 – Order status (donut chart)
# -----------------------------------------------------------------------------

def plot_order_status(output_dir: str) -> None:
    csv_path = os.path.join(output_dir, "order_status_analysis.csv")
    if not os.path.exists(csv_path):
        logger.warning("order_status_analysis.csv not found; skipping chart.")
        return

    df = pd.read_csv(csv_path)

    fig, ax = plt.subplots(figsize=(9, 9))
    wedges, texts, autotexts = ax.pie(
        df["transaction_count"],
        labels=df["order_status"],
        colors=ACCENT_COLORS[:len(df)],
        autopct="%1.1f%%",
        startangle=90,
        pctdistance=0.82,
        wedgeprops={"width": 0.55, "edgecolor": "#313244"},
    )
    for text in autotexts:
        text.set_fontsize(10)
    ax.set_title("Order Status Distribution", fontsize=16, fontweight="bold", pad=20)
    fig.tight_layout()
    _savefig(fig, os.path.join(output_dir, "chart_order_status.png"))


# -----------------------------------------------------------------------------
# Chart 6 – Top 10 states by revenue
# -----------------------------------------------------------------------------

def plot_top_states(output_dir: str, n: int = 10) -> None:
    csv_path = os.path.join(output_dir, "revenue_by_state.csv")
    if not os.path.exists(csv_path):
        logger.warning("revenue_by_state.csv not found; skipping chart.")
        return

    df = pd.read_csv(csv_path).head(n).sort_values("total_revenue")

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(df["state"], df["total_revenue"],
                   color=ACCENT_COLORS[2], edgecolor="#313244")
    ax.set_title(f"Top {n} States by Revenue", fontsize=16, fontweight="bold", pad=14)
    ax.set_xlabel("Total Revenue ($)")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.grid(axis="x")
    for bar in bars:
        w = bar.get_width()
        ax.text(w + 100, bar.get_y() + bar.get_height() / 2,
                f"${w:,.0f}", va="center", fontsize=9)
    fig.tight_layout()
    _savefig(fig, os.path.join(output_dir, "chart_top_states.png"))


# -----------------------------------------------------------------------------
# Orchestrator
# -----------------------------------------------------------------------------

def generate_all_charts(output_dir: str) -> None:
    """Generate every chart from the analytics CSV outputs."""
    logger.info("Generating charts in: %s", output_dir)
    plot_revenue_by_category(output_dir)
    plot_top_products(output_dir)
    plot_monthly_revenue(output_dir)
    plot_payment_distribution(output_dir)
    plot_order_status(output_dir)
    plot_top_states(output_dir)
    logger.info("All charts generated.")
