"""Fit a linear regression on factors_analysis/merged_results.csv.

Features:
- GPS.hdop
- GPS.vtec
- weather.Lufttemperatur.value
- weather.Relativ Luftfuktighet.value
- weather.Nederbördsmängd.value
- weather.Lufttryck reducerat havsytans nivå.value

Response:
- imprecision.imprecision_val
"""


from itertools import combinations
import numpy as np
import polars as pl
import os
import math
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression

sns.set_theme(style="whitegrid")

FEATURE_NAMES = [
    "GPS.hdop",
    "GPS.vtec",
    "weather.Lufttemperatur.value",
    "weather.Relativ Luftfuktighet.value",
    "weather.Nederbördsmängd.value",
    "weather.Lufttryck reducerat havsytans nivå.value",
]
RESPONSE_NAME = "imprecision.imprecision_val"
CSV_PATH = "merged_results.csv"
EXPORT_TXT_RESULTS = True
NORMALISE_FEATURES = False


# Since some columns have unusual names, this function helps finding them if needed
def find_column(columns: list[str], target: str) -> str:
    def normalize_column_name(name: str) -> str:
        return "".join(ch for ch in name.lower() if ch.isalnum())
    if target in columns:
        return target
    normalized_target = normalize_column_name(target)
    for column in columns:
        if normalize_column_name(column) == normalized_target:
            return column
    for column in columns:
        if normalized_target in normalize_column_name(column):
            return column
    raise KeyError(f"Could not find column for target '{target}' in CSV columns")


def fit_linear_regression(X: np.ndarray, y: np.ndarray) -> tuple[np.ndarray, np.ndarray, dict[str, float]]:
    X_design = np.column_stack([np.ones(X.shape[0], dtype=float), X])
    coefficients, residuals, rank, singular_values = np.linalg.lstsq(X_design, y, rcond=None)
    y_pred = X_design.dot(coefficients)

    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r2 = 1.0 - ss_res / ss_tot if ss_tot != 0.0 else float("nan")
    rmse = float(np.sqrt(ss_res / y.shape[0]))
    mae = float(np.mean(np.abs(y - y_pred)))

    return coefficients, y_pred, {
        "r2": r2,
        "rmse": rmse,
        "mae": mae,
        "observations": float(y.shape[0]),
        "rank": float(rank),
        "singular_values_0": float(singular_values[0]) if singular_values.size else float("nan"),
    }


def normalize_feature_matrix(X: np.ndarray) -> np.ndarray:
    means = np.nanmean(X, axis=0)
    stds = np.nanstd(X, axis=0, ddof=0)
    stds[stds == 0.0] = 1.0
    return (X - means) / stds


def evaluate_combinations(
    df: pl.DataFrame,
    feature_cols: list[str],
    response_col: str,
    normalize_features: bool = False,
) -> list[dict[str, object]]:
    results = []
    for k in range(1, len(feature_cols) + 1):
        for subset in combinations(feature_cols, k):
            subset_df = df.select(list(subset) + [response_col]).drop_nans()
            if subset_df.height == 0:
                continue

            X = subset_df.select(list(subset)).with_columns([pl.col(col).cast(pl.Float64) for col in subset]).to_numpy()
            if normalize_features:
                X = normalize_feature_matrix(X)
                pl.DataFrame(X).write_csv("../output/normalized_features_test.csv")
            y = subset_df.select(response_col).with_columns(pl.col(response_col).cast(pl.Float64)).to_numpy().flatten()
            coefficients, y_pred, metrics = fit_linear_regression(X, y)
            results.append({
                "features": list(subset),
                "coefficients": coefficients,
                "metrics": metrics,
            })
    return results

def print_result(label: str, results: dict[str, object]) -> None:
    metrics = results["metrics"]
    print(f"{label} model")
    print("-------------------------")
    print(f"Features: {', '.join(results['features'])}")
    print(f"Observations: {int(metrics['observations'])}")
    print(f"R^2: {metrics['r2']:.6g}")
    print(f"RMSE: {metrics['rmse']:.6g}")
    print(f"MAE: {metrics['mae']:.6g}")
    print("Coefficients:")
    print(f"  intercept: {results['coefficients'][0]:.6g}")
    for feature_name, coef in zip(results["features"], results["coefficients"][1:]):
        print(f"  {feature_name}: {coef:.6g}")
    print()


def linear_regression(df, feature_names, response_name, export_txt_results, normalise_features) -> None:
    # Change names of cols if needed, else just keep the same
    feature_cols = [find_column(df.columns, name) for name in feature_names]
    response_col = find_column(df.columns, response_name)

    results = evaluate_combinations(df, feature_cols, response_col, normalise_features)
    if not results:
        raise ValueError("No valid feature combinations found after filtering missing values.")

    sorted_results = sorted(results, key=lambda item: item["metrics"]["r2"] if not np.isnan(item["metrics"]["r2"]) else -float("inf"))

    print("Linear regression summary")
    print("=========================")
    print(f"Response: {response_col}")
    print(f"Total combinations evaluated: {len(results)}\n")
    if export_txt_results:
        export_txt_path = "../output/factors_linreg.txt"
        os.makedirs(os.path.dirname(export_txt_path), exist_ok=True)
        with open(export_txt_path, "w", encoding="utf-8") as out_file:
            out_file.write(str(sorted_results[-1]))
        print(f"Best results exported at", export_txt_path)

    print_result("Best", sorted_results[-1])
    #print_result("Worst", sorted_results[0])




# --------------------------------------------------------------------------- #
# 1. Target distribution
# --------------------------------------------------------------------------- #
def plot_target_distribution(df: pl.DataFrame, target: str):
    """Histogram + KDE of the target variable. Check for skew, outliers, multimodality."""
    values = df[target].to_numpy()
 
    fig, ax = plt.subplots(figsize=(7, 5))
    sns.histplot(values, kde=True, ax=ax, color="steelblue", edgecolor="white")
    ax.axvline(np.mean(values), color="red", linestyle="--", label=f"mean")
    ax.axvline(np.median(values), color="orange", linestyle="--", label=f"median")
    ax.set_title(f"Distribution of target: {target}")
    ax.set_xlabel(target)
    ax.legend()
    fig.tight_layout()
    return fig

 
# --------------------------------------------------------------------------- #
# 2. Feature distributions (grid of histograms)
# --------------------------------------------------------------------------- #
def plot_feature_distributions(df: pl.DataFrame, features: list[str]):
    """Grid of histograms, one per feature, to check ranges/skew before modeling."""
    n = len(features)
    ncols = 3
    nrows = math.ceil(n / ncols)
 
    fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 4 * nrows))
    axes = np.array(axes).reshape(-1)
 
    for i, feat in enumerate(features):
        values = df[feat].to_numpy()
        sns.histplot(values, kde=True, ax=axes[i], color="seagreen", edgecolor="white")
        axes[i].set_title(feat)
 
    for j in range(n, len(axes)):
        axes[j].axis("off")
 
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.suptitle("Feature distributions", y=0.995, fontsize=14)
    return fig
 
 
# --------------------------------------------------------------------------- #
# 3. Correlation heatmap (target + features)
# --------------------------------------------------------------------------- #
def plot_correlation_heatmap(df: pl.DataFrame, target: str, features: list[str]):
    """Pearson correlation heatmap across target and all features."""
    cols = [target] + features
    corr_df = df.select(cols).to_pandas().corr()  # small matrix; pandas is fine just for corr math
 
    fig, ax = plt.subplots(figsize=(7, 6))
    sns.heatmap(
        corr_df, annot=True, fmt=".2f", cmap="coolwarm", center=0,
        square=True, cbar_kws={"shrink": 0.8}, ax=ax,
    )
    ax.set_title("Correlation matrix (target + features)")
    fig.tight_layout()
    return fig
 
 
# --------------------------------------------------------------------------- #
# 4. Each feature vs target (grid of scatterplots)
# --------------------------------------------------------------------------- #
def plot_feature_vs_target_scatter(df: pl.DataFrame, target: str, features: list[str]):
    """Grid of scatterplots: each feature (x) against the target (y), with a trend line."""
    n = len(features)
    ncols = 3
    nrows = math.ceil(n / ncols)
 
    fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 4 * nrows))
    axes = np.array(axes).reshape(-1)
 
    y = df[target].to_numpy()
 
    for i, feat in enumerate(features):
        x = df[feat].to_numpy()
        sns.regplot(
            x=x, y=y, ax=axes[i], scatter_kws={"alpha": 0.6, "s": 20},
            line_kws={"color": "red"},
        )
        corr = np.corrcoef(x, y)[0, 1]
        axes[i].set_title(f"{feat} vs {target}  (r={corr:.2f})")
        axes[i].set_xlabel(feat)
        axes[i].set_ylabel(target)
 
    for j in range(n, len(axes)):
        axes[j].axis("off")
 
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.suptitle("Feature vs target relationships", y=0.995, fontsize=14)
    return fig
 
 
# --------------------------------------------------------------------------- #
# 5. Pairwise scatter matrix (features + target)
# --------------------------------------------------------------------------- #
def plot_pairwise_scatter_matrix(df: pl.DataFrame, target: str, features: list[str]):
    """Full pairplot across target + features to spot interactions/collinearity."""
    cols = [target] + features
    pdf = df.select(cols).to_pandas()  # seaborn's pairplot expects a pandas frame
 
    g = sns.pairplot(pdf, corner=True, plot_kws={"alpha": 0.6, "s": 15})
    g.fig.subplots_adjust(top=0.94)
    g.fig.suptitle("Pairwise relationships (target + features)", y=0.995, fontsize=14)
    return g.fig
 
 
# --------------------------------------------------------------------------- #
# 6. Residuals from a simple linear model
# --------------------------------------------------------------------------- #
def plot_residuals_vs_features(df: pl.DataFrame, target: str, features: list[str]):
    """
    Fit a quick multiple linear regression (target ~ features) and plot residuals
    against fitted values and against each feature. Useful for spotting
    nonlinearity, heteroscedasticity, or a feature that still explains residual error.
    """
    X = df.select(features).to_numpy()
    y = df[target].to_numpy()
 
    model = LinearRegression().fit(X, y)
    preds = model.predict(X)
    residuals = y - preds
 
    n_extra = len(features)
    ncols = 3
    nrows = math.ceil((n_extra + 1) / ncols)
 
    fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 4 * nrows))
    axes = np.array(axes).reshape(-1)
 
    # residuals vs fitted values
    sns.scatterplot(x=preds, y=residuals, ax=axes[0], alpha=0.6, s=20)
    axes[0].axhline(0, color="red", linestyle="--")
    axes[0].set_title(f"Residuals vs fitted (R²={model.score(X, y):.2f})")
    axes[0].set_xlabel("fitted values")
    axes[0].set_ylabel("residual")
 
    # residuals vs each feature
    for i, feat in enumerate(features, start=1):
        x = df[feat].to_numpy()
        sns.scatterplot(x=x, y=residuals, ax=axes[i], alpha=0.6, s=20, color="darkorange")
        axes[i].axhline(0, color="red", linestyle="--")
        axes[i].set_title(f"Residuals vs {feat}")
        axes[i].set_xlabel(feat)
        axes[i].set_ylabel("residual")
 
    for j in range(n_extra + 1, len(axes)):
        axes[j].axis("off")
 
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.suptitle("Residual diagnostics (linear model)", y=0.995, fontsize=14)
    return fig



if __name__ == "__main__":
    df = pl.read_csv(CSV_PATH).drop_nans()
    linear_regression(df, FEATURE_NAMES, RESPONSE_NAME, EXPORT_TXT_RESULTS, NORMALISE_FEATURES)

    plot_target_distribution(df, RESPONSE_NAME)
    plot_feature_distributions(df, FEATURE_NAMES)
    #plot_correlation_heatmap(df, RESPONSE_NAME, FEATURE_NAMES)
    plot_feature_vs_target_scatter(df, RESPONSE_NAME, FEATURE_NAMES)
    #plot_pairwise_scatter_matrix(df, RESPONSE_NAME, FEATURE_NAMES)
    plot_residuals_vs_features(df, RESPONSE_NAME, FEATURE_NAMES)

    plt.show()
