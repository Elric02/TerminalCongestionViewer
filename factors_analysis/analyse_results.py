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
from sklearn.model_selection import KFold
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
CSV_PATHS = ["merged_results_västerås_vastmanland.csv", "merged_results_västerås_ul.csv"]
EXPORT_TXT_RESULTS = False
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


def evaluate_kfold_regression(
    df: pl.DataFrame,
    feature_cols: list[str],
    response_col: str,
    k_folds: int = 5,
    normalize_features: bool = False,
    random_state: int = 42,
) -> dict[str, object]:
    subset_df = df.select(list(feature_cols) + [response_col]).drop_nans()
    if subset_df.height < 2:
        raise ValueError("Not enough rows to run k-fold validation.")

    X = subset_df.select(list(feature_cols)).with_columns([pl.col(col).cast(pl.Float64) for col in feature_cols]).to_numpy()
    y = subset_df.select(response_col).with_columns(pl.col(response_col).cast(pl.Float64)).to_numpy().flatten()

    n_splits = min(k_folds, len(y))
    if n_splits < 2:
        raise ValueError("Need at least 2 observations for k-fold validation.")

    kf = KFold(n_splits=n_splits, shuffle=True, random_state=random_state)
    fold_results = []

    for fold_idx, (train_idx, test_idx) in enumerate(kf.split(X), start=1):
        X_train, X_test = X[train_idx], X[test_idx]
        y_train, y_test = y[train_idx], y[test_idx]

        if normalize_features:
            means = np.nanmean(X_train, axis=0)
            stds = np.nanstd(X_train, axis=0, ddof=0)
            stds[stds == 0.0] = 1.0
            X_train = (X_train - means) / stds
            X_test = (X_test - means) / stds

        coefficients, _, _ = fit_linear_regression(X_train, y_train)

        X_design_test = np.column_stack([np.ones(X_test.shape[0], dtype=float), X_test])
        y_pred_test = X_design_test.dot(coefficients)

        ss_res = np.sum((y_test - y_pred_test) ** 2)
        ss_tot = np.sum((y_test - np.mean(y_test)) ** 2)
        r2 = 1.0 - ss_res / ss_tot if ss_tot != 0.0 else float("nan")
        rmse = float(np.sqrt(ss_res / y_test.shape[0]))
        mae = float(np.mean(np.abs(y_test - y_pred_test)))

        fold_results.append({
            "fold": fold_idx,
            "train_size": int(train_idx.size),
            "test_size": int(test_idx.size),
            "r2": float(r2),
            "rmse": float(rmse),
            "mae": float(mae),
            "actual": y_test.astype(float),
            "predicted": y_pred_test.astype(float),
        })

    r2_values = np.array([fold["r2"] for fold in fold_results], dtype=float)
    rmse_values = np.array([fold["rmse"] for fold in fold_results], dtype=float)
    mae_values = np.array([fold["mae"] for fold in fold_results], dtype=float)

    summary = {
        "r2_mean": float(np.nanmean(r2_values)),
        "r2_std": float(np.nanstd(r2_values, ddof=0)),
        "rmse_mean": float(np.nanmean(rmse_values)),
        "rmse_std": float(np.nanstd(rmse_values, ddof=0)),
        "mae_mean": float(np.nanmean(mae_values)),
        "mae_std": float(np.nanstd(mae_values, ddof=0)),
    }

    return {
        "folds": fold_results,
        "summary": summary,
    }


def plot_kfold_predictions(cv_results: dict[str, object]) -> plt.Figure:
    folds = cv_results["folds"]
    n_folds = len(folds)
    ncols = 3
    nrows = math.ceil(n_folds / ncols)

    fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 4 * nrows))
    axes = np.array(axes, dtype=object).reshape(-1)

    for i, fold in enumerate(folds):
        ax = axes[i]
        print(fold)
        actual = np.asarray(fold["actual"], dtype=float)
        predicted = np.asarray(fold["predicted"], dtype=float)

        ax.scatter(actual, predicted, alpha=0.7, s=20, color="steelblue")
        min_val = float(np.min([actual.min(), predicted.min()]))
        max_val = float(np.max([actual.max(), predicted.max()]))
        ax.plot([min_val, max_val], [min_val, max_val], color="red", linestyle="--", linewidth=1)

        ax.set_title(f"Fold {fold['fold']} (R²={fold['r2']:.3f})")
        ax.set_xlabel("Desired value")
        ax.set_ylabel("Guessed value")

    for j in range(n_folds, len(axes)):
        axes[j].axis("off")

    fig.tight_layout(rect=[0, 0, 1, 0.98])
    fig.suptitle("Actual vs predicted per CV fold", fontsize=14)
    return fig


def plot_kfold_predictions_multi_dataset(folds: list[dict]) -> plt.Figure:
    """Plot k-fold predictions organized by dataset."""
    datasets = sorted(set(fold["dataset"] for fold in folds))
    colors = ["steelblue", "darkorange", "seagreen", "crimson"]
    n_datasets = len(datasets)
    
    fig, axes = plt.subplots(1, n_datasets, figsize=(5 * n_datasets, 4))
    if n_datasets == 1:
        axes = [axes]
    else:
        axes = np.array(axes).reshape(-1)
    
    for d_idx, dataset in enumerate(datasets):
        ax = axes[d_idx]
        dataset_folds = [fold for fold in folds if fold["dataset"] == dataset]
        
        for fold in dataset_folds:
            actual = np.asarray(fold["actual"], dtype=float)
            predicted = np.asarray(fold["predicted"], dtype=float)
            ax.scatter(actual, predicted, alpha=0.6, s=20, color=colors[d_idx % len(colors)], label=f"Fold {fold['fold']}")
        
        # Plot perfect prediction line
        all_actual = np.concatenate([np.asarray(fold["actual"], dtype=float) for fold in dataset_folds])
        all_predicted = np.concatenate([np.asarray(fold["predicted"], dtype=float) for fold in dataset_folds])
        min_val = float(np.min([all_actual.min(), all_predicted.min()]))
        max_val = float(np.max([all_actual.max(), all_predicted.max()]))
        ax.plot([min_val, max_val], [min_val, max_val], color="red", linestyle="--", linewidth=1)
        
        # Calculate mean R² for dataset
        mean_r2 = np.mean([fold["r2"] for fold in dataset_folds])
        ax.set_title(f"{dataset} (Mean R²={mean_r2:.3f})")
        ax.set_xlabel("Desired value")
        ax.set_ylabel("Guessed value")
        #ax.legend(fontsize=8)
    
    fig.tight_layout(rect=[0, 0, 1, 0.98])
    fig.suptitle("Actual vs predicted for all CV folds per dataset (each terminal is a different dataset)", fontsize=14)
    return fig


def linear_regression(df, feature_names, response_name, export_txt_results, normalise_features) -> None:
    # Change names of cols if needed, else just keep the same
    feature_cols = [find_column(df.columns, name) for name in feature_names]
    response_col = find_column(df.columns, response_name)

    # Check if we have dataset column
    if "dataset" in df.columns:
        datasets = sorted(df["dataset"].unique().to_list())
        all_folds_results = []
        
        for dataset in datasets:
            dataset_df = df.filter(pl.col("dataset") == dataset)
            print("\n" + "="*60)
            print(f"Dataset: {dataset}")
            print("="*60)
            
            results = evaluate_combinations(dataset_df, feature_cols, response_col, normalise_features)
            if not results:
                print(f"No valid feature combinations found for {dataset}.")
                continue

            sorted_results = sorted(results, key=lambda item: item["metrics"]["r2"] if not np.isnan(item["metrics"]["r2"]) else -float("inf"))

            print("Linear regression summary")
            print("=========================")
            print(f"Response: {response_col}")
            print(f"Total combinations evaluated: {len(results)}\n")

            if export_txt_results:
                export_txt_path = f"../output/factors_linreg_{dataset}.txt"
                os.makedirs(os.path.dirname(export_txt_path), exist_ok=True)
                with open(export_txt_path, "w", encoding="utf-8") as out_file:
                    out_file.write(str(sorted_results[-1]))
                print(f"Best results exported at", export_txt_path)

            best_result = sorted_results[-1]
            print_result("Best", best_result)

            best_features = best_result["features"]
            cv_results = evaluate_kfold_regression(
                df=dataset_df,
                feature_cols=best_features,
                response_col=response_col,
                k_folds=5,
                normalize_features=normalise_features,
            )

            print("5-fold cross-validation")
            print("-----------------------")
            print(f"Features: {', '.join(best_features)}")
            for fold in cv_results["folds"]:
                print(
                    f"Fold {fold['fold']}: train={fold['train_size']}, test={fold['test_size']}, "
                    f"R^2={fold['r2']:.6g}, RMSE={fold['rmse']:.6g}, MAE={fold['mae']:.6g}"
                )
            summary = cv_results["summary"]
            print()
            print("Cross-validation summary")
            print("------------------------")
            print(f"Mean R^2: {summary['r2_mean']:.6g} ± {summary['r2_std']:.6g}")
            print(f"Mean RMSE: {summary['rmse_mean']:.6g} ± {summary['rmse_std']:.6g}")
            print(f"Mean MAE: {summary['mae_mean']:.6g} ± {summary['mae_std']:.6g}")
            print()
            
            # Store fold results with dataset info
            for fold in cv_results["folds"]:
                fold["dataset"] = dataset
            all_folds_results.extend(cv_results["folds"])
        
        # Create combined k-fold plot with all datasets
        if all_folds_results:
            plot_kfold_predictions_multi_dataset(all_folds_results)
    else:
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

        best_result = sorted_results[-1]
        print_result("Best", best_result)

        best_features = best_result["features"]
        cv_results = evaluate_kfold_regression(
            df=df,
            feature_cols=best_features,
            response_col=response_col,
            k_folds=5,
            normalize_features=normalise_features,
        )

        print("5-fold cross-validation")
        print("-----------------------")
        print(f"Features: {', '.join(best_features)}")
        for fold in cv_results["folds"]:
            print(
                f"Fold {fold['fold']}: train={fold['train_size']}, test={fold['test_size']}, "
                f"R^2={fold['r2']:.6g}, RMSE={fold['rmse']:.6g}, MAE={fold['mae']:.6g}"
            )
        summary = cv_results["summary"]
        print()
        print("Cross-validation summary")
        print("------------------------")
        print(f"Mean R^2: {summary['r2_mean']:.6g} ± {summary['r2_std']:.6g}")
        print(f"Mean RMSE: {summary['rmse_mean']:.6g} ± {summary['rmse_std']:.6g}")
        print(f"Mean MAE: {summary['mae_mean']:.6g} ± {summary['mae_std']:.6g}")
        print()

        # Add the fold-by-fold actual vs predicted plot
        plot_kfold_predictions(cv_results)


# --------------------------------------------------------------------------- #
# 1. Target distribution
# --------------------------------------------------------------------------- #
def plot_target_distribution(df: pl.DataFrame, target: str):
    """Histogram + KDE of the target variable. Check for skew, outliers, multimodality."""
    if "dataset" in df.columns:
        datasets = sorted(df["dataset"].unique().to_list())
        n_datasets = len(datasets)
        ncols = 2
        nrows = math.ceil(n_datasets / ncols)
        
        fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 4 * nrows))
        axes = np.array(axes).reshape(-1)
        
        colors = ["steelblue", "darkorange", "seagreen", "crimson"]
        
        # Calculate global min/max across all datasets
        all_values = df[target].to_numpy()
        x_min, x_max = float(np.nanmin(all_values)), float(np.nanmax(all_values))
        
        for i, dataset in enumerate(datasets):
            dataset_df = df.filter(pl.col("dataset") == dataset)
            values = dataset_df[target].to_numpy()
            sns.histplot(values, kde=True, ax=axes[i], color=colors[i % len(colors)], edgecolor="white")
            axes[i].axvline(np.mean(values), color="red", linestyle="--", label="mean")
            axes[i].axvline(np.median(values), color="orange", linestyle="--", label="median")
            axes[i].set_title(f"{dataset}: {target}")
            axes[i].set_xlabel(target)
            axes[i].set_xlim(x_min, x_max)
            axes[i].legend()
        
        # Hide unused subplots
        for j in range(n_datasets, len(axes)):
            axes[j].axis("off")
        
        fig.tight_layout(rect=[0, 0, 1, 0.96])
        fig.suptitle(f"Distribution of target: {target}", y=0.995, fontsize=14)
    else:
        fig, ax = plt.subplots(figsize=(7, 5))
        values = df[target].to_numpy()
        sns.histplot(values, kde=True, ax=ax, color="steelblue", edgecolor="white")
        ax.axvline(np.mean(values), color="red", linestyle="--", label="mean")
        ax.axvline(np.median(values), color="orange", linestyle="--", label="median")
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
    colors = ["seagreen", "steelblue", "darkorange", "crimson"]

    if "dataset" in df.columns:
        datasets = sorted(df["dataset"].unique().to_list())
        n_datasets = len(datasets)
        n_features = len(features)
        fig, axes = plt.subplots(
            n_datasets,
            n_features,
            figsize=(5 * n_features, 4 * n_datasets),
            squeeze=False,
        )

        for column, feat in enumerate(features):
            all_values = df[feat].drop_nulls().to_numpy()
            all_values = all_values[np.isfinite(all_values)]
            bin_edges = np.histogram_bin_edges(all_values, bins="auto")
            x_min = float(np.min(all_values))
            x_max = float(np.max(all_values))
            if x_min == x_max:
                x_min -= 0.5
                x_max += 0.5

            for row, dataset in enumerate(datasets):
                dataset_df = df.filter(pl.col("dataset") == dataset)
                values = dataset_df[feat].to_numpy()
                sns.histplot(
                    values,
                    bins=bin_edges,
                    kde=False,
                    ax=axes[row, column],
                    color=colors[row % len(colors)],
                    edgecolor="white",
                    alpha=0.6,
                )
                axes[row, column].set_title(f"{dataset}: {feat}")
                axes[row, column].set_xlim(x_min, x_max)
    else:
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
    
    colors = ["steelblue", "darkorange", "seagreen", "crimson"]
 
    for i, feat in enumerate(features):
        if "dataset" in df.columns:
            datasets = df["dataset"].unique().to_list()
            for j, dataset in enumerate(sorted(datasets)):
                dataset_df = df.filter(pl.col("dataset") == dataset)
                x = dataset_df[feat].to_numpy()
                y = dataset_df[target].to_numpy()
                color = colors[j % len(colors)]
                axes[i].scatter(x, y, alpha=0.6, s=20, color=color, label=dataset)
                if len(x) >= 2:
                    if np.ptp(x) == 0:
                        line_x = np.array([x[0], x[0]], dtype=float)
                        line_y = np.array([np.mean(y), np.mean(y)], dtype=float)
                    else:
                        line_x = np.array([np.min(x), np.max(x)], dtype=float)
                        line_y = np.polyval(np.polyfit(x, y, 1), line_x)
                    axes[i].plot(line_x, line_y, color=color, linewidth=2)
            axes[i].legend()
            # Calculate correlation on full data
            x_full = df[feat].to_numpy()
            y_full = df[target].to_numpy()
            corr = np.corrcoef(x_full, y_full)[0, 1]
        else:
            x = df[feat].to_numpy()
            y = df[target].to_numpy()
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
    if "dataset" in df.columns:
        cols.append("dataset")
    pdf = df.select(cols).to_pandas()  # seaborn's pairplot expects a pandas frame
    
    if "dataset" in pdf.columns:
        palette = {sorted(pdf["dataset"].unique())[i]: ["steelblue", "darkorange", "seagreen", "crimson"][i] for i in range(len(pdf["dataset"].unique()))}
        g = sns.pairplot(pdf, corner=True, plot_kws={"alpha": 0.6, "s": 15}, hue="dataset", palette=palette)
    else:
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
    
    colors = ["steelblue", "darkorange", "seagreen", "crimson"]
 
    n_extra = len(features)
    ncols = 3
    nrows = math.ceil((n_extra + 1) / ncols)
 
    fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 4 * nrows))
    axes = np.array(axes).reshape(-1)
 
    # residuals vs fitted values
    if "dataset" in df.columns:
        datasets = df["dataset"].unique().to_list()
        for i, dataset in enumerate(sorted(datasets)):
            dataset_df = df.filter(pl.col("dataset") == dataset)
            X_dataset = dataset_df.select(features).to_numpy()
            y_dataset = dataset_df[target].to_numpy()
            preds_dataset = model.predict(X_dataset)
            residuals_dataset = y_dataset - preds_dataset
            axes[0].scatter(preds_dataset, residuals_dataset, alpha=0.6, s=20, color=colors[i % len(colors)], label=dataset)
        axes[0].legend()
    else:
        axes[0].scatter(preds, residuals, alpha=0.6, s=20, color="steelblue")
    
    axes[0].axhline(0, color="red", linestyle="--")
    axes[0].set_title(f"Residuals vs fitted (R²={model.score(X, y):.2f})")
    axes[0].set_xlabel("fitted values")
    axes[0].set_ylabel("residual")
 
    # residuals vs each feature
    for i, feat in enumerate(features, start=1):
        if "dataset" in df.columns:
            datasets = df["dataset"].unique().to_list()
            for j, dataset in enumerate(sorted(datasets)):
                dataset_df = df.filter(pl.col("dataset") == dataset)
                x = dataset_df[feat].to_numpy()
                y_dataset = dataset_df[target].to_numpy()
                X_dataset = dataset_df.select(features).to_numpy()
                preds_dataset = model.predict(X_dataset)
                residuals_dataset = y_dataset - preds_dataset
                axes[i].scatter(x, residuals_dataset, alpha=0.6, s=20, color=colors[j % len(colors)], label=dataset)
            axes[i].legend()
        else:
            x = df[feat].to_numpy()
            axes[i].scatter(x, residuals, alpha=0.6, s=20, color="darkorange")
        
        axes[i].axhline(0, color="red", linestyle="--")
        axes[i].set_title(f"Residuals vs {feat}")
        axes[i].set_xlabel(feat)
        axes[i].set_ylabel("residual")
 
    for j in range(n_extra + 1, len(axes)):
        axes[j].axis("off")
 
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.suptitle("Residual diagnostics (linear model) (all terminals together as 1 dataset)", y=0.995, fontsize=14)
    return fig



if __name__ == "__main__":
    # Load all datasets and combine them
    dfs = []
    for csv_path in CSV_PATHS:
        df_temp = pl.read_csv(csv_path)
        # Extract dataset name from filename (e.g., "merged_results_linköping.csv" -> "linköping")
        dataset_name = csv_path.replace("merged_results_", "").replace(".csv", "")
        df_temp = df_temp.with_columns(pl.lit(dataset_name).alias("dataset"))
        # Cast all numeric columns to Float64 to ensure compatibility when concatenating
        df_temp = df_temp.with_columns([
            pl.col(col).cast(pl.Float64) if df_temp[col].dtype in [pl.Int64, pl.Int32, pl.Int16, pl.Int8, pl.Float32, pl.Float64] else pl.col(col)
            for col in df_temp.columns
        ])
        dfs.append(df_temp)
    
    # Combine all datasets
    df = pl.concat(dfs).drop_nans()
    linear_regression(df, FEATURE_NAMES, RESPONSE_NAME, EXPORT_TXT_RESULTS, NORMALISE_FEATURES)

    plot_target_distribution(df, RESPONSE_NAME)
    #plot_feature_distributions(df, FEATURE_NAMES)
    #plot_correlation_heatmap(df, RESPONSE_NAME, FEATURE_NAMES)
    plot_feature_vs_target_scatter(df, RESPONSE_NAME, FEATURE_NAMES)
    #plot_pairwise_scatter_matrix(df, RESPONSE_NAME, FEATURE_NAMES)
    plot_residuals_vs_features(df, RESPONSE_NAME, FEATURE_NAMES)

    plt.show()
