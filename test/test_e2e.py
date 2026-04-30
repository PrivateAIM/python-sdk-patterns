"""HALTA-FLAME: Federated Long COVID (PASC) analysis pipeline."""

import os
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import StandardScaler


from flame.star import StarModel, StarModelTester, StarAnalyzer, StarAggregator


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

RESULTS_DIR = Path("results/")
MAX_ITERATIONS = 10
CONVERGENCE_TOL = 1e-4

PASC_SYMPTOM_KEYWORDS: List[str] = [
    "fatigue", "tired", "dyspnea", "shortness_of_breath", "breath", "cough",
    "anosmia", "ageusia", "smell", "taste", "headache", "migraine",
    "brain", "fog", "cognitive", "memory", "concentration",
    "chest_pain", "chest", "palpit", "tachy", "arrhythm",
    "sleep", "insomnia", "anxiety", "depress", "mood",
    "myalgia", "muscle", "joint", "pain", "arthral",
    "fever", "nausea", "diarr", "gi_",
]

LABEL_COL = "pasc"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_symptom_columns(df: pd.DataFrame) -> List[str]:
    """Return columns whose names contain PASC symptom keywords."""
    seen: set = set()
    out: List[str] = []
    for c in df.columns:
        if c == LABEL_COL:
            continue
        cl = str(c).lower()
        for kw in PASC_SYMPTOM_KEYWORDS:
            if kw in cl:
                if c not in seen:
                    seen.add(c)
                    out.append(c)
                break
    return out


def _prepare_features(df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray, List[str]]:
    """Build feature matrix X and label vector y from the dataframe."""
    symptom_cols = _find_symptom_columns(df)
    feature_names: List[str] = list(symptom_cols)
    parts: List[np.ndarray] = []

    for col in symptom_cols:
        parts.append(pd.to_numeric(df[col], errors="coerce").fillna(0.0).values)

    if "age" in df.columns:
        parts.append(pd.to_numeric(df["age"], errors="coerce").fillna(0.0).values)
        feature_names.append("age")

    sex_col = "sex" if "sex" in df.columns else ("gender" if "gender" in df.columns else None)
    if sex_col:
        sex_numeric = (
            df[sex_col].astype(str).str.strip().str.lower()
            .map({"male": 0, "female": 1, "m": 0, "f": 1, "1": 0, "2": 1})
            .fillna(0).values.astype(float)
        )
        parts.append(sex_numeric)
        feature_names.append("sex_encoded")

    if not parts:
        raise ValueError("No features could be extracted from the data.")

    X = np.column_stack(parts)
    y = pd.to_numeric(df[LABEL_COL], errors="coerce").fillna(0).astype(int).values
    return X, y, feature_names


# ---------------------------------------------------------------------------
# Analyzer  (runs on each node)
# ---------------------------------------------------------------------------

class Analyzer(StarAnalyzer):
    """
    Each node parses its local CSV, computes descriptive statistics,
    trains a local linear SVM, and returns both to the aggregator.
    """

    def __init__(self, flame):
        super().__init__(flame)

    def analysis_method(self, data, aggregator_results):
        # --- Parse CSV bytes ------------------------------------------------
        file_bytes = [v for k, v in data[0].items() if k.endswith('labeled.csv')][0]
        df = pd.read_csv(BytesIO(file_bytes))
        node_id = getattr(self, "id", "unknown")


        # --- Descriptive statistics -----------------------------------------
        n_rows, n_cols = df.shape
        missing_by_col = df.isna().sum().astype(int).to_dict()
        total_missing = int(sum(missing_by_col.values()))

        # Age histogram (5-year bins, 0-100)
        age_hist, age_edges = None, None
        if "age" in df.columns:
            ages = pd.to_numeric(df["age"], errors="coerce").dropna()
            bins = np.arange(0, 105, 5)
            h, e = np.histogram(ages, bins=bins)
            age_hist = h.astype(int).tolist()
            age_edges = e.astype(float).tolist()

        # Sex distribution
        sex_col = "sex" if "sex" in df.columns else ("gender" if "gender" in df.columns else None)
        sex_counts = (
            df[sex_col].astype(str).value_counts(dropna=False).to_dict()
            if sex_col else {}
        )

        # PASC label distribution
        pasc_counts: Dict[str, int] = {}
        if LABEL_COL in df.columns:
            pasc_counts = {
                str(k): int(v)
                for k, v in df[LABEL_COL].value_counts(dropna=False).items()
            }

        # --- Local SVM training ---------------------------------------------
        X, y, feature_names = _prepare_features(df)
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        svm = SGDClassifier(
            loss="hinge",       # linear SVM
            penalty="l2",
            alpha=1e-4,
            max_iter=1000,
            tol=1e-3,
            random_state=42,
        )

        if aggregator_results is not None and "svm_coef" in aggregator_results:
            # Warm-start from the global (aggregated) model
            svm.partial_fit(X_scaled[:1], y[:1], classes=[0, 1])
            svm.coef_ = np.array(aggregator_results["svm_coef"])
            svm.intercept_ = np.array(aggregator_results["svm_intercept"])
            for _ in range(10):
                svm.partial_fit(X_scaled, y)
        else:
            svm.fit(X_scaled, y)

        y_pred = svm.predict(X_scaled)
        local_acc = float(accuracy_score(y, y_pred))

        self.flame.flame_log(
            f"HaltaAnalyzer ({node_id}): rows={n_rows}, "
            f"PASC={pasc_counts}, local_acc={local_acc:.4f}",
            log_type="notice",
        )

        return {
            "node_id": node_id,
            "n_rows": int(n_rows),
            "n_cols": int(n_cols),
            "missing_by_col": missing_by_col,
            "total_missing": total_missing,
            "age_edges": age_edges,
            "age_hist": age_hist,
            "sex_counts": sex_counts,
            "pasc_counts": pasc_counts,
            "feature_names": feature_names,
            "svm_coef": svm.coef_.tolist(),
            "svm_intercept": svm.intercept_.tolist(),
            "scaler_mean": scaler.mean_.tolist(),
            "scaler_scale": scaler.scale_.tolist(),
            "local_accuracy": local_acc,
            "local_n_samples": int(len(y)),
        }


# ---------------------------------------------------------------------------
# Aggregator  (central coordinator)
# ---------------------------------------------------------------------------

class Aggregator(StarAggregator):
    """
    Aggregates descriptive statistics, generates plots, and performs
    federated averaging of the SVM weights across nodes.
    """

    def __init__(self, flame):
        super().__init__(flame)

    # --- main aggregation ---------------------------------------------------

    def aggregation_method(self, analysis_results: list) -> Union[dict, list]:
        n_nodes = len(analysis_results)

        # Aggregate descriptive statistics
        total_rows = sum(r["n_rows"] for r in analysis_results)
        n_cols = max(r["n_cols"] for r in analysis_results)
        total_missing = sum(r["total_missing"] for r in analysis_results)

        missing_by_col: Dict[str, int] = {}
        for r in analysis_results:
            for k, v in r.get("missing_by_col", {}).items():
                missing_by_col[k] = missing_by_col.get(k, 0) + v

        age_edges = analysis_results[0].get("age_edges")
        age_hist = None
        if age_edges is not None:
            age_hist = np.zeros(len(age_edges) - 1)
            for r in analysis_results:
                if r.get("age_hist") is not None:
                    age_hist += np.array(r["age_hist"])
            age_hist = age_hist.tolist()

        sex_counts: Dict[str, int] = {}
        for r in analysis_results:
            for k, v in r.get("sex_counts", {}).items():
                sex_counts[k] = sex_counts.get(k, 0) + v

        pasc_counts: Dict[str, int] = {}
        for r in analysis_results:
            for k, v in r.get("pasc_counts", {}).items():
                pasc_counts[k] = pasc_counts.get(k, 0) + v

        # Federated SVM averaging (weighted by node sample size)
        total_samples = sum(r["local_n_samples"] for r in analysis_results)
        coef_avg: Optional[np.ndarray] = None
        intercept_avg: Optional[np.ndarray] = None
        for r in analysis_results:
            w = r["local_n_samples"] / total_samples
            coef = np.array(r["svm_coef"])
            intercept = np.array(r["svm_intercept"])
            if coef_avg is None:
                coef_avg = w * coef
                intercept_avg = w * intercept
            else:
                coef_avg += w * coef
                intercept_avg += w * intercept

        avg_accuracy = sum(
            r["local_accuracy"] * r["local_n_samples"] for r in analysis_results
        ) / total_samples

        per_node = [
            {
                "node": r["node_id"],
                "accuracy": r["local_accuracy"],
                "n_samples": r["local_n_samples"],
            }
            for r in analysis_results
        ]

        # Plots and description only on the first iteration
        if self.num_iterations <= 1:
            self._make_plots(age_edges, age_hist, sex_counts, pasc_counts)
            self._print_description(
                n_nodes, total_rows, n_cols, total_missing,
                sex_counts, pasc_counts, avg_accuracy, per_node,
            )

        self.flame.flame_log(
            f"HaltaAggregator iter={self.num_iterations}: "
            f"avg_acc={avg_accuracy:.4f}, nodes={n_nodes}",
            log_type="notice",
        )

        result = {
            "n_nodes": n_nodes,
            "total_rows": total_rows,
            "n_cols": n_cols,
            "total_missing": total_missing,
            "missing_by_col": missing_by_col,
            "age_edges": age_edges,
            "age_hist": age_hist,
            "sex_counts": sex_counts,
            "pasc_counts": pasc_counts,
            "svm_coef": coef_avg.tolist(),
            "svm_intercept": intercept_avg.tolist(),
            "avg_accuracy": avg_accuracy,
            "per_node": per_node,
            "iteration": self.num_iterations,
        }
        # if not has_converged progress with next iteration
        if not self.has_converged(result, self.latest_result):
            return result
        # else wrap final result file package
        else:
            result_dir = str(RESULTS_DIR)
            result_file_paths = [os.path.join(result_dir, "age_distribution_federated.png"),
                                 os.path.join(result_dir, "dataset_description_federated.txt"),
                                 os.path.join(result_dir, "pasc_distribution_federated.png"),
                                 os.path.join(result_dir, "sex_distribution_federated.png")]
            results = []
            for path in result_file_paths:
                try:
                    with open(path, 'r') as f:
                        results.append(f.read())
                except UnicodeDecodeError:
                    with open(path, 'rb') as f:
                        results.append(f.read())
            return results

    # --- convergence check --------------------------------------------------

    def has_converged(self, result, last_result) -> bool:
        it = self.num_iterations

        if last_result is not None and "svm_coef" in last_result:
            if isinstance(result, list):
                return True

            diff = np.linalg.norm(
                np.array(result["svm_coef"]) - np.array(last_result["svm_coef"])
            )
            self.flame.flame_log(
                f"Convergence: iter={it}, coef_diff={diff:.6f}",
                log_type="notice",
            )
            if diff < CONVERGENCE_TOL:
                print(f"\nConverged at iteration {it} (delta={diff:.6f} < {CONVERGENCE_TOL})")
                self._print_final(result)
                return True

        if it >= MAX_ITERATIONS:
            print(f"\nReached max iterations ({MAX_ITERATIONS}), stopping.")
            self._print_final(result)
            return True

        return False

    # --- private helpers ----------------------------------------------------

    def _make_plots(self, age_edges, age_hist, sex_counts, pasc_counts):
        """Generate federated distribution plots."""
        results_dir = str(RESULTS_DIR)
        if not os.path.exists(results_dir):
            os.makedirs(results_dir)
        try:
            # Age distribution
            if age_edges is not None and age_hist is not None:
                edges = np.array(age_edges, dtype=float)
                hist = np.array(age_hist, dtype=float)
                centers = (edges[:-1] + edges[1:]) / 2.0
                width = (edges[1] - edges[0]) * 0.9
                plt.figure(figsize=(10, 5))
                plt.bar(centers, hist, width=width, color="steelblue", edgecolor="white")
                plt.xlabel("Age")
                plt.ylabel("Count (federated)")
                plt.title("Federated Age Distribution")
                plt.tight_layout()
                plt.savefig(os.path.join(results_dir, "age_distribution_federated.png"), dpi=200)
                plt.close()

            # Sex distribution
            if sex_counts:
                keys = list(sex_counts.keys())
                vals = [sex_counts[k] for k in keys]
                plt.figure(figsize=(6, 5))
                plt.bar(keys, vals, color="coral", edgecolor="white")
                plt.xlabel("Sex")
                plt.ylabel("Count (federated)")
                plt.title("Federated Sex Distribution")
                plt.tight_layout()
                plt.savefig(os.path.join(results_dir, "sex_distribution_federated.png"), dpi=200)
                plt.close()

            # PASC label distribution
            if pasc_counts:
                labels = sorted(pasc_counts.keys(), key=str)
                vals = [pasc_counts[k] for k in labels]
                colors = ["forestgreen", "tomato"][: len(labels)]
                plt.figure(figsize=(6, 5))
                plt.bar(
                    [f"PASC={l}" for l in labels], vals,
                    color=colors, edgecolor="white",
                )
                plt.ylabel("Count (federated)")
                plt.title("Federated PASC Label Distribution")
                plt.tight_layout()
                plt.savefig(os.path.join(results_dir, "pasc_distribution_federated.png"), dpi=200)
                plt.close()
        except Exception as e:
            print(f"Plot error: {e}")

    def _print_description(self, n_nodes, total_rows, n_cols, total_missing,
                           sex_counts, pasc_counts, avg_accuracy, per_node):
        """Print and save a human-readable dataset description."""
        results_dir = str(RESULTS_DIR)
        sep = "=" * 60
        lines = [
            sep,
            "  HALTA Long Covid -- Federated Dataset Description",
            sep,
            f"  Nodes:             {n_nodes}",
            f"  Total rows:        {total_rows}",
            f"  Columns:           {n_cols}",
            f"  Total missing:     {total_missing}",
            "",
            "  Sex distribution:",
        ]
        for k, v in sex_counts.items():
            lines.append(f"    {k}: {v}")
        lines.append("")
        lines.append("  PASC label distribution:")
        for k, v in sorted(pasc_counts.items(), key=lambda x: str(x[0])):
            lines.append(f"    PASC={k}: {v}")
        lines.append("")
        lines.append(f"  Federated SVM accuracy (weighted avg): {avg_accuracy:.4f}")
        for n in per_node:
            lines.append(f"    node {n['node']}: acc={n['accuracy']:.4f}  (n={n['n_samples']})")
        lines.append(sep)

        desc = "\n".join(lines)
        self.flame.flame_log(desc, log_type='info')

        if not os.path.exists(results_dir):
            os.makedirs(results_dir)

        if os.path.exists(os.path.join(results_dir, "description.txt")):
            with open(os.path.join(results_dir, "dataset_description_federated.txt"), "a", encoding="utf-8") as f:
                f.write(desc + "\n")
        else:
            with open(os.path.join(results_dir, "dataset_description_federated.txt"), "w", encoding="utf-8") as f:
                f.write(desc + "\n")

    def _print_final(self, result):
        """Print final SVM model summary after convergence."""
        sep = "-" * 60
        lines = [
            "",
            sep,
            "  Final Federated SVM Model",
            sep,
            f"  Iterations:        {result['iteration']}",
            f"  Nodes:             {result['n_nodes']}",
            f"  Total samples:     {result['total_rows']}",
            f"  Avg accuracy:      {result['avg_accuracy']:.4f}",
        ]
        for n in result.get("per_node", []):
            lines.append(f"    node {n['node']}: acc={n['accuracy']:.4f}  (n={n['n_samples']})")
        lines.append(f"  Coef shape:        {np.array(result['svm_coef']).shape}")
        lines.append(sep)
        print("\n".join(lines))


if __name__ == "__main__":
    data = [[{'synthetic_eucare_1_labeled.csv': open('test/data/node1/synthetic_eucare_1_labeled.csv', 'rb').read()}],
            [{'synthetic_eucare_2_labeled.csv': open('test/data/node2/synthetic_eucare_2_labeled.csv', 'rb').read()}]]

    StarModelTester(
        data_splits=data,
        analyzer=Analyzer,
        aggregator=Aggregator,
        data_type='s3',
        query=[],
        multiple_results=True,
        simple_analysis=False,
        output_type=['bytes', 'str', 'bytes', 'bytes'],
        result_filepath=['test/results/age_distribution_federated_new.png',
                         'test/results/dataset_description_federated_new.txt',
                         'test/results/pasc_distribution_federated_new.png',
                         'test/results/sex_distribution_federated_new.png']
    )
    # StarModel(
    #     analyzer=Analyzer,
    #     aggregator=Aggregator,
    #     data_type='s3',
    #     query=[],
    #     multiple_results=True,
    #     simple_analysis=False,
    #     output_type=['bytes', 'str', 'bytes', 'bytes']
    # )

