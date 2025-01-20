import json
import pandas as pd
import os
from collections import Counter
from itertools import combinations


class DBTRefactorAnalyzer:
    def __init__(self, manifest_path):
        """Initialize analyzer with path to dbt manifest"""
        with open(manifest_path) as f:
            self.manifest = json.load(f)
        self.models = {
            k: v for k, v in self.manifest.get("nodes", {}).items() if v.get("resource_type") == "model"
        }

    def get_model_complexity_metrics(self):
        """Calculate complexity metrics for each model"""
        metrics = []

        for model_id, model in self.models.items():
            sql = model.get("raw_sql", "")
            refs = model.get("refs", [])
            sources = model.get("sources", [])

            metrics.append({
                "model": model_id,
                "num_joins": sql.lower().count(" join "),
                "num_ctes": sql.lower().count(" with "),
                "num_refs": len(refs) if refs else 0,
                "num_sources": len(sources) if sources else 0,
                "sql_length": len(sql),
            })

        return pd.DataFrame(metrics)

    def find_similar_transformations(self, similarity_threshold=0.8):
        """Find models with similar SQL transformations using tokenized comparison"""
        similar_models = []
        model_sqls = {
            model_id: Counter(model.get("raw_sql", "").lower().split())
            for model_id, model in self.models.items()
        }

        for (model_id1, sql1), (model_id2, sql2) in combinations(model_sqls.items(), 2):
            common_tokens = sum((sql1 & sql2).values())
            total_tokens = sum((sql1 | sql2).values())
            similarity = common_tokens / total_tokens if total_tokens else 0

            if similarity >= similarity_threshold:
                similar_models.append({
                    "model1": model_id1,
                    "model2": model_id2,
                    "similarity": round(similarity, 3),
                })

        return similar_models

    def detect_rejoined_upstream_concepts(self):
        """Identify models rejoining upstream references or sources"""
        rejoined_models = []

        for model_id, model in self.models.items():
            refs = model.get("refs", [])
            sources = model.get("sources", [])

            # Convert lists to tuples to ensure they are hashable
            unique_upstreams = set(map(tuple, refs)) | set(map(tuple, sources))

            if len(unique_upstreams) < len(refs + sources):  # Rejoining detected
                rejoined_models.append({
                    "model": model_id,
                    "num_rejoins": len(refs + sources) - len(unique_upstreams),
                    "upstream_concepts": [list(u) for u in unique_upstreams],  # Convert back to list for readability
                })

        return rejoined_models

    def generate_refactoring_report(self, output_dir="./dbt_analysis"):
        """Generate analysis report and save to CSV files"""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Get complexity metrics
        metrics_df = self.get_model_complexity_metrics()
        metrics_df.to_csv(f"{output_dir}/model_complexity_metrics.csv", index=False)

        # Find similar models
        similar_models = self.find_similar_transformations()
        if similar_models:
            similar_df = pd.DataFrame(similar_models)
            similar_df.to_csv(f"{output_dir}/similar_models.csv", index=False)

        # Detect rejoined upstream concepts
        rejoined_models = self.detect_rejoined_upstream_concepts()
        if rejoined_models:
            rejoined_df = pd.DataFrame(rejoined_models)
            rejoined_df.to_csv(f"{output_dir}/rejoined_upstream_concepts.csv", index=False)

        # Generate recommendations
        recommendations = []

        # Add complexity recommendations
        complex_models = metrics_df[
            (metrics_df["num_joins"] > 5) | (metrics_df["sql_length"] > 1000)
        ]
        for _, row in complex_models.iterrows():
            recommendations.append({
                "model": row["model"],
                "type": "complexity",
                "metrics": f"Joins: {row['num_joins']}, CTEs: {row['num_ctes']}",
                "suggestion": "Consider breaking into smaller models",
            })

        # Add similarity recommendations
        for similar in similar_models:
            recommendations.append({
                "model": similar["model1"],
                "type": "similarity",
                "metrics": f"Similarity: {similar['similarity']}",
                "suggestion": f"Very similar to {similar['model2']}, consider consolidating",
            })

        # Add rejoining recommendations
        for rejoin in rejoined_models:
            recommendations.append({
                "model": rejoin["model"],
                "type": "rejoining",
                "metrics": f"Rejoins: {rejoin['num_rejoins']}",
                "suggestion": "Refactor to reduce redundant upstream reuses",
            })

        if recommendations:
            recommendations_df = pd.DataFrame(recommendations)
            recommendations_df.to_csv(f"{output_dir}/refactoring_recommendations.csv", index=False)

        print(f"\nAnalysis complete! Files saved to: {output_dir}")
        print(f"Found {len(complex_models)} complex models, {len(similar_models)} similar model pairs, and "
              f"{len(rejoined_models)} models with rejoined upstream concepts")

        return {
            "complexity_metrics": metrics_df,
            "similar_models": similar_models,
            "rejoined_upstream_concepts": rejoined_models,
            "recommendations": recommendations,
        }
