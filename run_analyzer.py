from dbt_refactor_analyzer import DBTRefactorAnalyzer  # Ensure the file name matches the updated class
import sys

# Get manifest path and output directory from command line arguments, or use defaults
manifest_path = sys.argv[1] if len(sys.argv) > 1 else 'target/manifest.json'
output_dir = sys.argv[2] if len(sys.argv) > 2 else 'dbt_analysis_results'

# Run analysis
print(f"Analyzing dbt project from manifest: {manifest_path}")
analyzer = DBTRefactorAnalyzer(manifest_path)
results = analyzer.generate_refactoring_report(output_dir=output_dir)

# Print summary
print("\nSummary:")
print(f"- Complexity metrics saved to: {output_dir}/model_complexity_metrics.csv")
print(f"- Similar models saved to: {output_dir}/similar_models.csv")
print(f"- Rejoined upstream concepts saved to: {output_dir}/rejoined_upstream_concepts.csv")
print(f"- Refactoring recommendations saved to: {output_dir}/refactoring_recommendations.csv")
