from dbt_refactor_analyzer_simple import DBTRefactorAnalyzer
import sys

# Get manifest path from command line argument, or use default
manifest_path = sys.argv[1] if len(sys.argv) > 1 else 'target/manifest.json'
output_dir = sys.argv[2] if len(sys.argv) > 2 else 'dbt_analysis_results'

# Run analysis
print(f"Analyzing dbt project from manifest: {manifest_path}")
analyzer = DBTRefactorAnalyzer(manifest_path)
analyzer.generate_refactoring_report(output_dir=output_dir)
