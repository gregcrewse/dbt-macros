from dbt_refactor_analyzer import DBTRefactorAnalyzer
import sys
import os

# Get manifest path from command line argument, or use default
manifest_path = sys.argv[1] if len(sys.argv) > 1 else 'target/manifest.json'
output_dir = sys.argv[2] if len(sys.argv) > 2 else 'dbt_analysis_results'

# Run analysis
analyzer = DBTRefactorAnalyzer(manifest_path)
report = analyzer.generate_refactoring_report(output_dir=output_dir)

print(f"\nAnalysis complete! CSV files saved to: {output_dir}")
