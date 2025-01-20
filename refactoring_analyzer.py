import json
import pandas as pd
from collections import defaultdict

class DBTRefactorAnalyzer:
    def __init__(self, manifest_path):
        """Initialize analyzer with path to dbt manifest"""
        with open(manifest_path) as f:
            self.manifest = json.load(f)
        self.models = self.manifest['nodes']
        
    def find_redundant_joins(self):
        """Identify models that rejoin to upstream concepts"""
        redundant_joins = []
        
        for model_id, model in self.models.items():
            if model['resource_type'] != 'model':
                continue
                
            # Get all upstream models/sources
            upstream_refs = set()
            for ref in model.get('refs', []):
                upstream_refs.add(tuple(ref))
            
            # Track paths to each reference
            ref_paths = defaultdict(list)
            
            # Analyze SQL to find join patterns
            sql = model.get('raw_sql', '').lower()
            
            # Look for cases where we join back to an upstream model
            for ref in upstream_refs:
                ref_name = ref[-1]
                if f"join {ref_name}" in sql:
                    # Get path to this reference
                    path = self._get_path_to_ref(model_id, ref)
                    if path:
                        ref_paths[ref_name].append(path)
            
            # If we have multiple paths to the same reference, flag it
            for ref_name, paths in ref_paths.items():
                if len(paths) > 1:
                    redundant_joins.append({
                        'model': model_id,
                        'redundant_ref': ref_name,
                        'paths': paths
                    })
                    
        return redundant_joins
    
    def find_similar_transformations(self, similarity_threshold=0.8):
        """Find models with similar SQL transformations"""
        from difflib import SequenceMatcher
        
        similar_models = []
        processed_models = set()
        
        for model_id1, model1 in self.models.items():
            if model_id1 in processed_models or model1['resource_type'] != 'model':
                continue
                
            sql1 = model1.get('raw_sql', '').lower()
            
            for model_id2, model2 in self.models.items():
                if (model_id2 in processed_models or 
                    model2['resource_type'] != 'model' or 
                    model_id1 == model_id2):
                    continue
                    
                sql2 = model2.get('raw_sql', '').lower()
                
                similarity = SequenceMatcher(None, sql1, sql2).ratio()
                
                if similarity >= similarity_threshold:
                    similar_models.append({
                        'model1': model_id1,
                        'model2': model_id2,
                        'similarity': similarity
                    })
                    
            processed_models.add(model_id1)
            
        return similar_models
    
    def get_model_complexity_metrics(self):
        """Calculate complexity metrics for each model"""
        metrics = []
        
        for model_id, model in self.models.items():
            if model['resource_type'] != 'model':
                continue
                
            sql = model.get('raw_sql', '')
            
            metrics.append({
                'model': model_id,
                'num_joins': sql.lower().count('join'),
                'num_ctes': sql.lower().count('with'),
                'num_refs': len(model.get('refs', [])),
                'num_sources': len(model.get('sources', [])),
                'num_downstream': len(self._get_downstream_models(model_id))
            })
            
        return pd.DataFrame(metrics)
    
    def _get_path_to_ref(self, model_id, ref):
        """Get transformation path from model to referenced model"""
        path = []
        current = model_id
        
        while current:
            path.append(current)
            upstream = self._get_immediate_upstream(current)
            
            if not upstream:
                break
                
            # Look for ref in upstream models
            found = False
            for up_id in upstream:
                if up_id.endswith(ref[-1]):
                    current = up_id
                    found = True
                    break
                    
            if not found:
                current = upstream[0]
                
        return path if path else None
    
    def _get_immediate_upstream(self, model_id):
        """Get immediate upstream models"""
        model = self.models.get(model_id)
        if not model:
            return []
            
        upstream = []
        for ref in model.get('refs', []):
            ref_id = f"model.{self.manifest['metadata']['project_name']}.{ref[-1]}"
            if ref_id in self.models:
                upstream.append(ref_id)
                
        return upstream
    
    def _get_downstream_models(self, model_id):
        """Get all downstream dependent models"""
        downstream = set()
        
        for other_id, other in self.models.items():
            if other['resource_type'] != 'model':
                continue
                
            for ref in other.get('refs', []):
                # Extract just the model name from the full model_id
                current_model_name = model_id.split('.')[-1]
                if ref[-1] == current_model_name:
                    downstream.add(other_id)
                    
        return downstream
    
    def generate_refactoring_report(self, output_dir='./dbt_analysis'):
        """Generate comprehensive refactoring recommendations and save to CSV files
        
        Args:
            output_dir (str): Directory to save CSV output files
        """
        import os
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        report = {
            'redundant_joins': self.find_redundant_joins(),
            'similar_models': self.find_similar_transformations(),
            'complexity_metrics': self.get_model_complexity_metrics()
        }
        
        # Add specific recommendations
        recommendations = []
        
        # Check for highly complex models
        complex_df = report['complexity_metrics']
        complex_models = complex_df[
            (complex_df['num_joins'] > 5) | 
            (complex_df['num_refs'] > 5)
        ]['model'].tolist()
        
        if complex_models:
            recommendations.append({
                'type': 'complexity',
                'models': complex_models,
                'suggestion': 'Consider breaking these complex models into smaller, more focused transformations'
            })
            
        # Check for redundant patterns
        if report['redundant_joins']:
            models_with_redundant = [r['model'] for r in report['redundant_joins']]
            recommendations.append({
                'type': 'redundant_joins',
                'models': models_with_redundant,
                'suggestion': 'These models rejoin to upstream concepts. Consider refactoring to reuse existing transformations'
            })
            
        # Check for similar models
        if report['similar_models']:
            similar_pairs = [(s['model1'], s['model2']) for s in report['similar_models']]
            recommendations.append({
                'type': 'similar_logic',
                'model_pairs': similar_pairs,
                'suggestion': 'These model pairs have very similar logic. Consider consolidating into shared models'
            })
            
        report['recommendations'] = recommendations
        
        # Export results to CSV files
        # Complexity metrics
        complex_df = report['complexity_metrics']
        complex_df.to_csv(f'{output_dir}/model_complexity_metrics.csv', index=False)
        
        # Redundant joins
        if report['redundant_joins']:
            redundant_df = pd.DataFrame(report['redundant_joins'])
            # Convert paths list to string for CSV storage
            redundant_df['paths'] = redundant_df['paths'].apply(lambda x: ' -> '.join(x))
            redundant_df.to_csv(f'{output_dir}/redundant_joins.csv', index=False)
        
        # Similar models
        if report['similar_models']:
            similar_df = pd.DataFrame(report['similar_models'])
            similar_df.to_csv(f'{output_dir}/similar_models.csv', index=False)
        
        # Recommendations
        recommendations_data = []
        for rec in recommendations:
            if 'models' in rec:
                for model in rec['models']:
                    recommendations_data.append({
                        'type': rec['type'],
                        'model': model,
                        'related_model': None,
                        'suggestion': rec['suggestion']
                    })
            elif 'model_pairs' in rec:
                for model1, model2 in rec['model_pairs']:
                    recommendations_data.append({
                        'type': rec['type'],
                        'model': model1,
                        'related_model': model2,
                        'suggestion': rec['suggestion']
                    })
        
        if recommendations_data:
            recommendations_df = pd.DataFrame(recommendations_data)
            recommendations_df.to_csv(f'{output_dir}/refactoring_recommendations.csv', index=False)
        
        return report

# Example usage:
"""
analyzer = DBTRefactorAnalyzer('target/manifest.json')
report = analyzer.generate_refactoring_report()

# Print recommendations
for rec in report['recommendations']:
    print(f"\nRecommendation Type: {rec['type']}")
    print(f"Affected Models: {rec['models'] if 'models' in rec else rec['model_pairs']}")
    print(f"Suggestion: {rec['suggestion']}")

# Get detailed metrics
metrics_df = report['complexity_metrics']
print("\nModel Complexity Metrics:")
print(metrics_df.sort_values('num_joins', ascending=False).head())
"""
print("\nModel Complexity Metrics:")
print(metrics_df.sort_values('num_joins', ascending=False).head())
"""
