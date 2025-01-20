import json
import pandas as pd
import os

class DBTRefactorAnalyzer:
    def __init__(self, manifest_path):
        """Initialize analyzer with path to dbt manifest"""
        with open(manifest_path) as f:
            self.manifest = json.load(f)
        self.models = {k: v for k, v in self.manifest.get('nodes', {}).items() 
                      if v.get('resource_type') == 'model'}
    
    def get_model_complexity_metrics(self):
        """Calculate complexity metrics for each model"""
        metrics = []
        
        for model_id, model in self.models.items():
            sql = model.get('raw_sql', '')
            refs = model.get('refs', [])
            sources = model.get('sources', [])
            
            metrics.append({
                'model': model_id,
                'num_joins': sql.lower().count('join'),
                'num_ctes': sql.lower().count('with'),
                'num_refs': len(refs) if refs else 0,
                'num_sources': len(sources) if sources else 0,
                'sql_length': len(sql)
            })
            
        return pd.DataFrame(metrics)
    
    def find_similar_transformations(self, similarity_threshold=0.8):
        """Find models with similar SQL transformations"""
        from difflib import SequenceMatcher
        
        similar_models = []
        processed_models = set()
        
        for model_id1, model1 in self.models.items():
            if model_id1 in processed_models:
                continue
                
            sql1 = model1.get('raw_sql', '').lower()
            
            for model_id2, model2 in self.models.items():
                if (model_id2 in processed_models or model_id1 == model_id2):
                    continue
                    
                sql2 = model2.get('raw_sql', '').lower()
                
                similarity = SequenceMatcher(None, sql1, sql2).ratio()
                
                if similarity >= similarity_threshold:
                    similar_models.append({
                        'model1': model_id1,
                        'model2': model_id2,
                        'similarity': round(similarity, 3)
                    })
                    
            processed_models.add(model_id1)
            
        return similar_models
    
    def generate_refactoring_report(self, output_dir='./dbt_analysis'):
        """Generate analysis report and save to CSV files"""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # Get complexity metrics
        metrics_df = self.get_model_complexity_metrics()
        metrics_df.to_csv(f'{output_dir}/model_complexity_metrics.csv', index=False)
        
        # Find similar models
        similar_models = self.find_similar_transformations()
        if similar_models:
            similar_df = pd.DataFrame(similar_models)
            similar_df.to_csv(f'{output_dir}/similar_models.csv', index=False)
        
        # Generate recommendations
        complex_models = metrics_df[
            (metrics_df['num_joins'] > 5) | 
            (metrics_df['sql_length'] > 1000)
        ].sort_values('num_joins', ascending=False)
        
        recommendations = []
        
        # Add complex model recommendations
        if not complex_models.empty:
            for _, row in complex_models.iterrows():
                recommendations.append({
                    'model': row['model'],
                    'type': 'complexity',
                    'metrics': f"Joins: {row['num_joins']}, CTEs: {row['num_ctes']}, Refs: {row['num_refs']}",
                    'suggestion': 'Consider breaking into smaller models'
                })
        
        # Add similar model recommendations
        for similar in similar_models:
            recommendations.append({
                'model': similar['model1'],
                'type': 'similarity',
                'metrics': f"Similarity: {similar['similarity']}",
                'suggestion': f'Very similar to {similar["model2"]}, consider consolidating'
            })
        
        if recommendations:
            recommendations_df = pd.DataFrame(recommendations)
            recommendations_df.to_csv(f'{output_dir}/refactoring_recommendations.csv', index=False)
        
        print(f"\nAnalysis complete! Files saved to: {output_dir}")
        print(f"Found {len(complex_models)} complex models and {len(similar_models)} similar model pairs")
        
        return {
            'complexity_metrics': metrics_df,
            'similar_models': similar_models,
            'recommendations': recommendations
        }
