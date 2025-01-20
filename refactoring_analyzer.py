import json
import pandas as pd
import os
from difflib import SequenceMatcher

class DBTRefactorAnalyzer:
    def __init__(self, manifest_path):
        """Initialize analyzer with path to dbt manifest"""
        with open(manifest_path) as f:
            self.manifest = json.load(f)
        self.models = {k: v for k, v in self.manifest.get('nodes', {}).items() 
                      if v.get('resource_type') == 'model'}
        
    def get_model_parents(self, model_id):
        """Get immediate parent models of a given model"""
        model = self.models.get(model_id)
        if not model:
            return set()
        
        parents = set()
        for ref in model.get('depends_on', {}).get('nodes', []):
            if ref.startswith('model.'):
                parents.add(ref)
        return parents
    
    def get_model_children(self, model_id):
        """Get immediate child models of a given model"""
        children = set()
        for other_id, other in self.models.items():
            deps = other.get('depends_on', {}).get('nodes', [])
            if model_id in deps:
                children.add(other_id)
        return children
    
    def find_rejoined_concepts(self):
        """Find cases where a model rejoins to upstream concepts unnecessarily"""
        rejoined_patterns = []
        
        for model_id in self.models:
            # Get immediate parents of this model
            parents = self.get_model_parents(model_id)
            
            # For each parent, check its other children
            for parent in parents:
                parent_children = self.get_model_children(parent)
                
                # Look for siblings that this model depends on
                model_parents = self.get_model_parents(model_id)
                sibling_dependencies = parent_children.intersection(model_parents)
                
                for sibling in sibling_dependencies:
                    # Check if sibling only has this model as a child
                    sibling_children = self.get_model_children(sibling)
                    if len(sibling_children) == 1 and model_id in sibling_children:
                        rejoined_patterns.append({
                            'model': model_id,
                            'parent': parent,
                            'intermediate_model': sibling,
                            'suggestion': (
                                f"Model '{model_id}' rejoins to '{parent}' through '{sibling}'. "
                                f"Consider moving the logic from '{sibling}' into a CTE within '{model_id}' "
                                "since it has no other downstream dependencies."
                            )
                        })
        
        return rejoined_patterns
    
    def find_similar_models(self, similarity_threshold=0.7):
        """Find models with similar SQL content"""
        similar_pairs = []
        processed = set()
        
        def clean_sql(sql):
            """Basic SQL cleaning to focus on core logic"""
            sql = sql.lower()
            # Remove comments
            sql = '\n'.join(line for line in sql.split('\n') 
                          if not line.strip().startswith('--'))
            # Remove extra whitespace
            return ' '.join(sql.split())
        
        for model_id1, model1 in self.models.items():
            if model_id1 in processed:
                continue
                
            sql1 = clean_sql(model1.get('raw_sql', ''))
            
            for model_id2, model2 in self.models.items():
                if model_id1 >= model_id2:  # Skip self and processed pairs
                    continue
                    
                sql2 = clean_sql(model2.get('raw_sql', ''))
                
                # Compare SQL similarity
                similarity = SequenceMatcher(None, sql1, sql2).ratio()
                
                # Also compare referenced models
                refs1 = set(tuple(ref) for ref in model1.get('refs', []))
                refs2 = set(tuple(ref) for ref in model2.get('refs', []))
                ref_similarity = len(refs1.intersection(refs2)) / max(len(refs1.union(refs2)), 1)
                
                # Combined similarity score
                combined_similarity = (similarity * 0.7) + (ref_similarity * 0.3)
                
                if combined_similarity >= similarity_threshold:
                    similar_pairs.append({
                        'model1': model_id1,
                        'model2': model_id2,
                        'similarity_score': round(combined_similarity, 3),
                        'sql_similarity': round(similarity, 3),
                        'ref_similarity': round(ref_similarity, 3)
                    })
            
            processed.add(model_id1)
        
        return sorted(similar_pairs, key=lambda x: x['similarity_score'], reverse=True)
    
    def get_model_complexity_metrics(self):
        """Calculate complexity metrics for each model"""
        metrics = []
        
        for model_id, model in self.models.items():
            sql = model.get('raw_sql', '')
            metrics.append({
                'model': model_id,
                'num_joins': sql.lower().count('join'),
                'num_ctes': sql.lower().count('with'),
                'num_refs': len(model.get('refs', [])),
                'num_sources': len(model.get('sources', [])),
                'num_children': len(self.get_model_children(model_id)),
                'num_parents': len(self.get_model_parents(model_id)),
                'sql_length': len(sql)
            })
        
        return pd.DataFrame(metrics)
    
    def generate_refactoring_report(self, output_dir='./dbt_analysis'):
        """Generate comprehensive refactoring recommendations"""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Find rejoined concepts
        rejoined = self.find_rejoined_concepts()
        if rejoined:
            pd.DataFrame(rejoined).to_csv(
                f'{output_dir}/rejoined_concepts.csv', 
                index=False
            )
        
        # Find similar models
        similar = self.find_similar_models()
        if similar:
            pd.DataFrame(similar).to_csv(
                f'{output_dir}/similar_models.csv', 
                index=False
            )
        
        # Get complexity metrics
        metrics = self.get_model_complexity_metrics()
        metrics.to_csv(f'{output_dir}/model_complexity_metrics.csv', index=False)
        
        # Generate recommendations
        recommendations = []
        
        # Add recommendations for rejoined concepts
        for item in rejoined:
            recommendations.append({
                'model': item['model'],
                'type': 'rejoined_concept',
                'related_models': f"{item['parent']} -> {item['intermediate_model']}",
                'suggestion': item['suggestion']
            })
        
        # Add recommendations for similar models
        for item in similar[:10]:  # Top 10 most similar pairs
            recommendations.append({
                'model': item['model1'],
                'type': 'similar_model',
                'related_models': item['model2'],
                'suggestion': (
                    f"Shows {item['similarity_score']:.0%} similarity with {item['model2']}. "
                    "Consider consolidating these models."
                )
            })
        
        # Add recommendations for complex models
        complex_models = metrics[
            (metrics['num_joins'] > 5) | 
            (metrics['num_refs'] > 5) |
            (metrics['sql_length'] > 1000)
        ]
        
        for _, row in complex_models.iterrows():
            recommendations.append({
                'model': row['model'],
                'type': 'complexity',
                'related_models': '',
                'suggestion': (
                    f"Complex model with {row['num_joins']} joins, "
                    f"{row['num_refs']} refs, and {row['sql_length']} chars. "
                    "Consider breaking into smaller models."
                )
            })
        
        if recommendations:
            pd.DataFrame(recommendations).to_csv(
                f'{output_dir}/refactoring_recommendations.csv', 
                index=False
            )
        
        print(f"\nAnalysis complete! Files saved to: {output_dir}")
        print(f"Found:")
        print(f"- {len(rejoined)} cases of rejoined concepts")
        print(f"- {len(similar)} similar model pairs")
        print(f"- {len(complex_models)} complex models")
        
        return {
            'rejoined_concepts': rejoined,
            'similar_models': similar,
            'complexity_metrics': metrics,
            'recommendations': recommendations
        }
