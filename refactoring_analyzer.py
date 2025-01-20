import json
import pandas as pd
import os
from difflib import SequenceMatcher
from collections import defaultdict

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

    def find_combinable_intermediates(self):
        """Find intermediate models that could potentially be combined"""
        combinable = []
        int_models = {k: v for k, v in self.models.items() 
                     if k.split('.')[-1].startswith('int_')}
        
        for model_id, model in int_models.items():
            children = self.get_model_children(model_id)
            parents = self.get_model_parents(model_id)
            
            # Case 1: Intermediate model with single child
            if len(children) == 1:
                child_id = list(children)[0]
                child_model = self.models[child_id]
                
                # If child is also an intermediate model
                if child_id.split('.')[-1].startswith('int_'):
                    combinable.append({
                        'model': model_id,
                        'related_model': child_id,
                        'pattern': 'single_child',
                        'reason': f'Single child intermediate model that feeds into another intermediate model ({child_id})',
                        'suggestion': f'Consider combining logic into {child_id}'
                    })
            
            # Case 2: Intermediate model with single parent
            if len(parents) == 1:
                parent_id = list(parents)[0]
                # If parent is also an intermediate model
                if parent_id.split('.')[-1].startswith('int_'):
                    # Check if parent only feeds this and similar models
                    parent_children = self.get_model_children(parent_id)
                    if len(parent_children) <= 2:
                        combinable.append({
                            'model': model_id,
                            'related_model': parent_id,
                            'pattern': 'single_parent',
                            'reason': f'Single parent intermediate model that comes from another intermediate model ({parent_id})',
                            'suggestion': f'Consider combining logic with {parent_id}'
                        })
        
        return combinable

    def find_similar_models(self, similarity_threshold=0.8):
        """Find models with similar SQL content and dependencies"""
        similar_pairs = []
        processed = set()

        def get_model_signature(model):
            """Create a signature for the model based on its dependencies and structure"""
            refs = set(tuple(ref) for ref in model.get('refs', []))
            sources = set(tuple(src) for src in model.get('sources', []))
            sql = model.get('raw_sql', '').lower()
            # Count key SQL features
            features = {
                'joins': sql.count('join'),
                'group_by': sql.count('group by'),
                'window_funcs': sql.count('over ('),
                'ctes': sql.count('with'),
                'unions': sql.count('union'),
            }
            return (refs, sources, features)

        # Group models by rough signature first
        model_groups = defaultdict(list)
        for model_id, model in self.models.items():
            if model_id in processed:
                continue
            
            signature = get_model_signature(model)
            # Create a hash key for rough grouping
            key = (
                len(signature[0]),  # number of refs
                len(signature[1]),  # number of sources
                signature[2]['joins'],  # number of joins
                bool(signature[2]['group_by']),  # has group by
                bool(signature[2]['window_funcs'])  # has window functions
            )
            model_groups[key].append(model_id)

        # Compare only within similar groups
        for group in model_groups.values():
            if len(group) < 2:
                continue
                
            for i, model_id1 in enumerate(group):
                if model_id1 in processed:
                    continue
                    
                model1 = self.models[model_id1]
                sql1 = model1.get('raw_sql', '').lower()
                sig1 = get_model_signature(model1)
                
                for model_id2 in group[i+1:]:
                    model2 = self.models[model_id2]
                    sql2 = model2.get('raw_sql', '').lower()
                    sig2 = get_model_signature(model2)
                    
                    # Compare dependencies
                    ref_similarity = len(sig1[0].intersection(sig2[0])) / max(len(sig1[0].union(sig2[0])), 1)
                    source_similarity = len(sig1[1].intersection(sig2[1])) / max(len(sig1[1].union(sig2[1])), 1)
                    
                    # Compare SQL structure
                    feature_similarity = sum(1 for k in sig1[2] if sig1[2][k] == sig2[2][k]) / len(sig1[2])
                    
                    # Compare SQL content
                    sql_similarity = SequenceMatcher(None, sql1, sql2).ratio()
                    
                    # Calculate weighted similarity score
                    total_similarity = (
                        sql_similarity * 0.4 +
                        ref_similarity * 0.3 +
                        source_similarity * 0.2 +
                        feature_similarity * 0.1
                    )
                    
                    if total_similarity >= similarity_threshold:
                        similar_pairs.append({
                            'model1': model_id1,
                            'model2': model_id2,
                            'total_similarity': round(total_similarity, 3),
                            'sql_similarity': round(sql_similarity, 3),
                            'ref_similarity': round(ref_similarity, 3),
                            'source_similarity': round(source_similarity, 3),
                            'feature_similarity': round(feature_similarity, 3)
                        })
                
                processed.add(model_id1)
        
        return sorted(similar_pairs, key=lambda x: x['total_similarity'], reverse=True)

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

        # Find combinable intermediate models
        combinable = self.find_combinable_intermediates()
        if combinable:
            pd.DataFrame(combinable).to_csv(
                f'{output_dir}/combinable_intermediates.csv', 
                index=False
            )
        
        # Find similar models (with higher threshold for more selective results)
        similar = self.find_similar_models(similarity_threshold=0.85)
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

        # Add recommendations for combinable intermediates
        for item in combinable:
            recommendations.append({
                'model': item['model'],
                'type': 'combinable_intermediate',
                'related_models': item['related_model'],
                'suggestion': item['suggestion'],
                'reason': item['reason']
            })
        
        # Add recommendations for similar models
        for item in similar[:20]:  # Top 20 most similar pairs
            recommendations.append({
                'model': item['model1'],
                'type': 'similar_model',
                'related_models': item['model2'],
                'suggestion': (
                    f"Shows {item['total_similarity']:.0%} similarity with {item['model2']}. "
                    f"SQL similarity: {item['sql_similarity']:.0%}, "
                    f"Ref similarity: {item['ref_similarity']:.0%}"
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
        print(f"- {len(combinable)} combinable intermediate models")
        print(f"- {len(similar)} similar model pairs")
        print(f"- {len(complex_models)} complex models")
        
        return {
            'rejoined_concepts': rejoined,
            'combinable_intermediates': combinable,
            'similar_models': similar,
            'complexity_metrics': metrics,
            'recommendations': recommendations
        }
