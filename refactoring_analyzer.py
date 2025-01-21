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

    def get_model_refs(self, model_id):
        """Get all models referenced by this model"""
        model = self.models.get(model_id)
        if not model:
            return set()
        return set(model.get('depends_on', {}).get('nodes', []))
    
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

    def find_redundant_refs(self):
        """Find models that reference both a parent and that parent's parent"""
        redundant_refs = []
        
        for model_id, model in self.models.items():
            # Get all models this model references directly
            direct_refs = self.get_model_refs(model_id)
            
            # For each referenced model (parent)
            for parent_ref in direct_refs:
                if parent_ref not in self.models:
                    continue
                    
                # Get what the parent references (grandparents)
                parent_refs = self.get_model_refs(parent_ref)
                
                # Check if we reference any of our parent's refs (grandparents)
                redundant = direct_refs.intersection(parent_refs)
                
                if redundant:
                    for grandparent in redundant:
                        redundant_refs.append({
                            'model': model_id,
                            'parent': parent_ref,
                            'grandparent': grandparent,
                            'suggestion': (
                                f"Model '{model_id}' references both '{parent_ref}' and '{grandparent}', "
                                f"but '{parent_ref}' already includes '{grandparent}'. "
                                f"Consider removing the reference to '{grandparent}' and getting those "
                                f"columns through '{parent_ref}' instead."
                            )
                        })
        
        return redundant_refs

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

    def generate_refactored_sql(self, redundant_ref):
            """Generate refactored SQL code for a model with redundant refs"""
            model = self.models.get(redundant_ref['model'])
            parent = self.models.get(redundant_ref['parent'])
            grandparent = self.models.get(redundant_ref['grandparent'])
            
            if not all([model, parent, grandparent]):
                return None
            
            # Get original SQL - check different possible locations in the manifest
            original_sql = None
            if 'raw_code' in model:  # Try raw_code first
                original_sql = model['raw_code']
            elif 'raw_sql' in model:  # Try raw_sql next
                original_sql = model['raw_sql']
            elif 'compiled_code' in model:  # Try compiled_code as fallback
                original_sql = model['compiled_code']
            
            if not original_sql:
                print(f"DEBUG: Available keys in model: {list(model.keys())}")
                return None
            
            # Analyze how the grandparent is referenced
            gp_name = grandparent.get('name', grandparent['unique_id'].split('.')[-1])
            p_name = parent.get('name', parent['unique_id'].split('.')[-1])
            
            # Find ref patterns for grandparent (expanded to catch more variations)
            ref_patterns = [
                f"ref('{gp_name}')",
                f'ref("{gp_name}")',
                f"ref('{gp_name}') as",
                f'ref("{gp_name}") as',
                f"ref('{gp_name}') ",
                f'ref("{gp_name}") ',
                f"ref('{gp_name}')]",
                f'ref("{gp_name}")]'
            ]
            
            # Find the full ref line(s) that reference the grandparent
            sql_lines = original_sql.split('\n')
            gp_ref_lines = []
            ref_aliases = []
            
            for i, line in enumerate(sql_lines):
                line_lower = line.lower()
                if any(pattern.lower() in line_lower for pattern in ref_patterns):
                    gp_ref_lines.append((i, line))
                    # Try to extract alias - look for both 'as' and into CTE patterns
                    if ' as ' in line_lower:
                        alias = line_lower.split(' as ')[-1].strip().strip(',')
                        ref_aliases.append(alias)
                    elif 'with ' in line_lower or ',' in line_lower:
                        parts = line.split('ref')
                        if len(parts) > 1:
                            alias_part = parts[0].strip()
                            if alias_part:
                                ref_aliases.append(alias_part.strip())
            
            # If we found the references, create refactored SQL
            if gp_ref_lines:
                refactored_sql = []
                changes_made = []
                
                # Start with any config blocks
                in_config = False
                for line in sql_lines:
                    if '{{' in line and 'config' in line:
                        in_config = True
                        refactored_sql.append(line)
                        continue
                    if in_config:
                        refactored_sql.append(line)
                        if '}}' in line:
                            in_config = False
                        continue
                    break
                
                # Add comment explaining the change
                refactored_sql.extend([
                    f"-- Refactored to remove redundant reference to {gp_name}",
                    f"-- These columns are now accessed through {p_name}",
                    ""
                ])
                
                # Process the rest of the SQL
                for i, line in enumerate(sql_lines):
                    # Skip config blocks we already processed
                    if in_config:
                        if '}}' in line:
                            in_config = False
                        continue
                        
                    # Skip the lines that reference the grandparent
                    if any(i == idx for idx, _ in gp_ref_lines):
                        changes_made.append(f"Removed reference: {line.strip()}")
                        continue
                    
                    # Replace any references to the grandparent alias in joins
                    line_modified = line
                    for alias in ref_aliases:
                        if alias and alias in line:
                            # Check if this is a join condition
                            if 'join' in line.lower() and 'on' in line.lower():
                                # Replace the grandparent alias with the appropriate parent column
                                line_modified = line.replace(alias, p_name)
                                changes_made.append(f"Modified join: {line.strip()} -> {line_modified.strip()}")
                            # Also check for column references
                            elif alias + '.' in line:
                                line_modified = line.replace(alias + '.', p_name + '.')
                                changes_made.append(f"Modified column reference: {line.strip()} -> {line_modified.strip()}")
                    
                    refactored_sql.append(line_modified)
                
                return {
                    'original_sql': original_sql,
                    'refactored_sql': '\n'.join(refactored_sql),
                    'changes_made': changes_made,
                    'model_name': model.get('name', model['unique_id'].split('.')[-1]),
                    'removed_ref': gp_name,
                    'use_parent': p_name
                }
            
            return None
    
    def generate_refactoring_report(self, output_dir='./dbt_analysis'):
        """Generate comprehensive refactoring recommendations"""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Find redundant refs and generate SQL refactoring suggestions
        redundant = self.find_redundant_refs()
        refactored_models = []
        
        if redundant:
            print("\nDEBUG: Found redundant refs:")
            for ref in redundant:
                print(f"\nModel: {ref['model']}")
                model = self.models.get(ref['model'])
                print(f"Model name exists: {model is not None}")
                if model:
                    print(f"Model raw SQL exists: {'raw_sql' in model}")
                    print(f"Model name field exists: {'name' in model}")
                
            # Generate refactored SQL for each redundant ref
            for ref in redundant:
                print(f"\nAttempting to refactor: {ref['model']}")
                refactored = self.generate_refactored_sql(ref)
                if refactored:
                    print(f"Successfully refactored!")
                    refactored_models.append(refactored)
                    
                    # Save the refactored SQL to a file
                    model_dir = os.path.join(output_dir, 'refactored_models')
                    if not os.path.exists(model_dir):
                        os.makedirs(model_dir)
                    
                    with open(os.path.join(model_dir, f"{refactored['model_name']}.sql"), 'w') as f:
                        f.write(refactored['refactored_sql'])
                    
                    # Add detailed changes to the recommendation
                    ref['sql_changes'] = refactored['changes_made']
                    ref['refactored_file'] = f"refactored_models/{refactored['model_name']}.sql"
                else:
                    print(f"Failed to refactor - could not generate SQL")
            
            # Save redundant refs with SQL change details
            pd.DataFrame(redundant).to_csv(
                f'{output_dir}/redundant_refs.csv', 
                index=False
            )
            
            # Create a detailed refactoring guide
            with open(f'{output_dir}/refactoring_guide.md', 'w') as f:
                f.write("# DBT Model Refactoring Guide\n\n")
                
                for model in refactored_models:
                    f.write(f"## {model['model_name']}\n\n")
                    f.write(f"Remove reference to `{model['removed_ref']}` and use columns from `{model['use_parent']}`\n\n")
                    f.write("### Changes Made:\n")
                    for change in model['changes_made']:
                        f.write(f"- {change}\n")
                    f.write("\n### Original SQL:\n")
                    f.write("```sql\n")
                    f.write(model['original_sql'])
                    f.write("\n```\n\n")
                    f.write("### Refactored SQL:\n")
                    f.write("```sql\n")
                    f.write(model['refactored_sql'])
                    f.write("\n```\n\n")
        
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
        
        # Find similar models
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

        # Add recommendations for redundant refs
        for item in redundant:
            recommendations.append({
                'model': item['model'],
                'type': 'redundant_ref',
                'related_models': f"{item['parent']} -> {item['grandparent']}",
                'suggestion': item['suggestion'],
                'refactored_file': item.get('refactored_file', ''),
                'sql_changes': '\n'.join(item.get('sql_changes', []))
            })
        
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
        print(f"- {len(redundant)} cases of redundant refs (with SQL refactoring suggestions)")
        print(f"- {len(rejoined)} cases of rejoined concepts")
        print(f"- {len(combinable)} combinable intermediate models")
        print(f"- {len(similar)} similar model pairs")
        print(f"- {len(complex_models)} complex models")
        
        if refactored_models:
            print(f"\nRefactored SQL models saved to: {output_dir}/refactored_models/")
            print(f"Detailed refactoring guide available at: {output_dir}/refactoring_guide.md")
        
        return {
            'redundant_refs': redundant,
            'rejoined_concepts': rejoined,
            'combinable_intermediates': combinable,
            'similar_models': similar,
            'complexity_metrics': metrics,
            'recommendations': recommendations,
            'refactored_models': refactored_models if refactored_models else []
        }
