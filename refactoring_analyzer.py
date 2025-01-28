import json
import pandas as pd
import os
from difflib import SequenceMatcher
from collections import defaultdict
import re
from dataclasses import dataclass
from typing import Dict, List, Set, Optional, Tuple
import sqlparse
from sqlparse.sql import Token, TokenList, Identifier, Where
from sqlparse.tokens import Keyword, DML, Punctuation

@dataclass
class CTEReference:
    """Represents a CTE and its dependencies"""
    name: str
    dependencies: Set[str]
    columns_used: Set[str]
    filters: List[str]
    is_constant: bool
    raw_sql: str

@dataclass
class SQLComponent:
    """Represents the main components of a SQL query"""
    config: Optional[str]
    ctes: Dict[str, CTEReference]
    main_query: str
    column_refs: Dict[str, Set[str]]

@dataclass
class ModelDependency:
    """Represents dependencies between models"""
    source_model: str
    target_model: str
    dependency_type: str  # 'direct', 'indirect', 'redundant'
    path: List[str]
    columns_used: Set[str]

class DBTRefactorAnalyzer:
    def __init__(self, manifest_path):
        """Initialize analyzer with path to dbt manifest"""
        with open(manifest_path) as f:
            self.manifest = json.load(f)
        self.models = {k: v for k, v in self.manifest.get('nodes', {}).items() 
                      if v.get('resource_type') == 'model'}
        self.column_cache = {}
        self.dependency_graph = self._build_dependency_graph()
    
    def _build_dependency_graph(self) -> Dict[str, Set[str]]:
        """Build a graph of model dependencies"""
        graph = defaultdict(set)
        for model_id, model in self.models.items():
            deps = model.get('depends_on', {}).get('nodes', [])
            for dep in deps:
                if dep.startswith('model.'):
                    graph[model_id].add(dep)
        return graph

    def get_model_refs(self, model_id: str) -> Set[str]:
        """Get all models referenced by this model"""
        return self.dependency_graph.get(model_id, set())
    
    def get_model_parents(self, model_id: str) -> Set[str]:
        """Get immediate parent models of a given model"""
        return {ref for ref in self.get_model_refs(model_id) if ref.startswith('model.')}
    
    def get_model_children(self, model_id: str) -> Set[str]:
        """Get immediate child models of a given model"""
        return {model for model, deps in self.dependency_graph.items() if model_id in deps}

    def get_all_ancestors(self, model_id: str, max_depth: int = None) -> Set[str]:
        """Get all ancestor models up to max_depth levels up"""
        ancestors = set()
        to_visit = [(model_id, 0)]
        visited = set()

        while to_visit:
            current, depth = to_visit.pop(0)
            if current in visited:
                continue
            if max_depth is not None and depth > max_depth:
                continue

            visited.add(current)
            parents = self.get_model_parents(current)
            ancestors.update(parents)
            
            for parent in parents:
                to_visit.append((parent, depth + 1))

        return ancestors

    def get_all_descendants(self, model_id: str, max_depth: int = None) -> Set[str]:
        """Get all descendant models up to max_depth levels down"""
        descendants = set()
        to_visit = [(model_id, 0)]
        visited = set()

        while to_visit:
            current, depth = to_visit.pop(0)
            if current in visited:
                continue
            if max_depth is not None and depth > max_depth:
                continue

            visited.add(current)
            children = self.get_model_children(current)
            descendants.update(children)
            
            for child in children:
                to_visit.append((child, depth + 1))

        return descendants

    def parse_sql_components(self, sql: str) -> SQLComponent:
        """Parse SQL into detailed components including CTEs, configs, and column usage"""
        # Extract config block first
        config_pattern = r'({{\s*config[^}]+}})'
        config_match = re.search(config_pattern, sql, re.DOTALL)
        config = config_match.group(1) if config_match else None
        
        # Remove config block from SQL for further processing
        if config_match:
            sql = sql[config_match.end():].strip()
                
        def extract_columns(token_list):
            """Extract column references from a token list"""
            columns = set()
            for token in token_list.flatten():
                if token.ttype in (None,) and token.value not in ('(', ')', ',', '.'):
                    columns.add(token.value)
            return columns

        def parse_cte_structure(token_list):
            """Parse CTE structure including dependencies and filters"""
            cte_name = None
            dependencies = set()
            columns = set()
            filters = []
            
            for token in token_list.tokens:
                if token.ttype is None and token.value.lower() != 'as':
                    cte_name = token.value
                elif isinstance(token, TokenList):
                    # Extract ref dependencies
                    refs = re.findall(r"ref\(['\"]([^'\"]+)['\"]\)", str(token))
                    dependencies.update(refs)
                    
                    # Extract columns
                    for sub_token in token.flatten():
                        if sub_token.ttype in (None,) and sub_token.value not in ('(', ')', ',', '.'):
                            columns.add(sub_token.value)
                    
                    # Extract filters
                    if isinstance(token, Where):
                        filters.append(str(token))
                        
            return dependencies, columns, filters
        
        def is_constant_cte(token_list):
            """Check if CTE only contains constant values or simple selects"""
            sql = str(token_list)
            constant_patterns = [
                r'select\s+[^()]+\s+as\s+\w+\s*$',  # Simple column alias
                r'select\s+\d+',  # Numeric constant
                r"select\s+'[^']+'",  # String constant
                r'select\s+current_date',  # Date functions
                r'select\s+getdate\(\)',
                r'select\s+[^;]+from\s+\w+\s+where\s+1\s*=\s*1'  # Constant filter
            ]
            return any(re.search(pattern, sql, re.IGNORECASE) for pattern in constant_patterns)
    
        # Process tokens
        parsed = sqlparse.parse(sql)[0]
        ctes = {}
        column_refs = {}
        
        for token in parsed.tokens:
            if token.is_whitespace:
                continue
                
            if token.ttype is Keyword and token.value.lower() == 'with':
                continue
                
            if isinstance(token, Identifier) and token.has_alias():
                deps, cols, filters = parse_cte_structure(token)
                cte_name = token.get_name()
                if cte_name:
                    ctes[cte_name] = CTEReference(
                        name=cte_name,
                        dependencies=deps,
                        columns_used=cols,
                        filters=filters,
                        is_constant=is_constant_cte(token),
                        raw_sql=str(token)
                    )
                    column_refs[cte_name] = cols
        
        # Extract main query
        main_query_tokens = []
        in_main = False
        for token in parsed.tokens:
            if token.ttype is DML and token.value.lower() == 'select':
                in_main = True
            if in_main:
                main_query_tokens.append(str(token))
        
        return SQLComponent(
            config=config,
            ctes=ctes,
            main_query=''.join(main_query_tokens),
            column_refs=column_refs
        )
    
    def analyze_cte_dependencies(self, sql_component: SQLComponent) -> Dict[str, Set[str]]:
        """Analyze CTE dependencies including transitive dependencies"""
        direct_deps = {cte.name: cte.dependencies for cte in sql_component.ctes.values()}
        all_deps = {}
        
        def get_all_deps(cte_name: str, seen: Set[str]) -> Set[str]:
            if cte_name in seen:
                return set()
            if cte_name in all_deps:
                return all_deps[cte_name]
                
            deps = direct_deps.get(cte_name, set()).copy()
            seen.add(cte_name)
            
            for dep in direct_deps.get(cte_name, set()):
                deps.update(get_all_deps(dep, seen))
                
            all_deps[cte_name] = deps
            return deps
        
        for cte_name in direct_deps:
            if cte_name not in all_deps:
                all_deps[cte_name] = get_all_deps(cte_name, set())
            
        return all_deps
    
    def analyze_column_lineage(self, sql_component: SQLComponent) -> Dict[str, Set[str]]:
        """Analyze column lineage through CTEs and main query"""
        lineage = defaultdict(set)
        
        # Analyze column flow through CTEs
        cte_deps = self.analyze_cte_dependencies(sql_component)
        
        for cte_name, cte in sql_component.ctes.items():
            # Track direct column references
            for col in cte.columns_used:
                lineage[f"{cte_name}.{col}"].add(col)
                
            # Track columns from dependencies
            for dep in cte_deps[cte_name]:
                if dep in sql_component.column_refs:
                    for col in sql_component.column_refs[dep]:
                        lineage[f"{cte_name}.{col}"].add(f"{dep}.{col}")
        
        return dict(lineage)

    def generate_refactored_sql(self, redundant_ref):
            """Generate refactored SQL code for a model with redundant refs"""
            model = self.models.get(redundant_ref['model'])
            parent = self.models.get(redundant_ref['parent'])
            grandparent = self.models.get(redundant_ref['grandparent'])
            
            if not all([model, parent, grandparent]):
                return None
            
            # Get original SQL from available fields
            original_sql = None
            for field in ['raw_code', 'raw_sql', 'compiled_code', 'compiled_sql', 'sql']:
                if field in model and model[field]:
                    original_sql = model[field]
                    print(f"Found SQL in field: {field}")
                    break
            
            if not original_sql:
                return None
            
            # Parse SQL into components
            sql_component = self.parse_sql_components(original_sql)
            
            # Analyze dependencies
            deps = self.analyze_cte_dependencies(sql_component)
            
            # Get column lineage
            column_lineage = self.analyze_column_lineage(sql_component)
            
            # Get model names
            gp_name = grandparent.get('name', grandparent['unique_id'].split('.')[-1])
            p_name = parent.get('name', parent['unique_id'].split('.')[-1])
            
            # Track changes and refactoring decisions
            changes_made = []
            refactoring_decisions = {
                'removed_ctes': set(),
                'modified_ctes': {},
                'merged_filters': defaultdict(list),
                'column_mappings': {}
            }
            
            def should_remove_cte(cte_name: str, cte: CTEReference) -> bool:
                """Determine if a CTE should be removed based on analysis"""
                if gp_name not in cte.dependencies:
                    return False
                    
                # Don't remove if it has complex transformations
                if not cte.is_constant and len(cte.columns_used) > len(cte.filters):
                    return False
                    
                # Don't remove if other CTEs depend on it (unless they're also being removed)
                dependent_ctes = {name for name, deps in deps.items() if cte_name in deps}
                remaining_deps = dependent_ctes - refactoring_decisions['removed_ctes']
                if remaining_deps:
                    return False
                    
                return True
    
            def process_cte_filters(cte: CTEReference) -> List[str]:
                """Process and normalize filter conditions from a CTE"""
                processed_filters = []
                for filter_str in cte.filters:
                    # Replace CTE references with appropriate model references
                    filter_modified = filter_str
                    for old_cte in refactoring_decisions['removed_ctes']:
                        filter_modified = re.sub(
                            rf'\b{old_cte}\b',
                            f"ref('{p_name}')",
                            filter_modified,
                            flags=re.IGNORECASE
                        )
                    processed_filters.append(filter_modified)
                return processed_filters
    
            # First pass: identify CTEs to remove
            for cte_name, cte in sql_component.ctes.items():
                if should_remove_cte(cte_name, cte):
                    refactoring_decisions['removed_ctes'].add(cte_name)
                    changes_made.append(f"Removing CTE: {cte_name}")
                    
                    if cte.filters:
                        refactoring_decisions['merged_filters'][cte_name].extend(
                            process_cte_filters(cte)
                        )
    
            # Second pass: process remaining CTEs
            processed_ctes = []
            for cte_name, cte in sql_component.ctes.items():
                if cte_name in refactoring_decisions['removed_ctes']:
                    continue
                    
                # Modify CTE content
                modified_sql = cte.raw_sql
                
                # Replace references to removed CTEs
                for removed_cte in refactoring_decisions['removed_ctes']:
                    modified_sql = re.sub(
                        rf'\b{removed_cte}\b',
                        f"ref('{p_name}')",
                        modified_sql,
                        flags=re.IGNORECASE
                    )
                
                # Update filters if needed
                if cte.filters:
                    modified_filters = process_cte_filters(cte)
                    if modified_filters != cte.filters:
                        refactoring_decisions['modified_ctes'][cte_name] = modified_filters
                        changes_made.append(f"Modified filters in CTE: {cte_name}")
                
                # Add to processed CTEs with proper formatting
                prefix = 'with ' if len(processed_ctes) == 0 else ','
                processed_ctes.append(f"{prefix} {cte_name} as ({modified_sql})")
    
            # Generate refactored SQL
            refactored_sql = []
            
            # Add config
            if sql_component.config:
                refactored_sql.append(sql_component.config)
                refactored_sql.append('')
            
            # Add refactoring comments
            refactored_sql.extend([
                f"-- Refactored to remove redundant reference to {gp_name}",
                f"-- These columns are now accessed through {p_name}",
                ""
            ])
            
            # Add processed CTEs
            if processed_ctes:
                refactored_sql.extend(processed_ctes)
            
            # Process main query
            main_query = sql_component.main_query
            
            # Replace CTE references in main query
            for removed_cte in refactoring_decisions['removed_ctes']:
                main_query = re.sub(
                    rf'\b{removed_cte}\b',
                    f"ref('{p_name}')",
                    main_query,
                    flags=re.IGNORECASE
                )
            
            # Merge filters from removed CTEs
            merged_filters = []
            for filters in refactoring_decisions['merged_filters'].values():
                merged_filters.extend(filters)
            
            if merged_filters:
                # Add merged filters to WHERE clause
                if 'where' in main_query.lower():
                    for filter_condition in merged_filters:
                        main_query = re.sub(
                            r'where',
                            f"where {filter_condition} and",
                            main_query,
                            flags=re.IGNORECASE,
                            count=1
                        )
                else:
                    main_query += f"\nwhere " + " and ".join(merged_filters)
            
            refactored_sql.append(main_query)
            
            return {
                'original_sql': original_sql,
                'refactored_sql': '\n'.join(refactored_sql),
                'changes_made': changes_made,
                'model_name': model.get('name', model['unique_id'].split('.')[-1]),
                'removed_ref': gp_name,
                'use_parent': p_name,
                'refactoring_decisions': refactoring_decisions
            }

    def find_redundant_refs(self):
            """Find models that reference both a parent and that parent's parent"""
            redundant_refs = []
            
            def analyze_ref_necessity(model_id: str, direct_ref: str, indirect_ref: str) -> bool:
                """Analyze if the direct reference to a grandparent is truly necessary"""
                model = self.models[model_id]
                sql_component = self.parse_sql_components(model.get('raw_sql', ''))
                column_lineage = self.analyze_column_lineage(sql_component)
                
                # Check if columns from grandparent are used in ways that parent doesn't support
                grandparent_cols = {col for col in column_lineage.values() if indirect_ref in str(col)}
                parent_cols = {col for col in column_lineage.values() if direct_ref in str(col)}
                
                # If there are transformations that require direct grandparent access
                return not (grandparent_cols - parent_cols)
            
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
                    
                    for grandparent in redundant:
                        if analyze_ref_necessity(model_id, parent_ref, grandparent):
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
        
        def analyze_join_necessity(model_id: str, parent: str, sibling: str) -> bool:
            """Analyze if a rejoin to a sibling is truly necessary"""
            model = self.models[model_id]
            sql = model.get('raw_sql', '')
            sql_component = self.parse_sql_components(sql)
            
            # Check if the join adds new information or just rejoins same concepts
            join_conditions = []
            for cte in sql_component.ctes.values():
                if sibling in str(cte.raw_sql):
                    # Extract join conditions
                    join_pattern = rf"""
                        join\s+{sibling}\s+
                        (?:as\s+\w+\s+)?
                        on\s+
                        ([^()]+?)\s+
                        (?:where|group|order|limit|$)
                    """
                    matches = re.findall(join_pattern, str(cte.raw_sql), re.IGNORECASE | re.VERBOSE)
                    join_conditions.extend(matches)
            
            # If join conditions only use columns available in parent, it might be unnecessary
            if join_conditions:
                parent_cols = set(self.get_available_columns(parent))
                join_cols = set()
                for condition in join_conditions:
                    join_cols.update(re.findall(r'\b\w+\.\w+\b', condition))
                
                return all(col.split('.')[1] in parent_cols for col in join_cols)
            
            return False

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
                    
                    if (len(sibling_children) == 1 and 
                        model_id in sibling_children and 
                        analyze_join_necessity(model_id, parent, sibling)):
                        
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
    
        def get_available_columns(self, model_id: str) -> Set[str]:
            """Get all columns available in a model, including from its dependencies"""
            if model_id in self.column_cache:
                return self.column_cache[model_id]
                
            model = self.models.get(model_id)
            if not model:
                return set()
                
            # Parse SQL to get columns
            sql = model.get('raw_sql', '')
            sql_component = self.parse_sql_components(sql)
            
            columns = set()
            # Get columns from final SELECT
            if sql_component.main_query:
                # Extract column names from SELECT clause
                select_pattern = r'select(.*?)(?:from|$)'
                match = re.search(select_pattern, sql_component.main_query, re.IGNORECASE | re.DOTALL)
                if match:
                    cols = match.group(1).strip()
                    # Split on commas, handle aliases
                    for col in cols.split(','):
                        col = col.strip()
                        if ' as ' in col.lower():
                            columns.add(col.split(' as ')[-1].strip())
                        else:
                            columns.add(col.split('.')[-1].strip())
            
            self.column_cache[model_id] = columns
            return columns

    def find_similar_models(self, similarity_threshold=0.8):
            """Find models with similar SQL content and dependencies"""
            similar_pairs = []
            processed = set()
    
            def get_model_signature(model):
                """Create a detailed signature for the model based on its structure and patterns"""
                if not model.get('raw_sql'):
                    return None
                    
                sql_component = self.parse_sql_components(model['raw_sql'])
                
                # Get core characteristics
                refs = set(ref for cte in sql_component.ctes.values() for ref in cte.dependencies)
                sources = set(src for src in model.get('sources', []))
                
                # Analyze SQL patterns
                sql = model.get('raw_sql', '').lower()
                
                # Extract meaningful SQL characteristics
                characteristics = {
                    'joins': len(re.findall(r'\bjoin\b', sql)),
                    'left_joins': len(re.findall(r'left\s+join', sql)),
                    'inner_joins': len(re.findall(r'inner\s+join', sql)),
                    'group_by': len(re.findall(r'group\s+by', sql)),
                    'window_funcs': len(re.findall(r'over\s*\(', sql)),
                    'ctes': len(sql_component.ctes),
                    'unions': len(re.findall(r'\bunion\b', sql)),
                    'case_statements': len(re.findall(r'\bcase\b', sql)),
                    'aggregations': len(re.findall(r'\b(sum|avg|count|min|max)\s*\(', sql)),
                    'filters': len(re.findall(r'\bwhere\b', sql))
                }
                
                # Analyze CTE patterns
                cte_patterns = defaultdict(int)
                for cte in sql_component.ctes.values():
                    cte_sql = str(cte.raw_sql).lower()
                    if 'select distinct' in cte_sql:
                        cte_patterns['distinct_selects'] += 1
                    if 'row_number()' in cte_sql:
                        cte_patterns['row_numbers'] += 1
                    if 'partition by' in cte_sql:
                        cte_patterns['partitions'] += 1
                        
                # Combine all signature components
                return {
                    'refs': refs,
                    'sources': sources,
                    'characteristics': characteristics,
                    'cte_patterns': dict(cte_patterns),
                    'column_refs': sql_component.column_refs
                }
    
            def calculate_similarity(sig1, sig2):
                """Calculate detailed similarity score between two model signatures"""
                if not sig1 or not sig2:
                    return 0.0
                    
                # Calculate ref similarity
                ref_similarity = len(sig1['refs'].intersection(sig2['refs'])) / max(
                    len(sig1['refs'].union(sig2['refs'])), 1)
                    
                # Calculate source similarity
                source_similarity = len(sig1['sources'].intersection(sig2['sources'])) / max(
                    len(sig1['sources'].union(sig2['sources'])), 1)
                    
                # Calculate characteristics similarity
                char_similarity = sum(
                    1 for k, v in sig1['characteristics'].items()
                    if sig2['characteristics'].get(k) == v
                ) / len(sig1['characteristics'])
                
                # Calculate CTE pattern similarity
                pattern_keys = set(sig1['cte_patterns'].keys()).union(sig2['cte_patterns'].keys())
                if pattern_keys:
                    pattern_similarity = sum(
                        1 for k in pattern_keys
                        if sig1['cte_patterns'].get(k) == sig2['cte_patterns'].get(k)
                    ) / len(pattern_keys)
                else:
                    pattern_similarity = 1.0
                    
                # Calculate column reference similarity
                col_similarity = 0.0
                all_cols = set()
                shared_cols = set()
                
                for cte, cols in sig1['column_refs'].items():
                    all_cols.update(cols)
                    if cte in sig2['column_refs']:
                        shared_cols.update(cols.intersection(sig2['column_refs'][cte]))
                
                for cols in sig2['column_refs'].values():
                    all_cols.update(cols)
                    
                if all_cols:
                    col_similarity = len(shared_cols) / len(all_cols)
                
                # Weight the components
                weights = {
                    'ref': 0.25,
                    'source': 0.15,
                    'char': 0.25,
                    'pattern': 0.15,
                    'column': 0.20
                }
                
                total_similarity = (
                    ref_similarity * weights['ref'] +
                    source_similarity * weights['source'] +
                    char_similarity * weights['char'] +
                    pattern_similarity * weights['pattern'] +
                    col_similarity * weights['column']
                )
                
                return total_similarity
    
            # Group models by rough signature first
            model_groups = defaultdict(list)
            signatures = {}
            
            for model_id, model in self.models.items():
                if model_id in processed:
                    continue
                    
                signature = get_model_signature(model)
                if not signature:
                    continue
                    
                signatures[model_id] = signature
                
                # Create a rough grouping key
                key = (
                    len(signature['refs']),
                    len(signature['sources']),
                    signature['characteristics']['joins'] > 0,
                    signature['characteristics']['group_by'] > 0,
                    bool(signature['cte_patterns'])
                )
                model_groups[key].append(model_id)
    
            # Compare within similar groups
            for group in model_groups.values():
                if len(group) < 2:
                    continue
                    
                for i, model_id1 in enumerate(group):
                    if model_id1 in processed:
                        continue
                        
                    sig1 = signatures[model_id1]
                    
                    for model_id2 in group[i+1:]:
                        sig2 = signatures[model_id2]
                        
                        similarity = calculate_similarity(sig1, sig2)
                        
                        if similarity >= similarity_threshold:
                            similar_pairs.append({
                                'model1': model_id1,
                                'model2': model_id2,
                                'total_similarity': round(similarity, 3),
                                'shared_refs': list(sig1['refs'].intersection(sig2['refs'])),
                                'shared_patterns': {
                                    k: v for k, v in sig1['cte_patterns'].items()
                                    if sig2['cte_patterns'].get(k) == v
                                },
                                'suggestion': self._generate_similarity_suggestion(
                                    model_id1, model_id2, sig1, sig2)
                            })
                    
                    processed.add(model_id1)
            
            return sorted(similar_pairs, key=lambda x: x['total_similarity'], reverse=True)
    
        def _generate_similarity_suggestion(self, model1_id, model2_id, sig1, sig2):
            """Generate detailed suggestion for similar models"""
            model1_name = model1_id.split('.')[-1]
            model2_name = model2_id.split('.')[-1]
            
            shared_refs = sig1['refs'].intersection(sig2['refs'])
            shared_patterns = {
                k: v for k, v in sig1['cte_patterns'].items()
                if sig2['cte_patterns'].get(k) == v
            }
            
            suggestion = [
                f"Models '{model1_name}' and '{model2_name}' show significant similarity in structure and logic."
            ]
            
            if shared_refs:
                suggestion.append(f"They share {len(shared_refs)} upstream dependencies.")
                
            if shared_patterns:
                pattern_list = [f"{k} ({v} occurrences)" for k, v in shared_patterns.items()]
                suggestion.append(f"Common patterns: {', '.join(pattern_list)}.")
                
            suggestion.append(
                "Consider creating a shared intermediate model for common logic "
                "or combining these models if they serve similar business purposes."
            )
            
            return " ".join(suggestion)

    def find_combinable_intermediates(self):
            """Find intermediate models that could potentially be combined"""
            combinable = []
            
            def analyze_combination_feasibility(model1_id: str, model2_id: str) -> dict:
                """Analyze whether two models can be feasibly combined"""
                model1 = self.models[model1_id]
                model2 = self.models[model2_id]
                
                # Parse both models
                sql1 = self.parse_sql_components(model1.get('raw_sql', ''))
                sql2 = self.parse_sql_components(model2.get('raw_sql', ''))
                
                # Analyze dependencies
                deps1 = self.get_model_refs(model1_id)
                deps2 = self.get_model_refs(model2_id)
                shared_deps = deps1.intersection(deps2)
                
                # Analyze columns
                cols1 = self.get_available_columns(model1_id)
                cols2 = self.get_available_columns(model2_id)
                shared_cols = cols1.intersection(cols2)
                
                # Check for conflicting transformations
                conflicts = []
                if sql1.ctes.keys() & sql2.ctes.keys():
                    conflicts.append("Overlapping CTE names")
                    
                # Check for complex window functions or aggregations that might be hard to combine
                complexity_factors = []
                for sql in [str(sql1.main_query), str(sql2.main_query)]:
                    if 'partition by' in sql.lower():
                        complexity_factors.append("Uses window partitioning")
                    if 'dense_rank()' in sql.lower() or 'row_number()' in sql.lower():
                        complexity_factors.append("Uses ranking functions")
                        
                return {
                    'shared_dependencies': shared_deps,
                    'shared_columns': shared_cols,
                    'conflicts': conflicts,
                    'complexity_factors': complexity_factors,
                    'feasible': not conflicts and len(complexity_factors) < 2
                }
    
            # Find intermediate models
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
                        feasibility = analyze_combination_feasibility(model_id, child_id)
                        if feasibility['feasible']:
                            combinable.append({
                                'model': model_id,
                                'related_model': child_id,
                                'pattern': 'single_child',
                                'reason': (
                                    f'Single child intermediate model that feeds into another '
                                    f'intermediate model ({child_id})'
                                ),
                                'shared_deps': len(feasibility['shared_dependencies']),
                                'shared_cols': len(feasibility['shared_columns']),
                                'suggestion': self._generate_combination_suggestion(
                                    model_id, child_id, feasibility)
                            })
                
                # Case 2: Intermediate model with single parent
                if len(parents) == 1:
                    parent_id = list(parents)[0]
                    # If parent is also an intermediate model
                    if parent_id.split('.')[-1].startswith('int_'):
                        # Check if parent only feeds this and similar models
                        parent_children = self.get_model_children(parent_id)
                        if len(parent_children) <= 2:
                            feasibility = analyze_combination_feasibility(model_id, parent_id)
                            if feasibility['feasible']:
                                combinable.append({
                                    'model': model_id,
                                    'related_model': parent_id,
                                    'pattern': 'single_parent',
                                    'reason': (
                                        f'Single parent intermediate model that comes from another '
                                        f'intermediate model ({parent_id})'
                                    ),
                                    'shared_deps': len(feasibility['shared_dependencies']),
                                    'shared_cols': len(feasibility['shared_columns']),
                                    'suggestion': self._generate_combination_suggestion(
                                        model_id, parent_id, feasibility)
                                })
            
            return combinable
    
        def _generate_combination_suggestion(self, model1_id: str, model2_id: str, 
                                          feasibility: dict) -> str:
            """Generate detailed suggestion for combining intermediate models"""
            model1_name = model1_id.split('.')[-1]
            model2_name = model2_id.split('.')[-1]
            
            suggestion = [
                f"Models '{model1_name}' and '{model2_name}' are good candidates for combination."
            ]
            
            if feasibility['shared_dependencies']:
                suggestion.append(
                    f"They share {len(feasibility['shared_dependencies'])} upstream dependencies."
                )
                
            if feasibility['shared_columns']:
                suggestion.append(
                    f"They share {len(feasibility['shared_columns'])} columns, "
                    "suggesting overlapping business concepts."
                )
                
            suggestion.append(
                "Consider combining these models to reduce the number of intermediate "
                "transformations and simplify the DAG."
            )
            
            if feasibility.get('complexity_factors'):
                suggestion.append(
                    "Note: Some complex transformations present. "
                    "Review carefully before combining."
                )
                
            return " ".join(suggestion)
    
        def get_model_complexity_metrics(self):
            """Calculate complexity metrics for each model"""
            metrics = []
            
            for model_id, model in self.models.items():
                sql = model.get('raw_sql', '')
                if not sql:
                    continue
                    
                sql_component = self.parse_sql_components(sql)
                
                # Calculate various complexity metrics
                metrics.append({
                    'model': model_id,
                    'num_joins': len(re.findall(r'\bjoin\b', sql.lower())),
                    'num_ctes': len(sql_component.ctes),
                    'num_refs': len(model.get('refs', [])),
                    'num_sources': len(model.get('sources', [])),
                    'num_children': len(self.get_model_children(model_id)),
                    'num_parents': len(self.get_model_parents(model_id)),
                    'sql_length': len(sql),
                    'num_window_funcs': len(re.findall(r'over\s*\(', sql.lower())),
                    'num_aggregations': len(re.findall(r'\b(sum|avg|count|min|max)\s*\(', sql.lower())),
                    'num_case_statements': len(re.findall(r'\bcase\b', sql.lower())),
                    'complexity_score': self._calculate_complexity_score(sql_component)
                })
            
            return pd.DataFrame(metrics)

    def _calculate_complexity_score(self, sql_component: SQLComponent) -> float:
            """Calculate a complexity score for a model based on various factors"""
            weights = {
                'ctes': 1.0,
                'joins': 1.5,
                'window_funcs': 2.0,
                'aggregations': 1.0,
                'case_statements': 0.5,
                'dependencies': 1.0,
                'filters': 0.5
            }
            
            sql = str(sql_component.main_query)
            
            factors = {
                'ctes': len(sql_component.ctes),
                'joins': len(re.findall(r'\bjoin\b', sql.lower())),
                'window_funcs': len(re.findall(r'over\s*\(', sql.lower())),
                'aggregations': len(re.findall(r'\b(sum|avg|count|min|max)\s*\(', sql.lower())),
                'case_statements': len(re.findall(r'\bcase\b', sql.lower())),
                'dependencies': len(set().union(*(cte.dependencies for cte in sql_component.ctes.values()))),
                'filters': len(re.findall(r'\bwhere\b', sql.lower()))
            }
            
            score = sum(count * weights[factor] for factor, count in factors.items())
            
            # Normalize to 0-100 scale
            return min(100, score * 5)

    def _generate_markdown_report(self, output_dir: str, results: dict, recommendations: list):
        """Generate a detailed markdown report of all findings and recommendations"""
        report_path = os.path.join(output_dir, 'refactoring_guide.md')
        
        with open(report_path, 'w') as f:
            # Write header
            f.write("# DBT Model Refactoring Guide\n\n")
            
            # Write summary
            f.write("## Summary of Findings\n\n")
            f.write(f"- Found {len(results['redundant_refs'])} redundant references\n")
            f.write(f"- Found {len(results['rejoined_concepts'])} rejoined concepts\n")
            f.write(f"- Found {len(results['combinable_intermediates'])} combinable intermediate models\n")
            f.write(f"- Found {len(results['similar_models'])} similar model pairs\n")
            f.write("\n")
    
            # Group recommendations by priority
            priority_groups = {
                'High': [],
                'Medium': [],
                'Low': []
            }
            
            for rec in recommendations:
                priority_groups[rec['priority']].append(rec)
    
            # Write recommendations by priority
            for priority in ['High', 'Medium', 'Low']:
                if priority_groups[priority]:
                    f.write(f"## {priority} Priority Recommendations\n\n")
                    
                    for rec in priority_groups[priority]:
                        f.write(f"### {rec['model']}\n")
                        f.write(f"**Type**: {rec['type']}\n\n")
                        
                        if rec['related_models']:
                            f.write(f"**Related Models**: {rec['related_models']}\n\n")
                        
                        f.write(f"**Suggestion**: {rec['suggestion']}\n\n")
                        
                        if 'changes_made' in rec and rec['changes_made']:
                            f.write("**Proposed Changes**:\n")
                            f.write("```\n")
                            f.write(rec['changes_made'])
                            f.write("\n```\n\n")
                        
                        if 'refactored_file' in rec and rec['refactored_file']:
                            f.write(f"**Refactored SQL**: See [{rec['refactored_file']}]({rec['refactored_file']})\n\n")
                        
                        f.write("---\n\n")
    
            # Write detailed sections
            if results['redundant_refs']:
                f.write("## Detailed Analysis: Redundant References\n\n")
                for ref in results['redundant_refs']:
                    f.write(f"- Model `{ref['model']}` redundantly references `{ref['grandparent']}`\n")
                    f.write(f"  - Can access through: `{ref['parent']}`\n\n")
    
            if results['rejoined_concepts']:
                f.write("## Detailed Analysis: Rejoined Concepts\n\n")
                for concept in results['rejoined_concepts']:
                    f.write(f"- Model `{concept['model']}` rejoins through `{concept['intermediate_model']}`\n")
                    f.write(f"  - Original parent: `{concept['parent']}`\n\n")
    
            if results['combinable_intermediates']:
                f.write("## Detailed Analysis: Combinable Intermediates\n\n")
                for combo in results['combinable_intermediates']:
                    f.write(f"- Models `{combo['model']}` and `{combo['related_model']}` can be combined\n")
                    f.write(f"  - Pattern: {combo['pattern']}\n")
                    f.write(f"  - Reason: {combo['reason']}\n\n")
    
            if results['similar_models']:
                f.write("## Detailed Analysis: Similar Models\n\n")
                for pair in results['similar_models'][:10]:  # Top 10 most similar
                    f.write(f"- Models `{pair['model1']}` and `{pair['model2']}`\n")
                    f.write(f"  - Similarity Score: {pair['total_similarity']:.2%}\n")
                    if 'shared_patterns' in pair:
                        f.write("  - Shared Patterns: " + ", ".join(pair['shared_patterns'].keys()) + "\n\n")
    
            # Write appendix with metrics
            if not results['complexity_metrics'].empty:
                f.write("## Appendix: Model Complexity Metrics\n\n")
                f.write("Top 10 most complex models:\n\n")
                
                complex_models = results['complexity_metrics'].nlargest(10, 'complexity_score')
                f.write("| Model | Complexity Score | Joins | CTEs | Refs |\n")
                f.write("|-------|-----------------|-------|------|------|\n")
                
                for _, row in complex_models.iterrows():
                    f.write(f"| {row['model']} | {row['complexity_score']:.0f} | ")
                    f.write(f"{row['num_joins']} | {row['num_ctes']} | {row['num_refs']} |\n")
    
            # Write conclusion
            f.write("\n## Next Steps\n\n")
            f.write("1. Review the high-priority recommendations first\n")
            f.write("2. Test refactored models thoroughly\n")
            f.write("3. Consider implementing changes in phases\n")
            f.write("4. Update documentation after changes\n")
    
    def generate_refactoring_report(self, output_dir='./dbt_analysis'):
        """Generate comprehensive refactoring recommendations"""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Find all patterns
        redundant = self.find_redundant_refs()
        rejoined = self.find_rejoined_concepts()
        combinable = self.find_combinable_intermediates()
        similar = self.find_similar_models(similarity_threshold=0.85)
        metrics = self.get_model_complexity_metrics()
        
        # Generate refactored SQL for redundant refs
        refactored_models = []
        if redundant:
            print("\nProcessing redundant references...")
            for ref in redundant:
                print(f"\nAttempting to refactor: {ref['model']}")
                refactored = self.generate_refactored_sql(ref)
                if refactored:
                    print("Successfully refactored!")
                    refactored_models.append(refactored)
                    
                    # Save refactored SQL
                    model_dir = os.path.join(output_dir, 'refactored_models')
                    os.makedirs(model_dir, exist_ok=True)
                    
                    with open(os.path.join(model_dir, f"{refactored['model_name']}.sql"), 'w') as f:
                        f.write(refactored['refactored_sql'])
                    
                    # Add detailed changes to recommendation
                    ref['sql_changes'] = refactored['changes_made']
                    ref['refactored_file'] = f"refactored_models/{refactored['model_name']}.sql"
        
        # Save analysis results
        results = {
            'redundant_refs': redundant if redundant else [],
            'rejoined_concepts': rejoined if rejoined else [],
            'combinable_intermediates': combinable if combinable else [],
            'similar_models': similar if similar else [],
            'complexity_metrics': metrics if not metrics.empty else pd.DataFrame()
        }
        
        # Generate recommendations DataFrame
        recommendations = []
        
        # Add recommendations for redundant refs
        for item in redundant:
            recommendations.append({
                'model': item['model'],
                'type': 'redundant_ref',
                'related_models': f"{item['parent']} -> {item['grandparent']}",
                'suggestion': item['suggestion'],
                'refactored_file': item.get('refactored_file', ''),
                'priority': 'High',
                'changes_made': '\n'.join(item.get('sql_changes', []))
            })
        
        # Add recommendations for rejoined concepts
        for item in rejoined:
            recommendations.append({
                'model': item['model'],
                'type': 'rejoined_concept',
                'related_models': f"{item['parent']} -> {item['intermediate_model']}",
                'suggestion': item['suggestion'],
                'priority': 'Medium'
            })
        
        # Add recommendations for combinable intermediates
        for item in combinable:
            recommendations.append({
                'model': item['model'],
                'type': 'combinable_intermediate',
                'related_models': item['related_model'],
                'suggestion': item['suggestion'],
                'priority': 'Medium',
                'reason': item['reason']
            })
        
        # Add recommendations for similar models
        for item in similar[:20]:  # Top 20 most similar pairs
            recommendations.append({
                'model': item['model1'],
                'type': 'similar_model',
                'related_models': item['model2'],
                'suggestion': item['suggestion'],
                'priority': 'Low',
                'similarity_score': item['total_similarity']
            })
        
        # Add recommendations for complex models
        complex_models = metrics[
            (metrics['complexity_score'] > 70) |
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
                    f"Complex model with score {row['complexity_score']:.0f}/100. "
                    f"Has {row['num_joins']} joins, {row['num_refs']} refs, "
                    f"and {row['sql_length']} chars. Consider breaking into smaller models."
                ),
                'priority': 'Medium' if row['complexity_score'] > 85 else 'Low'
            })
        
        # Save all results
        if recommendations:
            pd.DataFrame(recommendations).to_csv(
                f'{output_dir}/refactoring_recommendations.csv', 
                index=False
            )
        
        # Save individual analysis results
        for name, data in results.items():
            if isinstance(data, pd.DataFrame) and not data.empty:
                data.to_csv(f'{output_dir}/{name}.csv', index=False)
            elif data:  # For list results
                pd.DataFrame(data).to_csv(f'{output_dir}/{name}.csv', index=False)
        
        # Generate detailed markdown report
        self._generate_markdown_report(output_dir, results, recommendations)
        
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
        
        return results

