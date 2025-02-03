import sqlparse
import os
import re
from typing import List, Dict, Optional
from dataclasses import dataclass
import argparse

@dataclass
class LintingError:
    message: str
    line_number: int
    suggestion: Optional[str] = None

class SQLLinter:
    def __init__(self):
        self.errors: List[LintingError] = []
        self.keywords = [
            "select", "from", "where", "join", "on", "group by", 
            "order by", "and", "or", "having", "left", "right", 
            "inner", "outer", "cross", "union", "intersect", "except"
        ]

    def check_case(self, sql: str) -> None:
        """Check if SQL is in lowercase and identifiers are in snake_case."""
        lines = sql.split('\n')
        for i, line in enumerate(lines):
            # Check for uppercase SQL
            if any(keyword.upper() in line for keyword in self.keywords):
                self.errors.append(
                    LintingError(
                        message="SQL keywords should be lowercase",
                        line_number=i + 1,
                        suggestion=line.lower()
                    )
                )
            
            # Check for non-snake_case identifiers
            identifiers = re.findall(r'\b[A-Za-z][A-Za-z0-9_]*\b', line)
            for identifier in identifiers:
                if not re.match(r'^[a-z][a-z0-9_]*$', identifier) and identifier.lower() not in self.keywords:
                    snake_case = ''.join(['_' + c.lower() if c.isupper() else c.lower() for c in identifier]).lstrip('_')
                    self.errors.append(
                        LintingError(
                            message=f"Identifier '{identifier}' should be in snake_case",
                            line_number=i + 1,
                            suggestion=snake_case
                        )
                    )

    def check_join_formatting(self, sql: str) -> None:
        """Check if JOIN and ON clauses are on separate lines."""
        lines = sql.split('\n')
        for i, line in enumerate(lines):
            if re.search(r'join.*on', line.lower()):
                self.errors.append(
                    LintingError(
                        message="JOIN and ON should be on separate lines",
                        line_number=i + 1,
                        suggestion=line.replace(' on ', '\n  on ')
                    )
                )

    def check_comma_style(self, sql: str) -> None:
        """Check for leading commas with proper spacing."""
        lines = sql.split('\n')
        for i, line in enumerate(lines):
            if ',' in line:
                # Check for trailing commas
                if line.rstrip().endswith(','):
                    fixed_line = line.rstrip(',').strip()
                    self.errors.append(
                        LintingError(
                            message="Use leading commas instead of trailing commas",
                            line_number=i + 1,
                            suggestion=fixed_line
                        )
                    )
                # Check for comma spacing
                if re.search(r'\S,\S', line):
                    self.errors.append(
                        LintingError(
                            message="Commas should have a space after them",
                            line_number=i + 1
                        )
                    )

    def check_comparison_operators(self, sql: str) -> None:
        """Check for != instead of <>."""
        lines = sql.split('\n')
        for i, line in enumerate(lines):
            if '<>' in line:
                self.errors.append(
                    LintingError(
                        message="Use != instead of <>",
                        line_number=i + 1,
                        suggestion=line.replace('<>', '!=')
                    )
                )

    def detect_and_fix_subqueries(self, sql: str) -> Optional[str]:
        """Detect subqueries and convert them to CTEs."""
        parsed = sqlparse.parse(sql)[0]
        subqueries = []
        cte_counter = 1

        def extract_subqueries(token):
            if isinstance(token, sqlparse.sql.Parenthesis):
                inner_sql = token.value[1:-1].strip()  # Remove outer parentheses
                if inner_sql.lower().startswith('select'):
                    subqueries.append((token, f"cte_{cte_counter}"))
                    return True
            return False

        # Find all subqueries
        for token in parsed.flatten():
            if extract_subqueries(token):
                cte_counter += 1

        if not subqueries:
            return None

        # Convert subqueries to CTEs
        modified_sql = sql
        ctes = []
        for subquery, cte_name in subqueries:
            ctes.append(f"WITH {cte_name} AS (\n{subquery.value[1:-1].strip()}\n)")
            modified_sql = modified_sql.replace(subquery.value, cte_name)

        # Combine CTEs and modified SQL
        final_sql = "\n".join(ctes) + "\n" + modified_sql
        return final_sql

    def lint_sql(self, sql: str) -> Dict:
        """Main linting function that runs all checks."""
        self.errors = []  # Reset errors for new run
        
        # Format SQL first
        formatted_sql = sqlparse.format(
            sql,
            keyword_case='lower',
            identifier_case='lower',
            reindent=True,
            indent_width=2
        )

        # Run all checks
        self.check_case(formatted_sql)
        self.check_join_formatting(formatted_sql)
        self.check_comma_style(formatted_sql)
        self.check_comparison_operators(formatted_sql)

        # Check for subqueries and suggest CTEs
        modified_sql = self.detect_and_fix_subqueries(formatted_sql)
        
        return {
            'errors': self.errors,
            'formatted_sql': formatted_sql,
            'cte_suggestion': modified_sql
        }

def lint_sql_file(file_path: str, linter: SQLLinter) -> None:
    """Lint a single SQL file."""
    with open(file_path, 'r') as f:
        sql = f.read()
    
    results = linter.lint_sql(sql)
    
    print(f"\nLinting results for {file_path}:")
    if results['errors']:
        print("\nErrors found:")
        for error in results['errors']:
            print(f"Line {error.line_number}: {error.message}")
            if error.suggestion:
                print(f"Suggestion: {error.suggestion}")
    else:
        print("No linting errors found.")
    
    if results['cte_suggestion']:
        print("\nSubquery detected! Suggested CTE version:")
        print(results['cte_suggestion'])

def lint_dbt_project(project_path: str, model_name: Optional[str] = None) -> None:
    """Lint entire DBT project or a specific model."""
    linter = SQLLinter()
    
    if model_name:
        # Lint specific model
        model_path = os.path.join(project_path, 'models', f"{model_name}.sql")
        if os.path.exists(model_path):
            lint_sql_file(model_path, linter)
        else:
            print(f"Model {model_name} not found.")
        return

    # Lint all SQL files in the project
    for root, _, files in os.walk(project_path):
        for file in files:
            if file.endswith('.sql'):
                file_path = os.path.join(root, file)
                lint_sql_file(file_path, linter)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='DBT SQL Linter')
    parser.add_argument('project_path', help='Path to DBT project')
    parser.add_argument('--model', help='Specific model to lint', default=None)
    
    args = parser.parse_args()
    lint_dbt_project(args.project_path, args.model)
