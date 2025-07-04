import ast
import astor
import os


class TryExceptContextFixer(ast.NodeTransformer):

    def visit_FunctionDef(self, node):
        """
        Visita cada función para inyectar un except en sus try sin except
        """
        func_name = node.name
        for stmt in node.body:
            if isinstance(stmt, ast.Try) and not stmt.handlers:
                print(
                    f"🔧 AUTO-FIX en función '{func_name}' línea {stmt.lineno}")
                new_handler = ast.ExceptHandler(type=ast.Name(id=
                    'Exception', ctx=ast.Load()), name='e', body=[ast.
                    Import(names=[ast.alias(name='traceback', asname=None)]
                    ), ast.Expr(value=ast.Call(func=ast.Attribute(value=ast
                    .Name(id='log', ctx=ast.Load()), attr='warning', ctx=
                    ast.Load()), args=[ast.JoinedStr(values=[ast.Str(s=
                    f'⚠️ AUTO-FIX en función {func_name}: '), ast.
                    FormattedValue(value=ast.Name(id='e', ctx=ast.Load()),
                    conversion=-1)])], keywords=[ast.keyword(arg='exc_info',
                    value=ast.NameConstant(value=True))]))])
                stmt.handlers.append(new_handler)
        return self.generic_visit(node)


def autoparchear_archivo(path):
    with open(path, encoding='utf-8') as f:
        src = f.read()
    tree = ast.parse(src)
    fixer = TryExceptContextFixer()
    tree = fixer.visit(tree)
    new_src = astor.to_source(tree)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(new_src)


for root, dirs, files in os.walk('.'):
    if 'venv' in root:
        continue
    for file in files:
        if file.endswith('.py'):
            autoparchear_archivo(os.path.join(root, file))
