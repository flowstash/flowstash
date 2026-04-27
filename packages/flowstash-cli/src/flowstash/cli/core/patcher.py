import libcst as cst
from libcst.matchers import matches, Call, Name

class WebhookDecoratorAnnotator(cst.CSTTransformer):
    """
    Finds the `@ingress.webhook(path="<path>", ...)` and adds or replaces
    `test_payload=FromFile("<fixture_path>")`.
    """
    def __init__(self, target_path: str, fixture_path: str, function_name: str, start_line: int):
        self.target_path = target_path
        self.fixture_path = fixture_path
        self.function_name = function_name
        self.start_line = start_line
        self.found = False

    def visit_FunctionDef(self, node: cst.FunctionDef) -> bool:
        # Check if function name matches
        if node.name.value != self.function_name:
            return True
        return True

    def leave_FunctionDef(self, original_node: cst.FunctionDef, updated_node: cst.FunctionDef) -> cst.CSTNode:
        if original_node.name.value != self.function_name:
            return updated_node

        new_decorators = []
        for decorator in updated_node.decorators:
            # Check if this is @ingress.webhook
            if isinstance(decorator.decorator, cst.Call):
                call = decorator.decorator
                func = call.func
                
                is_webhook = False
                if isinstance(func, cst.Attribute) and func.attr.value == "webhook":
                    if isinstance(func.value, cst.Name) and func.value.value == "ingress":
                        is_webhook = True
                elif isinstance(func, cst.Name) and func.value == "webhook":
                    is_webhook = True

                if is_webhook:
                    # Verify path matches
                    path_matches = False
                    for arg in call.args:
                        if arg.keyword and arg.keyword.value == "path":
                            # Extremely simple check for string value
                            if arg.value.value.strip("\"'") == self.target_path:
                                path_matches = True
                                break

                    if path_matches:
                        self.found = True
                        
                        # Remove existing test_payload if present
                        new_args = [arg for arg in call.args if not (arg.keyword and arg.keyword.value == "test_payload")]
                        
                        # Add test_payload=FromFile(...)
                        new_arg = cst.Arg(
                            keyword=cst.Name("test_payload"),
                            value=cst.Call(
                                func=cst.Name("FromFile"),
                                args=[cst.Arg(value=cst.SimpleString(f'"{self.fixture_path}"'))]
                            )
                        )
                        new_args.append(new_arg)
                        
                        new_call = call.with_changes(args=new_args)
                        new_decorators.append(decorator.with_changes(decorator=new_call))
                        continue

            new_decorators.append(decorator)

        if self.found:
            return updated_node.with_changes(decorators=new_decorators)
        
        return updated_node

def _ensure_from_file_import(source_code: str) -> str:
    """
    Ensures 'from flowstash.ingress import FromFile' is in the source code.
    Inserts it before the first 'def ', 'class ', or empty line.
    """
    if "from flowstash.ingress import FromFile" in source_code:
        return source_code

    lines = source_code.splitlines(keepends=True)
    insert_idx = 0
    
    for i, line in enumerate(lines):
        trimmed = line.strip()
        if trimmed.startswith("def ") or trimmed.startswith("class ") or not trimmed:
            insert_idx = i
            break
    else:
        insert_idx = len(lines)

    lines.insert(insert_idx, "from flowstash.ingress import FromFile\n")
    return "".join(lines)

def apply_patch(source_code: str, target_path: str, fixture_path: str, function_name: str, start_line: int) -> str:
    """
    Parses source_code, modifies the target webhook decorator, and returns new source code.
    If not found or not modified, returns the original source code.
    """
    try:
        module = cst.parse_module(source_code)
        transformer = WebhookDecoratorAnnotator(target_path, fixture_path, function_name, start_line)
        modified_module = module.visit(transformer)
        
        if transformer.found:
            new_code = modified_module.code
            
            # If FromFile was not in original code, ensure it's imported
            if "FromFile(" not in source_code:
                new_code = _ensure_from_file_import(new_code)
                
            return new_code
        return source_code
    except Exception as e:
        # Fallback to original if parsing fails
        import logging
        logging.getLogger(__name__).warning(f"Failed to patch source code: {e}")
        return source_code
