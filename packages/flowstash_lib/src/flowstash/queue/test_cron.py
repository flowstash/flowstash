import re
from pydantic import BaseModel, field_validator

class Schedule(BaseModel):
    cron: str

    @field_validator("cron")
    @classmethod
    def validate_cron_expression(cls, v: str) -> str:
        cron_expr = v.strip()
        
        # Handle optional timezone prefix
        if cron_expr.startswith("CRON_TZ="):
            parts = cron_expr.split(" ", 1)
            if len(parts) == 2:
                cron_expr = parts[1].strip()
            else:
                raise ValueError("Invalid cron expression format with CRON_TZ")

        # Disallow macros like @daily
        if cron_expr.startswith("@"):
            raise ValueError("Macro aliases (e.g., @daily) are not allowed")

        # Split into fields
        fields = cron_expr.split()
        if len(fields) != 5:
            raise ValueError(f"Cron expression must have exactly 5 fields, got {len(fields)}")

        # Check for disallowed characters and patterns
        # Only numbers, *, -, and , are allowed in basic cron fields
        allowed_pattern = re.compile(r"^[0-9\*\,\-]+$")
        
        for field in fields:
            if not allowed_pattern.match(field):
                raise ValueError(
                    f"Invalid characters in cron field '{field}'. "
                    "Only standard numeric syntax, '*', '-', and ',' are allowed. "
                    "Intervals (/), hashes (H), descriptors (L, W, etc.), and words are not allowed."
                )

        return v

def test(expr, should_pass):
    try:
        Schedule(cron=expr)
        if not should_pass:
            print(f"FAILED (should have rejected): {expr}")
        else:
            print(f"OK (passed as expected): {expr}")
    except ValueError as e:
        if should_pass:
            print(f"FAILED (should have passed): {expr} -> {e}")
        else:
            print(f"OK (rejected as expected): {expr}")

test("* * * * *", True)
test("CRON_TZ=America/New_York 0 4 * * *", True)
test("CRON_TZ=UTC 0,15,30,45 * * * *", True)
test("0-5 10 * * *", True)
test("0 4 * * 1-5", True)
test("* * * * * *", False) # 6 parts
test("* * * * * 2026", False) # 6 parts
test("@daily", False)
test("H * * * *", False)
test("*/15 * * * *", False)
test("* * L * *", False)
test("15W * * * *", False)
test("1#3 * * * *", False)
test("0 0 * * ?", False)
test("every 5 mins", False)
