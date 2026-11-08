"""Condition lookback and sustained-breach admission (also enforced in Rust)."""

MAX_CONDITION_MINUTES = 10080  # one week; includes the maximum breach duration
DURATION_ERROR = "Must be an integer between 1 and 10080 minutes."


def validate_condition_duration(value: int) -> None:
    if type(value) is not int or not 1 <= value <= MAX_CONDITION_MINUTES:
        raise ValueError(DURATION_ERROR)
