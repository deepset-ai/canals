from typing import Optional

from canals import component


@component
class SelfLoop:
    """
    Decreases the initial value in steps of 1 until the target value is reached.
    For no good reason it uses a self-loop to do so :)
    """

    def __init__(self, target: int = 0):
        self.target = target

    @component.output_types(current_value=int, final_result=int)
    def run(self, initial_value: Optional[int] = None, current_value: Optional[int] = None):
        """
        Decreases the initial value in steps of 1 until the target value is reached.
        """
        if initial_value:
            current_value = initial_value

        if current_value:
            current_value -= 1

        if current_value == self.target:
            return {"final_result": current_value}
        return {"current_value": current_value}
