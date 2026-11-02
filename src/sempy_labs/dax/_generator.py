from ._expressions import *


class Generator:

    def generate(self, expression):

        if isinstance(expression, Function):

            args = ", ".join(
                self.generate(arg) for arg in expression.args["expressions"]
            )

            return f"{expression.args['this']}({args})"

        elif isinstance(expression, Column):

            return f"'{expression.args['table']}'" f"[{expression.args['this']}]"

        elif isinstance(expression, Measure):

            return f"[{expression.args['this']}]"

        elif isinstance(expression, Literal):

            return str(expression.args["this"])

        elif isinstance(expression, Binary):

            left = self.generate(expression.args["this"])

            right = self.generate(expression.args["expression"])

            op = expression.args["operator"]

            return f"{left} {op} {right}"

        raise ValueError(f"Unsupported expression: {type(expression)}")


def to_dax(expression):
    return Generator().generate(expression)
