from dataclasses import dataclass, field


@dataclass
class Expression:
    args: dict = field(default_factory=dict)

    def walk(self):
        yield self

        for value in self.args.values():
            if isinstance(value, Expression):
                yield from value.walk()

            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, Expression):
                        yield from item.walk()

    def find_all(self, expression_type):
        for node in self.walk():
            if isinstance(node, expression_type):
                yield node

    def transform(self, func):
        new_node = func(self)

        for key, value in list(new_node.args.items()):

            if isinstance(value, Expression):
                new_node.args[key] = value.transform(func)

            elif isinstance(value, list):

                new_list = []

                for item in value:

                    if isinstance(item, Expression):
                        item = item.transform(func)

                    new_list.append(item)

                new_node.args[key] = new_list

        return new_node

    def dump(self, level=0):

        indent = "  " * level

        print(f"{indent}{self.__class__.__name__}")

        for key, value in self.args.items():

            if isinstance(value, Expression):

                print(f"{indent}  {key}:")
                value.dump(level + 2)

            elif isinstance(value, list):

                print(f"{indent}  {key}:")

                for item in value:

                    if isinstance(item, Expression):
                        item.dump(level + 2)
                    else:
                        print(f"{indent}    {item}")

            else:
                print(f"{indent}  {key}: {value}")


class Function(Expression):
    pass


class Column(Expression):
    pass


class Measure(Expression):
    pass


class Table(Expression):
    pass


class Literal(Expression):
    pass


class Binary(Expression):
    pass


class Unary(Expression):
    pass


class Var(Expression):
    pass


class VariableReference(Expression):
    pass


class Keyword(Expression):
    """A bare-identifier DAX enum constant (e.g. DAY, DESC, DENSE, ASC)."""

    pass


class VirtualColumn(Expression):
    """A bracketed reference to a column introduced by ADDCOLUMNS /
    SELECTCOLUMNS (rather than an existing model measure)."""

    pass


class Return(Expression):
    pass