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


class In(Expression):
    """An ``IN`` membership test: ``<expression> IN { <values> }``."""

    pass


class Set(Expression):
    """A brace-enclosed list of values, e.g. ``{ 1, 2, 3 }``."""

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


class Query(Expression):
    """A full DAX query: an optional ``DEFINE`` block of definitions followed
    by one or more ``EVALUATE`` statements.

    ``args`` keys:

    * ``definitions`` - list of definition nodes (``MeasureDefinition``,
      ``ColumnDefinition``, ``TableDefinition`` and/or ``Var``) declared in the
      ``DEFINE`` block (empty when there is no ``DEFINE`` block).
    * ``expressions`` - list of ``Evaluate`` nodes.
    """

    pass


class MeasureDefinition(Expression):
    """A measure declared in a ``DEFINE`` block:
    ``MEASURE Table[Name] = <expression>``.

    ``args`` keys: ``table`` (str), ``this`` (the measure name) and
    ``expression``.
    """

    pass


class ColumnDefinition(Expression):
    """A calculated column declared in a ``DEFINE`` block:
    ``COLUMN Table[Name] = <expression>``.

    ``args`` keys: ``table`` (str), ``this`` (the column name) and
    ``expression``.
    """

    pass


class TableDefinition(Expression):
    """A table declared in a ``DEFINE`` block: ``TABLE Name = <expression>``.

    ``args`` keys: ``this`` (the table name) and ``expression``.
    """

    pass


class Evaluate(Expression):
    """An ``EVALUATE`` statement with an optional ``ORDER BY`` / ``START AT``.

    ``args`` keys: ``this`` (the table expression), ``order_by`` (list of
    ``OrderBy`` nodes) and ``start_at`` (list of expressions).
    """

    pass


class OrderBy(Expression):
    """A single ``ORDER BY`` term: ``<expression> [ASC|DESC]``.

    ``args`` keys: ``this`` (the expression) and ``direction`` (``"ASC"``,
    ``"DESC"`` or ``None``).
    """

    pass
