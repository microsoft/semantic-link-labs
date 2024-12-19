import sempy
import sempy.fabric as fabric
import pandas as pd
import re
from datetime import datetime
from sempy_labs._helper_functions import (
    format_dax_object_name,
    generate_guid,
    _make_list_unique,
    resolve_dataset_name_and_id,
    resolve_workspace_name_and_id,
)
from sempy_labs._list_functions import list_relationships
from sempy_labs._refresh_semantic_model import refresh_semantic_model
from sempy_labs.directlake._dl_helper import check_fallback_reason
from contextlib import contextmanager
from typing import List, Iterator, Optional, Union, TYPE_CHECKING
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
import ast
from uuid import UUID

if TYPE_CHECKING:
    import Microsoft.AnalysisServices.Tabular
    import Microsoft.AnalysisServices.Tabular as TOM


class TOMWrapper:
    """
    Convenience wrapper around the TOM object model for a semantic model. Always use the connect_semantic_model function to make sure the TOM object is initialized correctly.

    `XMLA read/write endpoints <https://learn.microsoft.com/power-bi/enterprise/service-premium-connect-tools#to-enable-read-write-for-a-premium-capacity>`_ must be enabled if setting the readonly parameter to False.
    """

    _dataset_id: UUID
    _dataset_name: str
    _workspace_id: UUID
    _workspace_name: str
    _readonly: bool
    _tables_added: List[str]
    _table_map = dict
    _column_map = dict

    def __init__(self, dataset, workspace, readonly):

        (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
        (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)
        self._dataset_id = dataset_id
        self._dataset_name = dataset_name
        self._workspace_name = workspace_name
        self._workspace_id = workspace_id
        self._readonly = readonly
        self._tables_added = []

        self._tom_server = fabric.create_tom_server(
            readonly=readonly, workspace=workspace_id
        )
        self.model = self._tom_server.Databases[dataset_id].Model

        self._table_map = {}
        self._column_map = {}
        self._compat_level = self.model.Model.Database.CompatibilityLevel

        # Minimum campat level for lineage tags is 1540 (https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.table.lineagetag?view=analysisservices-dotnet#microsoft-analysisservices-tabular-table-lineagetag)
        if self._compat_level >= 1540:
            for t in self.model.Tables:
                if len(t.LineageTag) == 0:
                    t.LineageTag = generate_guid()
                self._table_map[t.LineageTag] = t.Name

            for c in self.all_columns():
                if len(c.LineageTag) == 0:
                    c.LineageTag = generate_guid()
                self._column_map[c.LineageTag] = [c.Name, c.DataType]

    def all_columns(self):
        """
        Outputs a list of all columns within all tables in the semantic model.

        Parameters
        ----------

        Returns
        -------
        Iterator[Microsoft.AnalysisServices.Tabular.Column]
            All columns within the semantic model.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        for t in self.model.Tables:
            for c in t.Columns:
                if c.Type != TOM.ColumnType.RowNumber:
                    yield c

    def all_calculated_columns(self):
        """
        Outputs a list of all calculated columns within all tables in the semantic model.

        Parameters
        ----------

        Returns
        -------
        Iterator[Microsoft.AnalysisServices.Tabular.Column]
            All calculated columns within the semantic model.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        for t in self.model.Tables:
            for c in t.Columns:
                if c.Type == TOM.ColumnType.Calculated:
                    yield c

    def all_calculated_tables(self):
        """
        Outputs a list of all calculated tables in the semantic model.

        Parameters
        ----------

        Returns
        -------
        Iterator[Microsoft.AnalysisServices.Tabular.Table]
            All calculated tables within the semantic model.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        for t in self.model.Tables:
            if any(
                p.SourceType == TOM.PartitionSourceType.Calculated for p in t.Partitions
            ):
                yield t

    def all_calculation_groups(self):
        """
        Outputs a list of all calculation groups in the semantic model.

        Parameters
        ----------

        Returns
        -------
        Iterator[Microsoft.AnalysisServices.Tabular.Table]
            All calculation groups within the semantic model.
        """

        for t in self.model.Tables:
            if t.CalculationGroup is not None:
                yield t

    def all_measures(self):
        """
        Outputs a list of all measures in the semantic model.

        Parameters
        ----------

        Returns
        -------
        Iterator[Microsoft.AnalysisServices.Tabular.Measure]
            All measures within the semantic model.
        """

        for t in self.model.Tables:
            for m in t.Measures:
                yield m

    def all_partitions(self):
        """
        Outputs a list of all partitions in the semantic model.

        Parameters
        ----------

        Returns
        -------
        Iterator[Microsoft.AnalysisServices.Tabular.Partition]
            All partitions within the semantic model.
        """

        for t in self.model.Tables:
            for p in t.Partitions:
                yield p

    def all_hierarchies(self):
        """
        Outputs a list of all hierarchies in the semantic model.

        Parameters
        ----------

        Returns
        -------
        Iterator[Microsoft.AnalysisServices.Tabular.Hierarchy]
            All hierarchies within the semantic model.
        """

        for t in self.model.Tables:
            for h in t.Hierarchies:
                yield h

    def all_levels(self):
        """
        Outputs a list of all levels in the semantic model.

        Parameters
        ----------

        Returns
        -------
        Iterator[Microsoft.AnalysisServices.Tabular.Level]
            All levels within the semantic model.
        """

        for table in self.model.Tables:
            for hierarchy in table.Hierarchies:
                for level in hierarchy.Levels:
                    yield level

    def all_calculation_items(self):
        """
        Outputs a list of all calculation items in the semantic model.

        Parameters
        ----------

        Returns
        -------
        Iterator[Microsoft.AnalysisServices.Tabular.CalculationItem]
            All calculation items within the semantic model.
        """

        for t in self.model.Tables:
            if t.CalculationGroup is not None:
                for ci in t.CalculationGroup.CalculationItems:
                    yield ci

    def all_rls(self):
        """
        Outputs a list of all row level security expressions in the semantic model.

        Parameters
        ----------

        Returns
        -------
        Iterator[Microsoft.AnalysisServices.Tabular.TablePermission]
            All row level security expressions within the semantic model.
        """

        for r in self.model.Roles:
            for tp in r.TablePermissions:
                yield tp

    def add_measure(
        self,
        table_name: str,
        measure_name: str,
        expression: str,
        format_string: Optional[str] = None,
        hidden: bool = False,
        description: Optional[str] = None,
        display_folder: Optional[str] = None,
        format_string_expression: Optional[str] = None,
        data_category: Optional[str] = None,
        lineage_tag: Optional[str] = None,
        source_lineage_tag: Optional[str] = None,
        detail_rows_expression: Optional[str] = None,
    ):
        """
        Adds a measure to the semantic model.

        Parameters
        ----------
        table_name : str
            Name of the table in which the measure will be created.
        measure_name : str
            Name of the measure.
        expression : str
            DAX expression of the measure.
        format_string : str, default=None
            Format string of the measure.
        hidden : bool, default=False
            Whether the measure will be hidden or visible.
        description : str, default=None
            A description of the measure.
        display_folder : str, default=None
            The display folder in which the measure will reside.
        format_string_expression : str, default=None
            The format string expression.
        data_category : str, default=None
            Specifies the type of data contained in the measure so that you can add custom behaviors based on measure type.
        lineage_tag : str, default=None
            A tag that represents the lineage of the object.
        source_lineage_tag : str, default=None
            A tag that represents the lineage of the source for the object.
        detail_rows_expression : str, default=None
            The detail rows expression.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        obj = TOM.Measure()
        obj.Name = measure_name
        obj.Expression = expression
        obj.IsHidden = hidden
        if format_string is not None:
            obj.FormatString = format_string
        if description is not None:
            obj.Description = description
        if display_folder is not None:
            obj.DisplayFolder = display_folder
        if format_string_expression is not None:
            fsd = TOM.FormatStringDefinition()
            fsd.Expression = format_string_expression
            obj.FormatStringDefinition = fsd
        if lineage_tag is not None:
            obj.LineageTag = lineage_tag
        else:
            obj.LineageTag = generate_guid()
        if source_lineage_tag is not None:
            obj.SourceLineageTag = source_lineage_tag
        if detail_rows_expression is not None:
            drd = TOM.DetailRowsDefinition()
            drd.Expression = detail_rows_expression
            obj.DetailRowsDefinition = drd
        if data_category is not None:
            obj.DataCategory = data_category

        self.model.Tables[table_name].Measures.Add(obj)

    def add_calculated_table_column(
        self,
        table_name: str,
        column_name: str,
        source_column: str,
        data_type: str,
        format_string: Optional[str] = None,
        hidden: bool = False,
        description: Optional[str] = None,
        display_folder: Optional[str] = None,
        data_category: Optional[str] = None,
        key: bool = False,
        summarize_by: Optional[str] = None,
        lineage_tag: Optional[str] = None,
        source_lineage_tag: Optional[str] = None,
    ):
        """
        Adds a calculated table column to a calculated table within a semantic model.

        Parameters
        ----------
        table_name : str
            Name of the table in which the column will be created.
        column_name : str
            Name of the column.
        source_column : str
            The source column for the column.
        data_type : str
            The data type of the column.
        format_string : str, default=None
            Format string of the column.
        hidden : bool, default=False
            Whether the column will be hidden or visible.
        description : str, default=None
            A description of the column.
        display_folder : str, default=None
            The display folder in which the column will reside.
        data_category : str, default=None
            The data category of the column.
        key : bool, default=False
            Marks the column as the primary key of the table.
        summarize_by : str, default=None
            Sets the value for the Summarize By property of the column.
            Defaults to None resolves to 'Default'.
        lineage_tag : str, default=None
            A tag that represents the lineage of the object.
        source_lineage_tag : str, default=None
            A tag that represents the lineage of the source for the object.
        """
        import Microsoft.AnalysisServices.Tabular as TOM
        import System

        data_type = (
            data_type.capitalize()
            .replace("Integer", "Int64")
            .replace("Datetime", "DateTime")
        )
        if summarize_by is None:
            summarize_by = "Default"
        summarize_by = (
            summarize_by.capitalize()
            .replace("Distinctcount", "DistinctCount")
            .replace("Avg", "Average")
        )

        obj = TOM.CalculatedTableColumn()
        obj.Name = column_name
        obj.SourceColumn = source_column
        obj.DataType = System.Enum.Parse(TOM.DataType, data_type)
        obj.IsHidden = hidden
        obj.IsKey = key
        obj.SummarizeBy = System.Enum.Parse(TOM.AggregateFunction, summarize_by)
        if format_string is not None:
            obj.FormatString = format_string
        if description is not None:
            obj.Description = description
        if display_folder is not None:
            obj.DisplayFolder = display_folder
        if data_category is not None:
            obj.DataCategory = data_category
        if lineage_tag is not None:
            obj.LineageTag = lineage_tag
        else:
            obj.LineageTag = generate_guid()
        if source_lineage_tag is not None:
            obj.SourceLineageTag = source_lineage_tag
        self.model.Tables[table_name].Columns.Add(obj)

    def add_data_column(
        self,
        table_name: str,
        column_name: str,
        source_column: str,
        data_type: str,
        format_string: Optional[str] = None,
        hidden: bool = False,
        description: Optional[str] = None,
        display_folder: Optional[str] = None,
        data_category: Optional[str] = None,
        key: bool = False,
        summarize_by: Optional[str] = None,
        lineage_tag: Optional[str] = None,
        source_lineage_tag: Optional[str] = None,
    ):
        """
        Adds a data column to a table within a semantic model.

        Parameters
        ----------
        table_name : str
            Name of the table in which the column will be created.
        column_name : str
            Name of the column.
        source_column : str
            The source column for the column.
        data_type : str
            The data type of the column.
        format_string : str, default=None
            Format string of the column.
        hidden : bool, default=False
            Whether the column will be hidden or visible.
        description : str, default=None
            A description of the column.
        display_folder : str, default=None
            The display folder in which the column will reside.
        data_category : str, default=None
            The data category of the column.
        key : bool, default=False
            Marks the column as the primary key of the table.
        summarize_by : str, default=None
            Sets the value for the Summarize By property of the column.
            Defaults to None resolves to 'Default'.
        lineage_tag : str, default=None
            A tag that represents the lineage of the object.
        source_lineage_tag : str, default=None
            A tag that represents the lineage of the source for the object.
        """
        import Microsoft.AnalysisServices.Tabular as TOM
        import System

        data_type = (
            data_type.capitalize()
            .replace("Integer", "Int64")
            .replace("Datetime", "DateTime")
        )
        if summarize_by is None:
            summarize_by = "Default"
        summarize_by = (
            summarize_by.capitalize()
            .replace("Distinctcount", "DistinctCount")
            .replace("Avg", "Average")
        )

        obj = TOM.DataColumn()
        obj.Name = column_name
        obj.SourceColumn = source_column
        obj.DataType = System.Enum.Parse(TOM.DataType, data_type)
        obj.IsHidden = hidden
        obj.IsKey = key
        obj.SummarizeBy = System.Enum.Parse(TOM.AggregateFunction, summarize_by)
        if format_string is not None:
            obj.FormatString = format_string
        if description is not None:
            obj.Description = description
        if display_folder is not None:
            obj.DisplayFolder = display_folder
        if data_category is not None:
            obj.DataCategory = data_category
        if lineage_tag is not None:
            obj.LineageTag = lineage_tag
        else:
            obj.LineageTag = generate_guid()
        if source_lineage_tag is not None:
            obj.SourceLineageTag = source_lineage_tag
        self.model.Tables[table_name].Columns.Add(obj)

    def add_calculated_column(
        self,
        table_name: str,
        column_name: str,
        expression: str,
        data_type: str,
        format_string: Optional[str] = None,
        hidden: bool = False,
        description: Optional[str] = None,
        display_folder: Optional[str] = None,
        data_category: Optional[str] = None,
        key: bool = False,
        summarize_by: Optional[str] = None,
        lineage_tag: Optional[str] = None,
        source_lineage_tag: Optional[str] = None,
    ):
        """
        Adds a calculated column to a table within a semantic model.

        Parameters
        ----------
        table_name : str
            Name of the table in which the column will be created.
        column_name : str
            Name of the column.
        expression : str
            The DAX expression for the column.
        data_type : str
            The data type of the column.
        format_string : str, default=None
            Format string of the column.
        hidden : bool, default=False
            Whether the column will be hidden or visible.
        description : str, default=None
            A description of the column.
        display_folder : str, default=None
            The display folder in which the column will reside.
        data_category : str, default=None
            The data category of the column.
        key : bool, default=False
            Marks the column as the primary key of the table.
        summarize_by : str, default=None
            Sets the value for the Summarize By property of the column.
            Defaults to None which resolves to 'Default'.
        lineage_tag : str, default=None
            A tag that represents the lineage of the object.
        source_lineage_tag : str, default=None
            A tag that represents the lineage of the source for the object.
        """
        import Microsoft.AnalysisServices.Tabular as TOM
        import System

        data_type = (
            data_type.capitalize()
            .replace("Integer", "Int64")
            .replace("Datetime", "DateTime")
        )
        if summarize_by is None:
            summarize_by = "Default"
        summarize_by = (
            summarize_by.capitalize()
            .replace("Distinctcount", "DistinctCount")
            .replace("Avg", "Average")
        )

        obj = TOM.CalculatedColumn()
        obj.Name = column_name
        obj.Expression = expression
        obj.IsHidden = hidden
        obj.DataType = System.Enum.Parse(TOM.DataType, data_type)
        obj.IsKey = key
        obj.SummarizeBy = System.Enum.Parse(TOM.AggregateFunction, summarize_by)
        if format_string is not None:
            obj.FormatString = format_string
        if description is not None:
            obj.Description = description
        if display_folder is not None:
            obj.DisplayFolder = display_folder
        if data_category is not None:
            obj.DataCategory = data_category
        if lineage_tag is not None:
            obj.LineageTag = lineage_tag
        else:
            obj.LineageTag = generate_guid()
        if source_lineage_tag is not None:
            obj.SourceLineageTag = source_lineage_tag
        self.model.Tables[table_name].Columns.Add(obj)

    def add_calculation_item(
        self,
        table_name: str,
        calculation_item_name: str,
        expression: str,
        ordinal: Optional[int] = None,
        description: Optional[str] = None,
        format_string_expression: Optional[str] = None,
    ):
        """
        Adds a `calculation item <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.calculationitem?view=analysisservices-dotnet>`_ to
          a `calculation group <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.calculationgroup?view=analysisservices-dotnet>`_ within a semantic model.

        Parameters
        ----------
        table_name : str
            Name of the table in which the calculation item will be created.
        calculation_item_name : str
            Name of the calculation item.
        expression : str
            The DAX expression for the calculation item.
        ordinal : int, default=None
            The ordinal of the calculation item.
        format_string_expression : str, default=None
            The format string expression for the calculation item.
        description : str, default=None
            A description of the calculation item.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        obj = TOM.CalculationItem()
        obj.Name = calculation_item_name
        obj.Expression = expression
        if ordinal is not None:
            obj.Ordinal = ordinal
        if description is not None:
            obj.Description = description
        if format_string_expression is not None:
            fsd = TOM.FormatStringDefinition()
            fsd.Expression = format_string_expression
            obj.FormatStringDefinition = fsd
        self.model.Tables[table_name].CalculationGroup.CalculationItems.Add(obj)

    def add_role(
        self,
        role_name: str,
        model_permission: Optional[str] = None,
        description: Optional[str] = None,
    ):
        """
        Adds a role to a semantic model.

        Parameters
        ----------
        role_name : str
            Name of the role.
        model_permission : str, default=None
            The model permission for the role.
            Defaults to None which resolves to 'Read'.
        description : str, default=None
            A description of the role.
        """
        import Microsoft.AnalysisServices.Tabular as TOM
        import System

        if model_permission is None:
            model_permission = "Read"

        obj = TOM.ModelRole()
        obj.Name = role_name
        obj.ModelPermission = System.Enum.Parse(TOM.ModelPermission, model_permission)
        if description is not None:
            obj.Description = description
        self.model.Roles.Add(obj)

    def set_rls(self, role_name: str, table_name: str, filter_expression: str):
        """
        Sets the row level security permissions for a table within a role.

        Parameters
        ----------
        role_name : str
            Name of the role.
        table_name : str
            Name of the table.
        filter_expression : str
            The DAX expression containing the row level security filter expression logic.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        tp = TOM.TablePermission()
        tp.Table = self.model.Tables[table_name]
        tp.FilterExpression = filter_expression

        if any(
            t.Name == table_name and r.Name == role_name
            for r in self.model.Roles
            for t in r.TablePermissions
        ):
            self.model.Roles[role_name].TablePermissions[
                table_name
            ].FilterExpression = filter_expression
        else:
            self.model.Roles[role_name].TablePermissions.Add(tp)

    def set_ols(
        self, role_name: str, table_name: str, column_name: str, permission: str
    ):
        """
        Sets the object level security permissions for a column within a role.

        Parameters
        ----------
        role_name : str
            Name of the role.
        table_name : str
            Name of the table.
        column_name : str
            Name of the column.
        permission : str
            The object level security permission for the column.
            `Permission valid values <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.metadatapermission?view=analysisservices-dotnet>`_
        """
        import Microsoft.AnalysisServices.Tabular as TOM
        import System

        permission = permission.capitalize()

        if permission not in ["Read", "None", "Default"]:
            raise ValueError(f"{icons.red_dot} Invalid 'permission' value.")

        cp = TOM.ColumnPermission()
        cp.Column = self.model.Tables[table_name].Columns[column_name]
        cp.MetadataPermission = System.Enum.Parse(TOM.MetadataPermission, permission)

        if any(
            c.Name == column_name and t.Name == table_name and r.Name == role_name
            for r in self.model.Roles
            for t in r.TablePermissions
            for c in t.ColumnPermissions
        ):
            self.model.Roles[role_name].TablePermissions[table_name].ColumnPermissions[
                column_name
            ].MetadataPermission = System.Enum.Parse(TOM.MetadataPermission, permission)
        else:
            self.model.Roles[role_name].TablePermissions[
                table_name
            ].ColumnPermissions.Add(cp)

    def add_hierarchy(
        self,
        table_name: str,
        hierarchy_name: str,
        columns: List[str],
        levels: Optional[List[str]] = None,
        hierarchy_description: Optional[str] = None,
        hierarchy_hidden: bool = False,
        lineage_tag: Optional[str] = None,
        source_lineage_tag: Optional[str] = None,
    ):
        """
        Adds a `hierarchy <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.hierarchy?view=analysisservices-dotnet>`_ to a table within a semantic model.

        Parameters
        ----------
        table_name : str
            Name of the table.
        hierarchy_name : str
            Name of the hierarchy.
        columns : List[str]
            Names of the columns to use within the hierarchy.
        levels : List[str], default=None
            Names of the levels to use within the hierarhcy (instead of the column names).
        hierarchy_description : str, default=None
            A description of the hierarchy.
        hierarchy_hidden : bool, default=False
            Whether the hierarchy is visible or hidden.
        lineage_tag : str, default=None
            A tag that represents the lineage of the object.
        source_lineage_tag : str, default=None
            A tag that represents the lineage of the source for the object.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        if isinstance(columns, str):
            raise ValueError(
                f"{icons.red_dot} The 'levels' parameter must be a list. For example: ['Continent', 'Country', 'City']"
            )

        if len(columns) == 1:
            raise ValueError(
                f"{icons.red_dot} There must be at least 2 levels in order to create a hierarchy."
            )

        if levels is None:
            levels = columns

        if len(columns) != len(levels):
            raise ValueError(
                f"{icons.red_dot} If specifying level names, you must specify a level for each column."
            )

        obj = TOM.Hierarchy()
        obj.Name = hierarchy_name
        obj.IsHidden = hierarchy_hidden
        if hierarchy_description is not None:
            obj.Description = hierarchy_description
        if lineage_tag is not None:
            obj.LineageTag = lineage_tag
        else:
            obj.LineageTag = generate_guid()
        if source_lineage_tag is not None:
            obj.SourceLineageTag = source_lineage_tag
        self.model.Tables[table_name].Hierarchies.Add(obj)

        for col in columns:
            lvl = TOM.Level()
            lvl.Column = self.model.Tables[table_name].Columns[col]
            lvl.Name = levels[columns.index(col)]
            lvl.Ordinal = columns.index(col)
            lvl.LineageTag = generate_guid()
            self.model.Tables[table_name].Hierarchies[hierarchy_name].Levels.Add(lvl)

    def add_relationship(
        self,
        from_table: str,
        from_column: str,
        to_table: str,
        to_column: str,
        from_cardinality: str,
        to_cardinality: str,
        cross_filtering_behavior: Optional[str] = None,
        is_active: bool = True,
        security_filtering_behavior: Optional[str] = None,
        rely_on_referential_integrity: bool = False,
    ):
        """
        Adds a `relationship <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.singlecolumnrelationship?view=analysisservices-dotnet>`_ to a semantic model.

        Parameters
        ----------
        from_table : str
            Name of the table on the 'from' side of the relationship.
        from_column : str
            Name of the column on the 'from' side of the relationship.
        to_table : str
            Name of the table on the 'to' side of the relationship.
        to_column : str
            Name of the column on the 'to' side of the relationship.
        from_cardinality : str
            The cardinality of the 'from' side of the relationship. Options: ['Many', 'One', 'None'].
        to_cardinality : str
                The cardinality of the 'to' side of the relationship. Options: ['Many', 'One', 'None'].
        cross_filtering_behavior : str, default=None
            Setting for the cross filtering behavior of the relationship. Options: ('Automatic', 'OneDirection', 'BothDirections').
            Defaults to None which resolves to 'Automatic'.
        is_active : bool, default=True
            Setting for whether the relationship is active or not.
        security_filtering_behavior : str, default=None
            Setting for the security filtering behavior of the relationship. Options: ('None', 'OneDirection', 'BothDirections').
            Defaults to None which resolves to 'OneDirection'.
        rely_on_referential_integrity : bool, default=False
            Setting for the rely on referential integrity of the relationship.
        """
        import Microsoft.AnalysisServices.Tabular as TOM
        import System

        if cross_filtering_behavior is None:
            cross_filtering_behavior = "Automatic"
        if security_filtering_behavior is None:
            security_filtering_behavior = "OneDirection"

        from_cardinality = from_cardinality.capitalize()
        to_cardinality = to_cardinality.capitalize()
        cross_filtering_behavior = cross_filtering_behavior.capitalize()
        security_filtering_behavior = security_filtering_behavior.capitalize()
        security_filtering_behavior = security_filtering_behavior.replace(
            "direct", "Direct"
        )
        cross_filtering_behavior = cross_filtering_behavior.replace("direct", "Direct")

        rel = TOM.SingleColumnRelationship()
        rel.FromColumn = self.model.Tables[from_table].Columns[from_column]
        rel.FromCardinality = System.Enum.Parse(
            TOM.RelationshipEndCardinality, from_cardinality
        )
        rel.ToColumn = self.model.Tables[to_table].Columns[to_column]
        rel.ToCardinality = System.Enum.Parse(
            TOM.RelationshipEndCardinality, to_cardinality
        )
        rel.IsActive = is_active
        rel.CrossFilteringBehavior = System.Enum.Parse(
            TOM.CrossFilteringBehavior, cross_filtering_behavior
        )
        rel.SecurityFilteringBehavior = System.Enum.Parse(
            TOM.SecurityFilteringBehavior, security_filtering_behavior
        )
        rel.RelyOnReferentialIntegrity = rely_on_referential_integrity

        self.model.Relationships.Add(rel)

    def add_calculation_group(
        self,
        name: str,
        precedence: int,
        description: Optional[str] = None,
        hidden: bool = False,
        column_name: str = "Name",
    ):
        """
        Adds a `calculation group <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.calculationgroup?view=analysisservices-dotnet>`_ to a semantic model.

        Parameters
        ----------
        name : str
            Name of the calculation group.
        precedence : int
            The precedence of the calculation group.
        description : str, default=None
            A description of the calculation group.
        hidden : bool, default=False
            Whether the calculation group is hidden/visible.
        column_name : str, default="Name"
            The name of the calculation group column.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        tbl = TOM.Table()
        tbl.Name = name
        tbl.CalculationGroup = TOM.CalculationGroup()
        tbl.CalculationGroup.Precedence = precedence
        tbl.IsHidden = hidden
        if description is not None:
            tbl.Description = description

        part = TOM.Partition()
        part.Name = name
        part.Source = TOM.CalculationGroupSource()
        part.Mode = TOM.ModeType.Import
        tbl.Partitions.Add(part)

        sortCol = "Ordinal"

        col1 = TOM.DataColumn()
        col1.Name = sortCol
        col1.SourceColumn = sortCol
        col1.IsHidden = True
        col1.DataType = TOM.DataType.Int64

        tbl.Columns.Add(col1)

        col2 = TOM.DataColumn()
        col2.Name = column_name
        col2.SourceColumn = column_name
        col2.DataType = TOM.DataType.String
        # col.SortByColumn = m.Tables[name].Columns[sortCol]
        tbl.Columns.Add(col2)

        self.model.DiscourageImplicitMeasures = True
        self.model.Tables.Add(tbl)

    def add_expression(
        self,
        name: str,
        expression: str,
        description: Optional[str] = None,
        lineage_tag: Optional[str] = None,
        source_lineage_tag: Optional[str] = None,
    ):
        """
        Adds an `expression <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.namedexpression?view=analysisservices-dotnet>`_ to a semantic model.

        Parameters
        ----------
        name : str
            Name of the expression.
        expression: str
            The M expression of the expression.
        description : str, default=None
            A description of the expression.
        lineage_tag : str, default=None
            A tag that represents the lineage of the object.
        source_lineage_tag : str, default=None
            A tag that represents the lineage of the source for the object.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        exp = TOM.NamedExpression()
        exp.Name = name
        if description is not None:
            exp.Description = description
        if lineage_tag is not None:
            exp.LineageTag = lineage_tag
        else:
            exp.LineageTag = generate_guid()
        if source_lineage_tag is not None:
            exp.SourceLineageTag = source_lineage_tag
        exp.Kind = TOM.ExpressionKind.M
        exp.Expression = expression

        self.model.Expressions.Add(exp)

    def add_translation(self, language: str):
        """
        Adds a `translation language <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.culture?view=analysisservices-dotnet>`_ (culture) to a semantic model.

        Parameters
        ----------
        language : str
            The language code (i.e. 'it-IT' for Italian).
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        if not any(c.Name == language for c in self.model.Cultures):
            cul = TOM.Culture()
            cul.Name = language
            lm = TOM.LinguisticMetadata()
            lm.ContentType = TOM.ContentType.Json
            lm.Content = f'{{"Version": "1.0.0", "Language": "{language}"}}'
            cul.LinguisticMetadata = lm
            self.model.Cultures.Add(cul)

    def add_perspective(self, perspective_name: str):
        """
        Adds a `perspective <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.perspective?view=analysisservices-dotnet>`_ to a semantic model.

        Parameters
        ----------
        perspective_name : str
            Name of the perspective.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        persp = TOM.Perspective()
        persp.Name = perspective_name
        self.model.Perspectives.Add(persp)

    def add_m_partition(
        self,
        table_name: str,
        partition_name: str,
        expression: str,
        mode: Optional[str] = None,
        description: Optional[str] = None,
    ):
        """
        Adds an M-partition to a table within a semantic model.

        Parameters
        ----------
        table_name : str
            Name of the table.
        partition_name : str
            Name of the partition.
        expression : str
            The M expression encapsulating the logic for the partition.
        mode : str, default=None
            The query mode for the partition.
            Defaults to None which resolves to 'Import'.
            `Valid mode values <https://learn.microsoft.com/en-us/dotnet/api/microsoft.analysisservices.tabular.modetype?view=analysisservices-dotnet>`_
        description : str, default=None
            A description for the partition.
        """
        import Microsoft.AnalysisServices.Tabular as TOM
        import System

        if mode is None:
            mode = "Default"
        else:
            mode = (
                mode.title()
                .replace("query", "Query")
                .replace(" ", "")
                .replace("lake", "Lake")
            )

        mp = TOM.MPartitionSource()
        mp.Expression = expression
        p = TOM.Partition()
        p.Name = partition_name
        p.Source = mp
        if description is not None:
            p.Description = description
        p.Mode = System.Enum.Parse(TOM.ModeType, mode)

        self.model.Tables[table_name].Partitions.Add(p)

    def add_entity_partition(
        self,
        table_name: str,
        entity_name: str,
        expression: Optional[str] = None,
        description: Optional[str] = None,
        schema_name: str = "dbo",
    ):
        """
        Adds an entity partition to a table within a semantic model.

        Parameters
        ----------
        table_name : str
            Name of the table.
        entity_name : str
            Name of the lakehouse/warehouse table.
        expression : TOM Object, default=None
            The expression used by the table.
            Defaults to None which resolves to the 'DatabaseQuery' expression.
        description : str, default=None
            A description for the partition.
        schema_name : str, default="dbo"
            The schema name.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        ep = TOM.EntityPartitionSource()
        ep.Name = table_name
        ep.EntityName = entity_name
        if expression is None:
            ep.ExpressionSource = self.model.Expressions["DatabaseQuery"]
        else:
            ep.ExpressionSource = self.model.Expressions[expression]
        ep.SchemaName = schema_name
        p = TOM.Partition()
        p.Name = table_name
        p.Source = ep
        p.Mode = TOM.ModeType.DirectLake
        if description is not None:
            p.Description = description

        self.model.Tables[table_name].Partitions.Add(p)
        self.model.Tables[table_name].SourceLineageTag = (
            f"[{schema_name}].[{entity_name}]"
        )

    def set_alternate_of(
        self,
        table_name: str,
        column_name: str,
        summarization_type: str,
        base_table: str,
        base_column: Optional[str] = None,
    ):
        """
        Sets the `alternate of <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.alternateof?view=analysisservices-dotnet>`_ property on a column.

        Parameters
        ----------
        table_name : str
            Name of the table.
        column_name : str
            Name of the column.
        summarization_type : str
            The summarization type for the column.
            `Summarization valid values <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.alternateof.summarization?view=analysisservices-dotnet#microsoft-analysisservices-tabular-alternateof-summarization>`_
        base_table : str
            Name of the base table for aggregation.
        base_column : str
            Name of the base column for aggregation
        """
        import Microsoft.AnalysisServices.Tabular as TOM
        import System

        if base_column is not None and base_table is None:
            raise ValueError(
                f"{icons.red_dot} If you specify the base table you must also specify the base column"
            )

        summarization_type = (
            summarization_type.replace(" ", "")
            .capitalize()
            .replace("Groupby", "GroupBy")
        )

        summarizationTypes = ["Sum", "GroupBy", "Count", "Min", "Max"]
        if summarization_type not in summarizationTypes:
            raise ValueError(
                f"{icons.red_dot} The 'summarization_type' parameter must be one of the following valuse: {summarizationTypes}."
            )

        ao = TOM.AlternateOf()
        ao.Summarization = System.Enum.Parse(TOM.SummarizationType, summarization_type)
        if base_column is not None:
            ao.BaseColumn = self.model.Tables[base_table].Columns[base_column]
        else:
            ao.BaseTable = self.model.Tables[base_table]

        self.model.Tables[table_name].Columns[column_name].AlternateOf = ao

        # Hide agg table and columns
        t = self.model.Tables[table_name]
        t.IsHidden = True
        for c in t.Columns:
            c.IsHidden = True

    def remove_alternate_of(self, table_name: str, column_name: str):
        """
        Removes the `alternate of <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.alternateof?view=analysisservices-dotnet>`_ property on a column.

        Parameters
        ----------
        table_name : str
            Name of the table.
        column_name : str
            Name of the column.

        Returns
        -------

        """

        self.model.Tables[table_name].Columns[column_name].AlternateOf = None

    def get_annotations(
        self, object
    ) -> "Microsoft.AnalysisServices.Tabular.Annotation":
        """
        Shows all `annotations <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.annotation?view=analysisservices-dotnet>`_ for a given object within a semantic model.

        Parameters
        ----------
        object : TOM Object
            An object (i.e. table/column/measure) within a semantic model.

        Returns
        -------
        Microsoft.AnalysisServices.Tabular.Annotation
            TOM objects of all the annotations on a particular object within the semantic model.
        """

        # df = pd.DataFrame(columns=['Name', 'Value'])

        for a in object.Annotations:
            # new_data = {'Name': a.Name, 'Value': a.Value}
            yield a
            # df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    def set_annotation(self, object, name: str, value: str):
        """
        Sets an `annotation <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.annotation?view=analysisservices-dotnet>`_ on an object within the semantic model.

        Parameters
        ----------
        object : TOM Object
            An object (i.e. table/column/measure) within a semantic model.
        name : str
            Name of the annotation.
        value : str
            Value of the annotation.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        ann = TOM.Annotation()
        ann.Name = name
        ann.Value = value

        if any(a.Name == name for a in object.Annotations):
            object.Annotations[name].Value = value
        else:
            object.Annotations.Add(ann)

    def get_annotation_value(self, object, name: str):
        """
        Obtains the `annotation <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.annotation?view=analysisservices-dotnet>`_ value for a given annotation on an object within the semantic model.

        Parameters
        ----------
        object : TOM Object
            An object (i.e. table/column/measure) within a semantic model.
        name : str
            Name of the annotation.

        Returns
        -------
        str
            The annotation value.
        """
        if any(a.Name == name for a in object.Annotations):
            value = object.Annotations[name].Value
        else:
            value = None

        return value

    def remove_annotation(self, object, name: str):
        """
        Removes an `annotation <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.annotation?view=analysisservices-dotnet>`_ on an object within the semantic model.

        Parameters
        ----------
        object : TOM Object
            An object (i.e. table/column/measure) within a semantic model.
        name : str
            Name of the annotation.
        """

        object.Annotations.Remove(name)

    def clear_annotations(self, object):
        """
        Removes all `annotations <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.annotation?view=analysisservices-dotnet>`_ on an object within the semantic model.

        Parameters
        ----------
        object : TOM Object
            An object (i.e. table/column/measure) within a semantic model.
        """

        object.Annotations.Clear()

    def get_extended_properties(
        self, object
    ) -> "Microsoft.AnalysisServices.Tabular.ExtendedProperty":
        """
        Retrieves all `extended properties <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.extendedproperty?view=analysisservices-dotnet>`_ on an object within the semantic model.

        Parameters
        ----------
        object : TOM Object
            An object (i.e. table/column/measure) within a semantic model.

        Returns
        -------
        Microsoft.AnalysisServices.Tabular.ExtendedPropertiesCollection
            TOM Objects of all the extended properties.
        """

        for a in object.ExtendedProperties:
            yield a

    def set_extended_property(
        self, object, extended_property_type: str, name: str, value: str
    ):
        """
        Sets an `extended property <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.extendedproperty?view=analysisservices-dotnet>`_ on an object within the semantic model.

        Parameters
        ----------
        object : TOM Object
            An object (i.e. table/column/measure) within a semantic model.
        extended_property_type : str
            The extended property type.
            `Extended property valid values <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.extendedpropertytype?view=analysisservices-dotnet>`_
        name : str
            Name of the extended property.
        value : str
            Value of the extended property.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        extended_property_type = extended_property_type.title()

        if extended_property_type == "Json":
            ep = TOM.JsonExtendedProperty()
        else:
            ep = TOM.StringExtendedProperty()

        ep.Name = name
        ep.Value = value

        if any(a.Name == name for a in object.Annotations):
            object.ExtendedProperties[name].Value = value
        else:
            object.ExtendedProperties.Add(ep)

    def get_extended_property_value(self, object, name: str):
        """
        Retrieves the value of an `extended property <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.extendedproperty?view=analysisservices-dotnet>`_ for an object within the semantic model.

        Parameters
        ----------
        object : TOM Object
            An object (i.e. table/column/measure) within a semantic model.
        name : str
            Name of the annotation.

        Returns
        -------
        str
            The extended property value.
        """
        if any(a.Name == name for a in object.ExtendedProperties):
            value = object.ExtendedProperties[name].Value
        else:
            value = None

        return value

    def remove_extended_property(self, object, name: str):
        """
        Removes an `extended property <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.extendedproperty?view=analysisservices-dotnet>`_ on an object within the semantic model.

        Parameters
        ----------
        object : TOM Object
            An object (i.e. table/column/measure) within a semantic model.
        name : str
            Name of the annotation.
        """

        object.ExtendedProperties.Remove(name)

    def clear_extended_properties(self, object):
        """
        Removes all `extended properties <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.extendedproperty?view=analysisservices-dotnet>`_ on an object within the semantic model.

        Parameters
        ----------
        object : TOM Object
            An object (i.e. table/column/measure) within a semantic model.
        """

        object.ExtendedProperties.Clear()

    def in_perspective(
        self,
        object: Union["TOM.Table", "TOM.Column", "TOM.Measure", "TOM.Hierarchy"],
        perspective_name: str,
    ):
        """
        Indicates whether an object is contained within a given `perspective <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.perspective?view=analysisservices-dotnet>`_.

        Parameters
        ----------
        object : TOM Object
            An object (i.e. table/column/measure) within a semantic model.
        perspecitve_name : str
            Name of the perspective.

        Returns
        -------
        bool
            An indication as to whether the object is contained within the given perspective.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        validObjects = [
            TOM.ObjectType.Table,
            TOM.ObjectType.Column,
            TOM.ObjectType.Measure,
            TOM.ObjectType.Hierarchy,
        ]
        objectType = object.ObjectType

        if objectType not in validObjects:
            raise ValueError(
                f"{icons.red_dot} Only the following object types are valid for perspectives: {validObjects}."
            )

        object.Model.Perspectives[perspective_name]

        try:
            if objectType == TOM.ObjectType.Table:
                object.Model.Perspectives[perspective_name].PerspectiveTables[
                    object.Name
                ]
            elif objectType == TOM.ObjectType.Column:
                object.Model.Perspectives[perspective_name].PerspectiveTables[
                    object.Parent.Name
                ].PerspectiveColumns[object.Name]
            elif objectType == TOM.ObjectType.Measure:
                object.Model.Perspectives[perspective_name].PerspectiveTables[
                    object.Parent.Name
                ].PerspectiveMeasures[object.Name]
            elif objectType == TOM.ObjectType.Hierarchy:
                object.Model.Perspectives[perspective_name].PerspectiveTables[
                    object.Parent.Name
                ].PerspectiveHierarchies[object.Name]
            return True
        except Exception:
            return False

    def add_to_perspective(
        self,
        object: Union["TOM.Table", "TOM.Column", "TOM.Measure", "TOM.Hierarchy"],
        perspective_name: str,
    ):
        """
        Adds an object to a `perspective <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.perspective?view=analysisservices-dotnet>`_.

        Parameters
        ----------
        object : TOM Object
            An object (i.e. table/column/measure) within a semantic model.
        perspective_name : str
            Name of the perspective.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        validObjects = [
            TOM.ObjectType.Table,
            TOM.ObjectType.Column,
            TOM.ObjectType.Measure,
            TOM.ObjectType.Hierarchy,
        ]
        objectType = object.ObjectType

        if objectType not in validObjects:
            raise ValueError(
                f"{icons.red_dot} Only the following object types are valid for perspectives: {validObjects}."
            )

        if any(p.Name == perspective_name for p in self.model.Perspectives):
            object.Model.Perspectives[perspective_name]
        else:
            raise ValueError(
                f"{icons.red_dot} The '{perspective_name}' perspective does not exist."
            )

        if objectType == TOM.ObjectType.Table:
            pt = TOM.PerspectiveTable()
            pt.Table = object
            object.Model.Perspectives[perspective_name].PerspectiveTables.Add(pt)
        elif objectType == TOM.ObjectType.Column:
            pc = TOM.PerspectiveColumn()
            pc.Column = object
            object.Model.Perspectives[perspective_name].PerspectiveTables[
                object.Parent.Name
            ].PerspectiveColumns.Add(pc)
        elif objectType == TOM.ObjectType.Measure:
            pm = TOM.PerspectiveMeasure()
            pm.Measure = object
            object.Model.Perspectives[perspective_name].PerspectiveTables[
                object.Parent.Name
            ].PerspectiveMeasures.Add(pm)
        elif objectType == TOM.ObjectType.Hierarchy:
            ph = TOM.PerspectiveHierarchy()
            ph.Hierarchy = object
            object.Model.Perspectives[perspective_name].PerspectiveTables[
                object.Parent.Name
            ].PerspectiveHierarchies.Add(ph)

    def remove_from_perspective(
        self,
        object: Union["TOM.Table", "TOM.Column", "TOM.Measure", "TOM.Hierarchy"],
        perspective_name: str,
    ):
        """
        Removes an object from a `perspective <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.perspective?view=analysisservices-dotnet>`_.

        Parameters
        ----------
        object : TOM Object
            An object (i.e. table/column/measure) within a semantic model.
        perspective_name : str
            Name of the perspective.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        validObjects = [
            TOM.ObjectType.Table,
            TOM.ObjectType.Column,
            TOM.ObjectType.Measure,
            TOM.ObjectType.Hierarchy,
        ]
        objectType = object.ObjectType

        if objectType not in validObjects:
            raise ValueError(
                f"{icons.red_dot} Only the following object types are valid for perspectives: {validObjects}."
            )

        if not any(p.Name == perspective_name for p in self.model.Perspectives):
            raise ValueError(
                f"{icons.red_dot} The '{perspective_name}' perspective does not exist."
            )

        if objectType == TOM.ObjectType.Table:
            pt = object.Model.Perspectives[perspective_name].PerspectiveTables[
                object.Name
            ]
            object.Model.Perspectives[perspective_name].PerspectiveTables.Remove(pt)
        elif objectType == TOM.ObjectType.Column:
            pc = (
                object.Model.Perspectives[perspective_name]
                .PerspectiveTables[object.Parent.Name]
                .PerspectiveColumns[object.Name]
            )
            object.Model.Perspectives[perspective_name].PerspectiveTables[
                object.Parent.Name
            ].PerspectiveColumns.Remove(pc)
        elif objectType == TOM.ObjectType.Measure:
            pm = (
                object.Model.Perspectives[perspective_name]
                .PerspectiveTables[object.Parent.Name]
                .PerspectiveMeasures[object.Name]
            )
            object.Model.Perspectives[perspective_name].PerspectiveTables[
                object.Parent.Name
            ].PerspectiveMeasures.Remove(pm)
        elif objectType == TOM.ObjectType.Hierarchy:
            ph = (
                object.Model.Perspectives[perspective_name]
                .PerspectiveTables[object.Parent.Name]
                .PerspectiveHierarchies[object.Name]
            )
            object.Model.Perspectives[perspective_name].PerspectiveTables[
                object.Parent.Name
            ].PerspectiveHierarchies.Remove(ph)

    def set_translation(
        self,
        object: Union[
            "TOM.Table", "TOM.Column", "TOM.Measure", "TOM.Hierarchy", "TOM.Level"
        ],
        language: str,
        property: str,
        value: str,
    ):
        """
        Sets a `translation <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.culture?view=analysisservices-dotnet>`_ value for an object's property.

        Parameters
        ----------
        object : TOM Object
            An object (i.e. table/column/measure) within a semantic model.
        language : str
            The language code.
        property : str
            The property to set. Options: 'Name', 'Description', 'Display Folder'.
        value : str
            The transation value.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        self.add_translation(language=language)

        property = property.title()

        validObjects = [
            TOM.ObjectType.Table,
            TOM.ObjectType.Column,
            TOM.ObjectType.Measure,
            TOM.ObjectType.Hierarchy,
            TOM.ObjectType.Level,
        ]

        if object.ObjectType not in validObjects:
            raise ValueError(
                f"{icons.red_dot} Translations can only be set to {validObjects}."
            )

        mapping = {
            "Name": TOM.TranslatedProperty.Caption,
            "Description": TOM.TranslatedProperty.Description,
            "Display Folder": TOM.TranslatedProperty.DisplayFolder,
        }

        prop = mapping.get(property)
        if prop is None:
            raise ValueError(
                f"{icons.red_dot} Invalid property value. Please choose from the following: {list(mapping.keys())}."
            )

        if not any(c.Name == language for c in self.model.Cultures):
            raise ValueError(
                f"{icons.red_dot} The '{language}' translation language does not exist in the semantic model."
            )

        object.Model.Cultures[language].ObjectTranslations.SetTranslation(
            object, prop, value
        )

        if object.ObjectType in [TOM.ObjectType.Table, TOM.ObjectType.Measure]:
            print(
                f"{icons.green_dot} The {property} property for the '{object.Name}' {str(object.ObjectType).lower()} has been translated into '{language}' as '{value}'."
            )
        elif object.ObjectType in [
            TOM.ObjectType.Column,
            TOM.ObjectType.Hierarchy,
            TOM.ObjectType.Level,
        ]:
            print(
                f"{icons.green_dot} The {property} property for the '{object.Parent.Name}'[{object.Name}] {str(object.ObjectType).lower()} has been translated into '{language}' as '{value}'."
            )

    def remove_translation(
        self,
        object: Union[
            "TOM.Table", "TOM.Column", "TOM.Measure", "TOM.Hierarchy", "TOM.Level"
        ],
        language: str,
    ):
        """
        Removes an object's `translation <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.culture?view=analysisservices-dotnet>`_ value.

        Parameters
        ----------
        object : TOM Object
            An object (i.e. table/column/measure) within a semantic model.
        language : str
            The language code.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        o = object.Model.Cultures[language].ObjectTranslations[
            object, TOM.TranslatedProperty.Caption
        ]
        object.Model.Cultures[language].ObjectTranslations.Remove(o)

    def remove_object(self, object):
        """
        Removes an object from a semantic model.

        Parameters
        ----------
        object : TOM Object
            An object (i.e. table/column/measure) within a semantic model.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        objType = object.ObjectType

        # Have to remove translations and perspectives on the object before removing it.
        if objType in [
            TOM.ObjectType.Table,
            TOM.ObjectType.Column,
            TOM.ObjectType.Measure,
            TOM.ObjectType.Hierarchy,
            TOM.ObjectType.Level,
        ]:
            for lang in object.Model.Cultures:
                try:
                    self.remove_translation(object=object, language=lang.Name)
                except Exception:
                    pass
        if objType in [
            TOM.ObjectType.Table,
            TOM.ObjectType.Column,
            TOM.ObjectType.Measure,
            TOM.ObjectType.Hierarchy,
            TOM.ObjectType.Level,
        ]:
            for persp in object.Model.Perspectives:
                try:
                    self.remove_from_perspective(
                        object=object, perspective_name=persp.Name
                    )
                except Exception:
                    pass

        if objType == TOM.ObjectType.Table:
            object.Parent.Tables.Remove(object.Name)
        elif objType == TOM.ObjectType.Column:
            object.Parent.Columns.Remove(object.Name)
        elif objType == TOM.ObjectType.Measure:
            object.Parent.Measures.Remove(object.Name)
        elif objType == TOM.ObjectType.Hierarchy:
            object.Parent.Hierarchies.Remove(object.Name)
        elif objType == TOM.ObjectType.Level:
            object.Parent.Levels.Remove(object.Name)
        elif objType == TOM.ObjectType.Partition:
            object.Parent.Partitions.Remove(object.Name)
        elif objType == TOM.ObjectType.Expression:
            object.Parent.Expressions.Remove(object.Name)
        elif objType == TOM.ObjectType.DataSource:
            object.Parent.DataSources.Remove(object.Name)
        elif objType == TOM.ObjectType.Role:
            object.Parent.Roles.Remove(object.Name)
        elif objType == TOM.ObjectType.Relationship:
            object.Parent.Relationships.Remove(object.Name)
        elif objType == TOM.ObjectType.Culture:
            object.Parent.Cultures.Remove(object.Name)
        elif objType == TOM.ObjectType.Perspective:
            object.Parent.Perspectives.Remove(object.Name)
        elif objType == TOM.ObjectType.CalculationItem:
            object.Parent.CalculationItems.Remove(object.Name)
        elif objType == TOM.ObjectType.TablePermission:
            object.Parent.TablePermissions.Remove(object.Name)

    def used_in_relationships(self, object: Union["TOM.Table", "TOM.Column"]):
        """
        Shows all `relationships <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.singlecolumnrelationship?view=analysisservices-dotnet>`_ in which a table/column is used.

        Parameters
        ----------
        object : TOM Object
            An object (i.e. table/column) within a semantic model.

        Returns
        -------
        Microsoft.AnalysisServices.Tabular.RelationshipCollection
            All relationships in which the table/column is used.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        objType = object.ObjectType

        if objType == TOM.ObjectType.Table:
            for r in self.model.Relationships:
                if r.FromTable.Name == object.Name or r.ToTable.Name == object.Name:
                    yield r  # , 'Table'
        elif objType == TOM.ObjectType.Column:
            for r in self.model.Relationships:
                if (
                    r.FromTable.Name == object.Parent.Name
                    and r.FromColumn.Name == object.Name
                ) or (
                    r.ToTable.Name == object.Parent.Name
                    and r.ToColumn.Name == object.Name
                ):
                    yield r  # , 'Column'

    def used_in_levels(self, column: "TOM.Column"):
        """
        Shows all `levels <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.level?view=analysisservices-dotnet>`_ in which a column is used.

        Parameters
        ----------
        object : TOM Object
            An column object within a semantic model.

        Returns
        -------
        Microsoft.AnalysisServices.Tabular.LevelCollection
            All levels in which the column is used.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        objType = column.ObjectType

        if objType == TOM.ObjectType.Column:
            for level in self.all_levels():
                if (
                    level.Parent.Table.Name == column.Parent.Name
                    and level.Column.Name == column.Name
                ):
                    yield level

    def used_in_hierarchies(self, column: "TOM.Column"):
        """
        Shows all `hierarchies <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.hierarchy?view=analysisservices-dotnet>`_ in which a column is used.

        Parameters
        ----------
        object : TOM Object
            An column object within a semantic model.

        Returns
        -------
        Microsoft.AnalysisServices.Tabular.HierarchyCollection
            All hierarchies in which the column is used.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        objType = column.ObjectType

        if objType == TOM.ObjectType.Column:
            for lev in self.all_levels():
                if (
                    lev.Parent.Table.Name == column.Parent.Name
                    and lev.Column.Name == column.Name
                ):
                    yield lev.Parent

    def used_in_sort_by(self, column: "TOM.Column"):
        """
        Shows all columns in which a column is used for sorting.

        Parameters
        ----------
        object : TOM Object
            An column object within a semantic model.

        Returns
        -------
        Microsoft.AnalysisServices.Tabular.ColumnCollection
            All columns in which the column is used for sorting.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        objType = column.ObjectType

        if objType == TOM.ObjectType.Column:
            for c in self.model.Tables[column.Parent.Name].Columns:
                if c.SortByColumn == column:
                    yield c

    def used_in_rls(
        self,
        object: Union["TOM.Table", "TOM.Column", "TOM.Measure"],
        dependencies: pd.DataFrame,
    ):
        """
        Identifies the row level security `filter expressions <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.tablepermission.filterexpression?view=analysisservices-dotnet#microsoft-analysisservices-tabular-tablepermission-filterexpression>`_ which reference a given object.

        Parameters
        ----------
        object : TOM Object
            An object (i.e. table/column) within a semantic model.
        dependencies : pandas.DataFrame
            A pandas dataframe with the output of the 'get_model_calc_dependencies' function.

        Returns
        -------
        Microsoft.AnalysisServices.Tabular.TableCollection, Microsoft.AnalysisServices.Tabular.ColumnCollection, Microsoft.AnalysisServices.Tabular.MeasureCollection

        """
        import Microsoft.AnalysisServices.Tabular as TOM

        objType = object.ObjectType

        df_filt = dependencies[dependencies["Object Type"] == "Rows Allowed"]

        if objType == TOM.ObjectType.Table:
            fil = df_filt[
                (df_filt["Referenced Object Type"] == "Table")
                & (df_filt["Referenced Table"] == object.Name)
            ]
            tbls = fil["Table Name"].unique().tolist()
            for t in self.model.Tables:
                if t.Name in tbls:
                    yield t
        elif objType == TOM.ObjectType.Column:
            fil = df_filt[
                (df_filt["Referenced Object Type"] == "Column")
                & (df_filt["Referenced Table"] == object.Parent.Name)
                & (df_filt["Referenced Object"] == object.Name)
            ]
            cols = fil["Full Object Name"].unique().tolist()
            for c in self.all_columns():
                if format_dax_object_name(c.Parent.Name, c.Name) in cols:
                    yield c
        elif objType == TOM.ObjectType.Measure:
            fil = df_filt[
                (df_filt["Referenced Object Type"] == "Measure")
                & (df_filt["Referenced Table"] == object.Parent.Name)
                & (df_filt["Referenced Object"] == object.Name)
            ]
            meas = fil["Object Name"].unique().tolist()
            for m in self.all_measures():
                if m.Name in meas:
                    yield m

    def used_in_data_coverage_definition(
        self,
        object: Union["TOM.Table", "TOM.Column", "TOM.Measure"],
        dependencies: pd.DataFrame,
    ):
        """
        Identifies the ... which reference a given object.

        Parameters
        ----------
        object : TOM Object
            An object (i.e. table/column) within a semantic model.
        dependencies : pandas.DataFrame
            A pandas dataframe with the output of the 'get_model_calc_dependencies' function.

        Returns
        -------
        Microsoft.AnalysisServices.Tabular.TableCollection, Microsoft.AnalysisServices.Tabular.ColumnCollection, Microsoft.AnalysisServices.Tabular.MeasureCollection
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        objType = object.ObjectType

        df_filt = dependencies[
            dependencies["Object Type"] == "Data Coverage Definition"
        ]

        if objType == TOM.ObjectType.Table:
            fil = df_filt[
                (df_filt["Referenced Object Type"] == "Table")
                & (df_filt["Referenced Table"] == object.Name)
            ]
            tbls = fil["Table Name"].unique().tolist()
            for t in self.model.Tables:
                if t.Name in tbls:
                    yield t
        elif objType == TOM.ObjectType.Column:
            fil = df_filt[
                (df_filt["Referenced Object Type"] == "Column")
                & (df_filt["Referenced Table"] == object.Parent.Name)
                & (df_filt["Referenced Object"] == object.Name)
            ]
            cols = fil["Full Object Name"].unique().tolist()
            for c in self.all_columns():
                if format_dax_object_name(c.Parent.Name, c.Name) in cols:
                    yield c
        elif objType == TOM.ObjectType.Measure:
            fil = df_filt[
                (df_filt["Referenced Object Type"] == "Measure")
                & (df_filt["Referenced Table"] == object.Parent.Name)
                & (df_filt["Referenced Object"] == object.Name)
            ]
            meas = fil["Object Name"].unique().tolist()
            for m in self.all_measures():
                if m.Name in meas:
                    yield m

    def used_in_calc_item(
        self,
        object: Union["TOM.Table", "TOM.Column", "TOM.Measure"],
        dependencies: pd.DataFrame,
    ):
        """
        Identifies the ... which reference a given object.

        Parameters
        ----------
        object : TOM Object
            An object (i.e. table/column) within a semantic model.
        dependencies : pandas.DataFrame
            A pandas dataframe with the output of the 'get_model_calc_dependencies' function.

        Returns
        -------
        Microsoft.AnalysisServices.Tabular.TableCollection, Microsoft.AnalysisServices.Tabular.ColumnCollection, Microsoft.AnalysisServices.Tabular.MeasureCollection
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        objType = object.ObjectType

        df_filt = dependencies[dependencies["Object Type"] == "Calculation Item"]

        if objType == TOM.ObjectType.Table:
            fil = df_filt[
                (df_filt["Referenced Object Type"] == "Table")
                & (df_filt["Referenced Table"] == object.Name)
            ]
            tbls = fil["Table Name"].unique().tolist()
            for t in self.model.Tables:
                if t.Name in tbls:
                    yield t
        elif objType == TOM.ObjectType.Column:
            fil = df_filt[
                (df_filt["Referenced Object Type"] == "Column")
                & (df_filt["Referenced Table"] == object.Parent.Name)
                & (df_filt["Referenced Object"] == object.Name)
            ]
            cols = fil["Full Object Name"].unique().tolist()
            for c in self.all_columns():
                if format_dax_object_name(c.Parent.Name, c.Name) in cols:
                    yield c
        elif objType == TOM.ObjectType.Measure:
            fil = df_filt[
                (df_filt["Referenced Object Type"] == "Measure")
                & (df_filt["Referenced Table"] == object.Parent.Name)
                & (df_filt["Referenced Object"] == object.Name)
            ]
            meas = fil["Object Name"].unique().tolist()
            for m in self.all_measures():
                if m.Name in meas:
                    yield m

    def all_hybrid_tables(self):
        """
        Outputs the `hybrid tables <https://learn.microsoft.com/power-bi/connect-data/service-dataset-modes-understand#hybrid-tables>`_ within a semantic model.

        Parameters
        ----------

        Returns
        -------
        Microsoft.AnalysisServices.Tabular.TableCollection
            All hybrid tables within a semantic model.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        for t in self.model.Tables:
            if any(p.Mode == TOM.ModeType.Import for p in t.Partitions):
                if any(p.Mode == TOM.ModeType.DirectQuery for p in t.Partitions):
                    yield t

    def all_date_tables(self):
        """
        Outputs the tables which are marked as `date tables <https://learn.microsoft.com/power-bi/transform-model/desktop-date-tables>`_ within a semantic model.

        Parameters
        ----------

        Returns
        -------
        Microsoft.AnalysisServices.Tabular.TableCollection
            All tables marked as date tables within a semantic model.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        for t in self.model.Tables:
            if t.DataCategory == "Time":
                if any(
                    c.IsKey and c.DataType == TOM.DataType.DateTime for c in t.Columns
                ):
                    yield t

    def is_hybrid_table(self, table_name: str):
        """
        Identifies if a table is a `hybrid table <https://learn.microsoft.com/power-bi/connect-data/service-dataset-modes-understand#hybrid-tables>`_.

        Parameters
        ----------
        table_name : str
            Name of the table.

        Returns
        -------
        bool
            Indicates if the table is a hybrid table.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        isHybridTable = False

        if any(
            p.Mode == TOM.ModeType.Import
            for p in self.model.Tables[table_name].Partitions
        ):
            if any(
                p.Mode == TOM.ModeType.DirectQuery
                for p in self.model.Tables[table_name].Partitions
            ):
                isHybridTable = True

        return isHybridTable

    def is_date_table(self, table_name: str):
        """
        Identifies if a table is marked as a `date tables <https://learn.microsoft.com/power-bi/transform-model/desktop-date-tables>`_.

        Parameters
        ----------
        table_name : str
            Name of the table.

        Returns
        -------
        bool
            Indicates if the table is marked as a date table.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        return any(
            c.IsKey and c.DataType == TOM.DataType.DateTime
            for c in self.all_columns()
            if c.Parent.Name == table_name and c.Parent.DataCategory == "Time"
        )

    def mark_as_date_table(
        self, table_name: str, column_name: str, validate: bool = False
    ):
        """
        Marks a table as a `date table <https://learn.microsoft.com/power-bi/transform-model/desktop-date-tables>`_.

        Parameters
        ----------
        table_name : str
            Name of the table.
        column_name : str
            Name of the date column in the table.
        validate : bool, default=False
            If True, performs a validation on if the the date table is viable.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        t = self.model.Tables[table_name]
        c = t.Columns[column_name]
        if c.DataType != TOM.DataType.DateTime:
            raise ValueError(
                f"{icons.red_dot} The column specified in the 'column_name' parameter in this function must be of DateTime data type."
            )

        if validate:
            dax_query = f"""
            define measure '{table_name}'[test] = 
            var mn = MIN('{table_name}'[{column_name}])
            var ma = MAX('{table_name}'[{column_name}])
            var x = COUNTROWS(DISTINCT('{table_name}'[{column_name}]))
            var y = DATEDIFF(mn, ma, DAY) + 1
            return if(y = x, 1,0)

            EVALUATE
            SUMMARIZECOLUMNS(
            "1",[test]
            )
            """
            df = fabric.evaluate_dax(
                dataset=self._dataset_id,
                workspace=self._workspace_id,
                dax_string=dax_query,
            )
            value = df["[1]"].iloc[0]
            if value != "1":
                raise ValueError(
                    f"{icons.red_dot} The '{column_name}' within the '{table_name}' table does not contain contiguous date values."
                )

        # Mark as a date table
        t.DataCategory = "Time"
        c.IsKey = True
        print(
            f"{icons.green_dot} The '{table_name}' table has been marked as a date table using the '{column_name}' column as its primary date key."
        )

    def has_aggs(self):
        """
        Identifies if a semantic model has any `aggregations <https://learn.microsoft.com/power-bi/transform-model/aggregations-advanced>`_.

        Parameters
        ----------

        Returns
        -------
        bool
            Indicates if the semantic model has any aggregations.
        """

        return any(c.AlternateOf is not None for c in self.all_columns())

    def is_agg_table(self, table_name: str):
        """
        Identifies if a table has `aggregations <https://learn.microsoft.com/power-bi/transform-model/aggregations-advanced>`_.

        Parameters
        ----------
        table_name : str
            Name of the table.

        Returns
        -------
        bool
            Indicates if the table has any aggregations.
        """

        t = self.model.Tables[table_name]

        return any(c.AlternateOf is not None for c in t.Columns)

    def has_hybrid_table(self):
        """
        Identifies if a semantic model has a `hybrid table <https://learn.microsoft.com/power-bi/connect-data/service-dataset-modes-understand#hybrid-tables>`_.

        Parameters
        ----------

        Returns
        -------
        bool
            Indicates if the semantic model has a hybrid table.
        """

        return any(self.is_hybrid_table(table_name=t.Name) for t in self.model.Tables)

    def has_date_table(self):
        """
        Identifies if a semantic model has a table marked as a `date table <https://learn.microsoft.com/power-bi/transform-model/desktop-date-tables>`_.

        Parameters
        ----------

        Returns
        -------
        bool
            Indicates if the semantic model has a table marked as a date table.
        """

        return any(self.is_date_table(table_name=t.Name) for t in self.model.Tables)

    def is_direct_lake(self):
        """
        Identifies if a semantic model is in `Direct Lake <https://learn.microsoft.com/fabric/get-started/direct-lake-overview>`_ mode.

        Parameters
        ----------

        Returns
        -------
        bool
            Indicates if the semantic model is in Direct Lake mode.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        return any(
            p.Mode == TOM.ModeType.DirectLake
            for t in self.model.Tables
            for p in t.Partitions
        )

    def is_field_parameter(self, table_name: str):
        """
        Identifies if a table is a `field parameter <https://learn.microsoft.com/power-bi/create-reports/power-bi-field-parameters>`_.

        Parameters
        ----------
        table_name : str
            Name of the table.

        Returns
        -------
        bool
            Indicates if the table is a field parameter.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        t = self.model.Tables[table_name]

        return (
            self.is_calculated_table(table_name=table_name)
            and t.Columns.Count == 4
            and any(
                "NAMEOF(" in p.Source.Expression.replace(" ", "") for p in t.Partitions
            )
            and all(
                "[Value" in c.SourceColumn
                for c in t.Columns
                if c.Type == TOM.ColumnType.Data
            )
            and any(
                ep.Name == "ParameterMetadata"
                for c in t.Columns
                for ep in c.ExtendedProperties
            )
        )

    def is_auto_date_table(self, table_name: str):
        """
        Identifies if a table is an `auto date/time table <https://learn.microsoft.com/power-bi/transform-model/desktop-auto-date-time>`_.

        Parameters
        ----------
        table_name : str
            Name of the table.

        Returns
        -------
        bool
            Indicates if the table is an auto-date table.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        isAutoDate = False

        t = self.model.Tables[table_name]

        if t.Name.startswith("LocalDateTable_") or t.Name.startswith(
            "DateTableTemplate_"
        ):
            if any(
                p.SourceType == TOM.PartitionSourceType.Calculated for p in t.Partitions
            ):
                isAutoDate = True

        return isAutoDate

    def set_kpi(
        self,
        measure_name: str,
        target: Union[int, float, str],
        lower_bound: float,
        upper_bound: float,
        lower_mid_bound: Optional[float] = None,
        upper_mid_bound: Optional[float] = None,
        status_type: Optional[str] = None,
        status_graphic: Optional[str] = None,
    ):
        """
        Sets the properties to add/update a `KPI <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.kpi?view=analysisservices-dotnet>`_ for a measure.

        Parameters
        ----------
        measure_name : str
            Name of the measure.
        target : str, int, float
            The target for the KPI. This can either be a number or the name of a different measure in the semantic model.
        lower_bound: float
            The lower bound for the KPI.
        upper_bound : float
            The upper bound for the KPI.
        lower_mid_bound : float, default=None
            The lower-mid bound for the KPI. Set this if status_type is 'Centered' or 'CenteredReversed'.
        upper_mid_bound : float, default=None
            The upper-mid bound for the KPI. Set this if status_type is 'Centered' or 'CenteredReversed'.
        status_type : str, default=None
            The status type of the KPI. Options: 'Linear', 'LinearReversed', 'Centered', 'CenteredReversed'.
            Defaults to None which resolvs to 'Linear'.
        status_graphic : str, default=None
            The status graphic for the KPI.
            Defaults to 'Three Circles Colored'.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        # https://github.com/m-kovalsky/Tabular/blob/master/KPI%20Graphics.md

        if measure_name == target:
            raise ValueError(
                f"{icons.red_dot} The 'target' parameter cannot be the same measure as the 'measure_name' parameter."
            )

        if status_graphic is None:
            status_graphic = "Three Circles Colored"

        valid_status_types = [
            "Linear",
            "LinearReversed",
            "Centered",
            "CenteredReversed",
        ]
        status_type = status_type
        if status_type is None:
            status_type = "Linear"
        else:
            status_type = status_type.title().replace(" ", "")

        if status_type not in valid_status_types:
            raise ValueError(
                f"{icons.red_dot} '{status_type}' is an invalid status_type. Please choose from these options: {valid_status_types}."
            )

        if status_type in ["Linear", "LinearReversed"]:
            if upper_bound is not None or lower_mid_bound is not None:
                raise ValueError(
                    f"{icons.red_dot} The 'upper_mid_bound' and 'lower_mid_bound' parameters are not used in the 'Linear' and 'LinearReversed' status types. Make sure these parameters are set to None."
                )

            elif upper_bound <= lower_bound:
                raise ValueError(
                    f"{icons.red_dot} The upper_bound must be greater than the lower_bound."
                )

        if status_type in ["Centered", "CenteredReversed"]:
            if upper_mid_bound is None or lower_mid_bound is None:
                raise ValueError(
                    f"{icons.red_dot} The 'upper_mid_bound' and 'lower_mid_bound' parameters are necessary in the 'Centered' and 'CenteredReversed' status types."
                )
            elif upper_bound <= upper_mid_bound:
                raise ValueError(
                    f"{icons.red_dot} The upper_bound must be greater than the upper_mid_bound."
                )
            elif upper_mid_bound <= lower_mid_bound:
                raise ValueError(
                    f"{icons.red_dot} The upper_mid_bound must be greater than the lower_mid_bound."
                )
            elif lower_mid_bound <= lower_bound:
                raise ValueError(
                    f"{icons.red_dot} The lower_mid_bound must be greater than the lower_bound."
                )

        try:
            table_name = next(
                m.Parent.Name for m in self.all_measures() if m.Name == measure_name
            )
        except Exception:
            raise ValueError(
                f"{icons.red_dot} The '{measure_name}' measure does not exist in the '{self._dataset_name}' semantic model within the '{self._workspace_name}'."
            )

        graphics = [
            "Cylinder",
            "Five Bars Colored",
            "Five Boxes Colored",
            "Gauge - Ascending",
            "Gauge - Descending",
            "Road Signs",
            "Shapes",
            "Standard Arrow",
            "Three Circles Colored",
            "Three Flags Colored",
            "Three Stars Colored",
            "Three Symbols Uncircled Colored",
            "Traffic Light",
            "Traffic Light - Single",
            "Variance Arrow",
            "Status Arrow - Ascending",
            "Status Arrow - Descending",
        ]

        if status_graphic not in graphics:
            raise ValueError(
                f"{icons.red_dot} The '{status_graphic}' status graphic is not valid. Please choose from these options: {graphics}."
            )

        measure_target = True

        try:
            float(target)
            tgt = str(target)
            measure_target = False
        except Exception:
            try:
                tgt = next(
                    format_dax_object_name(m.Parent.Name, m.Name)
                    for m in self.all_measures()
                    if m.Name == target
                )
            except Exception:
                raise ValueError(
                    f"{icons.red_dot} The '{target}' measure does not exist in the '{self._dataset_name}' semantic model within the '{self._workspace_name}'."
                )

        if measure_target:
            expr = f"var x = [{measure_name}]/[{target}]\nreturn"
        else:
            expr = f"var x = [{measure_name}\nreturn"

        if status_type == "Linear":
            expr = f"{expr}\nif(isblank(x),blank(),\n\tif(x<{lower_bound},-1,\n\t\tif(x<{upper_bound},0,1)))"
        elif status_type == "LinearReversed":
            expr = f"{expr}\nif(isblank(x),blank(),\nif(x<{lower_bound},1,\n\t\tif(x<{upper_bound},0,-1)))"
        elif status_type == "Centered":
            expr = f"{expr}\nif(isblank(x),blank(),\n\tif(x<{lower_mid_bound},\n\t\tif(x<{lower_bound},-1,0),\n\t\t\tif(x<{upper_mid_bound},1,\n\t\t\t\tif(x<{upper_bound}0,-1))))"
        elif status_type == "CenteredReversed":
            expr = f"{expr}\nif(isblank(x),blank(),\n\tif(x<{lower_mid_bound},\n\t\tif(x<{lower_bound},1,0),\n\t\t\tif(x<{upper_mid_bound},-1,\n\t\t\t\tif(x<{upper_bound}0,1))))"

        kpi = TOM.KPI()
        kpi.TargetExpression = tgt
        kpi.StatusGraphic = status_graphic
        kpi.StatusExpression = expr

        ms = self.model.Tables[table_name].Measures[measure_name]
        if ms.KPI is not None:
            ms.KPI.TargetExpression = tgt
            ms.KPI.StatusGraphic = status_graphic
            ms.KPI.StatusExpression = expr
        else:
            ms.KPI = kpi

    def set_aggregations(self, table_name: str, agg_table_name: str):
        """
        Sets the `aggregations <https://learn.microsoft.com/power-bi/transform-model/aggregations-advanced>`_ (alternate of) for all the columns in an aggregation table based on a base table.

        Parameters
        ----------
        table_name : str
            Name of the base table.
        agg_table_name : str
            Name of the aggregation table.

        Returns
        -------

        """

        import Microsoft.AnalysisServices.Tabular as TOM

        for c in self.model.Tables[agg_table_name].Columns:

            dataType = c.DataType

            if dataType in [
                TOM.DataType.String,
                TOM.DataType.Boolean,
                TOM.DataType.DateTime,
            ]:
                sumType = "GroupBy"
            else:
                sumType = "Sum"

            self.set_alternate_of(
                table_name=agg_table_name,
                column_name=c.Name,
                base_table=table_name,
                base_column=c.Name,
                summarization_type=sumType,
            )

    def set_is_available_in_mdx(
        self, table_name: str, column_name: str, value: bool = False
    ):
        """
        Sets the `IsAvailableInMDX <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.column.isavailableinmdx?view=analysisservices-dotnet#microsoft-analysisservices-tabular-column-isavailableinmdx>`_ property on a column.

        Parameters
        ----------
        table_name : str
            Name of the table.
        column_name : str
            Name of the column.
        value : bool, default=False
            The IsAvailableInMdx property value.
        """

        self.model.Tables[table_name].Columns[column_name].IsAvailableInMDX = value

    def set_summarize_by(
        self, table_name: str, column_name: str, value: Optional[str] = None
    ):
        """
        Sets the `SummarizeBy <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.column.summarizeby?view=analysisservices-dotnet#microsoft-analysisservices-tabular-column-summarizeby>`_ property on a column.

        Parameters
        ----------
        table_name : str
            Name of the table.
        column_name : str
            Name of the column.
        value : bool, default=None
            The SummarizeBy property value.
            Defaults to none which resolves to 'Default'.
            `Aggregate valid values <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.aggregatefunction?view=analysisservices-dotnet>`_
        """
        import Microsoft.AnalysisServices.Tabular as TOM
        import System

        values = [
            "Default",
            "None",
            "Sum",
            "Min",
            "Max",
            "Count",
            "Average",
            "DistinctCount",
        ]
        # https://learn.microsoft.com/en-us/dotnet/api/microsoft.analysisservices.tabular.column.summarizeby?view=analysisservices-dotnet#microsoft-analysisservices-tabular-column-summarizeby

        if value is None:
            value = "Default"
        value = (
            value.capitalize()
            .replace("Distinctcount", "DistinctCount")
            .replace("Avg", "Average")
        )

        if value not in values:
            raise ValueError(
                f"{icons.red_dot} '{value}' is not a valid value for the SummarizeBy property. These are the valid values: {values}."
            )

        self.model.Tables[table_name].Columns[column_name].SummarizeBy = (
            System.Enum.Parse(TOM.AggregateFunction, value)
        )

    def set_direct_lake_behavior(self, direct_lake_behavior: str):
        """
        Sets the `Direct Lake Behavior <https://learn.microsoft.com/fabric/get-started/direct-lake-overview#fallback-behavior>`_ property for a semantic model.

        Parameters
        ----------
        direct_lake_behavior : str
            The DirectLakeBehavior property value.
            `DirectLakeBehavior valid values <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.directlakebehavior?view=analysisservices-dotnet>`_
        """
        import Microsoft.AnalysisServices.Tabular as TOM
        import System

        direct_lake_behavior = direct_lake_behavior.capitalize()
        if direct_lake_behavior.startswith("Auto"):
            direct_lake_behavior = "Automatic"
        elif direct_lake_behavior.startswith("Directl") or direct_lake_behavior == "Dl":
            direct_lake_behavior = "DirectLakeOnly"
        elif direct_lake_behavior.startswith("Directq") or direct_lake_behavior == "Dq":
            direct_lake_behavior = "DirectQueryOnly"

        dlValues = ["Automatic", "DirectLakeOnly", "DirectQueryOnly"]

        if direct_lake_behavior not in dlValues:
            raise ValueError(
                f"{icons.red_dot} The 'direct_lake_behavior' parameter must be one of these values: {dlValues}."
            )

        self.model.DirectLakeBehavior = System.Enum.Parse(
            TOM.DirectLakeBehavior, direct_lake_behavior
        )

    def add_table(
        self,
        name: str,
        description: Optional[str] = None,
        data_category: Optional[str] = None,
        hidden: bool = False,
        lineage_tag: Optional[str] = None,
        source_lineage_tag: Optional[str] = None,
    ):
        """
        Adds a table to the semantic model.

        Parameters
        ----------
        name : str
            Name of the table.
        description : str, default=None
            A description of the table.
        data_catgegory : str, default=None
            The data category for the table.
        hidden : bool, default=False
            Whether the table is hidden or visible.
        lineage_tag : str, default=None
            A tag that represents the lineage of the object.
        source_lineage_tag : str, default=None
            A tag that represents the lineage of the source for the object.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        t = TOM.Table()
        t.Name = name
        if description is not None:
            t.Description = description
        if data_category is not None:
            t.DataCategory = data_category
        if lineage_tag is not None:
            t.LineageTag = lineage_tag
        else:
            t.LineageTag = generate_guid()
        if source_lineage_tag is not None:
            t.SourceLineageTag = source_lineage_tag
        t.Hidden = hidden
        self.model.Tables.Add(t)

    def add_calculated_table(
        self,
        name: str,
        expression: str,
        description: Optional[str] = None,
        data_category: Optional[str] = None,
        hidden: bool = False,
        lineage_tag: Optional[str] = None,
        source_lineage_tag: Optional[str] = None,
    ):
        """
        Adds a calculated table to the semantic model.

        Parameters
        ----------
        name : str
            Name of the table.
        expression : str
            The DAX expression for the calculated table.
        description : str, default=None
            A description of the table.
        data_catgegory : str, default=None
            The data category for the table.
        hidden : bool, default=False
            Whether the table is hidden or visible.
        lineage_tag : str, default=None
            A tag that represents the lineage of the object.
        source_lineage_tag : str, default=None
            A tag that represents the lineage of the source for the object.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        par = TOM.Partition()
        par.Name = name
        par.Mode = TOM.ModeType.Import

        parSource = TOM.CalculatedPartitionSource()
        parSource.Expression = expression
        par.Source = parSource

        t = TOM.Table()
        t.Name = name
        if description is not None:
            t.Description = description
        if data_category is not None:
            t.DataCategory = data_category
        if lineage_tag is not None:
            t.LineageTag = lineage_tag
        else:
            t.LineageTag = generate_guid()
        if source_lineage_tag is not None:
            t.SourceLineageTag = source_lineage_tag
        t.Hidden = hidden
        t.Partitions.Add(par)
        self.model.Tables.Add(t)

    def add_field_parameter(
        self, table_name: str, objects: List[str], object_names: List[str] = None
    ):
        """
        Adds a `field parameter <https://learn.microsoft.com/power-bi/create-reports/power-bi-field-parameters>`_ to the semantic model.

        Parameters
        ----------
        table_name : str
            Name of the table.
        objects : List[str]
            The columns/measures to be included in the field parameter.
            Columns must be specified as such : 'Table Name'[Column Name].
            Measures may be formatted as '[Measure Name]' or 'Measure Name'.
        object_names : List[str], default=None
            The corresponding visible name for the measures/columns in the objects list.
            Defaults to None which shows the measure/column name.
        """

        import Microsoft.AnalysisServices.Tabular as TOM

        if isinstance(objects, str):
            raise ValueError(
                f"{icons.red_dot} The 'objects' parameter must be a list of columns/measures."
            )

        if len(objects) == 1:
            raise ValueError(
                f"{icons.red_dot} There must be more than one object (column/measure) within the objects parameter."
            )

        if object_names is not None and len(objects) != len(object_names):
            raise ValueError(
                f"{icons.red_dot} If the 'object_names' parameter is specified, it must correspond exactly to the 'objects' parameter."
            )

        expr = ""
        i = 0
        for obj in objects:
            index = objects.index(obj)
            success = False
            for m in self.all_measures():
                obj_name = m.Name
                if obj == f"[{obj_name}]" or obj == obj_name:
                    if object_names is not None:
                        obj_name = object_names[index]
                    expr = f'{expr}\n\t("{obj_name}", NAMEOF([{m.Name}]), {str(i)}),'
                    success = True
            for c in self.all_columns():
                obj_name = c.Name
                fullObjName = format_dax_object_name(c.Parent.Name, c.Name)
                if obj == fullObjName or obj == c.Parent.Name + "[" + c.Name + "]":
                    if object_names is not None:
                        obj_name = object_names[index]
                    expr = f'{expr}\n\t("{obj_name}", NAMEOF({fullObjName}), {str(i)}),'
                    success = True
            if not success:
                raise ValueError(
                    f"{icons.red_dot} The '{obj}' object was not found in the '{self._dataset_name}' semantic model."
                )
            else:
                i += 1

        expr = "{" + expr.rstrip(",") + "\n}"

        self.add_calculated_table(name=table_name, expression=expr)

        col2 = table_name + " Fields"
        col3 = table_name + " Order"

        self.add_calculated_table_column(
            table_name=table_name,
            column_name=table_name,
            source_column="[Value1]",
            data_type="String",
            hidden=False,
        )
        self.add_calculated_table_column(
            table_name=table_name,
            column_name=col2,
            source_column="[Value2]",
            data_type="String",
            hidden=True,
        )
        self.add_calculated_table_column(
            table_name=table_name,
            column_name=col3,
            source_column="[Value3]",
            data_type="Int64",
            hidden=True,
        )

        self.set_extended_property(
            object=self.model.Tables[table_name].Columns[col2],
            extended_property_type="Json",
            name="ParameterMetadata",
            value='{"version":3,"kind":2}',
        )

        rcd = TOM.RelatedColumnDetails()
        gpc = TOM.GroupByColumn()
        gpc.GroupingColumn = self.model.Tables[table_name].Columns[col2]
        rcd.GroupByColumns.Add(gpc)

        # Update column properties
        self.model.Tables[table_name].Columns[col2].SortByColumn = self.model.Tables[
            table_name
        ].Columns[col3]
        self.model.Tables[table_name].Columns[table_name].RelatedColumnDetails = rcd

        self._tables_added.append(table_name)

    def remove_vertipaq_annotations(self):
        """
        Removes the annotations set using the set_vertipaq_annotations function.
        """

        for t in self.model.Tables:
            for a in t.Annotations:
                if a.Name.startswith("Vertipaq_"):
                    self.remove_annotation(object=t, name=a.Name)
            for c in t.Columns:
                for a in c.Annotations:
                    if a.Name.startswith("Vertipaq_"):
                        self.remove_annotation(object=c, name=a.Name)
            for h in t.Hierarchies:
                for a in h.Annotations:
                    if a.Name.startswith("Vertipaq_"):
                        self.remove_annotation(object=h, name=a.Name)
            for p in t.Partitions:
                for a in p.Annotations:
                    if a.Name.startswith("Vertipaq_"):
                        self.remove_annotation(object=p, name=a.Name)
        for r in self.model.Relationships:
            for a in r.Annotations:
                if a.Name.startswith("Veripaq_"):
                    self.remove_annotation(object=r, name=a.Name)

    def set_vertipaq_annotations(self):
        """
        Saves Vertipaq Analyzer statistics as annotations on objects in the semantic model.
        """

        from sempy_labs._list_functions import list_tables

        dfT = list_tables(
            dataset=self._dataset_id, workspace=self._workspace_id, extended=True
        )
        dfC = fabric.list_columns(
            dataset=self._dataset_id, workspace=self._workspace_id, extended=True
        )
        dfP = fabric.list_partitions(
            dataset=self._dataset_id, workspace=self._workspace_id, extended=True
        )
        dfH = fabric.list_hierarchies(
            dataset=self._dataset_id, workspace=self._workspace_id, extended=True
        )
        dfR = list_relationships(
            dataset=self._dataset_id, workspace=self._workspace_id, extended=True
        )

        for t in self.model.Tables:
            dfT_filt = dfT[dfT["Name"] == t.Name]
            if len(dfT_filt) > 0:
                row = dfT_filt.iloc[0]
                rowCount = str(row["Row Count"])
                totalSize = str(row["Total Size"])
                self.set_annotation(object=t, name="Vertipaq_RowCount", value=rowCount)
                self.set_annotation(
                    object=t, name="Vertipaq_TableSize", value=totalSize
                )
            for c in t.Columns:
                dfC_filt = dfC[
                    (dfC["Table Name"] == t.Name) & (dfC["Column Name"] == c.Name)
                ]
                if len(dfC_filt) > 0:
                    row = dfC_filt.iloc[0]
                    totalSize = str(row["Total Size"])
                    dataSize = str(row["Data Size"])
                    dictSize = str(row["Dictionary Size"])
                    hierSize = str(row["Hierarchy Size"])
                    card = str(row["Column Cardinality"])
                    self.set_annotation(
                        object=c, name="Vertipaq_TotalSize", value=totalSize
                    )
                    self.set_annotation(
                        object=c, name="Vertipaq_DataSize", value=dataSize
                    )
                    self.set_annotation(
                        object=c, name="Vertipaq_DictionarySize", value=dictSize
                    )
                    self.set_annotation(
                        object=c, name="Vertipaq_HierarchySize", value=hierSize
                    )
                    self.set_annotation(
                        object=c, name="Vertipaq_Cardinality", value=card
                    )
            for p in t.Partitions:
                dfP_filt = dfP[
                    (dfP["Table Name"] == t.Name) & (dfP["Partition Name"] == p.Name)
                ]
                if len(dfP_filt) > 0:
                    row = dfP_filt.iloc[0]
                    recordCount = str(row["Record Count"])
                    segmentCount = str(row["Segment Count"])
                    rpS = str(row["Records per Segment"])
                    self.set_annotation(
                        object=p, name="Vertipaq_RecordCount", value=recordCount
                    )
                    self.set_annotation(
                        object=p, name="Vertipaq_SegmentCount", value=segmentCount
                    )
                    self.set_annotation(
                        object=p, name="Vertipaq_RecordsPerSegment", value=rpS
                    )
            for h in t.Hierarchies:
                dfH_filt = dfH[
                    (dfH["Table Name"] == t.Name) & (dfH["Hierarchy Name"] == h.Name)
                ]
                if len(dfH_filt) > 0:
                    usedSize = str(dfH_filt["Used Size"].iloc[0])
                    self.set_annotation(
                        object=h, name="Vertipaq_UsedSize", value=usedSize
                    )
        for r in self.model.Relationships:
            dfR_filt = dfR[dfR["Relationship Name"] == r.Name]
            if len(dfR_filt) > 0:
                relSize = str(dfR_filt["Used Size"].iloc[0])
                self.set_annotation(object=r, name="Vertipaq_UsedSize", value=relSize)
        try:
            runId = self.get_annotation_value(object=self.model, name="Vertipaq_Run")
            runId = str(int(runId) + 1)
        except Exception:
            runId = "1"
        self.set_annotation(object=self.model, name="Vertipaq_Run", value=runId)

        icons.sll_tags.append("VertipaqAnnotations")

    def row_count(self, object: Union["TOM.Partition", "TOM.Table"]):
        """
        Obtains the row count of a table or partition within a semantic model.

        Parameters
        ----------
        object : TOM Object
            The table/partition object within the semantic model.

        Returns
        -------
        int
            Number of rows within the TOM object.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        objType = object.ObjectType

        if objType == TOM.ObjectType.Table:
            result = self.get_annotation_value(object=object, name="Vertipaq_RowCount")
        elif objType == TOM.ObjectType.Partition:
            result = self.get_annotation_value(
                object=object, name="Vertipaq_RecordCount"
            )

        return int(result) if result is not None else 0

    def records_per_segment(self, object: "TOM.Partition"):
        """
        Obtains the records per segment of a partition within a semantic model.

        Parameters
        ----------
        object : TOM Object
            The partition object within the semantic model.

        Returns
        -------
        float
            Number of records per segment within the partition.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        objType = object.ObjectType

        if objType == TOM.ObjectType.Partition:
            result = self.get_annotation_value(
                object=object, name="Vertipaq_RecordsPerSegment"
            )

        return float(result) if result is not None else 0

    def used_size(self, object: Union["TOM.Hierarchy", "TOM.Relationship"]):
        """
        Obtains the used size of a hierarchy or relationship within a semantic model.

        Parameters
        ----------
        object : TOM Object
            The hierarhcy/relationship object within the semantic model.

        Returns
        -------
        int
            Used size of the TOM object.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        objType = object.ObjectType

        if objType == TOM.ObjectType.Hierarchy:
            result = self.get_annotation_value(object=object, name="Vertipaq_UsedSize")
        elif objType == TOM.ObjectType.Relationship:
            result = self.get_annotation_value(object=object, name="Vertipaq_UsedSize")

        return int(result) if result is not None else 0

    def data_size(self, column: "TOM.Column"):
        """
        Obtains the data size of a column within a semantic model.

        Parameters
        ----------
        column : TOM Object
            The column object within the semantic model.

        Returns
        -------
        int
            Data size of the TOM column.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        objType = column.ObjectType

        if objType == TOM.ObjectType.Column:
            result = self.get_annotation_value(object=column, name="Vertipaq_DataSize")

        return int(result) if result is not None else 0

    def dictionary_size(self, column: "TOM.Column"):
        """
        Obtains the dictionary size of a column within a semantic model.

        Parameters
        ----------
        column : TOM Object
            The column object within the semantic model.

        Returns
        -------
        int
            Dictionary size of the TOM column.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        objType = column.ObjectType

        if objType == TOM.ObjectType.Column:
            result = self.get_annotation_value(
                object=column, name="Vertipaq_DictionarySize"
            )

        return int(result) if result is not None else 0

    def total_size(self, object: Union["TOM.Table", "TOM.Column"]):
        """
        Obtains the data size of a table/column within a semantic model.

        Parameters
        ----------
        object : TOM Object
            The table/column object within the semantic model.

        Returns
        -------
        int
            Total size of the TOM table/column.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        objType = object.ObjectType

        if objType == TOM.ObjectType.Column:
            result = self.get_annotation_value(object=object, name="Vertipaq_TotalSize")
        elif objType == TOM.ObjectType.Table:
            result = self.get_annotation_value(object=object, name="Vertipaq_TotalSize")

        return int(result) if result is not None else 0

    def cardinality(self, column: "TOM.Column"):
        """
        Obtains the cardinality of a column within a semantic model.

        Parameters
        ----------
        column : TOM Object
            The column object within the semantic model.

        Returns
        -------
        int
            Cardinality of the TOM column.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        objType = column.ObjectType

        if objType == TOM.ObjectType.Column:
            result = self.get_annotation_value(
                object=column, name="Vertipaq_Cardinality"
            )

        return int(result) if result is not None else 0

    def depends_on(self, object, dependencies: pd.DataFrame):
        """
        Obtains the objects on which the specified object depends.

        Parameters
        ----------
        object : TOM Object
            The TOM object within the semantic model.
        dependencies : pandas.DataFrame
            A pandas dataframe with the output of the 'get_model_calc_dependencies' function.

        Returns
        -------
        Microsoft.AnalysisServices.Tabular.TableCollection, Microsoft.AnalysisServices.Tabular.ColumnCollection, Microsoft.AnalysisServices.Tabular.MeasureCollection
            Objects on which the specified object depends.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        objType = object.ObjectType
        objName = object.Name
        objParentName = object.Parent.Name

        if objType == TOM.ObjectType.Table:
            objParentName = objName

        fil = dependencies[
            (dependencies["Object Type"] == str(objType))
            & (dependencies["Table Name"] == objParentName)
            & (dependencies["Object Name"] == objName)
        ]
        meas = (
            fil[fil["Referenced Object Type"] == "Measure"]["Referenced Object"]
            .unique()
            .tolist()
        )
        cols = (
            fil[fil["Referenced Object Type"] == "Column"][
                "Referenced Full Object Name"
            ]
            .unique()
            .tolist()
        )
        tbls = (
            fil[fil["Referenced Object Type"] == "Table"]["Referenced Table"]
            .unique()
            .tolist()
        )
        for m in self.all_measures():
            if m.Name in meas:
                yield m
        for c in self.all_columns():
            if format_dax_object_name(c.Parent.Name, c.Name) in cols:
                yield c
        for t in self.model.Tables:
            if t.Name in tbls:
                yield t

    def referenced_by(self, object, dependencies: pd.DataFrame):
        """
        Obtains the objects which reference the specified object.

        Parameters
        ----------
        object : TOM Object
            The TOM object within the semantic model.
        dependencies : pandas.DataFrame
            A pandas dataframe with the output of the 'get_model_calc_dependencies' function.

        Returns
        -------
        Microsoft.AnalysisServices.Tabular.TableCollection, Microsoft.AnalysisServices.Tabular.ColumnCollection, Microsoft.AnalysisServices.Tabular.MeasureCollection
            Objects which reference the specified object.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        objType = object.ObjectType
        objName = object.Name
        objParentName = object.Parent.Name

        if objType == TOM.ObjectType.Table:
            objParentName = objName

        fil = dependencies[
            (dependencies["Referenced Object Type"] == str(objType))
            & (dependencies["Referenced Table"] == objParentName)
            & (dependencies["Referenced Object"] == objName)
        ]
        meas = fil[fil["Object Type"] == "Measure"]["Object Name"].unique().tolist()
        cols = (
            fil[fil["Object Type"].isin(["Column", "Calc Column"])]["Full Object Name"]
            .unique()
            .tolist()
        )
        tbls = (
            fil[fil["Object Type"].isin(["Table", "Calc Table"])]["Table Name"]
            .unique()
            .tolist()
        )
        for m in self.all_measures():
            if m.Name in meas:
                yield m
        for c in self.all_columns():
            if format_dax_object_name(c.Parent.Name, c.Name) in cols:
                yield c
        for t in self.model.Tables:
            if t.Name in tbls:
                yield t

    def fully_qualified_measures(
        self, object: "TOM.Measure", dependencies: pd.DataFrame
    ):
        """
        Obtains all fully qualified measure references for a given object.

        Parameters
        ----------
        object : TOM Object
            The TOM object within the semantic model.
        dependencies : pandas.DataFrame
            A pandas dataframe with the output of the 'get_model_calc_dependencies' function.

        Returns
        -------
        Microsoft.AnalysisServices.Tabular.MeasureCollection
            All fully qualified measure references for a given object.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        dependencies = dependencies[
            dependencies["Object Name"] == dependencies["Parent Node"]
        ]

        for obj in self.depends_on(object=object, dependencies=dependencies):
            if obj.ObjectType == TOM.ObjectType.Measure:
                if (f"{obj.Parent.Name}[{obj.Name}]" in object.Expression) or (
                    format_dax_object_name(obj.Parent.Name, obj.Name)
                    in object.Expression
                ):
                    yield obj

    def unqualified_columns(self, object: "TOM.Column", dependencies: pd.DataFrame):
        """
        Obtains all unqualified column references for a given object.

        Parameters
        ----------
        object : TOM Object
            The TOM object within the semantic model.
        dependencies : pandas.DataFrame
            A pandas dataframe with the output of the 'get_model_calc_dependencies' function.

        Returns
        -------
        Microsoft.AnalysisServices.Tabular.ColumnCollection
            All unqualified column references for a given object.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        dependencies = dependencies[
            dependencies["Object Name"] == dependencies["Parent Node"]
        ]

        def create_pattern(tableList, b):
            patterns = [
                r"(?<!" + re.escape(table) + r")(?<!'" + re.escape(table) + r"')"
                for table in tableList
            ]
            combined_pattern = "".join(patterns) + re.escape(f"[{b}]")
            return combined_pattern

        for obj in self.depends_on(object=object, dependencies=dependencies):
            if obj.ObjectType == TOM.ObjectType.Column:
                tableList = []
                for c in self.all_columns():
                    if c.Name == obj.Name:
                        tableList.append(c.Parent.Name)
                if (
                    re.search(create_pattern(tableList, obj.Name), object.Expression)
                    is not None
                ):
                    yield obj

    def is_direct_lake_using_view(self):
        """
        Identifies whether a semantic model is in Direct lake mode and uses views from the lakehouse.

        Parameters
        ----------

        Returns
        -------
        bool
            An indicator whether a semantic model is in Direct lake mode and uses views from the lakehouse.
        """

        usingView = False

        if self.is_direct_lake():
            df = check_fallback_reason(
                dataset=self._dataset_id, workspace=self._workspace_id
            )
            df_filt = df[df["FallbackReasonID"] == 2]

            if len(df_filt) > 0:
                usingView = True

        return usingView

    def has_incremental_refresh_policy(self, table_name: str):
        """
        Identifies whether a table has an `incremental refresh <https://learn.microsoft.com/power-bi/connect-data/incremental-refresh-overview>`_ policy.

        Parameters
        ----------
        table_name : str
            Name of the table.

        Returns
        -------
        bool
            An indicator whether a table has an incremental refresh policy.
        """

        hasRP = False
        rp = self.model.Tables[table_name].RefreshPolicy

        if rp is not None:
            hasRP = True

        return hasRP

    def show_incremental_refresh_policy(self, table_name: str):
        """
        Prints the `incremental refresh <https://learn.microsoft.com/power-bi/connect-data/incremental-refresh-overview>`_ policy for a table.

        Parameters
        ----------
        table_name : str
            Name of the table.
        """

        import Microsoft.AnalysisServices.Tabular as TOM

        rp = self.model.Tables[table_name].RefreshPolicy

        if rp is None:
            print(
                f"{icons.yellow_dot} The '{table_name}' table in the '{self._dataset_name}' semantic model within the '{self._workspace_name}' workspace does not have an incremental refresh policy."
            )
        else:
            print(f"Table Name: {table_name}")
            rwGran = str(rp.RollingWindowGranularity).lower()
            icGran = str(rp.IncrementalGranularity).lower()
            if rp.RollingWindowPeriods > 1:
                print(
                    f"Archive data starting {icons.start_bold}{rp.RollingWindowPeriods} {rwGran}s{icons.end_bold} before refresh date."
                )
            else:
                print(
                    f"Archive data starting {icons.start_bold}{rp.RollingWindowPeriods} {rwGran}{icons.end_bold} before refresh date."
                )
            if rp.IncrementalPeriods > 1:
                print(
                    f"Incrementally refresh data {icons.start_bold}{rp.IncrementalPeriods} {icGran}s{icons.end_bold} before refresh date."
                )
            else:
                print(
                    f"Incrementally refresh data {icons.start_bold}{rp.IncrementalPeriods} {icGran}{icons.end_bold} before refresh date."
                )

            if rp.Mode == TOM.RefreshPolicyMode.Hybrid:
                print(
                    f"{icons.checked} Get the latest data in real time with DirectQuery (Premium only)"
                )
            else:
                print(
                    f"{icons.unchecked} Get the latest data in real time with DirectQuery (Premium only)"
                )
            if rp.IncrementalPeriodsOffset == -1:
                print(f"{icons.checked} Only refresh complete days")
            else:
                print(f"{icons.unchecked} Only refresh complete days")
            if len(rp.PollingExpression) > 0:
                pattern = r"\[([^\]]+)\]"
                match = re.search(pattern, rp.PollingExpression)
                if match:
                    col = match[0][1:-1]
                    fullCol = format_dax_object_name(table_name, col)
                    print(
                        f"{icons.checked} Detect data changes: {icons.start_bold}{fullCol}{icons.end_bold}"
                    )
            else:
                print(f"{icons.unchecked} Detect data changes")

    def update_incremental_refresh_policy(
        self,
        table_name: str,
        incremental_granularity: str,
        incremental_periods: int,
        rolling_window_granularity: str,
        rolling_window_periods: int,
        only_refresh_complete_days: bool = False,
        detect_data_changes_column: Optional[str] = None,
    ):
        """
        Updates the `incremental refresh <https://learn.microsoft.com/power-bi/connect-data/incremental-refresh-overview>`_ policy for a table within a semantic model.

        Parameters
        ----------
        table_name : str
            Name of the table.
        incremental_granularity : str
            Granularity of the (most recent) incremental refresh range.
        incremental_periods : int
            Number of periods for the incremental refresh range.
        rolling_window_granularity : str
            Target granularity of the rolling window for the whole semantic model.
        rolling_window_periods : int
            Number of periods for the rolling window for the whole semantic model.
        only_refresh_complete_days : bool, default=False
            Lag or leading periods from Now() to the rolling window head.
        detect_data_changes_column : str, default=None
            The column to use for detecting data changes.
            Defaults to None which resolves to not detecting data changes.
        """
        import Microsoft.AnalysisServices.Tabular as TOM
        import System

        if not self.has_incremental_refresh_policy(table_name=table_name):
            print(
                f"The '{table_name}' table does not have an incremental refresh policy."
            )
            return

        incGran = ["Day", "Month", "Quarter", "Year"]

        incremental_granularity = incremental_granularity.capitalize()
        rolling_window_granularity = rolling_window_granularity.capitalize()

        if incremental_granularity not in incGran:
            raise ValueError(
                f"{icons.red_dot} Invalid 'incremental_granularity' value. Please choose from the following options: {incGran}."
            )

        if rolling_window_granularity not in incGran:
            raise ValueError(
                f"{icons.red_dot} Invalid 'rolling_window_granularity' value. Please choose from the following options: {incGran}."
            )

        if rolling_window_periods < 1:
            raise ValueError(
                f"{icons.red_dot} Invalid 'rolling_window_periods' value. Must be a value greater than 0."
            )

        if incremental_periods < 1:
            raise ValueError(
                f"{icons.red_dot} Invalid 'incremental_periods' value. Must be a value greater than 0."
            )

        t = self.model.Tables[table_name]

        if detect_data_changes_column is not None:
            dc = t.Columns[detect_data_changes_column]

            if dc.DataType != TOM.DataType.DateTime:
                raise ValueError(
                    f"{icons.red_dot} Invalid 'detect_data_changes_column' parameter. This column must be of DateTime data type."
                )

        rp = TOM.BasicRefreshPolicy()
        rp.IncrementalPeriods = incremental_periods
        rp.IncrementalGranularity = System.Enum.Parse(
            TOM.RefreshGranularityType, incremental_granularity
        )
        rp.RollingWindowPeriods = rolling_window_periods
        rp.RollingWindowGranularity = System.Enum.Parse(
            TOM.RefreshGranularityType, rolling_window_granularity
        )
        rp.SourceExpression = t.RefreshPolicy.SourceExpression

        if only_refresh_complete_days:
            rp.IncrementalPeriodsOffset = -1
        else:
            rp.IncrementalPeriodOffset = 0

        if detect_data_changes_column is not None:
            fullDC = format_dax_object_name(table_name, detect_data_changes_column)
            ddcExpr = f"let Max{detect_data_changes_column} = List.Max({fullDC}), accountForNull = if Max{detect_data_changes_column} = null then #datetime(1901, 01, 01, 00, 00, 00) else Max{detect_data_changes_column} in accountForNull"
            rp.PollingExpression = ddcExpr
        else:
            rp.PollingExpression = None

        t.RefreshPolicy = rp

        self.show_incremental_refresh_policy(table_name=table_name)

    def add_incremental_refresh_policy(
        self,
        table_name: str,
        column_name: str,
        start_date: str,
        end_date: str,
        incremental_granularity: str,
        incremental_periods: int,
        rolling_window_granularity: str,
        rolling_window_periods: int,
        only_refresh_complete_days: bool = False,
        detect_data_changes_column: Optional[str] = None,
    ):
        """
        Adds an `incremental refresh <https://learn.microsoft.com/power-bi/connect-data/incremental-refresh-overview>`_ policy for a table within a semantic model.

        Parameters
        ----------
        table_name : str
            Name of the table.
        column_name : str
            The DateTime column to be used for the RangeStart and RangeEnd parameters.
        start_date : str
            The date to be used for the RangeStart parameter.
        end_date : str
            The date to be used for the RangeEnd parameter.
        incremental_granularity : str
            Granularity of the (most recent) incremental refresh range.
        incremental_periods : int
            Number of periods for the incremental refresh range.
        rolling_window_granularity : str
            Target granularity of the rolling window for the whole semantic model.
        rolling_window_periods : int
            Number of periods for the rolling window for the whole semantic model.
        only_refresh_complete_days : bool, default=False
            Lag or leading periods from Now() to the rolling window head.
        detect_data_changes_column : str, default=None
            The column to use for detecting data changes.
            Defaults to None which resolves to not detecting data changes.
        """
        import Microsoft.AnalysisServices.Tabular as TOM
        import System

        # https://learn.microsoft.com/en-us/power-bi/connect-data/incremental-refresh-configure

        incGran = ["Day", "Month", "Quarter", "Year"]

        incremental_granularity = incremental_granularity.capitalize()
        rolling_window_granularity = rolling_window_granularity.capitalize()

        if incremental_granularity not in incGran:
            raise ValueError(
                f"{icons.red_dot} Invalid 'incremental_granularity' value. Please choose from the following options: {incGran}."
            )

        if rolling_window_granularity not in incGran:
            raise ValueError(
                f"{icons.red_dot} Invalid 'rolling_window_granularity' value. Please choose from the following options: {incGran}."
            )

        if rolling_window_periods < 1:
            raise ValueError(
                f"{icons.red_dot} Invalid 'rolling_window_periods' value. Must be a value greater than 0."
            )

        if incremental_periods < 1:
            raise ValueError(
                f"{icons.red_dot} Invalid 'incremental_periods' value. Must be a value greater than 0."
            )

        date_format = "%m/%d/%Y"

        date_obj_start = datetime.strptime(start_date, date_format)
        start_year = date_obj_start.year
        start_month = date_obj_start.month
        start_day = date_obj_start.day

        date_obj_end = datetime.strptime(end_date, date_format)
        end_year = date_obj_end.year
        end_month = date_obj_end.month
        end_day = date_obj_end.day

        if date_obj_end <= date_obj_start:
            raise ValueError(
                f"{icons.red_dot} Invalid 'start_date' or 'end_date'. The 'end_date' must be after the 'start_date'."
            )

        t = self.model.Tables[table_name]

        c = t.Columns[column_name]
        fcName = format_dax_object_name(table_name, column_name)
        dType = c.DataType

        if dType != TOM.DataType.DateTime:
            raise ValueError(
                f"{icons.red_dot} The {fcName} column is of '{dType}' data type. The column chosen must be of DateTime data type."
            )

        if detect_data_changes_column is not None:
            dc = t.Columns[detect_data_changes_column]
            dcType = dc.DataType

            if dcType != TOM.DataType.DateTime:
                raise ValueError(
                    f"{icons.red_dot} Invalid 'detect_data_changes_column' parameter. This column must be of DateTime data type."
                )

        # Start changes:

        # Update partition expression
        i = 0
        for p in t.Partitions:
            if p.SourceType != TOM.PartitionSourceType.M:
                raise ValueError(
                    f"{icons.red_dot} Invalid partition source type. Incremental refresh can only be set up if the table's partition is an M-partition."
                )

            elif i == 0:
                text = p.Expression
                text = text.rstrip()

                ind = text.rfind(" ") + 1
                obj = text[ind:]
                pattern = r"in\s*[^ ]*"
                matches = list(re.finditer(pattern, text))

                if matches:
                    last_match = matches[-1]
                    text_before_last_match = text[: last_match.start()]

                    print(text_before_last_match)
                else:
                    raise ValueError(f"{icons.red_dot} Invalid M-partition expression.")

                endExpr = f'#"Filtered Rows IR" = Table.SelectRows({obj}, each [{column_name}] >= RangeStart and [{column_name}] <= RangeEnd)\n#"Filtered Rows IR"'
                finalExpr = text_before_last_match + endExpr

                p.Expression = finalExpr
            i += 1

        # Add expressions
        self.add_expression(
            name="RangeStart",
            expression=f'datetime({start_year}, {start_month}, {start_day}, 0, 0, 0) meta [IsParameterQuery=true, Type="DateTime", IsParameterQueryRequired=true]',
        )
        self.add_expression(
            name="RangeEnd",
            expression=f'datetime({end_year}, {end_month}, {end_day}, 0, 0, 0) meta [IsParameterQuery=true, Type="DateTime", IsParameterQueryRequired=true]',
        )

        # Update properties
        rp = TOM.BasicRefreshPolicy()
        rp.IncrementalPeriods = incremental_periods
        rp.IncrementalGranularity = System.Enum.Parse(
            TOM.RefreshGranularityType, incremental_granularity
        )
        rp.RollingWindowPeriods = rolling_window_periods
        rp.RollingWindowGranularity = System.Enum.Parse(
            TOM.RefreshGranularityType, rolling_window_granularity
        )

        if only_refresh_complete_days:
            rp.IncrementalPeriodsOffset = -1
        else:
            rp.IncrementalPeriodOffset = 0

        if detect_data_changes_column is not None:
            fullDC = format_dax_object_name(table_name, detect_data_changes_column)
            ddcExpr = f"let Max{detect_data_changes_column} = List.Max({fullDC}), accountForNull = if Max{detect_data_changes_column} = null then #datetime(1901, 01, 01, 00, 00, 00) else Max{detect_data_changes_column} in accountForNull"
            rp.PollingExpression = ddcExpr

        t.RefreshPolicy = rp

        self.show_incremental_refresh_policy(table_name=table_name)

    def apply_refresh_policy(
        self,
        table_name: str,
        effective_date: Optional[datetime] = None,
        refresh: bool = True,
        max_parallelism: Optional[int] = 0,
    ):
        """
        `Applies the incremental refresh <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.table.applyrefreshpolicy?view=analysisservices-dotnet#microsoft-analysisservices-tabular-table-applyrefreshpolicy(system-boolean-system-int32)>`_ policy for a table within a semantic model.

        Parameters
        ----------
        table_name : str
            Name of the table.
        effective_date : DateTime, default=None
            The effective date that is used when calculating the partitioning scheme.
        refresh : bool, default=True
            An indication if partitions of the table should be refreshed or not; the default behavior is to do the refresh.
        max_parallelism : int, default=0
            The degree of parallelism during the refresh execution.
        """

        self.model.Tables[table_name].ApplyRefreshPolicy(
            effectiveDate=effective_date,
            refresh=refresh,
            maxParallelism=max_parallelism,
        )

    def set_data_coverage_definition(
        self, table_name: str, partition_name: str, expression: str
    ):
        """
        Sets the `data coverage definition <https://learn.microsoft.com/analysis-services/tom/table-partitions?view=asallproducts-allversions>`_ for a partition.

        Parameters
        ----------
        table_name : str
            Name of the table.
        partition_name : str
            Name of the partition.
        expression : str
            DAX expression containing the logic for the data coverage definition.
        """
        import Microsoft.AnalysisServices.Tabular as TOM

        doc = "https://learn.microsoft.com/analysis-services/tom/table-partitions?view=asallproducts-allversions"

        t = self.model.Tables[table_name]
        p = t.Partitions[partition_name]

        ht = self.is_hybrid_table(table_name=table_name)

        if not ht:
            raise ValueError(
                f"{icons.red_dot} The `data coverage definition <https://learn.microsoft.com/analysis-services/tom/table-partitions?view=asallproducts-allversions>`_ property is only applicable to `hybrid tables <https://learn.microsoft.com/power-bi/connect-data/service-dataset-modes-understand#hybrid-tables>`_. See the documentation: {doc}."
            )
        if p.Mode != TOM.ModeType.DirectQuery:
            raise ValueError(
                f"{icons.red_dot} The `data coverage definition <https://learn.microsoft.com/analysis-services/tom/table-partitions?view=asallproducts-allversions>`_ property is only applicable to the DirectQuery partition of a `hybrid table <https://learn.microsoft.com/power-bi/connect-data/service-dataset-modes-understand#hybrid-tables>`_. See the documentation: {doc}."
            )

        dcd = TOM.DataCoverageDefinition()
        dcd.Expression = expression
        p.DataCoverageDefinition = dcd

    def set_encoding_hint(self, table_name: str, column_name: str, value: str):
        """
        Sets the `encoding hint <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.encodinghinttype?view=analysisservices-dotnet>`_ for a column.

        Parameters
        ----------
        table_name : str
            Name of the table.
        column_name : str
            Name of the column.
        value : str
            Encoding hint value.
            `Encoding hint valid values <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.encodinghinttype?view=analysisservices-dotnet>`_
        """
        import Microsoft.AnalysisServices.Tabular as TOM
        import System

        values = ["Default", "Hash", "Value"]
        value = value.capitalize()

        if value not in values:
            raise ValueError(
                f"{icons.red_dot} Invalid encoding hint value. Please choose from these options: {values}."
            )

        self.model.Tables[table_name].Columns[column_name].EncodingHint = (
            System.Enum.Parse(TOM.EncodingHintType, value)
        )

    def set_data_type(self, table_name: str, column_name: str, value: str):
        """
        Sets the `data type <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.datatype?view=analysisservices-dotnet>`_ for a column.

        Parameters
        ----------
        table_name : str
            Name of the table.
        column_name : str
            Name of the column.
        value : str
            The data type.
            `Data type valid values <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.datatype?view=analysisservices-dotnet>`_
        """
        import Microsoft.AnalysisServices.Tabular as TOM
        import System

        values = [
            "Binary",
            "Boolean",
            "DateTime",
            "Decimal",
            "Double",
            "Int64",
            "String",
        ]

        value = value.replace(" ", "").capitalize()
        if value == "Datetime":
            value = "DateTime"
        elif value.startswith("Int"):
            value = "Int64"
        elif value.startswith("Bool"):
            value = "Boolean"

        if value not in values:
            raise ValueError(
                f"{icons.red_dot} Invalid data type. Please choose from these options: {values}."
            )

        self.model.Tables[table_name].Columns[column_name].DataType = System.Enum.Parse(
            TOM.DataType, value
        )

    def add_time_intelligence(
        self, measure_name: str, date_table: str, time_intel: Union[str, List[str]]
    ):
        """
        Adds time intelligence measures

        Parameters
        ----------
        measure_name : str
            Name of the measure
        date_table : str
            Name of the date table.
        time_intel : str, List[str]
            Time intelligence measures to create (i.e. MTD, YTD, QTD).
        """

        table_name = None
        time_intel_options = ["MTD", "QTD", "YTD"]

        if isinstance(time_intel, str):
            time_intel = [time_intel]

        # Validate time intelligence variations
        for t in time_intel:
            t = t.capitalize()
            if t not in [time_intel_options]:
                raise ValueError(
                    f"{icons.red_dot} The '{t}' time intelligence variation is not supported. Valid options: {time_intel_options}."
                )

        # Validate measure and extract table name
        matching_measures = [
            m.Parent.Name for m in self.all_measures() if m.Name == measure_name
        ]

        if table_name is None:
            raise ValueError(
                f"{icons.red_dot} The '{measure_name}' is not a valid measure in the '{self._dataset_name}' semantic model within the '{self._workspace_name}' workspace."
            )

        table_name = matching_measures[0]
        # Validate date table
        if not self.is_date_table(date_table):
            raise ValueError(
                f"{icons.red_dot} The '{date_table}' table is not a valid date table in the '{self._dataset_name}' wemantic model within the '{self._workspace_name}' workspace."
            )

        # Extract date key from date table
        matching_columns = [
            c.Name
            for c in self.all_columns()
            if c.Parent.Name == date_table and c.IsKey
        ]

        if not matching_columns:
            raise ValueError(
                f"{icons.red_dot} The '{date_table}' table does not have a date key column in the '{self._dataset_name}' semantic model within the '{self._workspace_name}' workspace."
            )

        date_key = matching_columns[0]

        # Create the new time intelligence measures
        for t in time_intel:
            expr = f"CALCULATE([{measure_name}],DATES{t}('{date_table}'[{date_key}]))"
            new_meas_name = f"{measure_name} {t}"
            self.add_measure(
                table_name=table_name,
                measure_name=new_meas_name,
                expression=expr,
            )

    def update_m_partition(
        self,
        table_name: str,
        partition_name: str,
        expression: Optional[str] = None,
        mode: Optional[str] = None,
        description: Optional[str] = None,
    ):
        """
        Updates an M partition for a table within a semantic model.

        Parameters
        ----------
        table_name : str
            Name of the table.
        partition_name : str
            Name of the partition.
        expression : str, default=None
            The `M expression <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.mpartitionsource.expression?view=analysisservices-dotnet>`_ containing the logic for the partition.
            Defaults to None which keeps the existing setting.
        mode : str, default=None
            The query `mode <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.modetype?view=analysisservices-dotnet>`_ of the partition.
            Defaults to None which keeps the existing setting.
        description : str, default=None
            The description of the partition.
            Defaults to None which keeps the existing setting.
        """

        import Microsoft.AnalysisServices.Tabular as TOM
        import System

        p = self.model.Tables[table_name].Partitions[partition_name]
        if p.SourceType != TOM.PartitionSourceType.M:
            raise ValueError(
                f"{icons.red_dot} Invalid partition source type. This function is only for M partitions."
            )
        if expression is not None:
            p.Source.Expression = expression
        if mode is not None:
            p.Mode = System.Enum.Parse(TOM.ModeType, mode)
        if description is not None:
            p.Description = description

    def update_measure(
        self,
        measure_name: str,
        expression: Optional[str] = None,
        format_string: Optional[str] = None,
        hidden: Optional[bool] = None,
        description: Optional[str] = None,
        display_folder: Optional[str] = None,
        format_string_expression: Optional[str] = None,
    ):
        """
        Updates a measure within a semantic model.

        Parameters
        ----------
        measure_name : str
            Name of the measure.
        expression : str, default=None
            DAX expression of the measure.
            Defaults to None which keeps the existing setting.
        format_string : str, default=None
            Format string of the measure.
            Defaults to None which keeps the existing setting.
        hidden : bool, default=None
            Whether the measure will be hidden or visible.
            Defaults to None which keeps the existing setting.
        description : str, default=None
            A description of the measure.
            Defaults to None which keeps the existing setting.
        display_folder : str, default=None
            The display folder in which the measure will reside.
            Defaults to None which keeps the existing setting.
        format_string_expression : str, default=None
            The format string expression for the calculation item.
            Defaults to None which keeps the existing setting.
        """

        table_name = next(
            m.Parent.Name for m in self.all_measures() if m.Name == measure_name
        )
        m = self.model.Tables[table_name].Measures[measure_name]
        if expression is not None:
            m.Expression = expression
        if format_string is not None:
            m.FormatString = format_string
        if hidden is not None:
            m.IsHidden = hidden
        if description is not None:
            m.Description = description
        if display_folder is not None:
            m.DisplayFolder = display_folder
        if format_string_expression is not None:
            fsd = TOM.FormatStringDefinition()
            fsd.Expression = format_string_expression
            m.FormatStringDefinition = fsd

    def update_column(
        self,
        table_name: str,
        column_name: str,
        source_column: Optional[str] = None,
        data_type: Optional[str] = None,
        expression: Optional[str] = None,
        format_string: Optional[str] = None,
        hidden: Optional[bool] = None,
        description: Optional[str] = None,
        display_folder: Optional[str] = None,
        data_category: Optional[str] = None,
        key: Optional[bool] = None,
        summarize_by: Optional[str] = None,
        is_nullable: Optional[bool] = None,
        is_available_in_mdx: Optional[bool] = None,
    ):
        """
        Updates a column within a semantic model.

        Parameters
        ----------
        table_name : str
            Name of the table in which the column exists.
        column_name : str
            Name of the column.
        source_column : str, default=None
            The source column for the column (for data columns only).
            Defaults to None which keeps the existing setting.
        data_type : str, default=None
            The data type of the column.
            Defaults to None which keeps the existing setting.
        expression : str, default=None
            The DAX expression of the column (for calculated columns only).
            Defaults to None which keeps the existing setting.
        format_string : str, default=None
            Format string of the column.
            Defaults to None which keeps the existing setting.
        hidden : bool, default=None
            Whether the column will be hidden or visible.
            Defaults to None which keeps the existing setting.
        description : str, default=None
            A description of the column.
            Defaults to None which keeps the existing setting.
        display_folder : str, default=None
            The display folder in which the column will reside.
            Defaults to None which keeps the existing setting.
        data_category : str, default=None
            The data category of the column.
            Defaults to None which keeps the existing setting.
        key : bool, default=False
            Marks the column as the primary key of the table.
            Defaults to None which keeps the existing setting.
        summarize_by : str, default=None
            Sets the value for the Summarize By property of the column.
            Defaults to None which keeps the existing setting.
        is_nullable : bool, default=None
            If False, the column cannot contain nulls. Even if True, it may still not allow nulls if it's a key column.
        is_available_in_mdx : bool, default=None
            A boolean value that indicates whether the column can be excluded from usage in MDX query tools. False if the column can be excluded from usage in MDX query tools; otherwise true.
        """

        import Microsoft.AnalysisServices.Tabular as TOM
        import System

        c = self.model.Tables[table_name].Columns[column_name]
        if c.Type == TOM.ColumnType.Data:
            if source_column is not None:
                c.SourceColumn = source_column
        if c.Type == TOM.ColumnType.Calculated:
            if expression is not None:
                c.Expression = expression
        if data_type is not None:
            c.DataType = System.Enum.Parse(TOM.DataType, data_type)
        if format_string is not None:
            c.FormatString = format_string
        if hidden is not None:
            c.IsHidden = hidden
        if description is not None:
            c.Description = description
        if display_folder is not None:
            c.DisplayFolder = display_folder
        if key is not None:
            c.IsKey = key
        if data_category is not None:
            c.DataCategory = data_category
        if summarize_by is not None:
            c.SummarizeBy = System.Enum.Parse(TOM.AggregateFunction, summarize_by)
        if is_nullable is not None:
            c.IsNullable = is_nullable
        if is_available_in_mdx is not None:
            c.IsAvailableInMDX = is_available_in_mdx

    def update_role(
        self,
        role_name: str,
        model_permission: Optional[str] = None,
        description: Optional[str] = None,
    ):
        """
        Updates a role within a semantic model.

        Parameters
        ----------
        role_name : str
            Name of the role.
        model_permission : str, default=None
            The model permission for the role.
            Defaults to None which keeps the existing setting.
        description : str, default=None
            The description of the role.
            Defaults to None which keeps the existing setting.
        """

        import Microsoft.AnalysisServices.Tabular as TOM
        import System

        obj = self.model.Roles[role_name]

        if model_permission is not None:
            obj.ModelPermission = System.Enum.Parse(
                TOM.ModelPermission, model_permission
            )
        if description is not None:
            obj.Description = description

    def update_calculation_item(
        self,
        table_name: str,
        calculation_item_name: str,
        expression: Optional[str] = None,
        ordinal: Optional[int] = None,
        description: Optional[str] = None,
        format_string_expression: Optional[str] = None,
    ):
        """
        Updates a calculation item within a semantic model.

        Parameters
        ----------
        table_name : str
            Name of the calculation group (table).
        calculation_item_name : str
            Name of the calculation item.
        expression : str, default=None
            The DAX expression of the calculation item.
            Defaults to None which keeps the existing setting.
        ordinal : int, default=None
            The ordinal of the calculation item.
            Defaults to None which keeps the existing setting.
        description : str, default=None
            The description of the role.
            Defaults to None which keeps the existing setting.
        format_string_expression : str, default=None
            The format string expression for the calculation item.
            Defaults to None which keeps the existing setting.
        """

        obj = self.model.Tables[table_name].CalculationGroup.CalculationItems[
            calculation_item_name
        ]

        if expression is not None:
            obj.Expression = expression
        if format_string_expression is not None:
            fsd = TOM.FormatStringDefinition()
            fsd.Expression = format_string_expression
            obj.FormatStringDefinition.Expression = fsd
        if ordinal is not None:
            obj.Ordinal = ordinal
        if description is not None:
            obj.Description = description

    def set_sort_by_column(
        self, table_name: str, column_name: str, sort_by_column: str
    ):
        """
        Sets the sort by column for a column in a semantic model.

        Parameters
        ----------
        table_name : str
            Name of the table.
        column_name : str
            Name of the column.
        sort_by_column : str
            Name of the column to use for sorting. Must be of integer (Int64) data type.
        """

        sbc = self.model.Tables[table_name].Columns[sort_by_column]
        self.model.Tables[table_name].Columns[column_name].SortByColumn = sbc

    def remove_sort_by_column(self, table_name: str, column_name: str):
        """
        Removes the sort by column for a column in a semantic model.

        Parameters
        ----------
        table_name : str
            Name of the table.
        column_name : str
            Name of the column.
        """

        self.model.Tables[table_name].Columns[column_name].SortByColumn = None

    def is_calculated_column(self, table_name: str, column_name: str):
        """
        Identifies if a column is a calculated column.

        Parameters
        ----------
        table_name : str
            Name of the table in which the column resides.
        column_name : str
            Name of the column.

        Returns
        -------
        bool
            A boolean value indicating whether the column is a calculated column.
        """

        import Microsoft.AnalysisServices.Tabular as TOM

        c = self.model.Tables[table_name].Columns[column_name]
        return c.Type == TOM.ColumnType.Calculated

    def is_calculated_table(self, table_name: str):
        """
        Identifies if a table is a calculated table.

        Parameters
        ----------
        table_name : str
            Name of the table.

        Returns
        -------
        bool
            A boolean value indicating whether the table is a calculated table.
        """

        import Microsoft.AnalysisServices.Tabular as TOM

        isCalcTable = False
        t = self.model.Tables[table_name]
        if t.ObjectType == TOM.ObjectType.Table:
            if any(
                p.SourceType == TOM.PartitionSourceType.Calculated for p in t.Partitions
            ):
                isCalcTable = True
        return isCalcTable

    def update_lineage_tags(self):
        """
        Adds lineage and source lineage tags for relevant semantic model objects if they do not exist. Also updates schema name for Direct Lake (entity) partitions.
        """

        import Microsoft.AnalysisServices.Tabular as TOM

        for t in self.model.Tables:
            if len(t.LineageTag) == 0:
                t.LineageTag = generate_guid()
            if len(t.SourceLineageTag) == 0:
                if next(p.Mode for p in t.Partitions) == TOM.ModeType.DirectLake:
                    partition_name = next(p.Name for p in t.Partitions)
                    entity_name = t.Partitions[partition_name].Source.EntityName
                    schema_name = t.Partitions[partition_name].Source.SchemaName

                    # Update schema name and source lineage tag for DL (entity) partitions
                    if len(schema_name) == 0:
                        schema_name = icons.default_schema
                        t.Partitions[partition_name].Source.SchemaName = (
                            icons.default_schema
                        )
                    t.SourceLineageTag = f"[{schema_name}].[{entity_name}]"
        for c in self.all_columns():
            if len(c.LineageTag) == 0:
                c.LineageTag = generate_guid()
        for m in self.all_measures():
            if len(m.LineageTag) == 0:
                m.LineageTag = generate_guid()
        for h in self.all_hierarchies():
            if len(h.LineageTag) == 0:
                h.LineageTag = generate_guid()
        for lvl in self.all_levels():
            if len(lvl.LineageTag) == 0:
                lvl.LineageTag = generate_guid()
        for e in self.model.Expressions:
            if len(e.LineageTag) == 0:
                e.LineageTag = generate_guid()

    def add_changed_property(self, object, property: str):
        """
        Adds a `ChangedProperty <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.changedproperty.property?view=analysisservices-dotnet#microsoft-analysisservices-tabular-changedproperty-property>`_ property to a semantic model object. Only adds the property if it does not already exist for the object.

        Parameters
        ----------
        object : TOM Object
            The TOM object within the semantic model.
        property : str
            The property to set (i.e. 'Name', 'DataType').
        """

        import Microsoft.AnalysisServices.Tabular as TOM

        # Only add the property if it does not already exist for that object
        if not any(c.Property == property for c in object.ChangedProperties):
            cp = TOM.ChangedProperty()
            cp.Property = property
            object.ChangedProperties.Add(cp)

    def remove_changed_property(self, object, property: str):
        """
        Removes a `ChangedProperty <https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.changedproperty.property?view=analysisservices-dotnet#microsoft-analysisservices-tabular-changedproperty-property>`_ property to a semantic model object. Only adds the property if it does not already exist for the object.

        Parameters
        ----------
        object : TOM Object
            The TOM object within the semantic model.
        property : str
            The property to set (i.e. 'Name', 'DataType').
        """

        for cp in object.ChangedProperties:
            if cp.Property == property:
                object.ChangedProperties.Remove(cp)

    def generate_measure_descriptions(
        self,
        measure_name: Optional[str | List[str]] = None,
        max_batch_size: Optional[int] = 5,
    ) -> pd.DataFrame:
        """
        Auto-generates descriptions for measures using an LLM. This function requires a paid F-sku (Fabric) of F64 or higher.
        Setting the 'readonly' parameter in connect_semantic_model to True will allow you to see the auto-generated descriptions in a dataframe. Setting the 'readonly' parameter
        to False will update the descriptions for the measures within the 'measure_name' parameter.

        Parameters
        ----------
        measure_name : str | List[str], default=None
            The measure name (or a list of measure names).
            Defaults to None which generates descriptions for all measures in the semantic model.
        max_batch_size : int, default=5
            Sets the max batch size for each API call.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe showing the updated measure(s) and their new description(s).
        """
        icons.sll_tags.append("GenerateMeasureDescriptions")

        df = pd.DataFrame(
            columns=["Table Name", "Measure Name", "Expression", "Description"]
        )
        data = []

        # import concurrent.futures
        if measure_name is None:
            measure_name = [m.Name for m in self.all_measures()]

        if isinstance(measure_name, str):
            measure_name = [measure_name]

        client = fabric.FabricRestClient()

        if len(measure_name) > max_batch_size:
            measure_lists = [
                measure_name[i : i + max_batch_size]
                for i in range(0, len(measure_name), max_batch_size)
            ]
        else:
            measure_lists = [measure_name]

        # Each API call can have a max of 5 measures
        for measure_list in measure_lists:
            payload = {
                "scenarioDefinition": {
                    "generateModelItemDescriptions": {
                        "modelItems": [],
                    },
                },
                "workspaceId": self._workspace_id,
                "artifactInfo": {"artifactType": "SemanticModel"},
            }
            for m_name in measure_list:
                expr, t_name = next(
                    (ms.Expression, ms.Parent.Name)
                    for ms in self.all_measures()
                    if ms.Name == m_name
                )
                if t_name is None:
                    raise ValueError(
                        f"{icons.red_dot} The '{m_name}' measure does not exist in the '{self._dataset_name}' semantic model within the '{self._workspace_name}' workspace."
                    )

                new_item = {
                    "urn": m_name,
                    "type": 1,
                    "name": m_name,
                    "expression": expr,
                }
                payload["scenarioDefinition"]["generateModelItemDescriptions"][
                    "modelItems"
                ].append(new_item)

            response = client.post("/explore/v202304/nl2nl/completions", json=payload)
            if response.status_code != 200:
                raise FabricHTTPException(response)

            for item in response.json().get("modelItems", []):
                ms_name = item["urn"]
                if ms_name.startswith("urn: "):
                    ms_name = ms_name[5:]
                desc = item.get("description")
                (table_name, expr) = next(
                    (m.Parent.Name, m.Expression)
                    for m in self.all_measures()
                    if m.Name == ms_name
                )
                self.model.Tables[table_name].Measures[ms_name].Description = desc

                # Collect new descriptions in a dataframe
                new_data = {
                    "Table Name": table_name,
                    "Measure Name": ms_name,
                    "Expression": expr,
                    "Description": desc,
                }

                data.append(new_data)

        if data:
            df = pd.concat([df, pd.DataFrame(data)], ignore_index=True)

        return df

        # def process_measure(m):
        #     table_name = m.Parent.Name
        #     m_name = m.Name
        #     m_name_fixed = "1"
        #     expr = m.Expression
        #     if measure_name is None or m_name in measure_name:
        #         payload = {
        #             "scenarioDefinition": {
        #                 "generateModelItemDescriptions": {
        #                     "modelItems": [
        #                         {
        #                             "urn": f"modelobject://Table/{table_name}/Measure/{m_name_fixed}",
        #                             "type": 1,
        #                             "name": m_name,
        #                             "expression": expr,
        #                         }
        #                     ]
        #                 }
        #             },
        #             "workspaceId": workspace_id,
        #             "artifactInfo": {"artifactType": "SemanticModel"},
        #         }

        #         response = client.post(
        #             "/explore/v202304/nl2nl/completions", json=payload
        #         )
        #         if response.status_code != 200:
        #             raise FabricHTTPException(response)

        #         desc = response.json()["modelItems"][0]["description"]
        #         m.Description = desc

        # with concurrent.futures.ThreadPoolExecutor() as executor:
        #     executor.map(process_measure, self.all_measures())

    def set_value_filter_behavior(self, value_filter_behavior: str = "Automatic"):
        """
        Sets the `Value Filter Behavior <https://learn.microsoft.com/power-bi/transform-model/value-filter-behavior>`_ property for the semantic model.

        Parameters
        ----------
        value_filter_behavior : str , default="Automatic"
            Determines value filter behavior for SummarizeColumns. Valid options: 'Automatic', 'Independent', 'Coalesced'.
        """

        import Microsoft.AnalysisServices.Tabular as TOM
        import System

        value_filter_behavior = value_filter_behavior.capitalize()
        min_compat = 1606

        if self.model.Model.Database.CompatibilityLevel < min_compat:
            self.model.Model.Database.CompatibilityLevel = min_compat

        self.model.ValueFilterBehavior = System.Enum.Parse(
            TOM.ValueFilterBehaviorType, value_filter_behavior
        )

    def add_role_member(self, role_name: str, member: str | List[str]):
        """
        Adds an external model role member (AzureAD) to a role.

        Parameters
        ----------
        role_name : str
            The role name.
        member : str | List[str]
            The email address(es) of the member(s) to add.
        """

        import Microsoft.AnalysisServices.Tabular as TOM

        if isinstance(member, str):
            member = [member]

        role = self.model.Roles[role_name]
        current_members = [m.MemberName for m in role.Members]

        for m in member:
            if m not in current_members:
                rm = TOM.ExternalModelRoleMember()
                rm.IdentityProvider = "AzureAD"
                rm.MemberName = m
                role.Members.Add(rm)
                print(
                    f"{icons.green_dot} '{m}' has been added as a member of the '{role_name}' role."
                )
            else:
                print(
                    f"{icons.yellow_dot} '{m}' is already a member in the '{role_name}' role."
                )

    def remove_role_member(self, role_name: str, member: str | List[str]):
        """
        Removes an external model role member (AzureAD) from a role.

        Parameters
        ----------
        role_name : str
            The role name.
        member : str | List[str]
            The email address(es) of the member(s) to remove.
        """

        if isinstance(member, str):
            member = [member]

        role = self.model.Roles[role_name]
        current_members = {m.MemberName: m.Name for m in role.Members}
        for m in member:
            name = current_members.get(m)
            if name is not None:
                role.Members.Remove(role.Members[name])
                print(
                    f"{icons.green_dot} The '{m}' member has been removed from the '{role_name}' role."
                )
            else:
                print(
                    f"{icons.yellow_dot} '{m}' is not a member of the '{role_name}' role."
                )

    def close(self):

        if not self._readonly and self.model is not None:

            import Microsoft.AnalysisServices.Tabular as TOM

            # ChangedProperty logic (min compat level is 1567) https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.changedproperty?view=analysisservices-dotnet
            if self.model.Model.Database.CompatibilityLevel >= 1567:
                for t in self.model.Tables:
                    if any(
                        p.SourceType == TOM.PartitionSourceType.Entity
                        for p in t.Partitions
                    ):
                        if t.LineageTag in list(self._table_map.keys()):
                            if self._table_map.get(t.LineageTag) != t.Name:
                                self.add_changed_property(object=t, property="Name")

                for c in self.all_columns():
                    if c.LineageTag in list(self._column_map.keys()):
                        if any(
                            p.SourceType == TOM.PartitionSourceType.Entity
                            for p in c.Parent.Partitions
                        ):
                            if self._column_map.get(c.LineageTag)[0] != c.Name:
                                self.add_changed_property(object=c, property="Name")
                        if self._column_map.get(c.LineageTag)[1] != c.DataType:
                            self.add_changed_property(object=c, property="DataType")

            # SLL Tags
            tags = [f"{icons.sll_prefix}{a}" for a in icons.sll_tags]
            tags.append("SLL")

            if not any(a.Name == icons.sll_ann_name for a in self.model.Annotations):
                ann_list = _make_list_unique(tags)
                new_ann_value = str(ann_list).replace("'", '"')
                self.set_annotation(
                    object=self.model, name=icons.sll_ann_name, value=new_ann_value
                )
            else:
                try:
                    ann_value = self.get_annotation_value(
                        object=self.model, name=icons.sll_ann_name
                    )
                    ann_list = ast.literal_eval(ann_value)
                    ann_list += tags
                    ann_list = _make_list_unique(ann_list)
                    new_ann_value = str(ann_list).replace("'", '"')
                    self.set_annotation(
                        object=self.model, name=icons.sll_ann_name, value=new_ann_value
                    )
                except Exception:
                    pass

            self.model.SaveChanges()

            if len(self._tables_added) > 0:
                refresh_semantic_model(
                    dataset=self._dataset_id,
                    tables=self._tables_added,
                    workspace=self._workspace_id,
                )
            self.model = None

        self._tom_server.Dispose()


@log
@contextmanager
def connect_semantic_model(
    dataset: str | UUID, readonly: bool = True, workspace: Optional[str] = None
) -> Iterator[TOMWrapper]:
    """
    Connects to the Tabular Object Model (TOM) within a semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    readonly: bool, default=True
        Whether the connection is read-only or read/write. Setting this to False enables read/write which saves the changes made back to the server.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    typing.Iterator[TOMWrapper]
        A connection to the semantic model's Tabular Object Model.
    """

    # initialize .NET to make sure System and Microsoft.AnalysisServices.Tabular is defined
    sempy.fabric._client._utils._init_analysis_services()

    tw = TOMWrapper(dataset=dataset, workspace=workspace, readonly=readonly)
    try:
        yield tw
    finally:
        tw.close()
