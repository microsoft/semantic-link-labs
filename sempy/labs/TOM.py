import sempy
import sempy.fabric as fabric
import pandas as pd
import re
from datetime import datetime
from .HelperFunctions import format_dax_object_name
from .ListFunctions import list_relationships
from .RefreshSemanticModel import refresh_semantic_model
from .Fallback import check_fallback_reason
from contextlib import contextmanager
from typing import List, Optional, Union
from sempy._utils._log import log

green_dot = '\U0001F7E2'
yellow_dot = '\U0001F7E1'
red_dot = '\U0001F534'
in_progress = '⌛'
checked = '\u2611'
unchecked = '\u2610'
start_bold = '\033[1m'
end_bold = '\033[0m'

@log
@contextmanager
def connect_semantic_model(dataset: str, readonly: Optional[bool] = True, workspace: Optional[str] = None):

    """
    Connects to the Tabular Object Model (TOM) within a semantic model.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    readonly: bool, default=True
        Whether the connection is read-only or read/write. Setting this to False enables read/write which saves the changes made back to the server.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        A connection to the semantic model's Tabular Object Model.
    """

    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM
    import System

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    
    fpAdded = []

    class TOMWrapper:

        def __init__(self, dataset, workspace, readonly):
            
            tom_server = fabric.create_tom_server(readonly=readonly, workspace=workspace)
            self.model = tom_server.Databases.GetByName(dataset).Model

        def all_columns(self):

            """
            Outputs a list of all columns within all tables in the semantic model.

            Parameters
            ----------

            Returns
            -------
            str
                All columns within the semantic model.
            """

            for t in self.model.Tables:
                for c in t.Columns:
                    if not str(c.Type) == 'RowNumber':
                        yield c

        def all_calculated_columns(self):

            """
            Outputs a list of all calculated columns within all tables in the semantic model.

            Parameters
            ----------

            Returns
            -------
            str
                All calculated columns within the semantic model.
            """

            for t in self.model.Tables:
                for c in t.Columns:
                    if str(c.Type) == 'Calculated':
                        yield c

        def all_calculated_tables(self):

            """
            Outputs a list of all calculated tables in the semantic model.

            Parameters
            ----------

            Returns
            -------
            str
                All calculated tables within the semantic model.
            """

            for t in self.model.Tables:
                if any(str(p.SourceType) == 'Calculated' for p in t.Partitions):
                    yield t

        def all_calculation_groups(self):

            """
            Outputs a list of all calculation groups in the semantic model.

            Parameters
            ----------

            Returns
            -------
            str
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
            str
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
            str
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
            str
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
            str
                All levels within the semantic model.
            """

            for t in self.model.Tables:
                for h in t.Hierarchies:
                    for l in h.Levels:
                        yield l

        def all_calculation_items(self):

            """
            Outputs a list of all calculation items in the semantic model.

            Parameters
            ----------

            Returns
            -------
            str
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
            str
                All row level security expressions within the semantic model.
            """

            for r in self.model.Roles:
                for tp in r.TablePermissions:
                    yield tp

        def add_measure(self, table_name: str, measure_name: str, expression: str, format_string: Optional[str] = None, hidden: Optional[bool] = False, description: Optional[str] = None, display_folder: Optional[str] = None):

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

            Returns
            -------
            str
                None
            """

            obj = TOM.Measure()
            obj.Name= measure_name
            obj.Expression = expression
            obj.IsHidden = hidden
            if format_string is not None:
                obj.FormatString = format_string
            if description is not None:
                obj.Description = description
            if display_folder is not None:
                obj.DisplayFolder = display_folder

            self.model.Tables[table_name].Measures.Add(obj)

        def add_calculated_table_column(self, table_name: str, column_name: str, source_column: str, data_type: str, format_string: Optional[str] = None, hidden: Optional[bool] = False, description: Optional[str] = None, display_folder: Optional[str] = None, data_category: Optional[str] = None, key: Optional[bool] = False, summarize_by: Optional[str] = None):

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

            Returns
            -------
            str
                None
            """

            data_type = data_type.capitalize().replace('Integer', 'Int64').replace('Datetime', 'DateTime')
            if summarize_by is None:
                summarize_by = 'Default'
            summarize_by = summarize_by.capitalize().replace('Distinctcount', 'DistinctCount').replace('Avg', 'Average')

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
            self.model.Tables[table_name].Columns.Add(obj)

        def add_data_column(self, table_name: str, column_name: str, source_column: str, data_type: str, format_string: Optional[str] = None, hidden: Optional[bool] = False, description: Optional[str] = None, display_folder: Optional[str] = None, data_category: Optional[str] = None, key: Optional[bool] = False, summarize_by: Optional[str] = None):

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

            Returns
            -------
            str
                None
            """

            data_type = data_type.capitalize().replace('Integer', 'Int64').replace('Datetime', 'DateTime')
            if summarize_by is None:
                summarize_by = 'Default'
            summarize_by = summarize_by.capitalize().replace('Distinctcount', 'DistinctCount').replace('Avg', 'Average')

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
            self.model.Tables[table_name].Columns.Add(obj)

        def add_calculated_column(self, table_name: str, column_name: str, expression: str, data_type: str, format_string: Optional[str] = None, hidden: Optional[bool] = False, description: Optional[str] = None, display_folder: Optional[str] = None, data_category: Optional[str] = None, key: Optional[bool] = False, summarize_by: Optional[str] = None):

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
                Defaults to None resolves to 'Default'.

            Returns
            -------
            str
                None
            """

            data_type = data_type.capitalize().replace('Integer', 'Int64').replace('Datetime', 'DateTime')
            if summarize_by is None:
                summarize_by = 'Default'
            summarize_by = summarize_by.capitalize().replace('Distinctcount', 'DistinctCount').replace('Avg', 'Average')

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
            self.model.Tables[table_name].Columns.Add(obj)

        def add_calculation_item(self, table_name: str, calculation_item_name: str, expression: str, ordinal: Optional[int] = None, format_string_expression: Optional[str] = None, description: Optional[str] = None):

            """
            Adds a calculation item to a calculation group within a semantic model.

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

            Returns
            -------
            str
                None
            """

            obj = TOM.CalculationItem()
            fsd = TOM.FormatStringDefinition()
            obj.Name = calculation_item_name
            obj.Expression = expression
            if ordinal is not None:
                obj.Ordinal = ordinal
            if description is not None:
                obj.Description = description
            if format_string_expression is not None:
                obj.FormatStringDefinition = fsd.Expression = format_string_expression
            self.model.Tables[table_name].CalculationGroup.CalculationItems.Add(obj)

        def add_role(self, role_name: str, model_permission: Optional[str] = None, description: Optional[str] = None):

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

            Returns
            -------
            str
                None
            """

            if model_permission is None:
                model_permission = 'Read'

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

            Returns
            -------
            str
                None
            """

            tp = TOM.TablePermission()
            tp.Table = self.model.Tables[table_name]
            tp.FilterExpression = filter_expression

            try:
                self.model.Roles[role_name].TablePermissions[table_name].FilterExpression = filter_expression
            except:
                self.model.Roles[role_name].TablePermissions.Add(tp)

        def set_ols(self, role_name: str, table_name: str, column_name: str, permission: str):

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

            Returns
            -------
            str
                None
            """

            permission = permission.capitalize()

            if permission not in ['Read', 'None', 'Default']:
                print(f"ERROR! Invalid 'permission' value.")
                return

            cp = TOM.ColumnPermission()
            cp.Column = self.model.Tables[table_name].Columns[column_name]
            cp.MetadataPermission = System.Enum.Parse(TOM.MetadataPermission, permission)
            try:
                self.model.Roles[role_name].TablePermissions[table_name].ColumnPermissions[column_name].MetadataPermission = System.Enum.Parse(TOM.MetadataPermission, permission)
            except:
                self.model.Roles[role_name].TablePermissions[table_name].ColumnPermissions.Add(cp)

        def add_hierarchy(self, table_name: str, hierarchy_name: str, columns: List[str], levels: Optional[List[str]] = None, hierarchy_description: Optional[str] = None, hierarchy_hidden: Optional[bool] = False):

            """
            Adds a hierarchy to a table within a semantic model.

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

            Returns
            -------
            str
                None
            """

            if isinstance(columns, str):
                print(f"The 'levels' parameter must be a list. For example: ['Continent', 'Country', 'City']")
                return
            if len(columns) == 1:
                print(f"There must be at least 2 levels in order to create a hierarchy.")
                return
            
            if levels is None:
                levels = columns
            
            if len(columns) != len(levels):
                print(f"If specifying level names, you must specify a level for each column.")
                return
            
            obj = TOM.Hierarchy()
            obj.Name = hierarchy_name
            obj.IsHidden = hierarchy_hidden
            if hierarchy_description is not None:
                obj.Description = hierarchy_description
            self.model.Tables[table_name].Hierarchies.Add(obj)

            for col in columns:
                lvl = TOM.Level()
                lvl.Column = self.model.Tables[table_name].Columns[col]
                lvl.Name = levels[columns.index(col)]
                lvl.Ordinal = columns.index(col)
                self.model.Tables[table_name].Hierarchies[hierarchy_name].Levels.Add(lvl)

        def add_relationship(self, from_table: str, from_column: str, to_table: str, to_column: str, from_cardinality: str, to_cardinality: str, cross_filtering_behavior: Optional[str] = None, is_active: Optional[bool] = True, security_filtering_behavior: Optional[str] = None, rely_on_referential_integrity: Optional[bool] = False):

            """
            Adds a relationship to a semantic model.

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

            Returns
            -------
            str
                None
            """

            if cross_filtering_behavior is None:
                cross_filtering_behavior = 'Automatic'
            if security_filtering_behavior is None:
                security_filtering_behavior = 'OneDirection'

            from_cardinality = from_cardinality.capitalize()
            to_cardinality = to_cardinality.capitalize()
            cross_filtering_behavior = cross_filtering_behavior.capitalize()
            security_filtering_behavior = security_filtering_behavior.capitalize()
            security_filtering_behavior = security_filtering_behavior.replace('direct', 'Direct')
            cross_filtering_behavior = cross_filtering_behavior.replace('direct', 'Direct')

            rel = TOM.SingleColumnRelationship()
            rel.FromColumn = self.model.Tables[from_table].Columns[from_column]
            rel.FromCardinality = System.Enum.Parse(TOM.RelationshipEndCardinality, from_cardinality)
            rel.ToColumn = self.model.Tables[to_table].Columns[to_column]
            rel.ToCardinality = System.Enum.Parse(TOM.RelationshipEndCardinality, to_cardinality)
            rel.IsActive = is_active
            rel.CrossFilteringBehavior = System.Enum.Parse(TOM.CrossFilteringBehavior, cross_filtering_behavior)
            rel.SecurityFilteringBehavior = System.Enum.Parse(TOM.SecurityFilteringBehavior, security_filtering_behavior)
            rel.RelyOnReferentialIntegrity = rely_on_referential_integrity

            self.model.Relationships.Add(rel)

        def add_calculation_group(self, name: str, precedence: int, description: Optional[str] = None, hidden: Optional[bool] = False):

            """
            Adds a calculation group to a semantic model.

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

            Returns
            -------
            str
                None
            """

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
            tbl.Partitions.Add(part)

            sortCol = 'Ordinal'

            col1 = TOM.DataColumn()
            col1.Name = sortCol
            col1.SourceColumn = sortCol
            col1.IsHidden = True
            col1.DataType = System.Enum.Parse(TOM.DataType, 'Int64')

            tbl.Columns.Add(col1)

            col2 = TOM.DataColumn()
            col2.Name = 'Name'
            col2.SourceColumn = 'Name'
            col2.DataType = System.Enum.Parse(TOM.DataType, 'String')
            #col.SortByColumn = m.Tables[name].Columns[sortCol]
            tbl.Columns.Add(col2)

            self.model.DiscourageImplicitMeasures = True
            self.model.Tables.Add(tbl)

        def add_expression(self, name: str, expression: str, description: Optional[str] = None):

            """
            Adds an expression to a semantic model.

            Parameters
            ----------
            name : str
                Name of the expression.
            expression: str
                The M expression of the expression.
            description : str, default=None
                A description of the expression.        

            Returns
            -------
            str
                None
            """

            exp = TOM.NamedExpression()
            exp.Name = name
            if description is not None:
                exp.Description = description
            exp.Kind = TOM.ExpressionKind.M
            exp.Expression = expression

            self.model.Expressions.Add(exp)

        def add_translation(self, language: str):

            """
            Adds a translation language (culture) to a semantic model.

            Parameters
            ----------
            language : str
                The language code (i.e. 'it-IT' for Italian).

            Returns
            -------
            str
                None
            """

            cul = TOM.Culture()
            cul.Name = language

            try:
                self.model.Cultures.Add(cul)
            except:
                pass

        def add_perspective(self, perspective_name: str):

            """
            Adds a perspective to a semantic model.

            Parameters
            ----------
            perspective_name : str
                Name of the perspective.

            Returns
            -------
            str
                None
            """

            persp = TOM.Perspective()
            persp.Name = perspective_name
            self.model.Perspectives.Add(persp)

        def add_m_partition(self, table_name: str, partition_name: str, expression: str, mode: Optional[str] = None, description: Optional[str] = None):

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
            description : str, default=None
                A description for the partition.
            
            Returns
            -------
            str
                None
            """

            mode = mode.title().replace('query', 'Query').replace(' ','').replace('lake', 'Lake')

            mp = TOM.MPartitionSource()
            mp.Expression = expression
            p = TOM.Partition()
            p.Name = partition_name
            p.Source = mp
            if description is not None:
                p.Description = description
            if mode is None:
                mode = 'Default'
            p.Mode = System.Enum.Parse(TOM.ModeType, mode)

            self.model.Tables[table_name].Partitions.Add(p)

        def add_entity_partition(self, table_name: str, entity_name: str, expression: Optional[str] = None, description: Optional[str] = None):

            """
            Adds an entity partition to a table within a semantic model.

            Parameters
            ----------
            table_name : str
                Name of the table.
            entity_name : str
                Name of the lakehouse table.
            expression : TOM Object, default=None
                The expression used by the table.
                Defaults to None which resolves to the 'DatabaseQuery' expression.
            description : str, default=None
                A description for the partition.
            
            Returns
            -------
            str
                None
            """

            ep = TOM.EntityPartitionSource()
            ep.Name = table_name
            ep.EntityName = entity_name
            if expression is None:
                ep.ExpressionSource = self.model.Expressions['DatabaseQuery']
            else:
                ep.ExpressionSource = expression
            p = TOM.Partition()
            p.Name = table_name
            p.Source = ep
            p.Mode = TOM.ModeType.DirectLake
            if description is not None:
                p.Description = description
            
            self.model.Tables[table_name].Partitions.Add(p)

        def set_alternate_of(self, table_name: str, column_name: str, summarization_type: str, base_table: str, base_column: Optional[str] = None):

            """
            Sets the 'alternate of' property on a column.

            Parameters
            ----------
            table_name : str
                Name of the table.
            column_name : str
                Name of the column.
            summarization_type : str
                The summarization type for the column.
            base_table : str
                Name of the base table for aggregation.
            base_column : str
                Name of the base column for aggregation
            
            Returns
            -------
            str
                None
            """
            
            if base_column is not None and base_table is None:
                print(f"ERROR: If you specify the base table you must also specify the base column")

            summarization_type = summarization_type.replace(' ','').capitalize().replace('Groupby', 'GroupBy')

            summarizationTypes = ['Sum', 'GroupBy', 'Count', 'Min', 'Max']
            if summarization_type not in summarizationTypes:
                print(f"The 'summarization_type' parameter must be one of the following valuse: {summarizationTypes}.")
                return

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
            Removes the 'alternate of' property on a column.

            Parameters
            ----------
            table_name : str
                Name of the table.
            column_name : str
                Name of the column.
            
            Returns
            -------
            str
                None
            """

            self.model.Tables[table_name].Columns[column_name].AlternateOf = None

        def get_annotations(self, object):

            """
            Shows all annotations for a given object within a semantic model.

            Parameters
            ----------
            object : TOM Object
                An object (i.e. table/column/measure) within a semantic model.
            
            Returns
            -------
            str
                TOM objects of all the annotations on a particular object within the semantic model.
            """

            #df = pd.DataFrame(columns=['Name', 'Value'])

            for a in object.Annotations:
                #new_data = {'Name': a.Name, 'Value': a.Value}
                yield a
                #df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
        
        def set_annotation(self, object, name: str, value: str):

            """
            Sets an annotation on an object within the semantic model.

            Parameters
            ----------
            object : TOM Object
                An object (i.e. table/column/measure) within a semantic model.
            name : str
                Name of the annotation.
            value : str
                Value of the annotation.
            
            Returns
            -------
            str
                None
            """

            ann = TOM.Annotation()
            ann.Name = name
            ann.Value = value

            try:
                object.Annotations[name].Value = value
            except:
                object.Annotations.Add(ann)

        def get_annotation_value(self, object, name: str):

            """
            Obtains the annotation value for a given annotation on an object within the semantic model.

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

            return object.Annotations[name].Value

        def remove_annotation(self, object, name: str):

            """
            Removes an annotation on an object within the semantic model.

            Parameters
            ----------
            object : TOM Object
                An object (i.e. table/column/measure) within a semantic model.
            name : str
                Name of the annotation.
            
            Returns
            -------
            str
                None
            """

            object.Annotations.Remove(name)

        def clear_annotations(self, object):

            """
            Removes all annotations on an object within the semantic model.

            Parameters
            ----------
            object : TOM Object
                An object (i.e. table/column/measure) within a semantic model.
            
            Returns
            -------
            str
                None
            """

            object.Annotations.Clear()

        def get_extended_properties(self, object):

            """
            Retrieves all extended properties on an object within the semantic model.

            Parameters
            ----------
            object : TOM Object
                An object (i.e. table/column/measure) within a semantic model.
            
            Returns
            -------
            str
                TOM Objects
            """

            #df = pd.DataFrame(columns=['Name', 'Value', 'Type'])

            for a in object.ExtendedProperties:
                yield a
                #new_data = {'Name': a.Name, 'Value': a.Value, 'Type': a.Type}
                #df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

            #return df

        def set_extended_property(self, object, extended_property_type: str, name: str, value: str):

            """
            Sets an extended property on an object within the semantic model.

            Parameters
            ----------
            object : TOM Object
                An object (i.e. table/column/measure) within a semantic model.
            extended_property_type : str
                The extended property type. Options: 'Json', 'String'.
            name : str
                Name of the extended property.
            value : str
                Value of the extended property.
            
            Returns
            -------
            str
                None
            """

            extended_property_type = extended_property_type.title()

            if extended_property_type == 'Json':
                ep = TOM.JsonExtendedProperty()
            else:
                ep = TOM.StringExtendedProperty()

            ep.Name = name
            ep.Value = value

            try:
                object.ExtendedProperties[name].Value = value
            except:
                object.ExtendedProperties.Add(ep)

        def get_extended_property_value(self, object, name: str):

            """
            Retrieves the value of an extended property for an object within the semantic model.

            Parameters
            ----------
            object : TOM Object
                An object (i.e. table/column/measure) within a semantic model.
            name : str
                Name of the annotation.
            
            Returns
            -------
            str
                The extended property value
            """

            return object.ExtendedProperties[name].Value

        def remove_extended_property(self, object, name: str):

            """
            Removes an extended property on an object within the semantic model.

            Parameters
            ----------
            object : TOM Object
                An object (i.e. table/column/measure) within a semantic model.
            name : str
                Name of the annotation.
            
            Returns
            -------
            str
                None
            """

            object.ExtendedProperties.Remove(name)

        def clear_extended_properties(self, object):

            """
            Removes all extended properties on an object within the semantic model.

            Parameters
            ----------
            object : TOM Object
                An object (i.e. table/column/measure) within a semantic model.
            
            Returns
            -------
            str
                None
            """

            object.ExtendedProperties.Clear()
    
        def in_perspective(self, object, perspective_name: str):
            
            """
            Indicates whether an object is contained within a given perspective.

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

            validObjects = ['Table', 'Column', 'Measure', 'Hierarchy']
            objectType = str(object.ObjectType)

            if objectType not in validObjects:
                print(f"Only the following object types are valid for perspectives: {validObjects}.")
                return
            
            object.Model.Perspectives[perspective_name]

            try:        
                if objectType == 'Table':
                    object.Model.Perspectives[perspective_name].PerspectiveTables[object.Name]
                elif objectType == 'Column':
                    object.Model.Perspectives[perspective_name].PerspectiveTables[object.Parent.Name].PerspectiveColumns[object.Name]
                elif objectType == 'Measure':
                    object.Model.Perspectives[perspective_name].PerspectiveTables[object.Parent.Name].PerspectiveMeasures[object.Name]
                elif objectType == 'Hierarchy':
                    object.Model.Perspectives[perspective_name].PerspectiveTables[object.Parent.Name].PerspectiveHierarchies[object.Name]
                return True
            except:
                return False

        def add_to_perspective(self, object, perspective_name: str):

            """
            Adds an object to a perspective.

            Parameters
            ----------
            object : TOM Object
                An object (i.e. table/column/measure) within a semantic model.
            perspective_name : str
                Name of the perspective.
            
            Returns
            -------
            str
                None
            """

            validObjects = ['Table', 'Column', 'Measure', 'Hierarchy']
            objectType = str(object.ObjectType)

            if objectType not in validObjects:
                print(f"Only the following object types are valid for perspectives: {validObjects}.")
                return
            try:
                object.Model.Perspectives[perspective_name]
            except:
                print(f"The '{perspective_name}' perspective does not exist.")
                return

            #try:
            if objectType == 'Table':
                pt = TOM.PerspectiveTable()
                pt.Table = object
                object.Model.Perspectives[perspective_name].PerspectiveTables.Add(pt)
            elif objectType == 'Column':
                pc = TOM.PerspectiveColumn()
                pc.Column = object
                object.Model.Perspectives[perspective_name].PerspectiveTables[object.Parent.Name].PerspectiveColumns.Add(pc)
            elif objectType == 'Measure':
                pm = TOM.PerspectiveMeasure()
                pm.Measure = object
                object.Model.Perspectives[perspective_name].PerspectiveTables[object.Parent.Name].PerspectiveMeasures.Add(pm)
            elif objectType == 'Hierarchy':
                ph = TOM.PerspectiveHierarchy()
                ph.Hierarchy = object
                object.Model.Perspectives[perspective_name].PerspectiveTables[object.Parent.Name].PerspectiveHierarchies.Add(ph)
            #except:
            #    pass

        def remove_from_perspective(self, object, perspective_name: str):

            """
            Removes an object from a perspective.

            Parameters
            ----------
            object : TOM Object
                An object (i.e. table/column/measure) within a semantic model.
            perspective_name : str
                Name of the perspective.
            
            Returns
            -------
            str
                None
            """

            validObjects = ['Table', 'Column', 'Measure', 'Hierarchy']
            objectType = str(object.ObjectType)

            if objectType not in validObjects:
                print(f"Only the following object types are valid for perspectives: {validObjects}.")
                return
            try:
                object.Model.Perspectives[perspective_name]
            except:
                print(f"The '{perspective_name}' perspective does not exist.")
                return

            #try:    
            if objectType == 'Table':
                pt = object.Model.Perspectives[perspective_name].PerspectiveTables[object.Name]
                object.Model.Perspectives[perspective_name].PerspectiveTables.Remove(pt)
            elif objectType == 'Column':
                pc = object.Model.Perspectives[perspective_name].PerspectiveTables[object.Parent.Name].PerspectiveColumns[object.Name]
                object.Model.Perspectives[perspective_name].PerspectiveTables[object.Parent.Name].PerspectiveColumns.Remove(pc)
            elif objectType == 'Measure':
                pm = object.Model.Perspectives[perspective_name].PerspectiveTables[object.Parent.Name].PerspectiveMeasures[object.Name]
                object.Model.Perspectives[perspective_name].PerspectiveTables[object.Parent.Name].PerspectiveMeasures.Remove(pm)
            elif objectType == 'Hierarchy':
                ph = object.Model.Perspectives[perspective_name].PerspectiveTables[object.Parent.Name].PerspectiveHierarchies[object.Name]
                object.Model.Perspectives[perspective_name].PerspectiveTables[object.Parent.Name].PerspectiveHierarchies.Remove(ph)
            #except:
            #    pass

        def set_translation(self, object, language: str, property: str, value: str):

            """
            Sets a translation value for an object's property.

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
            
            Returns
            -------
            str
                None
            """

            self.add_translation(language = language)

            property = property.title()

            validObjects = ['Table', 'Column', 'Measure', 'Hierarchy'] #, 'Level'

            if str(object.ObjectType) not in validObjects:
                print(f"Translations can only be set to {validObjects}.")
                return

            mapping = {
                'Name': TOM.TranslatedProperty.Caption,
                'Description': TOM.TranslatedProperty.Description,
                'Display Folder': TOM.TranslatedProperty.DisplayFolder
            }

            prop = mapping.get(property)

            try:
                object.Model.Cultures[language]
            except:
                print(f"The '{language}' translation language does not exist in the semantic model.")
                return

            object.Model.Cultures[language].ObjectTranslations.SetTranslation(object, prop, value)


        def remove_translation(self, object, language: str):

            """
            Removes an object's translation value.

            Parameters
            ----------
            object : TOM Object
                An object (i.e. table/column/measure) within a semantic model.
            language : str
                The language code.
            
            Returns
            -------
            str
                None
            """

            o = object.Model.Cultures[language].ObjectTranslations[object, TOM.TranslatedProperty.Caption]
            object.Model.Cultures[language].ObjectTranslations.Remove(o)

        def remove_object(self, object):

            """
            Removes an object from a semantic model.

            Parameters
            ----------
            object : TOM Object
                An object (i.e. table/column/measure) within a semantic model.
            
            Returns
            -------
            str
                None
            """

            objType = str(object.ObjectType)

            # Have to remove translations and perspectives on the object before removing it.
            if objType in ['Table', 'Column', 'Measure', 'Hierarchy', 'Level']:
                for lang in object.Model.Cultures:
                    self.remove_translation(object = object, language = lang.Name)
            if objType in ['Table', 'Column', 'Measure', 'Hierarchy']:
                for persp in object.Model.Perspectives:
                    self.remove_from_perspective(object = object, perspective_name = persp.Name)

            if objType == 'Column':
                object.Parent.Columns.Remove(object.Name)
            elif objType == 'Measure':
                object.Parent.Measures.Remove(object.Name)
            elif objType == 'Hierarchy':
                object.Parent.Hierarchies.Remove(object.Name)
            elif objType == 'Level':
                object.Parent.Levels.Remove(object.Name)
            elif objType == 'Partition':
                object.Parent.Partitions.Remove(object.Name)
            elif objType == 'Expression':
                object.Parent.Expressions.Remove(object.Name)
            elif objType == 'DataSource':
                object.Parent.DataSources.Remove(object.Name)
            elif objType == 'Role':
                object.Parent.Roles.Remove(object.Name)
            elif objType == 'Relationship':
                object.Parent.Relationships.Remove(object.Name)
            elif objType == 'Culture':
                object.Parent.Cultures.Remove(object.Name)
            elif objType == 'Perspective':
                object.Parent.Perspectives.Remove(object.Name)
            elif objType == 'CalculationItem':
                object.Parent.CalculationItems.Remove(object.Name)
            elif objType == 'TablePermission':
                object.Parent.TablePermissions.Remove(object.Name)

        def used_in_relationships(self, object):

            """
            Shows all relationships in which a table/column is used.

            Parameters
            ----------
            object : TOM Object
                An object (i.e. table/column) within a semantic model.
            
            Returns
            -------
            TOM Object
                All relationships in which the table/column is used.
            """

            objType = str(object.ObjectType)

            if objType == 'Table':
                for r in self.model.Relationships:
                    if r.FromTable.Name == object.Name or r.ToTable.Name == object.Name:
                        yield r#, 'Table'
            elif objType == 'Column':
                for r in self.model.Relationships:
                    if (r.FromTable.Name == object.Parent.Name and r.FromColumn.Name == object.Name) or \
                        (r.ToTable.Name == object.Parent.Name and r.ToColumn.Name == object.Name):
                        yield r#, 'Column'
                    
        def used_in_levels(self, column):

            """
            Shows all levels in which a column is used.

            Parameters
            ----------
            object : TOM Object
                An column object within a semantic model.
            
            Returns
            -------
            TOM Object
                All levels in which the column is used.
            """

            objType = str(column.ObjectType)

            if objType == 'Column':
                for l in self.all_levels():
                    if l.Parent.Table.Name == column.Parent.Name and l.Column.Name == column.Name:
                        yield l
                    
        def used_in_hierarchies(self, column):

            """
            Shows all hierarchies in which a column is used.

            Parameters
            ----------
            object : TOM Object
                An column object within a semantic model.
            
            Returns
            -------
            TOM Object
                All levels in which the column is used.
            """

            objType = str(column.ObjectType)

            if objType == 'Column':
                for l in self.all_levels():
                    if l.Parent.Table.Name == column.Parent.Name and l.Column.Name == column.Name:
                        yield l.Parent

        def used_in_sort_by(self, column):

            """
            Shows all columns in which a column is used for sorting.

            Parameters
            ----------
            object : TOM Object
                An column object within a semantic model.
            
            Returns
            -------
            TOM Object
                All columns in which the column is used for sorting.
            """

            objType = str(column.ObjectType)

            if objType == 'Column':
                for c in self.model.Tables[column.Parent.Name].Columns:
                    if c.SortByColumn == column:
                        yield c

        def used_in_rls(self, object, dependencies: pd.DataFrame):

            """
            Identifies the filter expressions which reference a given object.

            Parameters
            ----------
            object : TOM Object
                An object (i.e. table/column) within a semantic model.
            dependencies : pandas.DataFrame
                A pandas dataframe with the output of the 'get_model_calc_dependencies' function.
            
            Returns
            -------
            TOM Object
                None
            """

            objType = str(object.ObjectType)
            
            df_filt = dependencies[dependencies['Object Type'] == 'Rows Allowed']

            if objType == 'Table':
                fil = df_filt[(df_filt['Referenced Object Type'] == 'Table') & (df_filt['Referenced Table'] == object.Name)]
                tbls = fil['Table Name'].unique().tolist()
                for t in self.model.Tables:
                    if t.Name in tbls:
                        yield t
            elif objType == 'Column':
                fil = df_filt[(df_filt['Referenced Object Type'] == 'Column') & (df_filt['Referenced Table'] == object.Parent.Name) & (df_filt['Referenced Object'] == object.Name)]
                cols = fil['Full Object Name'].unique().tolist()
                for c in self.all_columns():
                    if format_dax_object_name(c.Parent.Name, c.Name) in cols:
                        yield c
            elif objType == 'Measure':
                fil = df_filt[(df_filt['Referenced Object Type'] == 'Measure') & (df_filt['Referenced Table'] == object.Parent.Name) & (df_filt['Referenced Object'] == object.Name)]
                meas = fil['Object Name'].unique().tolist()
                for m in self.all_measures():
                    if m.Name in meas:
                        yield m

        def used_in_data_coverage_definition(self, object, dependencies: pd.DataFrame):

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
            TOM Object
                None
            """

            objType = str(object.ObjectType)
            
            df_filt = dependencies[dependencies['Object Type'] == 'Data Coverage Definition']

            if objType == 'Table':
                fil = df_filt[(df_filt['Referenced Object Type'] == 'Table') & (df_filt['Referenced Table'] == object.Name)]
                tbls = fil['Table Name'].unique().tolist()
                for t in self.model.Tables:
                    if t.Name in tbls:
                        yield t
            elif objType == 'Column':
                fil = df_filt[(df_filt['Referenced Object Type'] == 'Column') & (df_filt['Referenced Table'] == object.Parent.Name) & (df_filt['Referenced Object'] == object.Name)]
                cols = fil['Full Object Name'].unique().tolist()
                for c in self.all_columns():
                    if format_dax_object_name(c.Parent.Name, c.Name) in cols:
                        yield c
            elif objType == 'Measure':
                fil = df_filt[(df_filt['Referenced Object Type'] == 'Measure') & (df_filt['Referenced Table'] == object.Parent.Name) & (df_filt['Referenced Object'] == object.Name)]
                meas = fil['Object Name'].unique().tolist()
                for m in self.all_measures():
                    if m.Name in meas:
                        yield m
            
        def used_in_calc_item(self, object, dependencies: pd.DataFrame):

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
            TOM Object
                None
            """

            objType = str(object.ObjectType)
            
            df_filt = dependencies[dependencies['Object Type'] == 'Calculation Item']

            if objType == 'Table':
                fil = df_filt[(df_filt['Referenced Object Type'] == 'Table') & (df_filt['Referenced Table'] == object.Name)]
                tbls = fil['Table Name'].unique().tolist()
                for t in self.model.Tables:
                    if t.Name in tbls:
                        yield t
            elif objType == 'Column':
                fil = df_filt[(df_filt['Referenced Object Type'] == 'Column') & (df_filt['Referenced Table'] == object.Parent.Name) & (df_filt['Referenced Object'] == object.Name)]
                cols = fil['Full Object Name'].unique().tolist()
                for c in self.all_columns():
                    if format_dax_object_name(c.Parent.Name, c.Name) in cols:
                        yield c
            elif objType == 'Measure':
                fil = df_filt[(df_filt['Referenced Object Type'] == 'Measure') & (df_filt['Referenced Table'] == object.Parent.Name) & (df_filt['Referenced Object'] == object.Name)]
                meas = fil['Object Name'].unique().tolist()
                for m in self.all_measures():
                    if m.Name in meas:
                        yield m

        def hybrid_tables(self):

            """
            Outputs the hybrid tables within a semantic model.

            Parameters
            ----------
            
            Returns
            -------
            TOM Object
                All hybrid tables within a semantic model.
            """

            for t in self.model.Tables:
                if any(str(p.Mode) == 'Import' for p in t.Partitions):
                    if any(str(p.Mode) == 'DirectQuery' for p in t.Partitions):
                        yield t

        def date_tables(self):

            """
            Outputs the tables which are marked as date tables within a semantic model.

            Parameters
            ----------
            
            Returns
            -------
            TOM Object
                All tables marked as date tables within a semantic model.
            """

            for t in self.model.Tables:
                if t.DataCategory == 'Time':
                    if any(c.IsKey and str(c.DataType) == 'DateTime'for c in t.Columns):
                        yield t

        def is_hybrid_table(self, table_name: str):

            """
            Identifies if a table is a hybrid table.

            Parameters
            ----------
            table_name : str
                Name of the table.
            
            Returns
            -------
            bool
                Indicates if the table is a hybrid table.
            """

            isHybridTable = False

            if any(str(p.Mode) == 'Import' for p in self.model.Tables[table_name].Partitions):
                if any(str(p.Mode) == 'DirectQuery' for p in self.model.Tables[table_name].Partitions):
                    isHybridTable = True

            return isHybridTable

        def is_date_table(self, table_name: str):

            """
            Identifies if a table is marked as a date table.

            Parameters
            ----------
            table_name : str
                Name of the table.
            
            Returns
            -------
            bool
                Indicates if the table is marked as a date table.
            """

            isDateTable = False
            t = self.model.Tables[table_name]

            if t.DataCategory == 'Time':
                if any(c.IsKey and str(c.DataType) == 'DateTime'for c in t.Columns):
                    isDateTable = True

            return isDateTable
        
        def mark_as_date_table(self, table_name: str, column_name: str):

            """
            Marks a table as a date table.

            Parameters
            ----------
            table_name : str
                Name of the table.
            column_name : str
                Name of the date column in the table.
            
            Returns
            -------
            str
                None
            """

            t = self.model.Tables[table_name]
            c = t.Columns[column_name]
            if str(c.DataType) != 'DateTime':
                print(f"{red_dot} The column specified in the 'column_name' parameter in this function must be of DateTime data type.")
                return
                
            daxQuery = f"""
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
            df = fabric.evaluate_dax(dataset=dataset, workspace=workspace, dax_string = daxQuery)
            value = df['1'].iloc[0]
            if value != '1':
                print(f"{red_dot} The '{column_name}' within the '{table_name}' table does not contain contiguous date values.")
                return
    
            # Mark as a date table
            t.DataCategory = 'Time'
            c.Columns[column_name].IsKey = True            
            print(f"{green_dot} The '{table_name}' table has been marked as a date table using the '{column_name}' column as its primary date key.")
        
        def has_aggs(self):

            """
            Identifies if a semantic model has any aggregations.

            Parameters
            ----------
            
            Returns
            -------
            bool
                Indicates if the semantic model has any aggregations.
            """

            hasAggs = False

            for c in self.all_columns():
                if c.AlterateOf is not None:
                    hasAggs = True

            return hasAggs
        
        def is_agg_table(self, table_name: str):

            """
            Identifies if a table has aggregations.

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
            Identifies if a semantic model has a hybrid table.

            Parameters
            ----------
            
            Returns
            -------
            bool
                Indicates if the semantic model has a hybrid table.
            """

            hasHybridTable = False

            for t in self.model.Tables:
                if self.is_hybrid_table(table_name = t.Name):
                    hasHybridTable = True

            return hasHybridTable

        def has_date_table(self):

            """
            Identifies if a semantic model has a table marked as a date table.

            Parameters
            ----------
            
            Returns
            -------
            bool
                Indicates if the semantic model has a table marked as a date table.
            """

            hasDateTable = False

            for t in self.model.Tables:
                if self.is_date_table(table_name = t.Name):
                    hasDateTable = True

            return hasDateTable

        def is_direct_lake(self):

            """
            Identifies if a semantic model is in Direct Lake mode.

            Parameters
            ----------
            
            Returns
            -------
            bool
                Indicates if the semantic model is in Direct Lake mode.
            """

            return any(str(p.Mode) == 'DirectLake' for t in self.model.Tables for p in t.Partitions)

        def is_field_parameter(self, table_name: str):

            """
            Identifies if a table is a field parameter.

            Parameters
            ----------
            table_name : str
                Name of the table.
            
            Returns
            -------
            bool
                Indicates if the table is a field parameter.
            """

            t = self.model.Tables[table_name]

            return any(str(p.SourceType) == 'Calculated' and 'NAMEOF(' in p.Source.Expression for p in t.Partitions) and all('[Value' in c.SourceColumn for c in t.Columns if not str(c.Type) == 'RowNumber') and t.Columns.Count == 4
        
        def is_auto_date_table(self, table_name: str):

            """
            Identifies if a table is an auto-date table.

            Parameters
            ----------
            table_name : str
                Name of the table.
            
            Returns
            -------
            bool
                Indicates if the table is an auto-date table.
            """

            isAutoDate = False

            t = self.model.Tables[table_name]

            if t.Name.startswith('LocalDateTable_') or t.Name.startswith('DateTableTemplate_'):
                if any(str(p.SourceType) == 'Calculated' for p in t.Partitions):
                    isAutoDate = True

            return isAutoDate

        def set_kpi(self, measure_name: str, target: Union[int,float,str], lower_bound: float, upper_bound: float, lower_mid_bound: Optional[float] = None, upper_mid_bound: Optional[float] = None, status_type: Optional[str] = None, status_graphic: Optional[str] = None):

            """
            Sets the properties to add/update a KPI for a measure.

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
            
            Returns
            -------
            str
                None
            """

            #https://github.com/m-kovalsky/Tabular/blob/master/KPI%20Graphics.md

            if measure_name == target:
                print(f"The 'target' parameter cannot be the same measure as the 'measure_name' parameter.")
                return

            if status_graphic is None:
                status_graphic = 'Three Circles Colored'

            statusType = ['Linear', 'LinearReversed', 'Centered', 'CenteredReversed']
            status_type = status_type.title().replace(' ','')

            if status_type is None:
                status_type = 'Linear'

            if status_type not in statusType:
                print(f"'{status_type}' is an invalid status_type. Please choose from these options: {statusType}.")
                return

            if status_type in ['Linear', 'LinearReversed']:
                if upper_bound is not None or lower_mid_bound is not None:
                    print(f"The 'upper_mid_bound' and 'lower_mid_bound' parameters are not used in the 'Linear' and 'LinearReversed' status types. Make sure these parameters are set to None.")
                    return
                elif upper_bound <= lower_bound:
                    print(f"The upper_bound must be greater than the lower_bound.")
                    return
            
            if status_type in ['Centered', 'CenteredReversed']:
                if upper_mid_bound is None or lower_mid_bound is None:
                    print(f"The 'upper_mid_bound' and 'lower_mid_bound' parameters are necessary in the 'Centered' and 'CenteredReversed' status types.")
                    return
                elif upper_bound <= upper_mid_bound:
                    print(f"The upper_bound must be greater than the upper_mid_bound.")
                elif upper_mid_bound <= lower_mid_bound:
                    print(f"The upper_mid_bound must be greater than the lower_mid_bound.")
                elif lower_mid_bound <= lower_bound:
                    print(f"The lower_mid_bound must be greater than the lower_bound.")

            try:
                table_name = next(m.Parent.Name for m in self.all_measures() if m.Name == measure_name)
            except:
                print(f"The '{measure_name}' measure does not exist in the '{dataset}' semantic model within the '{workspace}'.")
                return
            
            graphics = ['Cylinder', 'Five Bars Colored', 'Five Boxes Colored', 'Gauge - Ascending', 'Gauge - Descending', 'Road Signs', 'Shapes', 'Standard Arrow', 'Three Circles Colored', 'Three Flags Colored', 'Three Stars Colored', 'Three Symbols Uncircled Colored', 'Traffic Light', 'Traffic Light - Single', 'Variance Arrow', 'Status Arrow - Ascending', 'Status Arrow - Descending']

            if status_graphic not in graphics:
                print(f"The '{status_graphic}' status graphic is not valid. Please choose from these options: {graphics}.")
                return

            measure_target = True

            try:
                float(target)
                tgt = str(target)
                measure_target = False
            except:
                try:
                    tgt = next(format_dax_object_name(m.Parent.Name, m.Name) for m in self.all_measures() if m.Name == target)
                except:
                    print(f"The '{target}' measure does not exist in the '{dataset}' semantic model within the '{workspace}'.")

            if measure_target:
                expr = f"var x = [{measure_name}]/[{target}]\nreturn"
            else:
                expr = f"var x = [{measure_name}\nreturn"

            if status_type == 'Linear':
                expr = f"{expr}\nif(isblank(x),blank(),\n\tif(x<{lower_bound},-1,\n\t\tif(x<{upper_bound},0,1)))"
            elif status_type == 'LinearReversed':
                expr = f"{expr}\nif(isblank(x),blank(),\nif(x<{lower_bound},1,\n\t\tif(x<{upper_bound},0,-1)))"
            elif status_type == 'Centered':
                expr = f"{expr}\nif(isblank(x),blank(),\n\tif(x<{lower_mid_bound},\n\t\tif(x<{lower_bound},-1,0),\n\t\t\tif(x<{upper_mid_bound},1,\n\t\t\t\tif(x<{upper_bound}0,-1))))"
            elif status_type == 'CenteredReversed':
                expr = f"{expr}\nif(isblank(x),blank(),\n\tif(x<{lower_mid_bound},\n\t\tif(x<{lower_bound},1,0),\n\t\t\tif(x<{upper_mid_bound},-1,\n\t\t\t\tif(x<{upper_bound}0,1))))"

            kpi = TOM.KPI()
            kpi.TargetExpression = tgt
            kpi.StatusGraphic = status_graphic
            kpi.StatusExpression = expr

            ms = self.model.Tables[table_name].Measures[measure_name]
            try:
                ms.KPI.TargetExpression = tgt
                ms.KPI.StatusGraphic = status_graphic
                ms.KPI.StatusExpression = expr
            except:
                ms.KPI = kpi

        def set_aggregations(self, table_name: str, agg_table_name: str):

            """
            Sets the aggregations (alternate of) for all the columns in an aggregation table based on a base table.

            Parameters
            ----------
            table_name : str
                Name of the base table.
            agg_table_name : str
                Name of the aggregation table.
            
            Returns
            -------
            str
                None
            """

            for c in self.model.Tables[agg_table_name].Columns:

                dataType = str(c.DataType)

                if dataType in ['String', 'Boolean', 'DateTime']:
                    sumType = 'GroupBy'
                else:
                    sumType = 'Sum'

                self.set_alternate_of(table_name = agg_table_name, column_name = c.Name, base_table = table_name, base_column = c.Name, summarization_type = sumType)

        def set_is_available_in_mdx(self, table_name: str, column_name: str, value: Optional[bool] = False):

            """
            Sets the IsAvailableInMdx property on a column.

            Parameters
            ----------
            table_name : str
                Name of the table.
            column_name : str
                Name of the column.
            value : bool, default=False
                The IsAvailableInMdx property value.
            
            Returns
            -------
            str
                None
            """

            self.model.Tables[table_name].Columns[column_name].IsAvailableInMdx = value

        def set_summarize_by(self, table_name: str, column_name: str, value: Optional[str] = None):

            """
            Sets the SummarizeBy property on a column.

            Parameters
            ----------
            table_name : str
                Name of the table.
            column_name : str
                Name of the column.
            value : bool, default=None
                The SummarizeBy property value.
                Defaults to none which resolves to 'Default'.
            
            Returns
            -------
            str
                None
            """

            values = ['Default', 'None', 'Sum', 'Min', 'Max', 'Count', 'Average', 'DistinctCount']
            #https://learn.microsoft.com/en-us/dotnet/api/microsoft.analysisservices.tabular.column.summarizeby?view=analysisservices-dotnet#microsoft-analysisservices-tabular-column-summarizeby

            if value is None:
                value = 'Default'
            value = value.capitalize().replace('Distinctcount', 'DistinctCount').replace('Avg', 'Average')

            if value not in values:
                print(f"'{value}' is not a valid value for the SummarizeBy property. These are the valid values: {values}.")
                return

            self.model.Tables[table_name].Columns[column_name].SummarizeBy = System.Enum.Parse(TOM.AggregateFunction, value)

        def set_direct_lake_behavior(self, direct_lake_behavior: str):

            """
            Sets the Direct Lake Behavior property for a semantic model.

            Parameters
            ----------
            direct_lake_behavior : str
                The DirectLakeBehavior property value.
            
            Returns
            -------
            str
                None
            """

            direct_lake_behavior = direct_lake_behavior.capitalize()
            if direct_lake_behavior.startswith('Auto'):
                direct_lake_behavior = 'Automatic'
            elif direct_lake_behavior.startswith('Directl') or direct_lake_behavior == 'Dl':
                direct_lake_behavior = 'DirectLakeOnly'
            elif direct_lake_behavior.startswith('Directq') or direct_lake_behavior == 'Dq':
                direct_lake_behavior = 'DirectQueryOnly'

            dlValues = ['Automatic', 'DirectLakeOnly', 'DirectQueryOnly']

            if direct_lake_behavior not in dlValues:
                print(f"The 'direct_lake_behavior' parameter must be one of these values: {dlValues}.")
                return

            self.model.DirectLakeBehavior = System.Enum.Parse(TOM.DirectLakeBehavior, direct_lake_behavior)

        def add_table(self, name: str, description: Optional[str] = None, data_category: Optional[str] = None, hidden: Optional[bool] = False):

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
            
            Returns
            -------
            str
                None
            """

            t = TOM.Table()
            t.Name = name
            if description is not None:
                t.Description = description
            if data_category is not None:
                t.DataCategory = data_category
            t.Hidden = hidden
            self.model.Tables.Add(t)

        def add_calculated_table(self, name: str, expression: str, description: Optional[str] = None, data_category: Optional[str] = None, hidden: Optional[bool] = False):

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
            
            Returns
            -------
            str
                None
            """

            par = TOM.Partition()
            par.Name = name

            parSource = TOM.CalculatedPartitionSource()
            parSource.Expression = expression
            par.Source = parSource

            t = TOM.Table()
            t.Name = name
            if description is not None:
                t.Description = description
            if data_category is not None:
                t.DataCategory = data_category
            t.Hidden = hidden
            t.Partitions.Add(par)
            self.model.Tables.Add(t)

        def add_field_parameter(self, table_name: str, objects: List[str]):

            """
            Adds a table to the semantic model.

            Parameters
            ----------
            table_name : str
                Name of the table.
            objects : List[str]
                The columns/measures to be included in the field parameter. 
                Columns must be specified as such : 'Table Name'[Column Name].
                Measures may be formatted as '[Measure Name]' or 'Measure Name'.
            
            Returns
            -------
            str
                None
            """

            if isinstance(objects, str):
                print(f"The 'objects' parameter must be a list of columns/measures.")
                return
            if len(objects) == 1:
                print(f"There must be more than one object (column/measure) within the objects parameter.")
                return
            
            expr = ''
            i=0
            for obj in objects:
                success = False
                for m in self.all_measures():
                    if obj == '[' + m.Name + ']' or obj == m.Name:
                        expr = expr + '\n\t' + '("' + m.Name + '", NAMEOF([' + m.Name + ']), ' + str(i) + '),'
                        success = True
                for c in self.all_columns():
                    fullObjName = format_dax_object_name(c.Parent.Name, c.Name)
                    if obj == fullObjName or obj == c.Parent.Name + '[' + c.Name + ']':
                        expr = expr + '\n\t' + '("' + c.Name + '", NAMEOF(' + fullObjName + '), ' + str(i) + '),'
                        success = True
                if not success:
                    print(f"The '{obj}' object was not found in the '{dataset}' semantic model.")
                    return
                else:
                    i+=1

            expr = '{' + expr.rstrip(',') + '\n}'

            self.add_calculated_table(name = table_name, expression = expr)

            col2 = table_name + ' Fields'
            col3 = table_name + ' Order'

            self.add_calculated_table_column(table_name = table_name, column_name = table_name, source_column = '[Value1]', data_type = 'String', hidden = False )
            self.add_calculated_table_column(table_name = table_name, column_name = col2, source_column = '[Value2]', data_type = 'String', hidden = True )
            self.add_calculated_table_column(table_name = table_name, column_name = col3, source_column = '[Value3]', data_type = 'Int64', hidden = True )

            self.set_extended_property(self = self, 
                object = self.model.Tables[table_name].Columns[col2],
                extended_property_type = 'Json',
                name = 'ParameterMetadata',
                value = '{"version":3,"kind":2}')

            rcd = TOM.RelatedColumnDetails()
            gpc = TOM.GroupByColumn()
            gpc.GroupingColumn = self.model.Tables[table_name].Columns[col2]
            rcd.GroupByColumns.Add(gpc)

            # Update column properties
            self.model.Tables[table_name].Columns[col2].SortByColumn = self.model.Tables[table_name].Columns[col3]
            self.model.Tables[table_name].Columns[table_name].RelatedColumnDetails = rcd

            fpAdded.append(table_name)

        def remove_vertipaq_annotations(self):

            """
            Removes the annotations set using the [set_vertipaq_annotations] function.

            Parameters
            ----------
            
            Returns
            -------
            str
                None
            """

            for t in self.model.Tables:
                for a in t.Annotations:
                    if a.Name.startswith('Vertipaq_'):
                        self.remove_annotation(object = t, name = a.Name)
                for c in t.Columns:
                    for a in c.Annotations:
                        if a.Name.startswith('Vertipaq_'):
                            self.remove_annotation(object = c, name = a.Name)
                for h in t.Hierarchies:
                    for a in h.Annotations:
                        if a.Name.startswith('Vertipaq_'):
                            self.remove_annotation(object = h, name = a.Name)
                for p in t.Partitions:
                    for a in p.Annotations:
                        if a.Name.startswith('Vertipaq_'):
                            self.remove_annotation(object = p, name = a.Name)
            for r in self.model.Relationships:
                for a in r.Annotations:
                    if a.Name.startswith('Veripaq_'):
                        self.remove_annotation(object = r, name = a.Name)

        def set_vertipaq_annotations(self):

            """
            Saves Vertipaq Analyzer statistics as annotations on objects in the semantic model.

            Parameters
            ----------
            
            Returns
            -------
            str
                None
            """

            dfT = fabric.list_tables(dataset = dataset, workspace = workspace, extended=True)
            dfC = fabric.list_columns(dataset = dataset, workspace = workspace, extended=True)
            #intList = ['Total Size']#, 'Data Size', 'Dictionary Size', 'Hierarchy Size']
            dfCSum = dfC.groupby(['Table Name'])['Total Size'].sum().reset_index()
            dfTable = pd.merge(dfT[['Name', 'Type', 'Row Count']], dfCSum[['Table Name', 'Total Size']], left_on = 'Name', right_on = 'Table Name', how = 'inner')
            dfP = fabric.list_partitions(dataset = dataset, workspace = workspace, extended=True)
            dfP['Records per Segment'] = round(dfP['Record Count'] / dfP['Segment Count'],2)
            dfH = fabric.list_hierarchies(dataset = dataset, workspace = workspace, extended=True)
            dfR = list_relationships(dataset = dataset, workspace = workspace, extended=True)

            for t in self.model.Tables:
                dfT_filt = dfTable[dfTable['Name'] == t.Name]
                rowCount = str(dfT_filt['Row Count'].iloc[0])
                totalSize = str(dfT_filt['Total Size'].iloc[0])
                self.set_annotation(object = t, name = 'Vertipaq_RowCount', value = rowCount)
                self.set_annotation(object = t, name = 'Vertipaq_TableSize', value = totalSize)
                for c in t.Columns:
                    dfC_filt = dfC[(dfC['Table Name'] == t.Name) & (dfC['Column Name'] == c.Name)]
                    totalSize = str(dfC_filt['Total Size'].iloc[0])
                    dataSize = str(dfC_filt['Data Size'].iloc[0])
                    dictSize = str(dfC_filt['Dictionary Size'].iloc[0])
                    hierSize = str(dfC_filt['Hierarchy Size'].iloc[0])
                    card = str(dfC_filt['Column Cardinality'].iloc[0])
                    self.set_annotation(object = c, name = 'Vertipaq_TotalSize', value = totalSize)
                    self.set_annotation(object = c, name = 'Vertipaq_DataSize', value = dataSize)
                    self.set_annotation(object = c, name = 'Vertipaq_DictionarySize', value = dictSize)
                    self.set_annotation(object = c, name = 'Vertipaq_HierarchySize', value = hierSize)
                    self.set_annotation(object = c, name = 'Vertipaq_Cardinality', value = card)
                for p in t.Partitions:
                    dfP_filt = dfP[(dfP['Table Name'] == t.Name) & (dfP['Partition Name'] == p.Name)]
                    recordCount = str(dfP_filt['Record Count'].iloc[0])
                    segmentCount = str(dfP_filt['Segment Count'].iloc[0])
                    rpS = str(dfP_filt['Records per Segment'].iloc[0])
                    self.set_annotation(object = p, name = 'Vertipaq_RecordCount', value = recordCount)
                    self.set_annotation(object = p, name = 'Vertipaq_SegmentCount', value = segmentCount)
                    self.set_annotation(object = p, name = 'Vertipaq_RecordsPerSegment', value = rpS)
                for h in t.Hierarchies:
                    dfH_filt = dfH[(dfH['Table Name'] == t.Name) & (dfH['Hierarchy Name'] == h.Name)]
                    usedSize = str(dfH_filt['Used Size'].iloc[0])
                    self.set_annotation(object = h, name = 'Vertipaq_UsedSize', value = usedSize)
            for r in self.model.Relationships:
                dfR_filt = dfR[dfR['Relationship Name'] == r.Name]
                relSize = str(dfR_filt['Used Size'].iloc[0])
                self.set_annotation(object = r, name = 'Vertipaq_UsedSize', value = relSize)

            try:
                runId = self.get_annotation_value(object = self.model, name = 'Vertipaq_Run')
                runId = str(int(runId) + 1)
            except:
                runId = '1'
            self.set_annotation(object = self.model, name = 'Vertipaq_Run', value = runId)

        def row_count(self, object):

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
            
            objType = str(object.ObjectType)
            
            if objType == 'Table':
                result = self.get_annotation_value(object = object, name = 'Vertipaq_RowCount')
            elif objType == 'Partition':
                result = self.get_annotation_value(object = object, name = 'Vertipaq_RecordCount')

            return int(result)
        
        def records_per_segment(self, object):

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
            
            objType = str(object.ObjectType)
            
            if objType == 'Partition':
                result = self.get_annotation_value(object = object, name = 'Vertipaq_RecordsPerSegment')

            return float(result)
        
        def used_size(self, object):

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
            
            objType = str(object.ObjectType)
            
            if objType == 'Hierarchy':
                result = self.get_annotation_value(object = object, name = 'Vertipaq_UsedSize')
            elif objType == 'Relationship':
                result = self.get_annotation_value(object = object, name = 'Vertipaq_UsedSize')

            return int(result)

        def data_size(self, column):

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
            
            objType = str(column.ObjectType)
            
            if objType == 'Column':
                result = self.get_annotation_value(object = column, name = 'Vertipaq_DataSize')

            return int(result)

        def dictionary_size(self, column):

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

            objType = str(column.ObjectType)

            if objType == 'Column':
                result = self.get_annotation_value(object = column, name = 'Vertipaq_DictionarySize')

            return int(result)
        
        def total_size(self, object):

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

            objType = str(object.ObjectType)
            
            if objType == 'Column':
                result = self.get_annotation_value(object = object, name = 'Vertipaq_TotalSize')
            elif objType == 'Table':
                result = self.get_annotation_value(object = object, name = 'Vertipaq_TotalSize')

            return int(result)

        def cardinality(self, column):

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
            
            objType = str(column.ObjectType)
            
            if objType == 'Column':
                result = self.get_annotation_value(object = column, name = 'Vertipaq_Cardinality')            

            return int(result)                
            
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
            TOM Object
                Objects on which the specified object depends.
            """

            objType = str(object.ObjectType)
            objName = str(object.Name)
            objParentName = str(object.Parent.Name)

            if objType == 'Table':
                objParentName = objName

            fil = dependencies[(dependencies['Object Type'] == objType) & (dependencies['Table Name'] == objParentName) & (dependencies['Object Name'] == objName)]
            meas = fil[fil['Referenced Object Type'] == 'Measure']['Referenced Object'].unique().tolist()
            cols = fil[fil['Referenced Object Type'] == 'Column']['Referenced Full Object Name'].unique().tolist()
            tbls = fil[fil['Referenced Object Type'] == 'Table']['Referenced Table'].unique().tolist()
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
            TOM Object
                Objects which reference the specified object.
            """

            objType = str(object.ObjectType)
            objName = str(object.Name)
            objParentName = str(object.Parent.Name)

            if objType == 'Table':
                objParentName = objName

            fil = dependencies[(dependencies['Referenced Object Type'] == objType) & (dependencies['Referenced Table'] == objParentName) & (dependencies['Referenced Object'] == objName)]
            meas = fil[fil['Object Type'] == 'Measure']['Object Name'].unique().tolist()
            cols = fil[fil['Object Type'].isin(['Column', 'Calc Column'])]['Full Object Name'].unique().tolist()
            tbls = fil[fil['Object Type'].isin(['Table', 'Calc Table'])]['Table Name'].unique().tolist()
            for m in self.all_measures():
                if m.Name in meas:
                    yield m
            for c in self.all_columns():
                if format_dax_object_name(c.Parent.Name, c.Name) in cols:
                    yield c
            for t in self.model.Tables:
                if t.Name in tbls:
                    yield t

        def fully_qualified_measures(self, object, dependencies: pd.DataFrame):

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
            TOM Object
                All fully qualified measure references for a given object.
            """
    
            for obj in self.depends_on(object = object, dependencies=dependencies):            
                if str(obj.ObjectType) == 'Measure':
                    if (obj.Parent.Name + obj.Name in object.Expression) or (format_dax_object_name(obj.Parent.Name, obj.Name) in object.Expression):
                        yield obj

        def unqualified_columns(self, object, dependencies: pd.DataFrame):

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
            TOM Object
                All unqualified column references for a given object.
            """
    
            def create_pattern(a, b):
                return r'(?<!'+a+'\[)(?<!' + a + "'\[)" + b
    
            for obj in self.depends_on(object = object, dependencies=dependencies):
                if str(obj.ObjectType) == 'Column':
                    if re.search(create_pattern(obj.Parent.Name, obj.Name), object.Expression) is not None:
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
                df = check_fallback_reason(dataset = dataset, workspace = workspace)
                df_filt = df[df['FallbackReasonID'] == 2]

                if len(df_filt) > 0:
                    usingView = True
            
            return usingView
        
        def has_incremental_refresh_policy(self, table_name: str):

            """
            Identifies whether a table has an incremental refresh policy.

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
            Shows the incremental refresh policy for a table.

            Parameters
            ----------
            table_name : str
                Name of the table.
            
            Returns
            -------
            str
                A printout showing the incremental refresh policy for the table.
            """

            rp = self.model.Tables[table_name].RefreshPolicy

            if rp is None:
                print(f"The '{table_name}' table in the '{dataset}' semantic model within the '{workspace}' workspace does not have an incremental refresh policy.")
            else:            
                print(f"Table Name: {table_name}")
                rwGran = str(rp.RollingWindowGranularity).lower()
                icGran = str(rp.IncrementalGranularity).lower()
                if rp.RollingWindowPeriods > 1:                    
                    print(f"Archive data starting {start_bold}{rp.RollingWindowPeriods} {rwGran}s{end_bold} before refresh date.")
                else:
                    print(f"Archive data starting {start_bold}{rp.RollingWindowPeriods} {rwGran}{end_bold} before refresh date.")
                if rp.IncrementalPeriods > 1:                    
                    print(f"Incrementally refresh data {start_bold}{rp.IncrementalPeriods} {icGran}s{end_bold} before refresh date.")
                else:
                    print(f"Incrementally refresh data {start_bold}{rp.IncrementalPeriods} {icGran}{end_bold} before refresh date.")

                if str(rp.Mode) == 'Hybrid':
                    print(f"{checked} Get the latest data in real time with DirectQuery (Premium only)")
                else:
                    print(f"{unchecked} Get the latest data in real time with DirectQuery (Premium only)")
                if rp.IncrementalPeriodsOffset == -1:
                    print(f"{checked} Only refresh complete days")
                else:
                    print(f"{unchecked} Only refresh complete days")
                if len(rp.PollingExpression) > 0:
                    pattern = r'\[([^\]]+)\]'
                    match = re.search(pattern, rp.PollingExpression)
                    if match:
                        col = match[0][1:-1]
                        fullCol = format_dax_object_name(table_name, col)
                        print(f"{checked} Detect data changes: {start_bold}{fullCol}{end_bold}")
                else:
                    print(f"{unchecked} Detect data changes")

        def update_incremental_refresh_policy(self, table_name: str, incremental_granularity: str, incremental_periods: int, rolling_window_granularity: str, rolling_window_periods: int, only_refresh_complete_days: Optional[bool] = False, detect_data_changes_column: Optional[str] = None):

            """
            Updates the incremental refresh policy for a table within a semantic model.

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

            Returns
            -------
            str
                None
            """

            if not self.has_incremental_refresh_policy(table_name = table_name):
                print(f"The '{table_name}' table does not have an incremental refresh policy.")
                return
            
            incGran = ['Day', 'Month', 'Quarter', 'Year']

            incremental_granularity = incremental_granularity.capitalize()
            rolling_window_granularity = rolling_window_granularity.capitalize()

            if incremental_granularity not in incGran:
                print(f"{red_dot} Invalid 'incremental_granularity' value. Please choose from the following options: {incGran}.")
                return
            if rolling_window_granularity not in incGran:
                print(f"{red_dot} Invalid 'rolling_window_granularity' value. Please choose from the following options: {incGran}.")
                return
            
            if rolling_window_periods < 1:
                print(f"{red_dot} Invalid 'rolling_window_periods' value. Must be a value greater than 0.")
                return
            if incremental_periods < 1:
                print(f"{red_dot} Invalid 'incremental_periods' value. Must be a value greater than 0.")
                return

            t = self.model.Tables[table_name]

            if detect_data_changes_column is not None:
                dc = t.Columns[detect_data_changes_column]
                dcType = str(dc.DataType)

                if dcType != 'DateTime':
                    print(f"{red_dot} Invalid 'detect_data_changes_column' parameter. This column must be of DateTime data type.")
                    return

            rp = TOM.BasicRefreshPolicy()
            rp.IncrementalPeriods = incremental_periods
            rp.IncrementalGranularity = System.Enum.Parse(TOM.RefreshGranularityType, incremental_granularity)
            rp.RollingWindowPeriods = rolling_window_periods
            rp.RollingWindowGranularity = System.Enum.Parse(TOM.RefreshGranularityType, rolling_window_granularity)
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

        def add_incremental_refresh_policy(self, table_name: str, column_name: str, start_date: str, end_date: str, incremental_granularity: str, incremental_periods: int, rolling_window_granularity: str, rolling_window_periods: int, only_refresh_complete_days: Optional[bool] = False, detect_data_changes_column: Optional[str] = None):

            """
            Adds anincremental refresh policy for a table within a semantic model.

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

            Returns
            -------
            str
                None
            """

            #https://learn.microsoft.com/en-us/power-bi/connect-data/incremental-refresh-configure

            incGran = ['Day', 'Month', 'Quarter', 'Year']

            incremental_granularity = incremental_granularity.capitalize()
            rolling_window_granularity = rolling_window_granularity.capitalize()

            if incremental_granularity not in incGran:
                print(f"{red_dot} Invalid 'incremental_granularity' value. Please choose from the following options: {incGran}.")
                return
            if rolling_window_granularity not in incGran:
                print(f"{red_dot} Invalid 'rolling_window_granularity' value. Please choose from the following options: {incGran}.")
                return
            
            if rolling_window_periods < 1:
                print(f"{red_dot} Invalid 'rolling_window_periods' value. Must be a value greater than 0.")
                return
            if incremental_periods < 1:
                print(f"{red_dot} Invalid 'incremental_periods' value. Must be a value greater than 0.")
                return
            
            date_format = '%m/%d/%Y'

            date_obj_start = datetime.strptime(start_date, date_format)
            start_year = date_obj_start.year
            start_month = date_obj_start.month
            start_day = date_obj_start.day

            date_obj_end = datetime.strptime(end_date, date_format)
            end_year = date_obj_end.year
            end_month = date_obj_end.month
            end_day = date_obj_end.day

            if date_obj_end <= date_obj_start:
                print(f"{red_dot} Invalid 'start_date' or 'end_date'. The 'end_date' must be after the 'start_date'.")
                return

            t = self.model.Tables[table_name]

            c = t.Columns[column_name]
            fcName = format_dax_object_name(table_name, column_name)
            dType = str(c.DataType)

            if dType != 'DateTime':
                print(f"{red_dot} The {fcName} column is of '{dType}' data type. The column chosen must be of DateTime data type.")
                return
            
            if detect_data_changes_column is not None:
                dc = t.Columns[detect_data_changes_column]
                dcType = str(dc.DataType)

                if dcType != 'DateTime':
                    print(f"{red_dot} Invalid 'detect_data_changes_column' parameter. This column must be of DateTime data type.")
                    return

            # Start changes:

            # Update partition expression
            i=0
            for p in t.Partitions:
                if str(p.SourceType) != 'M':
                    print(f"{red_dot} Invalid partition source type. Incremental refresh can only be set up if the table's partition is an M-partition.")
                    return
                elif i==0:
                    text = p.Expression
                    text = text.rstrip()

                    ind = text.rfind(' ') + 1
                    obj = text[ind:]
                    pattern = r"in\s*[^ ]*"
                    matches = list(re.finditer(pattern, text))

                    if matches:
                        last_match = matches[-1]
                        text_before_last_match = text[:last_match.start()]

                        print(text_before_last_match)
                    else:
                        print(f"{red_dot} Invalid M-partition expression.")
                        return
                        
                    endExpr = f'#"Filtered Rows IR" = Table.SelectRows({obj}, each [{column_name}] >= RangeStart and [{column_name}] <= RangeEnd)\n#"Filtered Rows IR"'
                    finalExpr = text_before_last_match + endExpr

                    p.Expression = finalExpr
                i+=1

            # Add expressions
            self.add_expression(name = 'RangeStart', expression = f'datetime({start_year}, {start_month}, {start_day}, 0, 0, 0) meta [IsParameterQuery=true, Type="DateTime", IsParameterQueryRequired=true]')
            self.add_expression(name = 'RangeEnd', expression = f'datetime({end_year}, {end_month}, {end_day}, 0, 0, 0) meta [IsParameterQuery=true, Type="DateTime", IsParameterQueryRequired=true]')

            # Update properties
            rp = TOM.BasicRefreshPolicy()
            rp.IncrementalPeriods = incremental_periods
            rp.IncrementalGranularity = System.Enum.Parse(TOM.RefreshGranularityType, incremental_granularity)
            rp.RollingWindowPeriods = rolling_window_periods
            rp.RollingWindowGranularity = System.Enum.Parse(TOM.RefreshGranularityType, rolling_window_granularity)

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

        def apply_refresh_policy(self, table_name: str, effective_date: Optional[datetime] = None, refresh: Optional[bool] = True, max_parallelism: Optional[int] = 0):

            """
            Applies the incremental refresh policy for a table within a semantic model.

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

            Returns
            -------
            str
                None
            """

            self.model.Tables[table_name].ApplyRefreshPolicy(effectiveDate = effective_date, refresh = refresh, maxParallelism = max_parallelism)

        def set_data_coverage_definition(self, table_name: str, partition_name: str, expression: str):

            """
            Sets the data coverage definition for a partition.

            Parameters
            ----------
            table_name : str
                Name of the table.
            partition_name : str
                Name of the partition.
            expression : str
                DAX expression containing the logic for the data coverage definition.

            Returns
            -------
            str
                None
            """

            doc = 'https://learn.microsoft.com/analysis-services/tom/table-partitions?view=asallproducts-allversions'

            t = self.model.Tables[table_name]
            p = t.Partitions[partition_name]

            ht = self.is_hybrid_table(table_name = table_name)
            mode = str(p.Mode)

            if not ht:
                print(f"The data coverage definition property is only applicable to hybrid tables. See the documentation: {doc}.")
                return
            if mode != 'DirectQuery':
                print(f"The data coverage definition property is only applicable to the DirectQuery partition of a hybrid table. See the documentation: {doc}.")
                return

            dcd = TOM.DataCoverageDefinition()
            dcd.Expression = expression
            p.DataCoverageDefinition = dcd

        def set_encoding_hint(self, table_name: str, column_name: str, value: str):

            """
            Sets the encoding hint for a column.

            Parameters
            ----------
            table_name : str
                Name of the table.
            column_name : str
                Name of the column.
            value : str
                Encoding hint value. Options: 'Value', 'Hash', 'Default'.

            Returns
            -------
            str
                None
            """

            values = ['Default', 'Hash', 'Value']
            value = value.capitalize()

            if value not in values:
                print(f"{red_dot} Invalid encoding hint value. Please choose from these options: {values}.")
                return

            self.model.Tables[table_name].Columns[column_name].EncodingHint = System.Enum.Parse(TOM.EncodingHintType, value)

        def set_data_type(self, table_name: str, column_name: str, value: str):

            """
            Sets the data type for a column.

            Parameters
            ----------
            table_name : str
                Name of the table.
            column_name : str
                Name of the column.
            value : str
                The data type.

            Returns
            -------
            str
                None
            """

            values = ['Binary', 'Boolean', 'DateTime', 'Decimal', 'Double', 'Int64', 'String']

            value = value.replace(' ','').capitalize()
            if value == 'Datetime':
                value = 'DateTime'
            elif value.startswith('Int'):
                value = 'Int64'
            elif value.startswith('Bool'):
                value = 'Boolean'
            
            if value not in values:
                print(f"{red_dot} Invalid data type. Please choose from these options: {values}.")
                return
            
            self.model.Tables[table_name].Columns[column_name].DataType = System.Enum.Parse(TOM.DataType, value)

        def add_time_intelligence(self, measure_name: str, date_table: str, time_intel: Union[str, List[str]]):

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

            Returns
            -------
            str
                None
            """

            table_name = None
            time_intel_options = ['MTD', 'QTD', 'YTD']

            if isinstance(time_intel, str):
                time_intel = [time_intel]
            
            # Validate time intelligence variations
            for t in time_intel:
                t = t.capitalize()
                if t not in [time_intel_options]:
                    print(f"The '{t}' time intelligence variation is not supported. Valid options: {time_intel_options}.")
                    return

            # Validate measure and extract table name
            for m in self.all_measures():
                if m.Name == measure_name:
                    table_name = m.Parent.Name

            if table_name is None:
                print(f"The '{measure_name}' is not a valid measure in the '{dataset}' semantic model within the '{workspace}' workspace.")
                return
            
            # Validate date table
            if not self.is_date_table(date_table):
                print(f"{red_dot} The '{date_table}' table is not a valid date table in the '{dataset}' wemantic model within the '{workspace}' workspace.")
                return
            
            # Extract date key from date table
            for c in self.all_columns():
                if c.Parent.Name == date_table and c.IsKey:
                    date_key = c.Name

            # Create the new time intelligence measures
            for t in time_intel:
                if t == 'MTD':
                    expr = f"CALCULATE([{measure_name}],DATES{time_intel}('{date_table}'[{date_key}]))"
                    new_meas_name = f"{measure_name} {t}"
                    self.add_measure(table_name = table_name, measure_name = new_meas_name, expression = expr)
            
        def close(self):
            if not readonly and self.model is not None:
                self.model.SaveChanges()

                if len(fpAdded) > 0:
                    refresh_semantic_model(dataset = dataset, tables = fpAdded, workspace = workspace)
                self.model = None

    tw = TOMWrapper(dataset = dataset, workspace = workspace, readonly = readonly)
    try:        
        yield tw 
    finally:
        tw.close()
