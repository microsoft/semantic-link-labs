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
def connect_semantic_model(dataset: str, readonly: bool = True, workspace: str | None = None):

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

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#all_columns
            
            """

            for t in self.model.Tables:
                for c in t.Columns:
                    if not str(c.Type) == 'RowNumber':
                        yield c

        def all_calculated_columns(self):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#all_calculated_columns
            
            """

            for t in self.model.Tables:
                for c in t.Columns:
                    if str(c.Type) == 'Calculated':
                        yield c

        def all_calculated_tables(self):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#all_calculated_tables
            
            """

            for t in self.model.Tables:
                if any(str(p.SourceType) == 'Calculated' for p in t.Partitions):
                    yield t

        def all_calculation_groups(self):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#all_calculation_groups

            """

            for t in self.model.Tables:
                if t.CalculationGroup is not None:
                    yield t

        def all_measures(self):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#all_measures
            
            """

            for t in self.model.Tables:
                for m in t.Measures:
                    yield m

        def all_partitions(self):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#all_partitions
            
            """

            for t in self.model.Tables:
                for p in t.Partitions:
                    yield p

        def all_hierarchies(self):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#all_hierarchies
            
            """

            for t in self.model.Tables:
                for h in t.Hierarchies:
                    yield h

        def all_levels(self):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#all_levels
            
            """

            for t in self.model.Tables:
                for h in t.Hierarchies:
                    for l in h.Levels:
                        yield l

        def all_calculation_items(self):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#all_calculationitems
            
            """

            for t in self.model.Tables:
                if t.CalculationGroup is not None:
                    for ci in t.CalculationGroup.CalculationItems:
                        yield ci

        def all_rls(self):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#all_rls
            
            """

            for r in self.model.Roles:
                for tp in r.TablePermissions:
                    yield tp

        def add_measure(self, table_name: str, measure_name: str, expression: str, format_string: str | None = None, hidden: bool = False, description: str | None = None, display_folder: str | None = None):

            """
            
            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_measure
            
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

        def add_calculated_table_column(self, table_name: str, column_name: str, source_column: str, data_type: str, format_string: str | None = None, hidden: bool = False, description: str | None = None, display_folder: str | None = None, data_category: str | None = None, key: bool = False, summarize_by: str | None = None):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_calculated_table_column
            
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

        def add_data_column(self, table_name: str, column_name: str, source_column: str, data_type: str, format_string: str | None = None, hidden: bool = False, description: str | None = None, display_folder: str | None = None, data_category: str | None = None, key: bool = False, summarize_by: str | None = None):

            """
            
            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_data_column
            
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

        def add_calculated_column(self, table_name: str, column_name: str, expression: str, data_type: str, format_string: str | None = None, hidden: bool = False, description: str | None = None, display_folder: str | None = None, data_category: str | None = None, key: bool = False, summarize_by: str | None = None):

            """
            
            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_calculated_column
            
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

        def add_calculation_item(self, table_name: str, calculation_item_name: str, expression: str, ordinal:int | None = None, format_string_expression: str | None = None, description: str | None = None):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_calculation_item
            
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

        def add_role(self, role_name: str, model_permission: str | None = None, description: str | None = None):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_role
            
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

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_rls
            
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

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_ols
            
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

        def add_hierarchy(self, table_name: str, hierarchy_name: str, columns: list, levels: list = None, hierarchy_description: str | None = None, hierarchy_hidden: bool = False):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_hierarchy
            
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

        def add_relationship(self, from_table: str, from_column: str, to_table: str, to_column: str, from_cardinality: str, to_cardinality: str, cross_filtering_behavior: str | None = None, is_active: bool = True, security_filtering_behavior: str | None = None, rely_on_referential_integrity: bool = False):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_relationship
            
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

        def add_calculation_group(self, name: str, precedence: int, description: str | None = None, hidden: bool = False):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_calculation_group
            
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

        def add_expression(self, name: str, expression: str, description: str | None = None):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_expression
            
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

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_translation
            
            """

            cul = TOM.Culture()
            cul.Name = language

            try:
                self.model.Cultures.Add(cul)
            except:
                pass

        def add_perspective(self, perspective_name: str):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_perspective
            
            """

            persp = TOM.Perspective()
            persp.Name = perspective_name
            self.model.Perspectives.Add(persp)

        def add_m_partition(self, table_name: str, partition_name: str, expression: str, mode: str | None = None, description: str | None = None):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_m_partition
            
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

        def add_entity_partition(self, table_name: str, entity_name: str, expression: str | None = None, description: str | None = None):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_entity_partition
            
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

        def set_alternate_of(self, table_name: str, column_name: str, base_table: str, base_column: str, summarization_type: str):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_alternate_of        
            
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

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#remove_alternate_of
            
            """

            self.model.Tables[table_name].Columns[column_name].AlternateOf = None

        def get_annotations(self, object):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_annotations

            """

            #df = pd.DataFrame(columns=['Name', 'Value'])

            for a in object.Annotations:
                #new_data = {'Name': a.Name, 'Value': a.Value}
                yield a
                #df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
        
        def set_annotation(self, object, name: str, value: str):

            """
            
            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_annotation
            
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

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_annotation_value
            
            """

            return object.Annotations[name].Value

        def remove_annotation(self, object, name: str):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#remove_annotation
            
            
            """

            object.Annotations.Remove(name)

        def clear_annotations(self, object):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#clear_annotations
            
            """

            object.Annotations.Clear()

        def get_extended_properties(self, object):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_extended_properties

            """

            #df = pd.DataFrame(columns=['Name', 'Value', 'Type'])

            for a in object.ExtendedProperties:
                yield a
                #new_data = {'Name': a.Name, 'Value': a.Value, 'Type': a.Type}
                #df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

            #return df

        def set_extended_property(self, object, extended_property_type: str, name: str, value: str):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_extended_property
            
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

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_extended_property_value
            
            """

            return object.ExtendedProperties[name].Value

        def remove_extended_property(self, object, name: str):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#remove_extended_property
            
            """

            object.ExtendedProperties.Remove(name)

        def clear_extended_properties(self, object):

            """
            
            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#clear_extended_properties
            
            """

            object.ExtendedProperties.Clear()
    
        def in_perspective(self, object, perspective_name: str):
            
            """
            
            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#in_perspective

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
            
            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_to_perspective
            
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

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#remove_from_perspective
            
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

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_translation
            
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

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#remove_translation

            """

            o = object.Model.Cultures[language].ObjectTranslations[object, TOM.TranslatedProperty.Caption]
            object.Model.Cultures[language].ObjectTranslations.Remove(o)

        def remove_object(self, object):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#remove_object

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

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#used_in_relationships
            
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

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#used_in_levels
            
            """

            objType = str(column.ObjectType)

            if objType == 'Column':
                for l in self.all_levels():
                    if l.Parent.Table.Name == column.Parent.Name and l.Column.Name == column.Name:
                        yield l
                    
        def used_in_hierarchies(self, column):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#used_in_hierarchies
            
            """

            objType = str(column.ObjectType)

            if objType == 'Column':
                for l in self.all_levels():
                    if l.Parent.Table.Name == column.Parent.Name and l.Column.Name == column.Name:
                        yield l.Parent

        def used_in_sort_by(self, column):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#used_in_sort_by
            
            """

            objType = str(column.ObjectType)

            if objType == 'Column':
                for c in self.model.Tables[column.Parent.Name].Columns:
                    if c.SortByColumn == column:
                        yield c

        def used_in_rls(self, object, dependencies):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#used_in_rls
            
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

        def used_in_data_coverage_definition(self, object, dependencies):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#used_in_data_coverage_definition
            
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
            
        def used_in_calc_item(self, object, dependencies):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#used_in_calc_item
            
            """

            objType = str(object.ObjectType)
            
            df_filt = dependencies[dependencies['Object Type'] == 'Calculation_item']

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

            for t in self.model.Tables:
                if any(str(p.Mode) == 'Import' for p in t.Partitions):
                    if any(str(p.Mode) == 'DirectQuery' for p in t.Partitions):
                        yield t

        def date_tables(self):

            for t in self.model.Tables:
                if t.DataCategory == 'Time':
                    if any(c.IsKey and str(c.DataType) == 'DateTime'for c in t.Columns):
                        yield t

        def is_hybrid_table(self, table_name: str):

            isHybridTable = False

            if any(str(p.Mode) == 'Import' for p in self.model.Tables[table_name].Partitions):
                if any(str(p.Mode) == 'DirectQuery' for p in self.model.Tables[table_name].Partitions):
                    isHybridTable = True

            return isHybridTable

        def is_date_table(self, table_name: str):

            isDateTable = False
            t = self.model.Tables[table_name]

            if t.DataCategory == 'Time':
                if any(c.IsKey and str(c.DataType) == 'DateTime'for c in t.Columns):
                    isDateTable = True

            return isDateTable
        
        def mark_as_date_table(self, table_name: str, column_name: str):

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

            hasAggs = False

            for c in self.all_columns():
                if c.AlterateOf is not None:
                    hasAggs = True

            return hasAggs
        
        def is_agg_table(self, table_name: str):

            t = self.model.Tables[table_name]

            return any(c.AlternateOf is not None for c in t.Columns)

        def has_hybrid_table(self):

            hasHybridTable = False

            for t in self.model.Tables:
                if self.is_hybrid_table(table_name = t.Name):
                    hasHybridTable = True

            return hasHybridTable

        def has_date_table(self):

            hasDateTable = False

            for t in self.model.Tables:
                if self.is_date_table(table_name = t.Name):
                    hasDateTable = True

            return hasDateTable

        def is_direct_lake(self):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#is_direct_lake
            
            """

            return any(str(p.Mode) == 'DirectLake' for t in self.model.Tables for p in t.Partitions)

        def is_field_parameter(self, table_name: str):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#is_field_parameter
            
            """

            t = self.model.Tables[table_name]

            return any(str(p.SourceType) == 'Calculated' and 'NAMEOF(' in p.Source.Expression for p in t.Partitions) and all('[Value' in c.SourceColumn for c in t.Columns if not str(c.Type) == 'RowNumber') and t.Columns.Count == 4
        
        def is_auto_date_table(self, table_name: str):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#is_auto_date_table
            
            """

            isAutoDate = False

            t = self.model.Tables[table_name]

            if t.Name.startswith('LocalDateTable_') or t.Name.startswith('DateTableTemplate_'):
                if any(str(p.SourceType) == 'Calculated' for p in t.Partitions):
                    isAutoDate = True

            return isAutoDate

        def set_kpi(self, measure_name: str, target: int | float | str, lower_bound: float, upper_bound: float, lower_mid_bound: float | None = None, upper_mid_bound: float | None = None, status_type: str| None = None, status_graphic: str | None = None):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_kpi
            
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

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_aggregations
            
            """

            for c in self.model.Tables[agg_table_name].Columns:

                dataType = str(c.DataType)

                if dataType in ['String', 'Boolean', 'DateTime']:
                    sumType = 'GroupBy'
                else:
                    sumType = 'Sum'

                self.set_alternate_of(table_name = agg_table_name, column_name = c.Name, base_table = table_name, base_column = c.Name, summarization_type = sumType)

        def set_is_available_in_mdx(self, table_name: str, column_name: str, value: bool = False):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_is_available_in_mdx
            
            """

            self.model.Tables[table_name].Columns[column_name].IsAvailableInMdx = value

        def set_summarize_by(self, table_name: str, column_name: str, value: str | None = None):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_summarize_by
            
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

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_direct_lake_behavior
            
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

        def add_table(self, name: str, description: str | None = None, data_category: str | None = None, hidden: bool = False):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_table

            """

            t = TOM.Table()
            t.Name = name
            if description is not None:
                t.Description = description
            if data_category is not None:
                t.DataCategory = data_category
            t.Hidden = hidden
            self.model.Tables.Add(t)

        def add_calculated_table(self, name: str, expression: str, description: str | None = None, data_category: str | None = None, hidden: bool = False):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_calculated_table

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

        def add_field_parameter(self, table_name: str, objects: list):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_field_parameter

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

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#remove_vertipaq_annotations

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

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_vertipaq_annotations

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

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#row_count
            
            """
            
            objType = str(object.ObjectType)
            
            if objType == 'Table':
                result = self.get_annotation_value(object = object, name = 'Vertipaq_RowCount')
            elif objType == 'Partition':
                result = self.get_annotation_value(object = object, name = 'Vertipaq_RecordCount')

            return int(result)
        
        def records_per_segment(self, object):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#records_per_segment
            
            """
            
            objType = str(object.ObjectType)
            
            if objType == 'Partition':
                result = self.get_annotation_value(object = object, name = 'Vertipaq_RecordsPerSegment')

            return float(result)
        
        def used_size(self, object):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#used_size
            
            """
            
            objType = str(object.ObjectType)
            
            if objType == 'Hierarchy':
                result = self.get_annotation_value(object = object, name = 'Vertipaq_UsedSize')
            elif objType == 'Relationship':
                result = self.get_annotation_value(object = object, name = 'Vertipaq_UsedSize')

            return int(result)

        def data_size(self, column):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#data_size
            
            """
            
            objType = str(column.ObjectType)
            
            if objType == 'Column':
                result = self.get_annotation_value(object = column, name = 'Vertipaq_DataSize')

            return int(result)

        def dictionary_size(self, column):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#dictionary_size
            
            """

            objType = str(column.ObjectType)

            if objType == 'Column':
                result = self.get_annotation_value(object = column, name = 'Vertipaq_DictionarySize')

            return int(result)
        
        def total_size(self, object):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#total_size

            """

            objType = str(object.ObjectType)
            
            if objType == 'Column':
                result = self.get_annotation_value(object = object, name = 'Vertipaq_TotalSize')
            elif objType == 'Table':
                result = self.get_annotation_value(object = object, name = 'Vertipaq_TotalSize')

            return int(result)

        def cardinality(self, column):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#cardinality
            
            """
            
            objType = str(column.ObjectType)
            
            if objType == 'Column':
                result = self.get_annotation_value(object = column, name = 'Vertipaq_Cardinality')            

            return int(result)                
            
        def depends_on(self, object, dependencies):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#depends_on

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

        def referenced_by(self, object, dependencies):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#referenced_by
            
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

        def fully_qualified_measures(self, object, dependencies):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#fully_qualified_measures

            """
    
            for obj in self.depends_on(object = object, dependencies=dependencies):            
                if str(obj.ObjectType) == 'Measure':
                    if (obj.Parent.Name + obj.Name in object.Expression) or (format_dax_object_name(obj.Parent.Name, obj.Name) in object.Expression):
                        yield obj

        def unqualified_columns(self, object, dependencies):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#unqualified_columns
            
            """
    
            def create_pattern(a, b):
                return r'(?<!'+a+'\[)(?<!' + a + "'\[)" + b
    
            for obj in self.depends_on(object = object, dependencies=dependencies):
                if str(obj.ObjectType) == 'Column':
                    if re.search(create_pattern(obj.Parent.Name, obj.Name), object.Expression) is not None:
                        yield obj

        def is_direct_lake_using_view(self):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#is_direct_lake_using_view

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

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#has_incremental_refresh_policy

            """

            hasRP = False
            rp = self.model.Tables[table_name].RefreshPolicy

            if rp is not None:
                hasRP = True

            return hasRP
        
        def show_incremental_refresh_policy(self, table_name: str):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#show_incremental_refresh_policy

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

        def update_incremental_refresh_policy(self, table_name: str, incremental_granularity: str, incremental_periods: int, rolling_window_granularity: str, rolling_window_periods: int, only_refresh_complete_days: bool = False, detect_data_changes_column: str | None = None):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#update_incremental_refresh_policy

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

        def add_incremental_refresh_policy(self, table_name: str, column_name: str, start_date: str, end_date: str, incremental_granularity: str, incremental_periods: int, rolling_window_granularity: str, rolling_window_periods: int, only_refresh_complete_days: bool = False, detect_data_changes_column: str | None = None):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_incremental_refresh_policy

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

        def apply_refresh_policy(self, table_name: str, effective_date: datetime = None, refresh: bool = True, max_parallelism: int = 0):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#apply_refresh_policy

            """

            self.model.Tables[table_name].ApplyRefreshPolicy(effectiveDate = effective_date, refresh = refresh, maxParallelism = max_parallelism)

        def set_data_coverage_definition(self, table_name: str, partition_name: str, expression: str):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_data_coverage_definition

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

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_encoding_hint

            """

            values = ['Default', 'Hash', 'Value']
            value = value.capitalize()

            if value not in values:
                print(f"{red_dot} Invalid encoding hint value. Please choose from these options: {values}.")
                return

            self.model.Tables[table_name].Columns[column_name].EncodingHint = System.Enum.Parse(TOM.EncodingHintType, value)

        def set_data_type(self, table_name: str, column_name: str, value: str):

            """

            Documentation is available here: https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_data_type

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
