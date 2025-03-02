import pandas as pd
from typing import Optional
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from uuid import UUID
import random
import holidays
import math
import numpy as np
import sempy_labs._icons as icons
from sempy_labs._helper_functions import _read_delta_table, _save_as_delta_table

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
    DateType,
    BooleanType,
    TimestampType,
    LongType,
)
from pyspark.sql.functions import (
    col,
    date_format,
    dayofmonth,
    dayofweek,
    expr,
    to_date,
    lag,
    sum,
    min,
    max,
    udf,
    last_day,
    lit,
    month,
    quarter,
    rand,
    year,
    row_number, 
    trim, 
    monotonically_increasing_id,
    explode, 
    array, 
    sequence, 
    when
)

def get_adventureworks_dw_property_bag(
    start_date: Optional[str | date] = None,
    years: Optional[int] = 4,
    sales_quota_count: Optional[int] = 223,
    survey_response_count: Optional[int] = 10000,
    fact_rows_in_millions: Optional[int] = 1,
    csv_download_url: Optional[
        str
    ] = "https://raw.githubusercontent.com/microsoft/sql-server-samples/refs/heads/master/samples/databases/adventure-works/data-warehouse-install-script",
) -> dict:
    """
    Generates a property bag for the provision_sample_delta_tables function.

    Parameters
    ----------
    start_date : str | date, default=None
        The start date for the date table. If specified as a string, must adhere to the format "%Y-%m-%d", such as "2025-01-25".
        Defaults to None which resolves to the current date minus the specified years.
    years : int, default=4
        The number of years that the date table covers.
        The value must be greater than 0. Defaults to 4.
    sales_quota_count : int, default=10000
        The number of transactions in the FactSalesQuota table.
        The value must be greater than 0. Defaults to 223 for 223 rows.
    survey_response_count : int, default=10000
        The number of transactions in the FactSurveyResponse table.
        The value must be greater than 0. Defaults to 10000 for 10000 rows.
    fact_rows_in_millions : int, default=100
        The approximate number of transactions in the FactInternetSales table in millions.
        The value must be greater than 0. Defaults to 1 for 1 million rows.
    csv_download_url : str, default = "https://raw.githubusercontent.com/microsoft/sql-server-samples/refs/heads/master/samples/databases/adventure-works/data-warehouse-install-script"
        The download URL for the AdventureWorksDW table csv files.

    Returns
    -------
    dict
        A dictionary wrapping the parameters passed into this function.

    """

    # years must be greater than 0.
    if years < 1:
        raise ValueError("Years must be greater than 0.")

    # num_rows_in_millions must be greater than 0.
    if fact_rows_in_millions < 1:
        raise ValueError("The number of rows in millions must be greater than 0.")

    # Make sure the start_date is valid
    if not start_date:
        start_date = date.today() - relativedelta(years=years)
    else:
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")

    property_bag = {}
    property_bag["start_date"] = start_date
    property_bag["years"] = years
    property_bag["sales_quota_count"] = sales_quota_count
    property_bag["survey_response_count"] = survey_response_count
    property_bag["fact_rows_in_millions"] = fact_rows_in_millions
    property_bag["csv_download_url"] = csv_download_url

    return property_bag


def provision_adventureworks_dw_delta_tables(
    workspace_id: Optional[UUID] = None,
    lakehouse_id: Optional[UUID] = None,
    table_properties: Optional[dict] = None,
):
    """
    Generates sample data for a date table.

    Parameters
    ----------
    workspace_id : uuid.UUID
        The Fabric workspace ID where the lakehouse is located.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    lakehouse_id : uuid.UUID
        The ID of the lakehouse where the delta tables should be added.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    table_properties: dict, default=None
        An arbirary dictionary of key/value pairs that the provision_perf_lab_lakehouse function passes to the table_generator function.
        The key/value pairs in the dictionary are specific to the table_generator function.
    """

    # The tables are related to each other through a variety of keys.
    # It is important to generate the tables in this order so that the keys are valid.
    _provision_aw_dimension_tables(workspace_id, lakehouse_id, table_properties)
    date_df = _provision_aw_dimdate_table(workspace_id, lakehouse_id, table_properties)
    _update_promotion_dates(workspace_id, lakehouse_id, date_df)
    _generate_fact_call_center(workspace_id, lakehouse_id, date_df)
    _generate_fact_currency_rates(workspace_id, lakehouse_id, date_df)
    _get_fact_finance_data(workspace_id, lakehouse_id, date_df)
    _generate_fact_sales_quota(workspace_id, lakehouse_id, date_df, table_properties)
    _generate_fact_survey_responses(
        workspace_id, lakehouse_id, date_df, table_properties
    )
    table_dict = _generate_sales_tables(
        workspace_id, lakehouse_id, date_df, table_properties
    )
    _save_fact_sales_tables(
        workspace_id,
        lakehouse_id,
        table_dict["FactInternetSales"],
        table_dict["FactInternetSalesReason"],
        table_dict["FactResellerSales"],
    )
    _generate__fact_product_inventory(workspace_id, lakehouse_id)


def _provision_aw_dimension_tables(
    workspace_id: UUID,
    lakehouse_id: UUID,
    table_properties: Optional[dict] = None,
):
    """
    Creates the following AdventureWorksDW dimension tables based on published csv files:
    DimAccount, DimCurrency, DimCustomer, DimDepartmentGroup, DimEmployee, DimGeography,
    DimOrganization, DimProduct, DimProductCategory, DimProductSubcategory, DimReseller,
    DimSalesReason, DimSalesTerritory, DimScenario, and ProspectiveBuyer
    Parameters
    ----------
    workspace_id : uuid.UUID
        The Fabric workspace ID where the lakehouse is located.
    lakehouse_id : uuid.UUID
        The ID of the lakehouse where the delta tables should be added.
    table_properties: dict, default=None
        An arbirary dictionary of key/value pairs that the provision_perf_lab_lakehouse function passes to the table_generator function.
        The key/value pairs must include the "csv_download_url" record which, by default, would point to
        https://raw.githubusercontent.com/microsoft/sql-server-samples/refs/heads/master/samples/databases/adventure-works/data-warehouse-install-script
    """

    spark = SparkSession.builder.getOrCreate()
    base_url = table_properties["csv_download_url"]

    # region Schema definitions for the dimension tables
    aw_schemas = {
        "DimEmployee": {
            "EmployeeKey": "Int64",
            "ParentEmployeeKey": "Int64",
            "EmployeeNationalIDAlternateKey": "string",
            "ParentEmployeeNationalIDAlternateKey": "string",
            "SalesTerritoryKey": "Int64",
            "FirstName": "string",
            "LastName": "string",
            "MiddleName": "string",
            "NameStyle": "string",
            "Title": "string",
            "HireDate": "string",
            "BirthDate": "string",
            "LoginID": "string",
            "EmailAddress": "string",
            "Phone": "string",
            "MaritalStatus": "string",
            "EmergencyContactName": "string",
            "EmergencyContactPhone": "string",
            "SalariedFlag": "string",
            "Gender": "string",
            "PayFrequency": "Int64",
            "BaseRate": "float64",
            "VacationHours": "Int64",
            "SickLeaveHours": "Int64",
            "CurrentFlag": "string",
            "SalesPersonFlag": "string",
            "DepartmentName": "string",
            "StartDate": "string",
            "EndDate": "string",
            "Status": "string",
            "EmployeePhoto": "string",
        },
        "DimCustomer": {
            "CustomerKey": "Int64",
            "GeographyKey": "Int64",
            "CustomerAlternateKey": "string",
            "Title": "string",
            "FirstName": "string",
            "MiddleName": "string",
            "LastName": "string",
            "NameStyle": "string",
            "BirthDate": "string",
            "MaritalStatus": "string",
            "Suffix": "string",
            "Gender": "string",
            "EmailAddress": "string",
            "YearlyIncome": "Int64",
            "TotalChildren": "Int64",
            "NumberChildrenAtHome": "Int64",
            "EnglishEducation": "string",
            "SpanishEducation": "string",
            "FrenchEducation": "string",
            "EnglishOccupation": "string",
            "SpanishOccupation": "string",
            "FrenchOccupation": "string",
            "HouseOwnerFlag": "string",
            "NumberCarsOwned": "Int64",
            "AddressLine1": "string",
            "AddressLine2": "string",
            "Phone": "string",
            "DateFirstPurchase": "string",
            "CommuteDistance": "string",
        },
        "DimProduct": {
            "ProductKey": "Int64",
            "ProductAlternateKey": "string",
            "ProductSubcategoryKey": "Int64",
            "WeightUnitMeasureCode": "string",
            "SizeUnitMeasureCode": "string",
            "EnglishProductName": "string",
            "SpanishProductName": "string",
            "FrenchProductName": "string",
            "StandardCost": "float64",
            "FinishedGoodsFlag": "string",
            "Color": "string",
            "SafetyStockLevel": "Int64",
            "ReorderPoint": "Int64",
            "ListPrice": "float64",
            "Size": "string",
            "SizeRange": "string",
            "Weight": "float64",
            "DaysToManufacture": "Int64",
            "ProductLine": "string",
            "DealerPrice": "float64",
            "Class": "string",
            "Style": "string",
            "ModelName": "string",
            "LargePhoto": "string",
            "EnglishDescription": "string",
            "FrenchDescription": "string",
            "ChineseDescription": "string",
            "ArabicDescription": "string",
            "HebrewDescription": "string",
            "ThaiDescription": "string",
            "GermanDescription": "string",
            "JapaneseDescription": "string",
            "TurkishDescription": "string",
            "StartDate": "string",
            "EndDate": "string",
            "Status": "string",
        },
        "DimReseller": {
            "ResellerKey": "Int64",
            "GeographyKey": "Int64",
            "ResellerAlternateKey": "string",
            "Phone": "string",
            "BusinessType": "string",
            "ResellerName": "string",
            "NumberEmployees": "Int64",
            "OrderFrequency": "string",
            "OrderMonth": "Int64",
            "FirstOrderYear": "Int64",
            "LastOrderYear": "Int64",
            "ProductLine": "string",
            "AddressLine1": "string",
            "AddressLine2": "string",
            "AnnualSales": "float64",
            "BankName": "string",
            "MinPaymentType": "Int64",
            "MinPaymentAmount": "float64",
            "AnnualRevenue": "float64",
            "YearOpened": "Int64",
        },
        "DimSalesTerritory": {
            "SalesTerritoryKey": "Int64",
            "SalesTerritoryAlternateKey": "Int64",
            "SalesTerritoryRegion": "string",
            "SalesTerritoryCountry": "string",
            "SalesTerritoryGroup": "string",
            "SalesTerritoryImage": "string",
        },
        "DimCurrency": {
            "CurrencyKey": "Int64",
            "CurrencyAlternateKey": "string",
            "CurrencyName": "string",
        },
        "DimAccount": {
            "AccountKey": "Int64",
            "ParentAccountKey": "Int64",
            "AccountCodeAlternateKey": "string",
            "ParentAccountCodeAlternateKey": "string",
            "AccountDescription": "string",
            "AccountType": "string",
            "Operator": "string",
            "CustomMembers": "string",
            "ValueType": "string",
            "CustomMemberOptions": "string",
        },
        "DimDepartmentGroup": {
            "DepartmentGroupKey": "Int64",
            "DepartmentGroupName": "string",
            "DepartmentGroupDescription": "string",
        },
        "DimGeography": {
            "GeographyKey": "Int64",
            "City": "string",
            "StateProvinceCode": "string",
            "StateProvinceName": "string",
            "CountryRegionCode": "string",
            "EnglishCountryRegionName": "string",
            "SpanishCountryRegionName": "string",
            "FrenchCountryRegionName": "string",
            "PostalCode": "string",
            "SalesTerritoryKey": "Int64",
            "IpAddressLocator": "string",
        },
        "DimOrganization": {
            "OrganizationKey": "Int64",
            "ParentOrganizationKey": "Int64",
            "PercentageOfOwnership": "float64",
            "OrganizationName": "string",
            "CurrencyKey": "Int64",
        },
        "DimProductCategory": {
            "ProductCategoryKey": "Int64",
            "ProductCategoryAlternateKey": "string",
            "EnglishProductCategoryName": "string",
            "SpanishProductCategoryName": "string",
            "FrenchProductCategoryName": "string",
        },
        "DimProductSubcategory": {
            "ProductSubcategoryKey": "Int64",
            "ProductSubcategoryAlternateKey": "string",
            "EnglishProductSubcategoryName": "string",
            "SpanishProductSubcategoryName": "string",
            "FrenchProductSubcategoryName": "string",
            "ProductCategoryKey": "Int64",
        },
        "DimSalesReason": {
            "SalesReasonKey": "Int64",
            "SalesReasonAlternateKey": "string",
            "SalesReasonName": "string",
            "SalesReasonType": "string",
        },
        "DimScenario": {"ScenarioKey": "Int64", "ScenarioName": "string"},
        "DimPromotion": {
            "PromotionKey": "Int64",
            "PromotionAlternateKey": "Int64",
            "EnglishPromotionName": "string",
            "SpanishPromotionName": "string",
            "FrenchPromotionName": "string",
            "DiscountPct": "float32",
            "EnglishPromotionType": "string",
            "SpanishPromotionType": "string",
            "FrenchPromotionType": "string",
            "EnglishPromotionCategory": "string",
            "SpanishPromotionCategory": "string",
            "FrenchPromotionCategory": "string",
            "StartDate": "string",
            "EndDate": "string",
            "MinQty": "Int64",
            "MaxQty": "Int64",
        },
        "ProspectiveBuyer": {
            "ProspectiveBuyerKey": "Int64",
            "ProspectiveBuyerAlternateKey": "Int64",
            "FirstName": "string",
            "MiddleName": "string",
            "LastName": "string",
            "Birthday": "string",
            "MaritalStatus": "string",
            "Gender": "string",
            "EmailAddress": "string",
            "YearlyIncome": "float64",
            "TotalChildren": "Int64",
            "NumberChildrenAtHome": "Int64",
            "Education": "string",
            "Occupation": "string",
            "HouseOwnerFlag": "Int64",
            "NumberCarsOwned": "Int64",
            "AddressLine1": "string",
            "AddressLine2": "string",
            "City": "string",
            "StateProvinceCode": "string",
            "PostalCode": "string",
            "Phone": "string",
            "Salutation": "string",
            "Unknown": "Int64",
        },
    }
    # endregion

    table_names = list(aw_schemas.keys())
    for index in range(len(table_names)):
        table_name = table_names[index]
        dtypes = aw_schemas[table_name]
        headers = list(dtypes.keys())

        url = f"{base_url}/{table_name}.csv"
        print(f"{icons.in_progress} Importing the {table_name} table from {url}")
        df = pd.read_csv(
            url, encoding="utf-16", sep="|", dtype=dtypes, names=headers, header=0
        )

        _save_as_delta_table(
            dataframe=spark.createDataFrame(df),
            delta_table_name=table_name,
            lakehouse=lakehouse_id,
            workspace=workspace_id,
        )


def _provision_aw_dimdate_table(
    workspace_id: UUID,
    lakehouse_id: UUID,
    table_properties: Optional[dict] = None,
) -> DataFrame:
    """
    Creates the DimDate AdventureWorksDW dimension table based on the start and end dates specified in the table properties.
    Parameters
    ----------
    workspace_id : uuid.UUID
        The Fabric workspace ID where the lakehouse is located.
    lakehouse_id : uuid.UUID
        The ID of the lakehouse where the delta tables should be added.
    table_properties: dict, default=None
        An arbirary dictionary of key/value pairs that the provision_perf_lab_lakehouse function passes to the table_generator function.

    Returns
    -------
    DataFrame
        A DataFrame with the DimDate data.
    """

    spark = (
        SparkSession.builder.appName("AdventureWorksDW")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    table_name = "DimDate"
    start_date = table_properties["start_date"]
    end_date = start_date + relativedelta(years=table_properties["years"])

    # Calculate the number of days between start_date and end_date
    num_days = spark.sql(
        f"SELECT datediff('{end_date}', '{start_date}') + 1 AS days"
    ).collect()[0]["days"]

    print(
        f"{icons.in_progress} Generating the {table_name} table from {start_date} to {end_date}"
    )

    date_df = (
        spark.range(0, num_days)
        .select(
            expr(f"date_add('{start_date}', cast(id as int))").alias(
                "FullDateAlternateKey"
            )
        )
        .withColumn(
            "DateKey", date_format("FullDateAlternateKey", "yyyyMMdd").cast("int")
        )
        .withColumn(
            "DayNumberOfWeek", date_format(col("FullDateAlternateKey"), "u").cast("int")
        )
        .withColumn(
            "EnglishDayNameOfWeek", date_format(col("FullDateAlternateKey"), "EEEE")
        )
        .withColumn(
            "SpanishDayNameOfWeek",
            expr(
                "CASE WHEN DayNumberOfWeek = 1 THEN 'Lunes' WHEN DayNumberOfWeek = 2 THEN 'Martes' WHEN DayNumberOfWeek = 3 THEN 'Miércoles' WHEN DayNumberOfWeek = 4 THEN 'Jueves' WHEN DayNumberOfWeek = 5 THEN 'Viernes' WHEN DayNumberOfWeek = 6 THEN 'Sábado' ELSE 'Domingo' END"
            ),
        )
        .withColumn(
            "FrenchDayNameOfWeek",
            expr(
                "CASE WHEN DayNumberOfWeek = 1 THEN 'Lundi' WHEN DayNumberOfWeek = 2 THEN 'Mardi' WHEN DayNumberOfWeek = 3 THEN 'Mercredi' WHEN DayNumberOfWeek = 4 THEN 'Jeudi' WHEN DayNumberOfWeek = 5 THEN 'Vendredi' WHEN DayNumberOfWeek = 6 THEN 'Samedi' ELSE 'Dimanche' END"
            ),
        )
        .withColumn(
            "DayNumberOfMonth",
            date_format(col("FullDateAlternateKey"), "d").cast("int"),
        )
        .withColumn(
            "DayNumberOfYear", date_format(col("FullDateAlternateKey"), "D").cast("int")
        )
        .withColumn(
            "WeekNumberOfYear",
            date_format(col("FullDateAlternateKey"), "w").cast("int"),
        )
        .withColumn(
            "EnglishMonthName", date_format(col("FullDateAlternateKey"), "MMMM")
        )
        .withColumn(
            "SpanishMonthName",
            expr(
                "CASE WHEN Month(FullDateAlternateKey) = 1 THEN 'Enero' WHEN Month(FullDateAlternateKey) = 2 THEN 'Febrero' WHEN Month(FullDateAlternateKey) = 3 THEN 'Marzo' WHEN Month(FullDateAlternateKey) = 4 THEN 'Abril' WHEN Month(FullDateAlternateKey) = 5 THEN 'Mayo' WHEN Month(FullDateAlternateKey) = 6 THEN 'Junio' WHEN Month(FullDateAlternateKey) = 7 THEN 'Julio' WHEN Month(FullDateAlternateKey) = 8 THEN 'Agosto' WHEN Month(FullDateAlternateKey) = 9 THEN 'Septiembre' WHEN Month(FullDateAlternateKey) = 10 THEN 'Octubre' WHEN Month(FullDateAlternateKey) = 11 THEN 'Noviembre' ELSE 'Diciembre' END"
            ),
        )
        .withColumn(
            "FrenchMonthName",
            expr(
                "CASE WHEN Month(FullDateAlternateKey) = 1 THEN 'Janvier' WHEN Month(FullDateAlternateKey) = 2 THEN 'Février' WHEN Month(FullDateAlternateKey) = 3 THEN 'Mars' WHEN Month(FullDateAlternateKey) = 4 THEN 'Avril' WHEN Month(FullDateAlternateKey) = 5 THEN 'Mai' WHEN Month(FullDateAlternateKey) = 6 THEN 'Juin' WHEN Month(FullDateAlternateKey) = 7 THEN 'Juillet' WHEN Month(FullDateAlternateKey) = 8 THEN 'Août' WHEN Month(FullDateAlternateKey) = 9 THEN 'Septembre' WHEN Month(FullDateAlternateKey) = 10 THEN 'Octobre' WHEN Month(FullDateAlternateKey) = 11 THEN 'Novembre' ELSE 'Décembre' END"
            ),
        )
        .withColumn(
            "MonthNumberOfYear",
            date_format(col("FullDateAlternateKey"), "M").cast("int"),
        )
        .withColumn("CalendarQuarter", expr("CEIL(Month(FullDateAlternateKey) / 3)"))
        .withColumn(
            "CalendarYear", date_format(col("FullDateAlternateKey"), "y").cast("int")
        )
        .withColumn(
            "CalendarSemester",
            expr("CASE WHEN Month(FullDateAlternateKey) <= 6 THEN 1 ELSE 2 END"),
        )
        .withColumn("FiscalYear", expr("year(add_months(FullDateAlternateKey, 6))"))
        .withColumn(
            "FiscalQuarter", expr("quarter(add_months(FullDateAlternateKey, 6))")
        )
        .withColumn(
            "FiscalSemester",
            expr("ceil(quarter(add_months(FullDateAlternateKey, 6))/2)"),
        )
    )

    _save_as_delta_table(
        dataframe=date_df,
        delta_table_name=table_name,
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )

    return date_df


def _update_promotion_dates(
    workspace_id: UUID,
    lakehouse_id: UUID,
    dim_date_df: DataFrame,
):
    """
    Updates the DimPromotion AdventureWorksDW table with dates that match the DimDate table.
    Parameters
    ----------
    workspace_id : uuid.UUID
        The Fabric workspace ID where the lakehouse is located.
    lakehouse_id : uuid.UUID
        The ID of the lakehouse where the delta tables should be added.
    dim_date_df : DataFrame
        A DataFrame with the DimDate data.
    """

    # region Schema definitions for the dimension tables
    no_promo_schema = StructType(
        [
            StructField("PromotionKey", IntegerType(), True),
            StructField("PromotionAlternateKey", IntegerType(), True),
            StructField("EnglishPromotionName", StringType(), True),
            StructField("SpanishPromotionName", StringType(), True),
            StructField("FrenchPromotionName", StringType(), True),
            StructField("DiscountPct", FloatType(), True),
            StructField("EnglishPromotionType", StringType(), True),
            StructField("SpanishPromotionType", StringType(), True),
            StructField("FrenchPromotionType", StringType(), True),
            StructField("EnglishPromotionCategory", StringType(), True),
            StructField("SpanishPromotionCategory", StringType(), True),
            StructField("FrenchPromotionCategory", StringType(), True),
            StructField("StartDate", StringType(), True),
            StructField("EndDate", StringType(), True),
            StructField("MinQty", IntegerType(), True),
            StructField("MaxQty", IntegerType(), True),
        ]
    )
    # endregion

    spark = SparkSession.builder.getOrCreate()
    table_name = "DimPromotion"

    dim_promotion_df = _read_delta_table(
        path=table_name,
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )

    min_date = dim_date_df.select(min("FullDateAlternateKey")).collect()[0][0]
    max_date = dim_date_df.select(max("FullDateAlternateKey")).collect()[0][0]

    print(
        f"{icons.in_progress} Updating the {table_name} table with promotion start dates between {min_date} and {max_date}"
    )

    dates = (
        dim_date_df.select("FullDateAlternateKey").rdd.flatMap(lambda x: x).collect()
    )

    @udf(DateType())
    def get_start_date():
        return random.choice(dates)

    no_promo_row = [
        (
            1,
            1,
            "No Discount",
            "Sin descuento",
            "Aucune remise",
            0.0,
            "No Discount",
            "Sin descuento",
            "Aucune remise",
            "No Discount",
            "Sin descuento",
            "Aucune remise",
            min_date.strftime("%Y-%m-%d"),
            max_date.strftime("%Y-%m-%d"),
            0,
            None,
        )
    ]
    dim_promotion_df = spark.createDataFrame(no_promo_row, no_promo_schema).union(
        dim_promotion_df.filter(dim_promotion_df.PromotionKey != 1)
        .withColumn("StartDate", get_start_date())
        .withColumn("EndDate", expr("date_add(StartDate, cast(rand() * 365 as int))"))
    )

    _save_as_delta_table(
        dataframe=dim_promotion_df,
        delta_table_name=table_name,
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )


def _generate_fact_call_center(
    workspace_id: UUID,
    lakehouse_id: UUID,
    dim_date_df: DataFrame,
):
    """
    Generates the FactCallCenter AdventureWorksDW table based on dates from the DimDate table.
    Parameters
    ----------
    workspace_id : uuid.UUID
        The Fabric workspace ID where the lakehouse is located.
    lakehouse_id : uuid.UUID
        The ID of the lakehouse where the delta tables should be added.
    dim_date_df : DataFrame
        A DataFrame with the DimDate data.
    """

    # Initialize Spark session
    spark = SparkSession.builder.getOrCreate()
    table_name = "FactCallCenter"

    print(
        f"{icons.in_progress} Generating the {table_name} table aligned with the DimDate table."
    )

    # region UDFs for the table generation
    # ---------- Wage Type --------------
    def is_us_holiday(check_date):
        us_holidays = holidays.US(years=check_date.year)
        return check_date in us_holidays

    def is_weekend(check_date):
        # Check if the date is Saturday (5) or Sunday (6)
        return check_date.weekday() >= 5

    @udf(StringType())
    def get_wage_type(date):
        if is_us_holiday(date):
            return "holiday"
        elif is_weekend(date):
            return "weekend"
        else:
            return "weekday"

    # ------------ Shift Type --------------
    shift_types = ["AM", "PM1", "PM2", "midnight"]
    shift_types_df = spark.createDataFrame(
        [(shift,) for shift in shift_types], ["ShiftType"]
    )

    # ------------ Operators --------------
    @udf(IntegerType())
    def get_operator_count(wage_type, min_operators, max_operators):
        if wage_type == "weekday":
            return random.randint(min_operators, max_operators)
        else:
            return 0

    # ------------ Calls --------------
    @udf(IntegerType())
    def get_calls_count(operators):
        calls_per_operator = random.randint(20, 40)
        return operators * calls_per_operator

    # ------------ AutomaticResponses --------------
    @udf(IntegerType())
    def get_auto_repsonse_count(wage_type, min, max):
        repsonse_count = random.randint(min, max)
        if wage_type == "weekday":
            return repsonse_count
        else:
            return int(repsonse_count / 10)

    # ------------ Orders --------------
    @udf(IntegerType())
    def get_orders_count(operators):
        orders_per_operator = random.randint(10, 25)
        return operators * orders_per_operator

    # ------------ IssuesRaised --------------
    @udf(IntegerType())
    def get_issues_count(wage_type):
        if wage_type == "weekday":
            return random.randint(0, 5)
        else:
            return 0

    # ------------- [AverageTimePerIssue] ------------
    @udf(IntegerType())
    def get_avg_issue_time(wage_type):
        time = random.randint(40, 120)
        if wage_type == "weekday":
            return time
        else:
            return 0

    # ------------- ServiceGrade --------------
    @udf(FloatType())
    def get_service_grade():
        return round(random.uniform(0.02, 0.25), 2)

    # endregion

    call_center_stats = dim_date_df.select(
        "FullDateAlternateKey", "DateKey"
    ).withColumn("WageType", get_wage_type(col("FullDateAlternateKey")))

    window_spec = Window.orderBy("DateKey")
    call_center_stats = (
        call_center_stats.crossJoin(shift_types_df)
        .withColumn("FactCallCenterID", row_number().over(window_spec))
        .withColumn(
            "LevelOneOperators", get_operator_count(col("WageType"), lit(2), lit(4))
        )
        .withColumn(
            "LevelTwoOperators", get_operator_count(col("WageType"), lit(8), lit(14))
        )
        .withColumn(
            "TotalOperators", col("LevelOneOperators") + col("LevelTwoOperators")
        )
        .withColumn("Calls", get_calls_count(col("TotalOperators")))
        .withColumn(
            "AutomaticResponses",
            get_auto_repsonse_count(col("WageType"), lit(58), lit(514)),
        )
        .withColumn("Orders", get_orders_count(col("TotalOperators")))
        .withColumn("IssuesRaised", get_issues_count(col("WageType")))
        .withColumn("AverageTimePerIssue", get_avg_issue_time(col("WageType")))
        .withColumn("ServiceGrade", get_service_grade())
        .withColumnRenamed("FullDateAlternateKey", "Date")
        .withColumnRenamed("ShiftType", "Shift")
    )

    _save_as_delta_table(
        dataframe=call_center_stats.select(
            "FactCallCenterID",
            "DateKey",
            "WageType",
            "Shift",
            "LevelOneOperators",
            "LevelTwoOperators",
            "TotalOperators",
            "Calls",
            "AutomaticResponses",
            "Orders",
            "IssuesRaised",
            "AverageTimePerIssue",
            "ServiceGrade",
            "Date",
        ),
        delta_table_name=table_name,
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )


def _get_base_exhange_rates() -> DataFrame:
    """
    Generates ficticous exchange rates for the AdeventureWorksDW currencies.
    Returns
    -------
    DataFrame
        A DataFrame with the currency exchange rates.
    """
    # Initialize Spark session
    spark = SparkSession.builder.getOrCreate()

    # Define the CSV data as a string
    csv_data = """CurrencyKey,CurrencyAlternateKey,EndOfDayRate
    35,AED,3.6725
    1,AFA,86.868195
    54,ALL,106.0
    4,AMD,24.322692
    66,ANG,1.79
    51,AOA,830.0
    3,ARS,1056.002525
    86,ATS,13.7603
    6,AUD,1.591856
    5,AWG,1.791856
    7,AZM,1.897273
    12,BBD,2.0
    93,BDT,86.868195
    13,BEF,0.025
    18,BGN,1.897273
    9,BHD,0.376
    17,BND,1.355452
    15,BOB,6.9
    16,BRL,5.787771
    8,BSD,1.0
    71,BTN,86.868195
    19,CAD,1.432839
    92,CHF,0.911621
    22,CLP,961.729379
    103,CNY,32.818406
    23,COP,4129.943003
    24,CRC,586.0
    26,CYP,0.585274
    27,CZK,24.322692
    29,DEM,1.95583
    28,DKK,7.236404
    30,DOP,58.187576
    2,DZD,139.054263
    49,EEK,15.6466
    33,EGP,30.0
    89,ESP,166.386
    36,EUR,0.97006
    59,FIM,5.94573
    37,FJD,2.2
    39,FRF,6.55957
    98,GBP,0.787564
    20,GHC,13.860409
    32,GRD,340.75
    79,GTQ,7.8
    41,HKD,7.790752
    25,HRK,7.236404
    38,HUF,392.595513
    82,IDR,16380.300738
    44,IEP,0.787564
    68,ILS,3.595758
    43,INR,86.868195
    42,ISK,142.218447
    45,ITL,1936.27
    46,JMD,154.0
    47,JOD,0.709
    102,JPY,142.218447
    48,KES,140.0
    101,KRW,142.218447
    50,KWD,0.30885
    53,LBP,1507.5
    90,LKR,297.425121
    56,LTL,3.4528
    52,LVL,0.702804
    62,MAD,10.0
    58,MTL,0.4293
    60,MUR,46.997144
    81,MVR,15.4
    61,MXN,20.623594
    57,MYR,4.469848
    64,NAD,18.445902
    63,NGN,460.0
    67,NLG,2.20371
    72,NOK,11.208655
    65,NPR,139.054263
    70,NZD,1.769854
    74,OMR,0.385014
    11,PAB,1.0
    73,PEN,3.8
    76,PHP,58.187576
    75,PKR,279.616453
    105,PLN,4.060131
    77,PLZ,4.060131
    78,PTE,200.482
    40,PYG,7200.0
    55,ROL,4.828176
    83,RUB,96.293408
    84,RUR,96.293408
    85,SAR,3.75
    91,SEK,10.924034
    87,SGD,1.355452
    94,SIT,239.64
    88,SKK,24.322692
    34,SVC,8.75
    10,THB,34.094074
    96,TND,2.8
    97,TRL,36.02337
    95,TTD,6.80057
    69,TWD,32.818406
    100,USD,1.0
    99,UYU,42.0
    14,VEB,24.322692
    31,VND,16380.300738
    21,XOF,0.00154
    80,ZAR,18.445902
    104,ZWD,361.9"""

    # Convert the CSV string to a list of rows
    data = [row.split(",") for row in csv_data.split("\n")]

    # Extract the header and rows
    header = data[0]
    rows = data[1:]

    # Create a DataFrame from the rows and header
    return (
        spark.createDataFrame(rows, schema=header)
        .withColumn("EndOfDayRate", col("EndOfDayRate").cast(FloatType()))
        .drop("CurrencyKey")
    )


def _generate_fact_currency_rates(
    workspace_id: UUID,
    lakehouse_id: UUID,
    dim_date_df: DataFrame,
):
    """
    Generates the FactCurrencyRate AdventureWorksDW table.
    Parameters
    ----------
    workspace_id : uuid.UUID
        The Fabric workspace ID where the lakehouse is located.
    lakehouse_id : uuid.UUID
        The ID of the lakehouse where the delta tables should be added.
    dim_date_df : DataFrame
        A DataFrame with the DimDate data.
    """

    spark = SparkSession.builder.getOrCreate()
    table_name = "FactCurrencyRate"

    print(
        f"{icons.in_progress} Generating the {table_name} table aligned with the DimDate table."
    )

    dim_currency_df = _read_delta_table(
        path="DimCurrency",
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )

    currency_keys_df = dim_currency_df.select("CurrencyKey", "CurrencyAlternateKey")
    date_keys_df = dim_date_df.select(
        "DateKey", "FullDateAlternateKey"
    ).withColumnRenamed("FullDateAlternateKey", "Date")

    # Cross join CurrencyKeys and DateKeys to generate all combinations
    fact_currency_rate_df = currency_keys_df.crossJoin(date_keys_df)

    # Join in the base exchange rates
    fact_currency_rate_df = fact_currency_rate_df.join(
        _get_base_exhange_rates(), on="CurrencyAlternateKey", how="inner"
    )

    # Define a UDF to modify the exchange rate by using a random seed number.
    # The modify_rate UDF could be enhanced by
    # looking up actual exchange rates based on the given date.
    @udf(FloatType())
    def modify_rate(rate, date, seed):
        percentage_change = seed * 0.6 - 0.3  # Random value between -0.3 and 0.3
        return rate + rate * percentage_change

    fact_currency_rate_df = fact_currency_rate_df.withColumn(
        "Seed1", rand()
    ).withColumn("Seed2", rand())
    fact_currency_rate_df = fact_currency_rate_df.withColumn(
        "AverageRate", modify_rate(col("EndOfDayRate"), col("Date"), col("Seed1"))
    ).withColumn(
        "EndOfDayRate", modify_rate(col("EndOfDayRate"), col("Date"), col("Seed2"))
    )

    _save_as_delta_table(
        dataframe=fact_currency_rate_df.drop("CurrencyAlternateKey")
        .drop("Seed1")
        .drop("Seed2"),
        delta_table_name=table_name,
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )


def _get_fact_finance_data(
    workspace_id: UUID,
    lakehouse_id: UUID,
    dim_date_df: DataFrame,
):
    """
    Generates the FactFinance AdventureWorksDW table.
    Parameters
    ----------
    workspace_id : uuid.UUID
        The Fabric workspace ID where the lakehouse is located.
    lakehouse_id : uuid.UUID
        The ID of the lakehouse where the delta tables should be added.
    dim_date_df : DataFrame
        A DataFrame with the DimDate data.
    """

    spark = SparkSession.builder.getOrCreate()
    table_name = "FactFinance"

    print(
        f"{icons.in_progress} Generating the {table_name} table aligned with the various AdventureWorksDW dimension tables."
    )

    dim_org_df = _read_delta_table(
        path="DimOrganization",
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )
    dim_account_df = _read_delta_table(
        path="DimAccount",
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )
    dim_dept_group_df = _read_delta_table(
        path="DimDepartmentGroup",
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )
    dim_scenario_key_df = _read_delta_table(
        path="DimScenario",
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )

    first_days_df = dim_date_df.filter(
        dayofmonth(col("FullDateAlternateKey")) == 1
    ).select("DateKey")

    # region UDFs for the table generation
    # Function to generate a list of random rows for each DateKey
    def generate_random_rows(datekey):
        num_rows = random.randint(800, 1100)
        return [(datekey,) for _ in range(num_rows)]

    # ---------------------OrganizationKey-------------------------------------
    org_keys = dim_org_df.select("OrganizationKey").rdd.flatMap(lambda x: x).collect()

    @udf(IntegerType())
    def get_org_key():
        return random.choice(org_keys)

    # ---------------------AccountKey-------------------------------------
    account_keys = (
        dim_account_df.select("AccountKey").rdd.flatMap(lambda x: x).collect()
    )

    @udf(IntegerType())
    def get_account_keys():
        return random.choice(account_keys)

    # ---------------------DepartmentGroupKey-------------------------------------
    dept_group_keys = (
        dim_dept_group_df.select("DepartmentGroupKey")
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    @udf(IntegerType())
    def get_dept_group_key():
        return random.choice(dept_group_keys)

    # ---------------------ScenarioKey-------------------------------------
    scenario_keys = (
        dim_scenario_key_df.select("ScenarioKey").rdd.flatMap(lambda x: x).collect()
    )

    @udf(IntegerType())
    def get_scenario_key():
        return random.choice(scenario_keys)

    # ---------------------Amount------------------------------------
    @udf(FloatType())
    def get_amount():
        return random.uniform(-1000000.00, 5000000.00)

    # ----------------------------------------------------------
    # endregion

    fact_finance_df = (
        first_days_df.rdd.flatMap(lambda row: generate_random_rows(row.DateKey))
        .toDF(["DateKey"])
        .withColumn("FinanceKey", monotonically_increasing_id())
        .withColumn("OrganizationKey", get_org_key())
        .withColumn("AccountKey", get_account_keys())
        .withColumn("DepartmentGroupKey", get_dept_group_key())
        .withColumn("ScenarioKey", get_scenario_key())
        .withColumn("Amount", get_amount())
    )

    _save_as_delta_table(
        dataframe=fact_finance_df,
        delta_table_name=table_name,
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )


def _generate_fact_sales_quota(
    workspace_id: UUID,
    lakehouse_id: UUID,
    dim_date_df: DataFrame,
    table_properties: Optional[dict] = None,
):
    """
    Generates the FactFinance AdventureWorksDW table.
    Parameters
    ----------
    workspace_id : uuid.UUID
        The Fabric workspace ID where the lakehouse is located.
    lakehouse_id : uuid.UUID
        The ID of the lakehouse where the delta tables should be added.
    dim_date_df : DataFrame
        A DataFrame with the DimDate data.
    table_properties: dict, default=None
        An arbirary dictionary of key/value pairs that the provision_perf_lab_lakehouse function passes to the table_generator function.
    """

    spark = SparkSession.builder.getOrCreate()
    table_name = "FactSalesQuota"

    print(
        f"{icons.in_progress} Generating the {table_name} table aligned with the DimDate and DimEmployee tables."
    )

    dim_employee_df = _read_delta_table(
        path="DimEmployee",
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )

    row_count = table_properties["sales_quota_count"]

    employee_keys = (
        dim_employee_df.select("EmployeeKey").rdd.flatMap(lambda x: x).collect()
    )
    date_keys = dim_date_df.select("DateKey").rdd.flatMap(lambda x: x).collect()

    # Define a UDF to assign random EmployeeKey
    @udf(IntegerType())
    def get_random_employee_key():
        return random.choice(employee_keys)

    # Define a UDF to assign random DateKey
    @udf(IntegerType())
    def get_date_key():
        return random.choice(date_keys)

    # Define a UDF to generate random SalesAmountQuota
    @udf(IntegerType())
    def get_sales_quota():
        if random.random() < 0.9:  # 90% chance for smaller numbers
            return random.randint(1000, 200000)
        else:  # 10% chance for larger numbers
            return random.randint(200000, 2000000)

    # Generate a DataFrame using spark.range
    sales_quota_df = (
        spark.range(0, row_count)
        .toDF("SalesQuotaKey")
        .withColumn("EmployeeKey", get_random_employee_key())
        .withColumn("DateKey", get_date_key())
        .withColumn("SalesAmountQuota", get_sales_quota().cast(FloatType()))
    )

    _save_as_delta_table(
        dataframe=sales_quota_df,
        delta_table_name=table_name,
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )


def _generate_fact_survey_responses(
    workspace_id: UUID,
    lakehouse_id: UUID,
    dim_date_df: DataFrame,
    table_properties: Optional[dict] = None,
):
    """
    Generates the FactSalesQuota AdventureWorksDW table.
    Parameters
    ----------
    workspace_id : uuid.UUID
        The Fabric workspace ID where the lakehouse is located.
    lakehouse_id : uuid.UUID
        The ID of the lakehouse where the delta tables should be added.
    dim_date_df : DataFrame
        A DataFrame with the DimDate data.
    table_properties: dict, default=None
        An arbirary dictionary of key/value pairs that the provision_perf_lab_lakehouse function passes to the table_generator function.
    """

    spark = SparkSession.builder.getOrCreate()
    table_name = "FactSurveyResponse"

    print(
        f"{icons.in_progress} Generating the {table_name} table aligned with DimDate and other dimension tables."
    )

    dim_customer_df = _read_delta_table(
        path="DimCustomer",
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )
    dim_product_category_df = _read_delta_table(
        path="DimProductCategory",
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )
    dim_product_subcategory_df = _read_delta_table(
        path="DimProductSubcategory",
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )

    row_count = table_properties["survey_response_count"]

    date_keys = (
        dim_date_df.select("FullDateAlternateKey").rdd.flatMap(lambda x: x).collect()
    )
    customer_keys = (
        dim_customer_df.select("CustomerKey").rdd.flatMap(lambda x: x).collect()
    )

    # ----------------------
    product_category_fields = dim_product_category_df.select(
        "ProductCategoryKey", "EnglishProductCategoryName"
    ).collect()

    # Define the schema for the returned row
    category_schema = StructType(
        [
            StructField("ProductCategoryKey", LongType(), True),
            StructField("EnglishProductCategoryName", StringType(), True),
        ]
    )

    # Define the UDF
    @udf(category_schema)
    def get_product_category():
        return random.choice(product_category_fields)
        # return Row(ProductCategoryKey=random_row["ProductCategoryKey"], EnglishProductCategoryName=random_row["EnglishProductCategoryName"])

    @udf(LongType())
    def get_product_category_key():
        return random.choice(product_category_fields)["ProductCategoryKey"]

    # -----------------------

    product_subcategory_fields = dim_product_subcategory_df.select(
        "ProductSubcategoryKey", "EnglishProductSubcategoryName"
    ).collect()
    subcategory_schema = StructType(
        [
            StructField("ProductSubcategoryKey", LongType(), True),
            StructField("EnglishProductSubcategoryName", StringType(), True),
        ]
    )

    # Define the UDF
    @udf(subcategory_schema)
    def get_product_subcategory():
        return random.choice(product_subcategory_fields)

    @udf(LongType())
    def get_product_subcategory_key():
        return random.choice(product_subcategory_fields)["ProductSubcategoryKey"]

    # -----------------------

    # Define a UDF to assign random EmployeeKey
    @udf(IntegerType())
    def get_random_customer_key():
        return random.choice(customer_keys)

    # Define a UDF to assign random DateKey
    @udf(DateType())
    def get_date_key():
        return random.choice(date_keys)

    # Generate a DataFrame using spark.range
    survey_responses = (
        spark.range(0, row_count)
        .toDF("SurveyResponseKey")
        .withColumn("Date", get_date_key())
        .withColumn("DateKey", date_format("Date", "yyyyMMdd").cast("int"))
        .withColumn("CustomerKey", get_random_customer_key())
        .withColumn("category_row", get_product_category())
        .withColumn(
            "ProductCategoryKey", col("category_row").getField("ProductCategoryKey")
        )
        .withColumn(
            "EnglishProductCategoryName",
            col("category_row").getField("EnglishProductCategoryName"),
        )
        .withColumn("subcategory_row", get_product_subcategory())
        .withColumn(
            "ProductSubcategoryKey",
            col("subcategory_row").getField("ProductSubcategoryKey"),
        )
        .withColumn(
            "EnglishProductSubcategoryName",
            col("subcategory_row").getField("EnglishProductSubcategoryName"),
        )
    )
    _save_as_delta_table(
        dataframe=survey_responses.select(
            "SurveyResponseKey",
            "DateKey",
            "CustomerKey",
            "ProductCategoryKey",
            "EnglishProductCategoryName",
            "ProductSubcategoryKey",
            "EnglishProductSubcategoryName",
        ),
        delta_table_name=table_name,
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )


def _get_product_offerings(
    dim_product_df: DataFrame,
    dim_promotion_df: DataFrame,
) -> DataFrame:
    """
    Generates a DataFrame with product offerings.
    A product offering is a product with a regular price plus a possible promotion.

    Parameters
    ----------
    dim_product_df : DataFrame
        A DataFrame with the DimProduct data.
    dim_promotion_df : DataFrame
        A DataFrame with the DimPromotion data.

    Returns
    -------
    DataFrame
        A DataFrame with the assigned product offerings.
    """

    spark = SparkSession.builder.getOrCreate()

    dim_promotion_df = (
        dim_promotion_df.select("PromotionKey", "DiscountPct", "StartDate", "EndDate")
        .withColumn("StartDate", col("StartDate").cast(DateType()))
        .withColumn("EndDate", col("EndDate").cast(DateType()))
    )

    # Collect promotions and products as lists
    promotions = dim_promotion_df.collect()
    products = dim_product_df.where(dim_product_df.StandardCost.isNotNull()).collect()

    # Create a list to hold the assigned promotions
    assigned_promotions = []

    # Randomly assign one or two promotions to each product
    for product in products:
        num_promotions = random.randint(1, 2)
        selected_promotions = random.sample(promotions, num_promotions)
        for promotion in selected_promotions:
            assigned_promotions.append(
                (
                    product["ProductKey"],
                    product["StandardCost"],
                    product["ListPrice"],
                    product["ReorderPoint"],
                    product["SafetyStockLevel"],
                    promotion["PromotionKey"],
                    promotion["DiscountPct"],
                    promotion["StartDate"],
                    promotion["EndDate"],
                )
            )

    # Create a DataFrame for the assigned promotions
    assigned_promotions_df = spark.createDataFrame(
        assigned_promotions,
        [
            "ProductKey",
            "StandardCost",
            "ListPrice",
            "ReorderPoint",
            "SafetyStockLevel",
            "PromotionKey",
            "DiscountPct",
            "StartDate",
            "EndDate",
        ],
    )

    return assigned_promotions_df


def _explode_rows(
    table_df: DataFrame,
    approx_max_rows: int,
) -> DataFrame:
    """
    Generates a larger DataFrame by repeating each row as often as necessary
    to reach the specified approximate number of rows
    with weekdays replicated 10 times more than weekends.

    Parameters
    ----------
    table_df : DataFrame
        A DataFrame with the table data. The DataFrame must include a date column
        so that weekdays can be replicated 10 times more than weekends.
    approx_max_rows : int
        The approximate size of the DataFrame after processing.

    Returns
    -------
    DataFrame
        A DataFrame with the approximate number of rows.
    """
    spark = SparkSession.builder.getOrCreate()

    if approx_max_rows <= table_df.count():
        return table_df.limit(approx_max_rows)

    # Identify the date column dynamically (assuming it's the first DateType column)
    date_column = [
        field.name
        for field in table_df.schema.fields
        if isinstance(field.dataType, DateType)
    ][0]

    # Calculate the number of weekdays and weekends
    weekdays_count = table_df.filter(dayofweek(table_df[date_column]) < 6).count()
    weekends_count = table_df.filter(dayofweek(table_df[date_column]) >= 6).count()

    # Calculate average replication factors
    avg_weekdays = approx_max_rows / (weekdays_count + weekends_count / 10)
    avg_weekends = avg_weekdays / 10

    min_weekdays = int(avg_weekdays * 0.8) + 1
    max_weekdays = int(avg_weekdays * 1.2) + 2

    min_weekends = int(avg_weekends * 0.8) + 1
    max_weekends = int(avg_weekends * 1.2) + 2

    # Function to generate random replication of
    # with weekdays replicated 10 times more than weekends
    def replicate_row(row):
        date_value = getattr(row, date_column)
        if date_value.weekday() < 5:  # Weekday
            num_replicates = np.random.randint(min_weekdays, max_weekdays)
        else:  # Weekend
            num_replicates = np.random.randint(min_weekends, max_weekends)
        return [row] * num_replicates

    # Apply the replication function to each row and explode the array
    replicated_rows = table_df.rdd.flatMap(replicate_row).collect()

    return spark.createDataFrame(replicated_rows, table_df.schema)


def _get_sales_territories(
    dim_currency_df: DataFrame,
    dim_geography_df: DataFrame,
) -> DataFrame:
    """
    Generates a DataFrame of sales territories based on "GeographyKey", "SalesTerritoryKey", and "CurrencyKey".

    Parameters
    ----------
    dim_currency_df : DataFrame
        A DataFrame with DimCurrency data.
    dim_geography_df : DataFrame
        A DataFrame with DimGeography data.

    Returns
    -------
    DataFrame
        A DataFrame with the sales territories.
    """
    spark = SparkSession.builder.getOrCreate()

    geo_data = [
        ("AU", "AUD"),
        ("CA", "CAD"),
        ("GB", "GBP"),
        ("DE", "EUR"),
        ("US", "USD"),
        ("FR", "EUR"),
    ]
    geo_df = spark.createDataFrame(
        geo_data, ["CountryRegionCode", "CurrencyAlternateKey"]
    )

    dim_currency_df = dim_currency_df.select(
        "CurrencyKey", "CurrencyAlternateKey"
    ).distinct()
    dim_geo_currency_df = geo_df.join(
        dim_currency_df, on="CurrencyAlternateKey", how="inner"
    )

    dim_sales_territory_df = (
        dim_geo_currency_df.join(dim_geography_df, on="CountryRegionCode", how="inner")
        .select("GeographyKey", "SalesTerritoryKey", "CurrencyKey")
        .distinct()
    )

    return dim_sales_territory_df


def _generate_sales_tables(
    workspace_id: UUID,
    lakehouse_id: UUID,
    dim_date_df: DataFrame,
    table_properties: Optional[dict] = None,
) -> dict:
    """
    Generates DataFrames for the FactInternetSales, FactInternetSalesReason, and FactResellerSales tables.
    Parameters
    ----------
    workspace_id : uuid.UUID
        The Fabric workspace ID where the lakehouse is located.
    lakehouse_id : uuid.UUID
        The ID of the lakehouse where the delta tables should be added.
    dim_date_df : DataFrame
        A DataFrame with the DimDate data.
    table_properties: dict, default=None
        An arbirary dictionary of key/value pairs that the provision_perf_lab_lakehouse function passes to the table_generator function.

    Returns
    -------
    dict
        A dictionary with the keys FactInternetSales, FactInternetSalesReason, and FactResellerSale for the corresponding AdventureWorksDW fact sales tables.
    """

    spark = SparkSession.builder.getOrCreate()

    print(
        f"{icons.in_progress} Generating the FactInternetSales, FactInternetSalesReason, and FactResellerSales tables."
    )

    approx_max_rows = table_properties["fact_rows_in_millions"] * 1000000

    # region Loading Delta table data
    dim_currency_df = _read_delta_table(
        path="DimCurrency",
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )
    dim_geography_df = _read_delta_table(
        path="DimGeography",
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )
    dim_customer_df = _read_delta_table(
        path="DimCustomer",
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )
    dim_reseller_df = _read_delta_table(
        path="DimReseller",
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )
    dim_employee_df = _read_delta_table(
        path="DimEmployee",
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )
    dim_product_df = _read_delta_table(
        path="DimProduct",
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )
    dim_promotion_df = _read_delta_table(
        path="DimPromotion",
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )
    dim_sales_reason = _read_delta_table(
        path="DimSalesReason",
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )
    # endregion

    # Orders can have multiple lines, but all items are purchased together on the order date.
    # So, 1) explode the dates into the required number of order dates,
    # Then, 2) add all information that is common for all order lines, such as the customer, etc.
    # Then, 3) add the line-specific information, such as product, quantity, and so forth.

    # ***************** 1) explode the dates into the required number of order dates ***************
    sales_df = (
        dim_date_df.select("FullDateAlternateKey", "DateKey")
        .withColumnRenamed("DateKey", "OrderDateKey")
        .withColumnRenamed("FullDateAlternateKey", "Date")
    )

    sales_df = _explode_rows(sales_df, approx_max_rows * 2)

    # Add a row index for later joins
    sales_df = sales_df.withColumn("ix", monotonically_increasing_id())

    # ***************** Calculate the number of digits required for the SO number ***************
    #  ***************  to add a unique sales order ID with the required number of digits  ******
    row_count = sales_df.count()
    num_digits = math.ceil(math.log10(row_count + 1))
    sales_df = sales_df.withColumn(
        "SalesOrderNumber",
        expr(
            f"concat('SO', lpad(monotonically_increasing_id() + 1, {num_digits}, '0'))"
        ),
    )

    # ***************** 2) add all information that is common for all order lines  ***************
    dim_sales_territory_df = _get_sales_territories(dim_currency_df, dim_geography_df)

    # What describes a customer? CustomerKey, SalesTerritoryKey and CurrencyKey
    dim_customer_df = dim_customer_df.select("CustomerKey", "GeographyKey").distinct()
    dim_customer_df = (
        dim_customer_df.join(dim_sales_territory_df, on="GeographyKey", how="inner")
        .withColumn("ResellerKey", lit(None))
        .withColumn("EmployeeKey", lit(None))
        .withColumn("CarrierTrackingNumber", lit(None))
        .withColumn("CustomerPONumber", lit(None))
        .withColumn("RevisionNumber", when(rand() < 0.99, 1).otherwise(2))
        .select(
            "CustomerKey",
            "ResellerKey",
            "EmployeeKey",
            "CurrencyKey",
            "SalesTerritoryKey",
            "CarrierTrackingNumber",
            "CustomerPONumber",
            "RevisionNumber",
        )
    )

    # What describes a employee taking care of a reseller? EmployeeKey, ResellerKey, SalesTerritoryKey and CurrencyKey
    po_start = random.randint(1000000, 9999999)
    dim_reseller_df = dim_reseller_df.join(
        dim_sales_territory_df, on="GeographyKey", how="inner"
    )
    dim_reseller_df = (
        dim_reseller_df.join(dim_employee_df, on="SalesTerritoryKey", how="inner")
        .withColumn("CustomerKey", lit(None))
        .withColumn("CarrierTrackingNumber", expr("uuid()"))
        .withColumn(
            "CustomerPONumber",
            expr(
                f"concat('PO', lpad(monotonically_increasing_id() + {po_start}, {num_digits}, '0'))"
            ),
        )
        .withColumn("RevisionNumber", when(rand() < 0.99, 1).otherwise(2))
        .select(
            "CustomerKey",
            "ResellerKey",
            "EmployeeKey",
            "CurrencyKey",
            "SalesTerritoryKey",
            "CarrierTrackingNumber",
            "CustomerPONumber",
            "RevisionNumber",
        )
    )

    common_fields = dim_customer_df.union(dim_reseller_df).collect()

    def get_random_row():
        return random.choice(common_fields)

    shuffled_rows = [get_random_row() for _ in range(sales_df.count())]

    # Define the schema for the returned rows
    common_schema = StructType(
        [
            StructField("CustomerKey", LongType(), True),
            StructField("ResellerKey", LongType(), True),
            StructField("EmployeeKey", LongType(), True),
            StructField("CurrencyKey", LongType(), True),
            StructField("SalesTerritoryKey", LongType(), True),
            StructField("CarrierTrackingNumber", StringType(), True),
            StructField("CustomerPONumber", StringType(), True),
            StructField("RevisionNumber", IntegerType(), True),
        ]
    )

    shuffled_rows_df = spark.createDataFrame(shuffled_rows, common_schema)
    shuffled_rows_df = shuffled_rows_df.withColumn("ix", monotonically_increasing_id())

    # Add the CustomerKey column to sales_df
    sales_df = sales_df.join(shuffled_rows_df, on="ix", how="inner")

    # Add the DueDateKey
    @udf(IntegerType())
    def get_due_date_key(date):
        choice = random.choice([7, 14, "1 month"])
        if choice == "1 month":
            new_date = date + relativedelta(months=1)
        else:
            new_date = date + timedelta(days=choice)
        return int(new_date.strftime("%Y%m%d"))

    sales_df = sales_df.withColumn("DueDateKey", get_due_date_key(col("Date")))

    # ***** 3) add the line-specific information, such as product, quantity, and so forth. ************
    product_offerings = _get_product_offerings(
        dim_product_df, dim_promotion_df
    ).collect()

    data = []
    for ix in range(sales_df.count()):
        # order_date = sales_df.filter(sales_df.ix == ix).select("Date").collect()[0]["Date"]
        chosen_product = random.choice(product_offerings)
        order_line_count = random.randint(1, 10)
        for order_line_id in range(1, order_line_count + 1):

            data.append(
                (
                    ix,
                    order_line_id,
                    chosen_product[0],  # ["ProductKey"],
                    chosen_product[1],  # ["StandardCost"],
                    chosen_product[2],  # ["ListPrice"],
                    chosen_product[3],  # ["ReorderPoint"],
                    chosen_product[4],  # ["SafetyStockLevel"],
                    chosen_product[5],  # ["PromotionKey"],
                    chosen_product[6],  # ["DiscountPct"],
                    chosen_product[7],  # ["StartDate"],
                    chosen_product[8],  # ["EndDate"],
                )
            )

    order_lines_df = spark.createDataFrame(
        data,
        [
            "ix",
            "SalesOrderLineNumber",
            "ProductKey",
            "StandardCost",
            "ListPrice",
            "ReorderPoint",
            "SafetyStockLevel",
            "PromotionKey",
            "DiscountPct",
            "StartDate",
            "EndDate",
        ],
    )

    sales_df = sales_df.join(order_lines_df, on="ix", how="inner")

    sales_df = sales_df.withColumn(
        "ShipDateKey",
        date_format(expr("date_add(Date, CAST(FLOOR(rand() * 8) AS INT))"), "yyyyMMdd"),
    ).withColumn(
        "PromotionKey",
        when(
            (col("Date") >= col("StartDate")) & (col("Date") <= col("EndDate")),
            col("PromotionKey"),
        ).otherwise(1),
    )

    sales_df = (
        sales_df.withColumn("OrderQuantity", expr("CAST(FLOOR(RAND() * 8 + 1) AS INT)"))
        .withColumnRenamed("ListPrice", "UnitPrice")
        .withColumn(
            "DiscountPct",
            when(col("PromotionKey") == 1, 0).otherwise(col("DiscountPct")),
        )
        .withColumnRenamed("DiscountPct", "UnitPriceDiscountPct")
        .withColumnRenamed("StandardCost", "ProductStandardCost")
    )

    sales_df = sales_df.withColumn(
        "ExtendedAmount", expr("UnitPrice * OrderQuantity")
    ).withColumn("TotalProductCost", expr("ProductStandardCost * OrderQuantity"))

    sales_df = sales_df.withColumn(
        "FreightAmount", expr("ROUND(ProductStandardCost * 0.1 / 5) * 5")
    ).withColumn(
        "DiscountAmount", expr("UnitPriceDiscountPct * UnitPrice * OrderQuantity")
    )

    sales_df = sales_df.withColumn(
        "SalesAmount", expr("ExtendedAmount - DiscountAmount")
    )
    sales_df = sales_df.withColumn("TaxAmount", expr("SalesAmount * 0.10"))

    internet_sales_df = sales_df.filter(sales_df["CustomerKey"].isNotNull())

    sales_reason_keys = (
        dim_sales_reason.select("SalesReasonKey").rdd.flatMap(lambda x: x).collect()
    )

    @udf(IntegerType())
    def get_sales_reason_key():
        return random.choice(sales_reason_keys)

    internet_sales_df = internet_sales_df.withColumn(
        "SalesReasonKey", get_sales_reason_key()
    )

    return {
        "FactInternetSales": internet_sales_df.select(
            "SalesOrderNumber",
            "SalesOrderLineNumber",
            "CustomerKey",
            "ProductKey",
            "OrderDateKey",
            "DueDateKey",
            "ShipDateKey",
            "PromotionKey",
            "CurrencyKey",
            "SalesTerritoryKey",
            "OrderQuantity",
            "UnitPrice",
            "ExtendedAmount",
            "UnitPriceDiscountPct",
            "DiscountAmount",
            "ProductStandardCost",
            "TotalProductCost",
            "SalesAmount",
            "TaxAmount",
            "FreightAmount",
            "CarrierTrackingNumber",
            "CustomerPONumber",
            "RevisionNumber",
        ),
        "FactInternetSalesReason": internet_sales_df.select(
            "SalesOrderNumber", "SalesOrderLineNumber", "SalesReasonKey"
        ),
        "FactResellerSales": sales_df.filter(sales_df["CustomerKey"].isNull()).select(
            "SalesOrderNumber",
            "SalesOrderLineNumber",
            "ResellerKey",
            "ProductKey",
            "OrderDateKey",
            "DueDateKey",
            "ShipDateKey",
            "EmployeeKey",
            "PromotionKey",
            "CurrencyKey",
            "SalesTerritoryKey",
            "OrderQuantity",
            "UnitPrice",
            "ExtendedAmount",
            "UnitPriceDiscountPct",
            "DiscountAmount",
            "ProductStandardCost",
            "TotalProductCost",
            "SalesAmount",
            "TaxAmount",
            "FreightAmount",
            "CarrierTrackingNumber",
            "CustomerPONumber",
            "RevisionNumber",
        ),
    }


def _save_fact_sales_tables(
    workspace_id: UUID,
    lakehouse_id: UUID,
    fact_internet_sales_df: Optional[DataFrame] = None,
    fact_internet_sales_reason_df: Optional[DataFrame] = None,
    fact_reseller_sales_df: Optional[DataFrame] = None,
):
    """
    Saves DataFrames for fact sales tables as Delta tables in the specified lakehouse.

    Parameters
    ----------
    workspace_id : uuid.UUID
        The Fabric workspace ID where the lakehouse is located.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    lakehouse_id : uuid.UUID
        The ID of the lakehouse where the delta tables should be added.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    fact_internet_sales_df: DataFrame, default = None
        The DataFrame for the FactInternetSales table.
    fact_internet_sales_reason_df: DataFrame, default = None
        The DataFrame for the FactInternetSalesReason table.
    fact_reseller_sales_df: DataFrame, default = None
        The DataFrame for the FactResellerSales table.
    """
    if fact_internet_sales_df:
        _save_as_delta_table(
            dataframe=fact_internet_sales_df,
            delta_table_name="FactInternetSales",
            lakehouse=lakehouse_id,
            workspace=workspace_id,
        )

    if fact_internet_sales_reason_df:
        _save_as_delta_table(
            dataframe=fact_internet_sales_reason_df,
            delta_table_name="FactInternetSalesReason",
            lakehouse=lakehouse_id,
            workspace=workspace_id,
        )

    if fact_reseller_sales_df:
        _save_as_delta_table(
            dataframe=fact_reseller_sales_df,
            delta_table_name="FactResellerSales",
            lakehouse=lakehouse_id,
            workspace=workspace_id,
        )


def _generate__fact_product_inventory(
    workspace_id: UUID,
    lakehouse_id: UUID,
):
    """
    Generates a FactProductInventory table based on the order quantities from the FactInternetSales and FactResellerSales tables.

    Parameters
    ----------
    workspace_id : uuid.UUID
        The Fabric workspace ID where the lakehouse is located.
    lakehouse_id : uuid.UUID
        The ID of the lakehouse where the delta tables should be added.
    """
    internet_sales_quantities_df = _read_delta_table(
        path="FactInternetSales",
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    ).select("OrderDateKey", "ProductKey", "UnitPrice", "OrderQuantity")

    reseller_sales_quantities_df = _read_delta_table(
        path="FactResellerSales",
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    ).select("OrderDateKey", "ProductKey", "UnitPrice", "OrderQuantity")

    df = (
        internet_sales_quantities_df.union(reseller_sales_quantities_df)
        .groupBy("OrderDateKey", "ProductKey", "UnitPrice")
        .agg(sum("OrderQuantity").alias("TotalOrderQuantity"))
    )

    # Calculate the maximum TotalOrderQuantity for each ProductKey and join with date-product-quantity_df
    max_quantities_df = df.groupBy("ProductKey").agg(
        (max("TotalOrderQuantity") * 10).alias("InitialQuantityOnHand")
    )

    df = df.join(max_quantities_df, on="ProductKey", how="left").orderBy("OrderDateKey")

    # Calculate QuantityOnHand and UnitsIn
    window_spec = (
        Window.partitionBy("ProductKey")
        .orderBy("OrderDateKey")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    df = df.withColumn(
        "CumulativeOrderQuantity",
        sum("TotalOrderQuantity").over(window_spec) % col("InitialQuantityOnHand"),
    )

    # Calculate the previous CumulativeOrderQuantity
    window_spec = Window.partitionBy("ProductKey").orderBy("OrderDateKey")
    df = df.withColumn(
        "PrevCumulativeOrderQuantity",
        lag("CumulativeOrderQuantity", 1).over(window_spec),
    )

    df = df.withColumn(
        "QuantityOnHand", col("InitialQuantityOnHand") - col("CumulativeOrderQuantity")
    )

    # Add the new column based on the comparison
    df = df.withColumn(
        "UnitsIn",
        when(
            col("PrevCumulativeOrderQuantity") > col("CumulativeOrderQuantity"),
            col("InitialQuantityOnHand"),
        ).otherwise(
            when(
                row_number().over(window_spec) == 1, col("InitialQuantityOnHand")
            ).otherwise(0)
        ),
    )

    df = df.withColumn(
        "MovementDate", to_date(col("OrderDateKey").cast("string"), "yyyyMMdd")
    )

    _save_as_delta_table(
        dataframe=df.select(
            "ProductKey",
            col("OrderDateKey").alias("DateKey"),
            "MovementDate",
            col("UnitPrice").alias("UnitCost"),
            "UnitsIn",
            col("TotalOrderQuantity").alias("UnitsOut"),
            col("QuantityOnHand").alias("UnitsBalance"),
        ),
        delta_table_name="FactProductInventory",
        lakehouse=lakehouse_id,
        workspace=workspace_id,
    )
