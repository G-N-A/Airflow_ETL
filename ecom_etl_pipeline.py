#!/usr/bin/env python3
"""
ecom_etl_pyspark.py

PySpark ETL pipeline for e-commerce data:
- Extracts data from source SQL Server database
- Performs data cleaning, validation, and transformations
- Loads cleaned data into target database
- Includes data quality checks and error handling

Usage:
    spark-submit ecom_etl_pyspark.py \
        --source-jdbc-url "jdbc:sqlserver://localhost:1433;databaseName=ecom_db" \
        --target-jdbc-url "jdbc:sqlserver://localhost:1433;databaseName=ecom_dwh" \
        --source-user sa --source-password "Gova#ss123" \
        --target-user sa --target-password "Gova#ss123"
"""

import argparse
import logging
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional
from decimal import Decimal

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    abs, col, countDistinct, when, isnan, isnull, trim, upper, lower, regexp_replace,
    to_timestamp, date_format, year, month, dayofmonth, quarter,
    sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    row_number, dense_rank, coalesce, lit, current_timestamp,
    concat_ws, md5, sha2, substring, length, split, array_contains,
    stddev, variance, percentile_approx, collect_list, collect_set,
    explode, first, last, lead, lag, datediff, months_between
)

from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, TimestampType, BooleanType, LongType
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EcommerceETL:
    """Main ETL pipeline class for e-commerce data processing."""
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize the ETL pipeline.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary with connection details
        """
        self.spark = spark
        self.config = config
        self.source_jdbc_url = config['source_jdbc_url']
        self.target_jdbc_url = config['target_jdbc_url']
        self.source_properties = {
            "user": config['source_user'],
            "password": config['source_password'],
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }
        self.target_properties = {
            "user": config['target_user'],
            "password": config['target_password'],
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }
        self.batch_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        self.data_quality_results = {}
        
    def extract_table(self, table_name: str) -> DataFrame:
        """
        Extract data from a source table.
        
        Args:
            table_name: Name of the table to extract
            
        Returns:
            DataFrame containing the extracted data
        """
        try:
            logger.info(f"Extracting data from table: {table_name}")
            df = self.spark.read.jdbc(
                url=self.source_jdbc_url,
                table=table_name,
                properties=self.source_properties
            )
            record_count = df.count()
            logger.info(f"Extracted {record_count} records from {table_name}")
            return df
        except Exception as e:
            logger.error(f"Failed to extract data from {table_name}: {str(e)}")
            raise
    
    def clean_customers(self, df: DataFrame) -> DataFrame:
        """
        Clean and transform customer data.
        
        Args:
            df: Raw customer DataFrame
            
        Returns:
            Cleaned customer DataFrame
        """
        logger.info("Cleaning customer data...")
        
        # Initial record count
        initial_count = df.count()
        
        # Remove duplicates based on email
        df = df.dropDuplicates(['email'])
        
        # Clean and standardize fields
        df_cleaned = df.select(
            col('customer_id'),
            trim(upper(col('first_name'))).alias('first_name'),
            trim(upper(col('last_name'))).alias('last_name'),
            trim(lower(col('email'))).alias('email'),
            # Clean phone numbers - remove special characters
            regexp_replace(col('phone'), '[^0-9+]', '').alias('phone'),
            trim(col('country')).alias('country'),
            trim(col('state')).alias('state'),
            trim(col('city')).alias('city'),
            # Standardize postal codes
            regexp_replace(trim(col('postal_code')), '[^A-Z0-9]', '').alias('postal_code'),
            trim(col('street')).alias('street'),
            col('signup_date'),
            col('last_login'),
            trim(upper(col('segment'))).alias('segment'),
            # Ensure lifetime_value is not negative
            when(col('lifetime_value') < 0, 0).otherwise(col('lifetime_value')).alias('lifetime_value')
        )
        
        # Validate email format
        df_cleaned = df_cleaned.filter(
            col('email').rlike('^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$')
        )
        
        # Fill missing segments with 'UNKNOWN'
        df_cleaned = df_cleaned.fillna({'segment': 'UNKNOWN'})
        
        # Add derived columns
        df_cleaned = df_cleaned.withColumn(
            'customer_since_days',
            datediff(current_timestamp(), col('signup_date'))
        ).withColumn(
            'is_active',
            when(datediff(current_timestamp(), col('last_login')) <= 90, True).otherwise(False)
        ).withColumn(
            'customer_tier',
            when(col('lifetime_value') >= 10000, 'PLATINUM')
            .when(col('lifetime_value') >= 5000, 'GOLD')
            .when(col('lifetime_value') >= 1000, 'SILVER')
            .otherwise('BRONZE')
        ).withColumn(
            'email_domain',
            split(col('email'), '@').getItem(1)
        )
        
        # Add data quality flag
        df_cleaned = df_cleaned.withColumn(
            'data_quality_flag',
            when(
                col('phone').isNull() | 
                col('country').isNull() | 
                col('city').isNull(), 
                'INCOMPLETE'
            ).otherwise('COMPLETE')
        )
        
        # Add ETL metadata
        df_cleaned = df_cleaned.withColumn('etl_batch_id', lit(self.batch_id)) \
                               .withColumn('etl_timestamp', current_timestamp())
        
        final_count = df_cleaned.count()
        logger.info(f"Customer cleaning complete. Records: {initial_count} -> {final_count}")
        
        # Store data quality metrics
        self.data_quality_results['customers'] = {
            'initial_count': initial_count,
            'final_count': final_count,
            'duplicates_removed': initial_count - final_count
        }
        
        return df_cleaned
    
    def clean_products(self, df: DataFrame) -> DataFrame:
        """
        Clean and transform product data.
        
        Args:
            df: Raw product DataFrame
            
        Returns:
            Cleaned product DataFrame
        """
        logger.info("Cleaning product data...")
        
        initial_count = df.count()
        
        # Remove duplicates based on SKU
        df = df.dropDuplicates(['sku'])
        
        # Clean and transform
        df_cleaned = df.select(
            col('product_id'),
            trim(upper(col('sku'))).alias('sku'),
            trim(col('product_name')).alias('product_name'),
            trim(upper(col('category'))).alias('category'),
            trim(col('brand')).alias('brand'),
            trim(col('description')).alias('description'),
            # Ensure prices are positive
            when(col('price') <= 0, None).otherwise(col('price')).alias('price'),
            when(col('rrp') <= 0, None).otherwise(col('rrp')).alias('rrp'),
            col('created_at'),
            when(col('popularity') < 0, 0).otherwise(col('popularity')).alias('popularity')
        )
        
        # Filter out products with no price
        df_cleaned = df_cleaned.filter(col('price').isNotNull())
        
        # Calculate discount percentage
        df_cleaned = df_cleaned.withColumn(
            'discount_percentage',
            when(col('rrp').isNotNull() & (col('rrp') > 0),
                 ((col('rrp') - col('price')) / col('rrp') * 100))
            .otherwise(0)
        )
        
        # Add price tier
        df_cleaned = df_cleaned.withColumn(
            'price_tier',
            when(col('price') >= 1000, 'PREMIUM')
            .when(col('price') >= 100, 'MID_RANGE')
            .otherwise('BUDGET')
        )
        
        # Standardize missing brands
        df_cleaned = df_cleaned.fillna({'brand': 'GENERIC'})
        
        # Add product age in days
        df_cleaned = df_cleaned.withColumn(
            'product_age_days',
            datediff(current_timestamp(), col('created_at'))
        )
        
        # Add ETL metadata
        df_cleaned = df_cleaned.withColumn('etl_batch_id', lit(self.batch_id)) \
                               .withColumn('etl_timestamp', current_timestamp())
        
        final_count = df_cleaned.count()
        logger.info(f"Product cleaning complete. Records: {initial_count} -> {final_count}")
        
        self.data_quality_results['products'] = {
            'initial_count': initial_count,
            'final_count': final_count,
            'invalid_prices_removed': initial_count - final_count
        }
        
        return df_cleaned
    
    def clean_orders(self, df: DataFrame) -> DataFrame:
        """
        Clean and transform order data.
        
        Args:
            df: Raw order DataFrame
            
        Returns:
            Cleaned order DataFrame
        """
        logger.info("Cleaning order data...")
        
        initial_count = df.count()
        
        # Remove duplicates based on order_uuid
        df = df.dropDuplicates(['order_uuid'])
        
        # Clean and validate
        df_cleaned = df.select(
            col('order_id'),
            col('order_uuid'),
            col('customer_id'),
            col('order_date'),
            trim(upper(col('status'))).alias('status'),
            trim(col('payment_method')).alias('payment_method'),
            # Ensure amounts are non-negative
            when(col('subtotal') < 0, 0).otherwise(col('subtotal')).alias('subtotal'),
            when(col('shipping') < 0, 0).otherwise(col('shipping')).alias('shipping'),
            when(col('tax') < 0, 0).otherwise(col('tax')).alias('tax'),
            when(col('discount') < 0, 0).otherwise(col('discount')).alias('discount'),
            when(col('total_amount') < 0, 0).otherwise(col('total_amount')).alias('total_amount'),
            trim(lower(col('placed_via'))).alias('placed_via')
        )
        
        # Validate total amount calculation
        df_cleaned = df_cleaned.withColumn(
            'calculated_total',
            col('subtotal') + col('shipping') + col('tax') - col('discount')
        ).withColumn(
            'amount_mismatch_flag',
            when(abs(col('total_amount') - col('calculated_total')) > 0.01, True).otherwise(False)
        )
        
        # Add time-based features
        df_cleaned = df_cleaned.withColumn('order_year', year(col('order_date'))) \
                               .withColumn('order_month', month(col('order_date'))) \
                               .withColumn('order_day', dayofmonth(col('order_date'))) \
                               .withColumn('order_quarter', quarter(col('order_date'))) \
                               .withColumn('order_day_of_week', date_format(col('order_date'), 'EEEE'))
        
        # Add order value category
        df_cleaned = df_cleaned.withColumn(
            'order_value_category',
            when(col('total_amount') >= 500, 'HIGH')
            .when(col('total_amount') >= 100, 'MEDIUM')
            .otherwise('LOW')
        )
        
        # Add days since order
        df_cleaned = df_cleaned.withColumn(
            'days_since_order',
            datediff(current_timestamp(), col('order_date'))
        )
        
        # Standardize status values
        df_cleaned = df_cleaned.withColumn(
            'status',
            when(col('status').isin(['PENDING', 'PROCESSING']), 'IN_PROGRESS')
            .when(col('status').isin(['SHIPPED', 'IN_TRANSIT']), 'SHIPPED')
            .when(col('status') == 'DELIVERED', 'COMPLETED')
            .when(col('status').isin(['CANCELLED', 'RETURNED', 'REFUNDED']), 'CANCELLED')
            .otherwise('UNKNOWN')
        )
        
        # Add ETL metadata
        df_cleaned = df_cleaned.withColumn('etl_batch_id', lit(self.batch_id)) \
                               .withColumn('etl_timestamp', current_timestamp())
        
        final_count = df_cleaned.count()
        logger.info(f"Order cleaning complete. Records: {initial_count} -> {final_count}")
        
        self.data_quality_results['orders'] = {
            'initial_count': initial_count,
            'final_count': final_count,
            'amount_mismatches': df_cleaned.filter(col('amount_mismatch_flag') == True).count()
        }
        
        return df_cleaned
    
    def clean_order_items(self, df: DataFrame) -> DataFrame:
        """
        Clean and transform order items data.
        
        Args:
            df: Raw order items DataFrame
            
        Returns:
            Cleaned order items DataFrame
        """
        logger.info("Cleaning order items data...")
        
        initial_count = df.count()
        
        # Remove duplicates
        df = df.dropDuplicates(['order_item_id'])
        
        # Clean and validate
        df_cleaned = df.select(
            col('order_item_id'),
            col('order_id'),
            col('product_id'),
            trim(upper(col('sku'))).alias('sku'),
            # Ensure positive quantities
            when(col('quantity') <= 0, 1).otherwise(col('quantity')).alias('quantity'),
            # Ensure positive prices
            when(col('unit_price') < 0, 0).otherwise(col('unit_price')).alias('unit_price'),
            when(col('line_total') < 0, 0).otherwise(col('line_total')).alias('line_total')
        )
        
        # Validate line total calculation
        df_cleaned = df_cleaned.withColumn(
            'calculated_line_total',
            col('quantity') * col('unit_price')
        ).withColumn(
            'line_total_mismatch',
            when(abs(col('line_total') - col('calculated_line_total')) > 0.01, True).otherwise(False)
        )
        
        # Add quantity categories
        df_cleaned = df_cleaned.withColumn(
            'quantity_category',
            when(col('quantity') >= 10, 'BULK')
            .when(col('quantity') >= 3, 'MULTIPLE')
            .otherwise('SINGLE')
        )
        
        # Add ETL metadata
        df_cleaned = df_cleaned.withColumn('etl_batch_id', lit(self.batch_id)) \
                               .withColumn('etl_timestamp', current_timestamp())
        
        final_count = df_cleaned.count()
        logger.info(f"Order items cleaning complete. Records: {initial_count} -> {final_count}")
        
        self.data_quality_results['order_items'] = {
            'initial_count': initial_count,
            'final_count': final_count,
            'line_total_mismatches': df_cleaned.filter(col('line_total_mismatch') == True).count()
        }
        
        return df_cleaned
    
    def clean_inventory(self, df: DataFrame) -> DataFrame:
        """
        Clean and transform inventory data.
        
        Args:
            df: Raw inventory DataFrame
            
        Returns:
            Cleaned inventory DataFrame
        """
        logger.info("Cleaning inventory data...")
        
        initial_count = df.count()
        
        # Remove duplicates based on product_id
        df = df.dropDuplicates(['product_id'])
        
        # Clean and validate
        df_cleaned = df.select(
            col('inventory_id'),
            col('product_id'),
            # Ensure non-negative stock levels
            when(col('stock_level') < 0, 0).otherwise(col('stock_level')).alias('stock_level'),
            when(col('reorder_threshold') < 0, 0).otherwise(col('reorder_threshold')).alias('reorder_threshold'),
            col('last_restocked')
        )
        
        # Add stock status
        df_cleaned = df_cleaned.withColumn(
            'stock_status',
            when(col('stock_level') == 0, 'OUT_OF_STOCK')
            .when(col('stock_level') <= col('reorder_threshold'), 'LOW_STOCK')
            .when(col('stock_level') > col('reorder_threshold') * 5, 'OVERSTOCK')
            .otherwise('NORMAL')
        )
        
        # Add days since last restock
        df_cleaned = df_cleaned.withColumn(
            'days_since_restock',
            when(col('last_restocked').isNotNull(),
                 datediff(current_timestamp(), col('last_restocked')))
            .otherwise(None)
        )
        
        # Add restock urgency
        df_cleaned = df_cleaned.withColumn(
            'restock_urgency',
            when(col('stock_status') == 'OUT_OF_STOCK', 'CRITICAL')
            .when(col('stock_status') == 'LOW_STOCK', 'HIGH')
            .when(col('days_since_restock') > 90, 'MEDIUM')
            .otherwise('LOW')
        )
        
        # Add ETL metadata
        df_cleaned = df_cleaned.withColumn('etl_batch_id', lit(self.batch_id)) \
                               .withColumn('etl_timestamp', current_timestamp())
        
        final_count = df_cleaned.count()
        logger.info(f"Inventory cleaning complete. Records: {initial_count} -> {final_count}")
        
        self.data_quality_results['inventory'] = {
            'initial_count': initial_count,
            'final_count': final_count,
            'out_of_stock_count': df_cleaned.filter(col('stock_status') == 'OUT_OF_STOCK').count()
        }
        
        return df_cleaned
    
    def clean_clickstream(self, df: DataFrame) -> DataFrame:
        """
        Clean and transform clickstream data.
        
        Args:
            df: Raw clickstream DataFrame
            
        Returns:
            Cleaned clickstream DataFrame
        """
        logger.info("Cleaning clickstream data...")
        
        initial_count = df.count()
        
        # Remove exact duplicates
        df = df.dropDuplicates()
        
        # Clean and validate
        df_cleaned = df.select(
            col('click_id'),
            col('customer_id'),
            col('session_id'),
            trim(col('page')).alias('page'),
            trim(lower(col('page_url'))).alias('page_url'),
            col('product_id'),
            col('timestamp'),
            trim(upper(col('device'))).alias('device'),
            col('user_agent'),
            trim(lower(col('referrer'))).alias('referrer'),
            trim(lower(col('utm_source'))).alias('utm_source'),
            # Validate IP address format
            when(col('ip_address').rlike('^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$'), 
                 col('ip_address')).otherwise(None).alias('ip_address')
        )
        
        # Filter out invalid timestamps
        df_cleaned = df_cleaned.filter(col('timestamp').isNotNull())
        
        # Add session duration and page sequence
        window_spec = Window.partitionBy('session_id').orderBy('timestamp')
        df_cleaned = df_cleaned.withColumn(
            'page_sequence',
            row_number().over(window_spec)
        ).withColumn(
            'time_on_page',
            (lead('timestamp').over(window_spec).cast('long') - col('timestamp').cast('long'))
        )
        
        # Add time-based features
        df_cleaned = df_cleaned.withColumn('click_hour', date_format(col('timestamp'), 'HH')) \
                               .withColumn('click_day_of_week', date_format(col('timestamp'), 'EEEE')) \
                               .withColumn('is_weekend', 
                                         when(date_format(col('timestamp'), 'EEEE').isin(['Saturday', 'Sunday']), True)
                                         .otherwise(False))
        
        # Categorize referrer sources
        df_cleaned = df_cleaned.withColumn(
            'referrer_category',
            when(col('referrer').contains('google'), 'SEARCH_ENGINE')
            .when(col('referrer').contains('facebook') | col('referrer').contains('instagram'), 'SOCIAL_MEDIA')
            .when(col('referrer').contains('email'), 'EMAIL')
            .when(col('referrer').isNull() | (col('referrer') == ''), 'DIRECT')
            .otherwise('OTHER')
        )
        
        # Add bot detection flag (simple heuristic)
        df_cleaned = df_cleaned.withColumn(
            'potential_bot',
            when(col('user_agent').contains('bot') | 
                 col('user_agent').contains('crawler') |
                 col('user_agent').contains('spider'), True)
            .otherwise(False)
        )
        
        # Add ETL metadata
        df_cleaned = df_cleaned.withColumn('etl_batch_id', lit(self.batch_id)) \
                               .withColumn('etl_timestamp', current_timestamp())
        
        final_count = df_cleaned.count()
        logger.info(f"Clickstream cleaning complete. Records: {initial_count} -> {final_count}")
        
        self.data_quality_results['clickstream'] = {
            'initial_count': initial_count,
            'final_count': final_count,
            'potential_bots': df_cleaned.filter(col('potential_bot') == True).count()
        }
        
        return df_cleaned
    
    def create_customer_360(self, customers_df: DataFrame, orders_df: DataFrame, 
                           clickstream_df: DataFrame) -> DataFrame:
        """
        Create a comprehensive customer 360 view.
        
        Args:
            customers_df: Cleaned customers DataFrame
            orders_df: Cleaned orders DataFrame
            clickstream_df: Cleaned clickstream DataFrame
            
        Returns:
            Customer 360 DataFrame
        """
        logger.info("Creating Customer 360 view...")
        
        # Aggregate order metrics
        order_metrics = orders_df.groupBy('customer_id').agg(
            count('order_id').alias('total_orders'),
            spark_sum('total_amount').alias('total_spent'),
            avg('total_amount').alias('avg_order_value'),
            spark_max('order_date').alias('last_order_date'),
            spark_min('order_date').alias('first_order_date'),
            collect_set('payment_method').alias('payment_methods_used'),
            collect_set('placed_via').alias('channels_used')
        )
        
        # Calculate order frequency
        order_metrics = order_metrics.withColumn(
            'days_between_first_last_order',
            datediff(col('last_order_date'), col('first_order_date'))
        ).withColumn(
            'order_frequency',
            when(col('days_between_first_last_order') > 0,
                 col('total_orders') / col('days_between_first_last_order'))
            .otherwise(0)
        )
        
        # Aggregate clickstream metrics
        click_metrics = clickstream_df.filter(col('customer_id').isNotNull()).groupBy('customer_id').agg(
            count('click_id').alias('total_clicks'),
            countDistinct('session_id').alias('total_sessions'),
            avg('time_on_page').alias('avg_time_on_page'),
            collect_set('device').alias('devices_used'),
            spark_sum(when(col('page') == 'Product', 1).otherwise(0)).alias('product_views'),
            spark_sum(when(col('page') == 'Cart', 1).otherwise(0)).alias('cart_views')
        )
        
        # Join all metrics with customer data
        customer_360 = customers_df.join(order_metrics, on='customer_id', how='left') \
                                   .join(click_metrics, on='customer_id', how='left')
        
        # Fill nulls for customers with no orders or clicks
        customer_360 = customer_360.fillna({
            'total_orders': 0,
            'total_spent': 0,
            'avg_order_value': 0,
            'order_frequency': 0,
            'total_clicks': 0,
            'total_sessions': 0,
            'product_views': 0,
            'cart_views': 0
        })
        
        # Calculate derived metrics
        customer_360 = customer_360.withColumn(
            'days_since_last_order',
            when(col('last_order_date').isNotNull(),
                 datediff(current_timestamp(), col('last_order_date')))
            .otherwise(None)
        ).withColumn(
            'customer_value_score',
            (col('total_spent') * 0.4 + 
             col('total_orders') * 50 * 0.3 +
             col('order_frequency') * 1000 * 0.3)
        ).withColumn(
            'engagement_score',
            when(col('total_sessions') > 0,
                 (col('total_clicks') / col('total_sessions')) * 10)
            .otherwise(0)
        ).withColumn(
            'conversion_rate',
            when(col('total_sessions') > 0,
                 (col('total_orders') / col('total_sessions')) * 100)
            .otherwise(0)
        )
        
        # Segment customers based on RFM-like analysis
        customer_360 = customer_360.withColumn(
            'customer_segment',
            when((col('days_since_last_order') <= 30) & (col('total_orders') >= 5) & (col('total_spent') >= 1000), 'CHAMPION')
            .when((col('days_since_last_order') <= 60) & (col('total_orders') >= 3), 'LOYAL')
            .when((col('days_since_last_order') <= 90) & (col('total_orders') >= 1), 'POTENTIAL_LOYALIST')
            .when((col('days_since_last_order') > 180) & (col('total_orders') >= 3), 'AT_RISK')
            .when((col('days_since_last_order') > 365) | col('days_since_last_order').isNull(), 'LOST')
            .otherwise('NEW')
        )
        
        # Add ETL metadata
        customer_360 = customer_360.withColumn('etl_batch_id', lit(self.batch_id)) \
                                   .withColumn('etl_timestamp', current_timestamp())
        
        logger.info(f"Customer 360 view created with {customer_360.count()} records")
        
        return customer_360
    
    def create_product_analytics(self, products_df: DataFrame, order_items_df: DataFrame,
                                inventory_df: DataFrame, clickstream_df: DataFrame) -> DataFrame:
        """
        Create product analytics view.
        
        Args:
            products_df: Cleaned products DataFrame
            order_items_df: Cleaned order items DataFrame
            inventory_df: Cleaned inventory DataFrame
            clickstream_df: Cleaned clickstream DataFrame
            
        Returns:
            Product analytics DataFrame
        """
        logger.info("Creating Product Analytics view...")
        
        # Sales metrics
        sales_metrics = order_items_df.groupBy('product_id').agg(
            spark_sum('quantity').alias('total_quantity_sold'),
            spark_sum('line_total').alias('total_revenue'),
            count('order_item_id').alias('times_ordered'),
            avg('unit_price').alias('avg_selling_price'),
            stddev('unit_price').alias('price_variance')
        )
        
        # Click metrics
        click_metrics = clickstream_df.filter(col('product_id').isNotNull()).groupBy('product_id').agg(
            count('click_id').alias('total_views'),
            countDistinct('session_id').alias('unique_sessions_viewed'),
            countDistinct('customer_id').alias('unique_viewers')
        )
        
        # Join all metrics
        product_analytics = products_df.join(sales_metrics, on='product_id', how='left') \
                                       .join(inventory_df.select('product_id', 'stock_level', 'stock_status'), 
                                             on='product_id', how='left') \
                                       .join(click_metrics, on='product_id', how='left')
        
        # Fill nulls
        product_analytics = product_analytics.fillna({
            'total_quantity_sold': 0,
            'total_revenue': 0,
            'times_ordered': 0,
            'total_views': 0,
            'unique_sessions_viewed': 0,
            'unique_viewers': 0
        })
        
        # Calculate conversion rate and other metrics
        product_analytics = product_analytics.withColumn(
            'conversion_rate',
            when(col('total_views') > 0,
                 (col('times_ordered') / col('total_views')) * 100)
            .otherwise(0)
        ).withColumn(
            'inventory_turnover',
            when(col('stock_level') > 0,
                 col('total_quantity_sold') / col('stock_level'))
            .otherwise(None)
        ).withColumn(
            'revenue_per_view',
            when(col('total_views') > 0,
                 col('total_revenue') / col('total_views'))
            .otherwise(0)
        ).withColumn(
            'performance_score',
            (col('total_revenue') * 0.4 +
             col('conversion_rate') * 10 * 0.3 +
             col('total_views') * 0.3)
        )
        
        # Categorize product performance
        product_analytics = product_analytics.withColumn(
            'performance_category',
            when(col('performance_score') >= percentile_approx(col('performance_score'), 0.8).over(Window.partitionBy()), 'TOP_PERFORMER')
            .when(col('performance_score') >= percentile_approx(col('performance_score'), 0.5).over(Window.partitionBy()), 'GOOD_PERFORMER')
            .when(col('performance_score') >= percentile_approx(col('performance_score'), 0.2).over(Window.partitionBy()), 'AVERAGE_PERFORMER')
            .otherwise('POOR_PERFORMER')
        )
        
        # Add ETL metadata
        product_analytics = product_analytics.withColumn('etl_batch_id', lit(self.batch_id)) \
                                             .withColumn('etl_timestamp', current_timestamp())
        
        logger.info(f"Product Analytics view created with {product_analytics.count()} records")
        
        return product_analytics
    
    def load_to_target(self, df: DataFrame, table_name: str, mode: str = 'overwrite'):
        """
        Load DataFrame to target database.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            mode: Write mode ('overwrite' or 'append')
        """
        try:
            logger.info(f"Loading {df.count()} records to {table_name} in {mode} mode...")
            
            df.write.jdbc(
                url=self.target_jdbc_url,
                table=table_name,
                mode=mode,
                properties=self.target_properties
            )
            
            logger.info(f"Successfully loaded data to {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to load data to {table_name}: {str(e)}")
            raise
    
    def generate_data_quality_report(self) -> DataFrame:
        """
        Generate a data quality report.
        
        Returns:
            DataFrame containing data quality metrics
        """
        logger.info("Generating data quality report...")
        
        report_data = []
        for table, metrics in self.data_quality_results.items():
            for metric_name, metric_value in metrics.items():
                report_data.append({
                    'batch_id': self.batch_id,
                    'table_name': table,
                    'metric_name': metric_name,
                    'metric_value': Decimal(metric_value),
                    'timestamp': datetime.now(timezone.utc)
                })
        
        schema = StructType([
            StructField('batch_id', StringType(), False),
            StructField('table_name', StringType(), False),
            StructField('metric_name', StringType(), False),
            StructField('metric_value', DecimalType(20, 2), False),
            StructField('timestamp', TimestampType(), False)
        ])
        
        report_df = self.spark.createDataFrame(report_data, schema)
        
        return report_df
    
    def run_full_pipeline(self):
        """
        Run the complete ETL pipeline.
        """
        try:
            logger.info(f"Starting ETL pipeline - Batch ID: {self.batch_id}")
            
            # Extract raw data
            customers_raw = self.extract_table('customers')
            products_raw = self.extract_table('products')
            orders_raw = self.extract_table('orders')
            order_items_raw = self.extract_table('order_items')
            inventory_raw = self.extract_table('inventory')
            clickstream_raw = self.extract_table('clickstream')
            
            # Clean and transform data
            customers_clean = self.clean_customers(customers_raw)
            products_clean = self.clean_products(products_raw)
            orders_clean = self.clean_orders(orders_raw)
            order_items_clean = self.clean_order_items(order_items_raw)
            inventory_clean = self.clean_inventory(inventory_raw)
            clickstream_clean = self.clean_clickstream(clickstream_raw)
            
            # Create analytical views
            customer_360 = self.create_customer_360(customers_clean, orders_clean, clickstream_clean)
            product_analytics = self.create_product_analytics(products_clean, order_items_clean, 
                                                             inventory_clean, clickstream_clean)
            
            # Generate data quality report
            dq_report = self.generate_data_quality_report()
            
            # Load to target database
            self.load_to_target(customers_clean, 'dim_customers', 'overwrite')
            self.load_to_target(products_clean, 'dim_products', 'overwrite')
            self.load_to_target(orders_clean, 'fact_orders', 'overwrite')
            self.load_to_target(order_items_clean, 'fact_order_items', 'overwrite')
            self.load_to_target(inventory_clean, 'dim_inventory', 'overwrite')
            self.load_to_target(clickstream_clean, 'fact_clickstream', 'overwrite')
            self.load_to_target(customer_360, 'analytics_customer_360', 'overwrite')
            self.load_to_target(product_analytics, 'analytics_product_performance', 'overwrite')
            self.load_to_target(dq_report, 'etl_data_quality_log', 'append')
            
            logger.info(f"ETL pipeline completed successfully - Batch ID: {self.batch_id}")
            
            # Print summary
            print("\n" + "="*60)
            print(f"ETL Pipeline Summary - Batch ID: {self.batch_id}")
            print("="*60)
            for table, metrics in self.data_quality_results.items():
                print(f"\n{table.upper()}:")
                for metric, value in metrics.items():
                    print(f"  {metric}: {value}")
            print("="*60 + "\n")
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {str(e)}")
            raise


def main():
    """Main entry point for the ETL pipeline."""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='E-commerce ETL Pipeline')
    parser.add_argument('--source-jdbc-url', required=True, help='Source database JDBC URL')
    parser.add_argument('--target-jdbc-url', required=True, help='Target database JDBC URL')
    parser.add_argument('--source-user', required=True, help='Source database username')
    parser.add_argument('--source-password', required=True, help='Source database password')
    parser.add_argument('--target-user', required=True, help='Target database username')
    parser.add_argument('--target-password', required=True, help='Target database password')
    parser.add_argument('--app-name', default='EcommerceETL', help='Spark application name')
    parser.add_argument('--master', default='local[*]', help='Spark master URL')
    
    args = parser.parse_args()
    
    # Configuration
    config = {
        'source_jdbc_url': args.source_jdbc_url,
        'target_jdbc_url': args.target_jdbc_url,
        'source_user': args.source_user,
        'source_password': args.source_password,
        'target_user': args.target_user,
        'target_password': args.target_password
    }
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName(args.app_name) \
        .master(args.master) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.sql.statistics.histogram.enabled", "true") \
        .config("spark.sql.cbo.enabled", "true") \
        .config("spark.sql.cbo.joinReorder.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Run ETL pipeline
        etl = EcommerceETL(spark, config)
        etl.run_full_pipeline()
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()