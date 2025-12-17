SELECT distinct 
    user_email,
    execution_date,
    QUERY,
    FROM `peya-data-platform-pro.platform_usage.usage_metrics_per_user_daily`  d, unnest(referenced_tables) as rf
  WHERE execution_date >= DATE_ADD( current_date, INTERVAL -6 month)
  AND lower(query) like '%base_data_stacking_analysis%'
  ORDER BY 2