drop view measurements_index;
create view measurements_index as
SELECT 
        `block_data_index`.`measurement_id` AS `measurement_id`,
        `block_data_index`.`measurement_name` AS `measurement_name`,
        `block_data_index`.`location` AS `location`,
        `block_data_index`.`location` AS `column_name`,
        0 AS `min_value`,
        0 AS `max_value`,
        NULL AS `annotation`,
        'Blocks Track' AS `chart_type`,
        'range' AS `type`,
        '[]' AS 'metadata'
    FROM
        `block_data_index` 
    UNION ALL SELECT 
        `gene_data_index`.`measurement_id` AS `measurement_id`,
        `gene_data_index`.`measurement_name` AS `measurement_name`,
        `gene_data_index`.`location` AS `location`,
        `gene_data_index`.`column_name` AS `column_name`,
        `gene_data_index`.`min_value` AS `min_value`,
        `gene_data_index`.`max_value` AS `max_value`,
        NULL AS `annotation`,
        'Scatter Plot' AS `chart_type`,
        'range' AS `type`,
        '["probe"]' AS `metadata`
    FROM
        `gene_data_index` 
    UNION ALL SELECT 
        `bp_data_index`.`measurement_id` AS `measurement_id`,
        `bp_data_index`.`measurement_name` AS `measurement_name`,
        `bp_data_index`.`location` AS `location`,
        `bp_data_index`.`column_name` AS `column_name`,
        `bp_data_index`.`min_value` AS `min_value`,
        `bp_data_index`.`max_value` AS `max_value`,
        `bp_data_index`.`annotation` AS `annotation`,
        'Heatmap Plot' AS `chart_type`,
        'feature' AS `type`,
        '[]' AS `metadata`
    FROM
        `bp_data_index`
	UNION ALL SELECT
        `genome_data_index`.`measurement_id` AS `measurement_id`,
        `genome_data_index`.`measurement_name` AS `measurement_name`,
        `genome_data_index`.`location` AS `location`,
        `genome_data_index`.`location` AS `column_name`,
        0 AS `min_value`,
        0 AS `max_value`,
        NULL AS `annotation`,
        'Genes Track' AS `chart_type`,
        'range' AS `type`,
        '["entrez", "exon_starts", "exon_ends", "gene"]' AS `metadata`
	FROM
		`genome_data_index`