SELECT IF(
(
    SELECT MAX(block_slot)
    FROM `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.beacon_blocks`
    WHERE DATE(block_timestamp) <= '{{ds}}'
) + 1 =
(
    SELECT COUNT(*) FROM `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.beacon_blocks`
    WHERE DATE(block_timestamp) <= '{{ds}}'
), 1,
CAST((SELECT 'Total number of beacon blocks is not equal to last block number plus one on {{ds}}') AS INT64))
