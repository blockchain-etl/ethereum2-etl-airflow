SELECT IF(
(
    SELECT MAX(epoch)
    FROM `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.beacon_committees`
    WHERE DATE(epoch_timestamp) <= '{{ds}}'
) + 1 =
(
    SELECT COUNT(DISTINCT epoch) FROM `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.beacon_committees`
    WHERE DATE(epoch_timestamp) <= '{{ds}}'
), 1,
CAST((SELECT 'Total number of epochs in beacon committees is not equal to last epoch plus one on {{ds}}') AS INT64))
