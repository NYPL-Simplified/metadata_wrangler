UPDATE licensepools SET data_source_id = (
    SELECT id FROM datasources WHERE name = 'Library Simplified Internal Process'
) WHERE id IS NOT NULL;
