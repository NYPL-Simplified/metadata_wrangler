-- Restore all matching books to the 'registered' state so Content
-- Cafe will process them again.
update coveragerecords set status='registered' where identifier_id in (select primary_identifier_id from editions where title='No content currently exists for this item') and data_source_id in (select id from datasources where name='Content Cafe') and operation is null;

-- Remove the records of whatever the Content Cafe coverage provider
-- did the first time.
delete from coveragerecords where identifier_id in (select primary_identifier_id from editions where title='No content currently exists for this item') and data_source_id in (select id from datasources where name='Content Cafe') and operation is not null;

-- Remove the bad data manually. The new 'title' is null, and the
-- metadata layer will not overwrite a non-null value with null.
update editions set title=null where title='No content currently exists for this item';
