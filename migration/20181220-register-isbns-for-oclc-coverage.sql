-- Any existing OCLC Classify coverage for ISBNs doesn't count -- it
-- needs to be redone. (But there shouldn't be any.)
delete from coveragerecords where id in (
 select cr.id from coveragerecords cr join datasources ds on cr.data_source_id=ds.id join identifiers i on cr.identifier_id=i.id where ds.name='OCLC Classify' and operation is null and i.type='ISBN'
);

-- For every ISBN associated with a collection and not already
-- registered, create a coverage record in the 'registered' state for
-- the OCLC CLassify coverage provider.
insert into coveragerecords (
 identifier_id, data_source_id, timestamp, status
) select 
 distinct(i.id), ds.id, now(), coverage_status('registered')
from collections_identifiers ci
 join identifiers i on ci.identifier_id=i.id and i.type='ISBN'
 join datasources ds on ds.name='OCLC Classify'
 left join coveragerecords cr on i.id=cr.identifier_id and cr.data_source_id=5 and operation is null
 where cr.id is null;
