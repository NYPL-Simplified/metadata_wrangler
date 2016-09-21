delete from coveragerecords where id in (
    select cr.id from coveragerecords cr
    join datasources d on cr.data_source_id = d.id
    join identifiers i on cr.identifier_id = i.id
    where
        d.name = 'OCLC Classify' and
        i.type in ('Bibliotheca ID', '3M ID') and
        cr.status = 'transient failure'
);    
