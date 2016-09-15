delete from coveragerecords
    where operation = 'resolve-identifier' and
    identifier_id in (
        select id from identifiers where type in ('3M ID', 'Bibliotheca ID')
    );
