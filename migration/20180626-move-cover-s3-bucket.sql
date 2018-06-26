-- Replace all possible ways 'book-covers.nypl.org' might show up in
-- a representation's mirror_url.
update representations set mirror_url=replace(replace(replace(replace(replace(mirror_url, 'https://book-covers.nypl.org', 'https://covers.nypl.org'), 'https://s3.amazonaws.com/book-covers.nypl.org', 'https://covers.nypl.org'), 'http://covers.nypl.org', 'https://covers.nypl.org'), 'http://book-covers.nypl.org', 'https://covers.nypl.org'), 'http://s3.amazonaws.com/book-covers.nypl.org', 'https://covers.nypl.org') where mirror_url like '%covers.nypl.org%';

-- Make sure that any work whose OPDS entry contains the old URL
-- eventually has a new OPDS entry generated.
delete from workcoveragerecords where operation='generate-opds' and work_id in (select id from works where verbose_opds_entry like '%book-covers.nypl.org%');
