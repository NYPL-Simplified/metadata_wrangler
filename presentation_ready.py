from nose.tools import set_trace
from core.monitor import Monitor
from core.model import (
    DataSource,
    Identifier,
    Work,
)
from appeal import AppealCalculator
from gutenberg import OCLCMonitorForGutenberg
from amazon import AmazonCoverageProvider
from oclc import LinkedDataCoverageProvider
from viaf import VIAFClient

class MakePresentationReadyMonitor(Monitor):
    """Make works presentation ready.

    This is an EXTREMELY complicated process, but all the work can be
    delegated to other bits of code.
    """

    def __init__(self, data_directory):
        super(MakePresentationReadyMonitor, self).__init__(
            "Make Works Presentation Ready")
        self.data_directory = data_directory

    def run_once(self, _db, start, cutoff):

        appeal_calculator = AppealCalculator(_db, self.data_directory)

        coverage_providers = dict(
            oclc_gutenberg = OCLCMonitorForGutenberg(_db),
            oclc_linked_data = LinkedDataCoverageProvider(_db),
            amazon = AmazonCoverageProvider(_db),
        )
        unready_works = _db.query(Work).filter(
            Work.presentation_ready==False).filter(
                Work.presentation_ready_exception==None).order_by(
                    Work.last_update_time.desc()).limit(10)
        while unready_works.count():
            for work in unready_works.all():
                self.make_work_ready(_db, work, appeal_calculator, 
                                     coverage_providers)
                # try:
                #     self.make_work_ready(_db, work, appeal_calculator,
                #                          coverage_providers)
                #     work.presentation_ready = True
                # except Exception, e:
                #     work.presentation_ready_exception = str(e)
                _db.commit()

    def make_work_ready(self, _db, work, appeal_calculator, coverage_providers):
        """Either make a work presentation ready, or raise an exception
        explaining why that's not possible.
        """
        did_oclc_lookup = False
        for edition in work.editions:
            # OCLC Lookup on all Gutenberg editions.
            if edition.data_source.name==DataSource.GUTENBERG:
                coverage_providers['oclc_gutenberg'].ensure_coverage(edition)
                did_oclc_lookup = True

        primary_edition = work.primary_edition
        if did_oclc_lookup:
            oclc_ids = primary_edition.equivalent_identifiers(
                type=[Identifier.OCLC_WORK, Identifier.OCLC_NUMBER])
            for o in oclc_ids:
                coverage_providers['oclc_linked_data'].ensure_coverage(o)

        # OCLC Linked Data on all ISBNs. Amazon on all ISBNs + ASINs.
        # equivalent_identifiers = primary_edition.equivalent_identifiers(
        #     type=[Identifier.ASIN, Identifier.ISBN])
        # for identifier in equivalent_identifiers:
        #     coverage_providers['amazon'].ensure_coverage(identifier)
        #     if identifier.type==Identifier.ISBN:
        #         coverage_providers['oclc_linked_data'].ensure_coverage(
        #             identifier)

        # VIAF on all contributors.
        viaf = VIAFClient(_db)
        for edition in work.editions:
            for contributor in primary_edition.contributors:
                viaf.process_contributor(contributor)

        # Calculate appeal. This will obtain Amazon reviews as a side effect.
        appeal_calculator.calculate_for_work(work)

        # Calculate presentation.
        set_trace()
        work.calculate_presentation()

        # All done!
        work.set_presentation_ready()
