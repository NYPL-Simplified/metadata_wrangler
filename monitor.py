import datetime
import time

from model import (
    get_one_or_create,
    Timestamp,
)

class Monitor(object):

    def __init__(self, name, interval_seconds=1*60, default_start_time=None):
        self.service_name = name
        self.interval_seconds = interval_seconds
        self.stop_running = False
        if not default_start_time:
             default_start_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=60)
        self.default_start_time = default_start_time

    def run(self, _db):
        
        timestamp, new = get_one_or_create(
            _db, Timestamp,
            service=self.service_name,
            type="monitor",
            create_method_kwargs=dict(
                timestamp=self.default_start_time
            )
        )
        start = timestamp.timestamp

        while not self.stop_running:
            cutoff = datetime.datetime.utcnow()
            self.run_once(start, cutoff)
            duration = datetime.datetime.utcnow() - cutoff
            to_sleep = self.interval_seconds-duration.seconds-1
            self.cleanup()
            timestamp.timestamp = cutoff
            _db.commit()
            if to_sleep > 0:
                time.sleep(to_sleep)
                start = cutoff

    def run_once(self):
        raise NotImplementedError()

    def cleanup(self):
        pass
