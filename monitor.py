from model import Timestamp

class Monitor(object):

    def __init__(self, name, interval_seconds=1*60):
        
        self.interval_seconds = interval_seconds
        self.stop_running = False

    def run(self, _db):
        
        timestamp, new = get_one_or_create(
            _db, Timestamp,
            service=self.service_name,
            type="monitor")
        start = timestamp.timestamp or datetime.datetime.utcnow() - 60

        while True:
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
            if self.stop_running:
                break

    def run_once(self):
        raise NotImplementedError()

    def cleanup(self):
        pass
