import datetime

class Finish():
    """Estimate when a long running job will finish."""

    def __init__(self, total_count,
                 start_count = 0,
                 start_time = None ):
        self.start_count = start_count
        if start_time:
            self.start_time = start_time
        else:
            self.start_time = datetime.datetime.now()
        self.total_count = total_count 

    def __str__(self):
        return ('{}; {} to {}'.format(self.start_time.isoformat(),
                                     self.start_count, self.total_count))

    def est_complete(self, current_count):
        """Estimate date/time of completion. Use '.isoformat' on result."""
        elapsed = datetime.datetime.now() - self.start_time
        perc_complete = (current_count-self.start_count)/self.total_count*1.0
        if perc_complete == 0:
            perc_complete += .0001
        done = ((self.start_time + elapsed / perc_complete)
                    .isoformat(timespec='minutes'))
        return ('{}/{}: {}'
                .format(current_count-self.start_count,
                        self.total_count,
                        done))


