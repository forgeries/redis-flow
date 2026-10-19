class BaseProcess(object):
    """Define task handlers and lifecycle hooks for a worker."""

    def start_jobs(self):
        """Return an iterable of initial Task or Queue objects, or None."""

        pass

    def process(self, job):
        """Handle tasks without a callback. Overrides receive meta as keyword arguments."""

        pass

    def err_job(self, job, e):
        """Attach the exception message and return the task for the error queue.

        Return None in an override to leave the source task available for retry.
        """
        job.meta.update({"err_msg": str(e)})
        return job

    def start_callback(self):
        """Run once when the scheduler starts its workers."""

        pass

    def end_callback(self):
        """Run when the scheduler becomes idle or completes shutdown."""

        pass

    @property
    def name(self):
        """Return the process name used to dispatch persisted tasks."""
        return self.__class__.__name__

    def close(self):
        """Release process resources when the scheduler shuts down."""
        pass
