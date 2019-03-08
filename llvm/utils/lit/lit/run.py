import multiprocessing
import time

import lit.Test
import lit.util
import lit.worker


# No-operation semaphore for supporting `None` for parallelism_groups.
#   lit_config.parallelism_groups['my_group'] = None
class NopSemaphore(object):
    def acquire(self): pass
    def release(self): pass


def create_run(tests, lit_config, workers, progress_callback, max_failures,
               timeout):
    assert workers > 0
    if workers == 1:
        run = SerialRun()
    else:
        run = ParallelRun()
        run.workers = workers

    run.tests = tests
    run.lit_config = lit_config
    run.progress_callback = progress_callback
    run.max_failures = max_failures
    run.timeout = timeout
    return run


class MaxFailuresError(Exception):
    pass
class TimeoutError(Exception):
    pass


class Run(object):
    """A concrete, configured testing run."""

    def execute(self):
        """
        Execute the tests in the run using up to the specified number of
        parallel tasks, and inform the caller of each individual result. The
        provided tests should be a subset of the tests available in this run
        object.

        The progress_callback will be invoked for each completed test.

        If timeout is non-None, it should be a time in seconds after which to
        stop executing tests.

        Returns the elapsed testing time.

        Upon completion, each test in the run will have its result
        computed. Tests which were not actually executed (for any reason) will
        be marked SKIPPED.
        """
        self.failure_count = 0
        self.hit_max_failures = False

        # TODO(yln): this code below never runs because timeout is always set in cl_arguments.py
        # move this code there instead of positive infinity
        # Larger timeouts (one year, positive infinity) don't work on Windows.
        one_week = 7 * 24 * 60 * 60  # days * hours * minutes * seconds
        timeout = self.timeout or one_week

        deadline = time.time() + timeout
        try:
            self._execute(deadline)
        finally:
            for test in self.tests:
                if not test.result:
                    test.setResult(lit.Test.Result(lit.Test.SKIPPED))

    # TODO(yln): as the comment says.. this is racing with the main thread waiting
    # for results
    def _process_result(self, test, result):
        # Don't add any more test results after we've hit the maximum failure
        # count.  Otherwise we're racing with the main thread, which is going
        # to terminate the process pool soon.
        # TODO(yln): hit_max_failures can be removed
        # TODO(yln): raise MaxFailuresError here
        if self.hit_max_failures:
            return

        test.setResult(result)

        # Use test.isFailure() for correct XFAIL and XPASS handling
        if test.isFailure():
            self.failure_count += 1
            if self.failure_count == self.max_failures:
                self.hit_max_failures = True

        self.progress_callback(test)


class SerialRun(Run):
    def _execute(self, deadline):
        for test in self.tests:
            result = lit.worker._execute(test, self.lit_config)
            self._process_result(test, result)
            if self.hit_max_failures:
                raise MaxFailuresError()
            if time.time() > deadline:
                raise TimeoutError()


class ParallelRun(Run):
    def _execute(self, deadline):
        semaphores = {
            k: NopSemaphore() if v is None else
            multiprocessing.BoundedSemaphore(v) for k, v in
            self.lit_config.parallelism_groups.items()}

        self._increase_process_limit()

        # Start a process pool. Copy over the data shared between all test runs.
        # FIXME: Find a way to capture the worker process stderr. If the user
        # interrupts the workers before we make it into our task callback, they
        # will each raise a KeyboardInterrupt exception and print to stderr at
        # the same time.
        pool = multiprocessing.Pool(self.workers, lit.worker.initialize,
                                    (self.lit_config, semaphores))

        self._install_win32_signal_handler(pool)

        async_results = [
            pool.apply_async(lit.worker.execute, args=[test],
                callback=lambda r, t=test: self._process_result(t, r))
            for test in self.tests]
        pool.close()

        try:
            self._process_results(async_results, deadline)
        except:
            pool.terminate()
            raise
        finally:
            pool.join()

    def _process_results(self, async_results, deadline):
        for ar in async_results:
            timeout = deadline - time.time()
            try:
                ar.get(timeout)
                # TODO(yln): process result here, instead of callback
            except multiprocessing.TimeoutError:
                raise TimeoutError()
            if self.hit_max_failures:
                raise MaxFailuresError()

    # TODO(yln): interferes with progress bar
    # Some tests use threads internally, and at least on Linux each of these
    # threads counts toward the current process limit. Try to raise the (soft)
    # process limit so that tests don't fail due to resource exhaustion.
    def _increase_process_limit(self):
        ncpus = lit.util.detectCPUs()
        desired_limit = self.workers * ncpus * 2 # the 2 is a safety factor

        # Importing the resource module will likely fail on Windows.
        try:
            import resource
            NPROC = resource.RLIMIT_NPROC

            soft_limit, hard_limit = resource.getrlimit(NPROC)
            desired_limit = min(desired_limit, hard_limit)

            if soft_limit < desired_limit:
                resource.setrlimit(NPROC, (desired_limit, hard_limit))
                self.lit_config.note('Raised process limit from %d to %d' % \
                                        (soft_limit, desired_limit))
        except Exception as ex:
            self.lit_config.warning('Failed to raise process limit: %s' % ex)

    def _install_win32_signal_handler(self, pool):
        if lit.util.win32api is not None:
            def console_ctrl_handler(type):
                print('\nCtrl-C detected, terminating.')
                pool.terminate()
                pool.join()
                lit.util.abort_now()
                return True
            lit.util.win32api.SetConsoleCtrlHandler(console_ctrl_handler, True)
