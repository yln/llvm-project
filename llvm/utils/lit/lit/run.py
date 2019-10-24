import multiprocessing
import time

import lit.Test
import lit.util
import lit.worker


class MaxFailuresError(Exception):
    pass
class TimeoutError(Exception):
    pass


class Run(object):
    """A concrete, configured testing run."""

    def __init__(self, tests, lit_config, workers, progress_callback,
                 max_failures, timeout):
        self.tests = tests
        self.lit_config = lit_config
        self.workers = workers
        self.progress_callback = progress_callback
        self.max_failures = max_failures
        self.timeout = timeout
        assert workers > 0

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
        self.failures = 0

        deadline = time.time() + self.timeout
        try:
            self._execute(deadline)
        finally:
            for test in self.tests:
                if not test.result:
                    test.setResult(lit.Test.Result(lit.Test.SKIPPED))

    def _execute(self, deadline):
        self._increase_process_limit()

        # 'not None' to support the the following:
        #   lit_config.parallelism_groups['my_group'] = None
        semaphores = {
            k: multiprocessing.BoundedSemaphore(v) for k, v in
            self.lit_config.parallelism_groups.items() if v is not None}

        pool = multiprocessing.Pool(self.workers, lit.worker.initialize,
                                    (self.lit_config, semaphores))

        self._install_win32_signal_handler(pool)

        async_results = [
            pool.apply_async(lit.worker.execute, args=[test], callback=
                             lambda pt, t=test: self._process_result(t, pt))
            for test in self.tests]
        pool.close()

        try:
            self._wait_for(async_results, deadline)
        except:
            pool.terminate()
            raise
        finally:
            pool.join()

    def _process_result(self, test, pickled_test):
        # Don't add any more test results after we've hit the maximum failure
        # count.  Otherwise we're racing with the main thread, which is going
        # to terminate the process pool soon.
        if self.failures >= self.max_failures:
            return

        self._apply_result(test, pickled_test)

        # Use test.isFailure() for correct XFAIL and XPASS handling
        if test.isFailure():
            self.failures += 1

        self.progress_callback(test)

    def _apply_result(self, test, pickled_test):
        # Do not use Test.setResult here; XFAIL and XPASS have been accounted
        # for.  We also want to copy xfails, requires, unsupported to ensure
        # the getters (e.g., Test.getMissingRequiredFeatures) work correctly.
        test.result = pickled_test.result
        test.xfails = pickled_test.xfails
        test.requires = pickled_test.requires
        test.unsupported = pickled_test.unsupported

    def _wait_for(self, async_results, deadline):
        for ar in async_results:
            if self.failures >= self.max_failures:
                raise MaxFailuresError()
            timeout = deadline - time.time()
            try:
                ar.get(timeout)
            except multiprocessing.TimeoutError:
                raise TimeoutError()

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
