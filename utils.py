import sys
import time
import logging


def dec_err_handler(retries=0):
    """
    Decorator function to handle logging and retries.
    Usage: Call without the retries parameter to have it log exceptions only.
    Assumptions: 1) args[0] is "self", and "self.logger" has been instantiated.
    Ref: https://stackoverflow.com/questions/11731136/python-class-method-decorator-with-self-arguments
    :retries: Number of times to retry, in addition to original try.
    """
    def wrap(f):  # Doing the wrapping here. Called during the decoration of the function.
        def wrapped_err_handler(*args, **kwargs):  # This way, kwargs can be handled too.
            logger = args[0].logger  # args[0] is intended to be "self". Assumes that self.logger has already been created on __init__.

            if not isinstance(logger, logging.Logger):  # Ensures that a Logger object is provided.
                print('[ERROR] Please provide an instance of class: logging.Logger')
                sys.exit('[ERROR] Please provide an instance of class: logging.Logger')

            for i in range(retries + 1):  # First attempt 0 does not count as a retry.
                try:
                    f(*args, **kwargs)
                    #print('No exception encountered')
                    break  # So you don't run f() multiple times!
                except Exception as ex:
                    # To only log exceptions.
                    #print('Exception: ' + str(ex))
                    if i > 0:
                        logger.info(f'[RETRYING] {f.__name__}: {i}/{retries}')
                        time.sleep(2 ** i)  # Exponential backoff. Pause processing for an increasing number of seconds, with each error.
                    logger.error(ex)

        wrapped_err_handler.__name__ = f.__name__  # Nicety. Rename the error handler function name to that of the wrapped function.
        return wrapped_err_handler
    return wrap
