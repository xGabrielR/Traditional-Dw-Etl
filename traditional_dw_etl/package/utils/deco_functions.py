from functools import wraps
from datetime import datetime
from time import sleep, perf_counter
from memory_profiler import memory_usage

# TODO: Fix This Deco.

def mem_profile(function):
    @wraps(function)

    def closure(*args, **kwargs):
        logs_path = f"logs_{datetime.now().strftime('%Y-%m-%d')}.txt"

        fn_kwargs_str = ', '.join(f'{k}={v}' for k, v in kwargs.items())

        print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] | [INFO] | {function.__name__}({fn_kwargs_str})', file=open(logs_path, 'a'))
        

        t = perf_counter()
        retval = function(*args, **kwargs)
        elapsed = perf_counter() - t

        print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] | [INFO] | TIME RUN - {elapsed:.7f}', file=open(logs_path, 'a'))

        mem, retval = memory_usage((function, args, kwargs), retval=True, timeout=200, interval=1e-7)

        print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] | [INFO] | MEMORY USED - {max(mem) - min(mem)}', file=open(logs_path, 'a'))
        
        return retval

    return closure


def retry_get_stagging_data(retry_times=5, time_sleep_per_retry=5, logs_path=f"logs_{datetime.now().strftime('%Y-%m-%d')}.txt"):
    retry_count = 0

    def decorator(function):
        
        async def closure(*args, **kwargs):
            nonlocal retry_count

            try:
                return await function(*args, **kwargs)

            except Exception as e:
                retry_count += 1

                stage_name = args[-1]

                print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] | [CRITICAL] | ERROR, RETRYING...', file=open(logs_path, 'a'))
                print(f'FUNCTION ERROR: {function.__name__}, "{stage_name}"\nERROR: {e}\n', file=open(logs_path, 'a'))

                if retry_count < retry_times:
                    sleep(time_sleep_per_retry)
                    
                    return await closure(*args, **kwargs)
            
            # If Fail All times, return Blank.
            return {stage_name: []}
                
        return closure
    
    return decorator