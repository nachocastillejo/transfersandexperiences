import time
import psutil
import os
import logging
import threading
from functools import wraps
from flask import current_app, has_app_context

def get_memory_usage():
    """Returns the current memory usage of the process in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

def performance_monitor(f):
    """
    A decorator that logs the execution time and memory usage of a function.
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        start_mem = get_memory_usage()
        
        # Choose appropriate logger (works inside and outside Flask app context)
        logger = current_app.logger if has_app_context() else logging.getLogger("performance")

        # Log entry point
        logger.info(f"ENTERING: '{f.__name__}'. Memory usage: {start_mem:.2f} MB")
        
        # Peak memory sampler (lightweight, separate thread)
        sample_interval_ms_str = os.getenv("PERF_MONITOR_SAMPLE_MS", "100")
        try:
            sample_interval_ms = max(10, min(1000, int(float(sample_interval_ms_str))))
        except Exception:
            sample_interval_ms = 100
        sample_interval_sec = sample_interval_ms / 1000.0

        peak_mem_mb = start_mem
        stop_event = threading.Event()

        def _sample_peak_memory():
            nonlocal peak_mem_mb
            # first sample after a short delay to avoid double-counting start
            while not stop_event.is_set():
                current_mem = get_memory_usage()
                if current_mem > peak_mem_mb:
                    peak_mem_mb = current_mem
                stop_event.wait(sample_interval_sec)

        sampler_thread = threading.Thread(target=_sample_peak_memory, name=f"perf-sampler:{f.__name__}")
        sampler_thread.daemon = True
        sampler_thread.start()

        # Execute wrapped function
        result = f(*args, **kwargs)
        
        end_time = time.time()
        end_mem = get_memory_usage()
        stop_event.set()
        try:
            sampler_thread.join(timeout=1.0)
        except Exception:
            pass
        
        total_time = end_time - start_time
        mem_diff = end_mem - start_mem
        
        logger.info(
            f"EXITING: '{f.__name__}'. "
            f"Execution time: {total_time:.4f} seconds. "
            f"Memory usage: {end_mem:.2f} MB (change: {mem_diff:+.2f} MB). "
            f"Peak during run: {peak_mem_mb:.2f} MB (delta vs start: {peak_mem_mb - start_mem:+.2f} MB)"
        )
            
        return result
    return wrapper
