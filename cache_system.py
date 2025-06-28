"""
Caching system for the scheduler to optimize performance.

This module provides various caching mechanisms to reduce redundant calculations
and improve overall scheduler performance.
"""

import logging
import time
from typing import Any, Callable, Dict, Optional, Tuple, Union, List
from functools import wraps
from collections import OrderedDict, defaultdict
from datetime import datetime, date
from scheduler_config import CacheConfig, PerformanceMetrics


class LRUCache:
    """
    Least Recently Used cache implementation with size limit and TTL support.
    """
    
    def __init__(self, max_size: int = 1000, ttl_seconds: Optional[int] = None):
        """
        Initialize LRU cache.
        
        Args:
            max_size: Maximum number of items to cache
            ttl_seconds: Time to live in seconds (None for no expiry)
        """
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.cache = OrderedDict()
        self.timestamps = {} if ttl_seconds else None
        self.hits = 0
        self.misses = 0
    
    def get(self, key: Any) -> Tuple[bool, Any]:
        """
        Get value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Tuple of (found, value)
        """
        if key not in self.cache:
            self.misses += 1
            return False, None
        
        # Check TTL if enabled
        if self.ttl_seconds and self.timestamps:
            if time.time() - self.timestamps[key] > self.ttl_seconds:
                self._remove(key)
                self.misses += 1
                return False, None
        
        # Move to end (most recently used)
        value = self.cache.pop(key)
        self.cache[key] = value
        self.hits += 1
        return True, value
    
    def put(self, key: Any, value: Any):
        """
        Put value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
        """
        # Remove existing entry if present
        if key in self.cache:
            self.cache.pop(key)
        
        # Add new entry
        self.cache[key] = value
        if self.timestamps is not None:
            self.timestamps[key] = time.time()
        
        # Evict oldest if over capacity
        if len(self.cache) > self.max_size:
            oldest_key = next(iter(self.cache))
            self._remove(oldest_key)
    
    def _remove(self, key: Any):
        """Remove key from cache and timestamps"""
        self.cache.pop(key, None)
        if self.timestamps:
            self.timestamps.pop(key, None)
    
    def clear(self):
        """Clear all cached items"""
        self.cache.clear()
        if self.timestamps:
            self.timestamps.clear()
        self.hits = 0
        self.misses = 0
    
    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate"""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0
    
    @property
    def size(self) -> int:
        """Get current cache size"""
        return len(self.cache)


class ConstraintCache:
    """
    Specialized cache for constraint checking results.
    """
    
    def __init__(self, max_size: int = CacheConfig.CONSTRAINT_CACHE_SIZE):
        """
        Initialize constraint cache.
        
        Args:
            max_size: Maximum number of constraint results to cache
        """
        self.cache = LRUCache(max_size, CacheConfig.DEFAULT_CACHE_TTL)
        self.invalidation_patterns = defaultdict(set)
    
    def check_constraint(self, worker_id: str, date: Union[datetime, date], 
                        constraint_type: str, context: Dict[str, Any]) -> Tuple[bool, Any]:
        """
        Check constraint with caching.
        
        Args:
            worker_id: Worker ID
            date: Date for constraint check
            constraint_type: Type of constraint
            context: Additional context for constraint check
            
        Returns:
            Tuple of (found_in_cache, result)
        """
        # Create cache key
        cache_key = self._create_constraint_key(worker_id, date, constraint_type, context)
        
        found, result = self.cache.get(cache_key)
        if found:
            logging.debug(f"Constraint cache hit: {constraint_type} for {worker_id} on {date}")
            return True, result
        
        logging.debug(f"Constraint cache miss: {constraint_type} for {worker_id} on {date}")
        return False, None
    
    def store_constraint_result(self, worker_id: str, date: Union[datetime, date],
                               constraint_type: str, context: Dict[str, Any], result: Any):
        """
        Store constraint check result in cache.
        
        Args:
            worker_id: Worker ID
            date: Date for constraint check
            constraint_type: Type of constraint
            context: Additional context for constraint check
            result: Result to cache
        """
        cache_key = self._create_constraint_key(worker_id, date, constraint_type, context)
        self.cache.put(cache_key, result)
        
        # Track invalidation patterns
        self.invalidation_patterns[f"worker:{worker_id}"].add(cache_key)
        self.invalidation_patterns[f"date:{date}"].add(cache_key)
        self.invalidation_patterns[f"type:{constraint_type}"].add(cache_key)
        
        logging.debug(f"Stored constraint result: {constraint_type} for {worker_id} on {date}")
    
    def invalidate_worker(self, worker_id: str):
        """Invalidate all cached results for a worker"""
        pattern = f"worker:{worker_id}"
        if pattern in self.invalidation_patterns:
            for cache_key in self.invalidation_patterns[pattern]:
                self.cache._remove(cache_key)
            del self.invalidation_patterns[pattern]
            logging.debug(f"Invalidated constraint cache for worker {worker_id}")
    
    def invalidate_date(self, date: Union[datetime, date]):
        """Invalidate all cached results for a date"""
        pattern = f"date:{date}"
        if pattern in self.invalidation_patterns:
            for cache_key in self.invalidation_patterns[pattern]:
                self.cache._remove(cache_key)
            del self.invalidation_patterns[pattern]
            logging.debug(f"Invalidated constraint cache for date {date}")
    
    def _create_constraint_key(self, worker_id: str, date: Union[datetime, date],
                              constraint_type: str, context: Dict[str, Any]) -> str:
        """Create a unique cache key for constraint check"""
        # Convert date to string for consistent key generation
        date_str = date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date)
        
        # Create a deterministic context string
        context_str = '|'.join(f"{k}:{v}" for k, v in sorted(context.items()))
        
        return f"{worker_id}:{date_str}:{constraint_type}:{hash(context_str)}"
    
    @property
    def hit_rate(self) -> float:
        """Get cache hit rate"""
        return self.cache.hit_rate
    
    def clear(self):
        """Clear all cached data"""
        self.cache.clear()
        self.invalidation_patterns.clear()


class WorkerLookupCache:
    """
    Cache for worker lookup operations.
    """
    
    def __init__(self, max_size: int = CacheConfig.WORKER_LOOKUP_CACHE_SIZE):
        """Initialize worker lookup cache"""
        self.cache = LRUCache(max_size)
        self.worker_index = {}  # Quick worker ID to data mapping
        self.last_update = None
    
    def build_index(self, workers_data: List[Dict[str, Any]]):
        """
        Build worker lookup index.
        
        Args:
            workers_data: List of worker dictionaries
        """
        self.worker_index.clear()
        
        for worker in workers_data:
            worker_id = worker['id']
            self.worker_index[worker_id] = worker
        
        self.last_update = time.time()
        logging.debug(f"Built worker lookup index for {len(workers_data)} workers")
    
    def get_worker(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """
        Get worker data by ID.
        
        Args:
            worker_id: Worker ID to lookup
            
        Returns:
            Worker data dictionary or None if not found
        """
        found, result = self.cache.get(f"worker:{worker_id}")
        if found:
            return result
        
        # Fallback to index
        worker_data = self.worker_index.get(worker_id)
        if worker_data:
            self.cache.put(f"worker:{worker_id}", worker_data)
        
        return worker_data
    
    def get_incompatible_workers(self, worker_id: str) -> List[str]:
        """
        Get list of workers incompatible with given worker.
        
        Args:
            worker_id: Worker ID to check
            
        Returns:
            List of incompatible worker IDs
        """
        cache_key = f"incompatible:{worker_id}"
        found, result = self.cache.get(cache_key)
        if found:
            return result
        
        worker = self.get_worker(worker_id)
        if not worker:
            return []
        
        incompatible = worker.get('incompatible_with', [])
        self.cache.put(cache_key, incompatible)
        return incompatible
    
    def clear(self):
        """Clear all cached data"""
        self.cache.clear()
        self.worker_index.clear()


class DateCalculationCache:
    """
    Cache for date-related calculations.
    """
    
    def __init__(self, max_size: int = CacheConfig.DATE_CALCULATION_CACHE_SIZE):
        """Initialize date calculation cache"""
        self.cache = LRUCache(max_size)
    
    def is_weekend(self, date: Union[datetime, date]) -> bool:
        """
        Check if date is weekend with caching.
        
        Args:
            date: Date to check
            
        Returns:
            True if weekend
        """
        cache_key = f"weekend:{date}"
        found, result = self.cache.get(cache_key)
        if found:
            return result
        
        # Calculate weekend status
        weekday = date.weekday() if hasattr(date, 'weekday') else date.date().weekday()
        is_weekend = weekday >= 5  # Saturday (5) or Sunday (6)
        
        self.cache.put(cache_key, is_weekend)
        return is_weekend
    
    def is_holiday(self, date: Union[datetime, date], holidays: List[date]) -> bool:
        """
        Check if date is holiday with caching.
        
        Args:
            date: Date to check
            holidays: List of holiday dates
            
        Returns:
            True if holiday
        """
        date_obj = date.date() if hasattr(date, 'date') else date
        cache_key = f"holiday:{date_obj}:{len(holidays)}"
        
        found, result = self.cache.get(cache_key)
        if found:
            return result
        
        is_holiday = date_obj in holidays
        self.cache.put(cache_key, is_holiday)
        return is_holiday
    
    def get_weekend_start(self, date: Union[datetime, date]) -> date:
        """
        Get start of weekend for given date.
        
        Args:
            date: Date to check
            
        Returns:
            Start date of weekend (Saturday)
        """
        date_obj = date.date() if hasattr(date, 'date') else date
        cache_key = f"weekend_start:{date_obj}"
        
        found, result = self.cache.get(cache_key)
        if found:
            return result
        
        # Calculate weekend start (Saturday)
        days_since_saturday = (date_obj.weekday() + 2) % 7
        weekend_start = date_obj - timedelta(days=days_since_saturday)
        
        self.cache.put(cache_key, weekend_start)
        return weekend_start
    
    def clear(self):
        """Clear all cached data"""
        self.cache.clear()


class CacheManager:
    """
    Central manager for all caching systems.
    """
    
    def __init__(self):
        """Initialize cache manager with all cache types"""
        self.constraint_cache = ConstraintCache()
        self.worker_cache = WorkerLookupCache()
        self.date_cache = DateCalculationCache()
        
        self.performance_stats = {
            'cache_hits': 0,
            'cache_misses': 0,
            'total_operations': 0,
            'start_time': time.time()
        }
        
        logging.info("Cache manager initialized")
    
    def invalidate_worker_caches(self, worker_id: str):
        """
        Invalidate all caches related to a specific worker.
        
        Args:
            worker_id: Worker ID to invalidate
        """
        self.constraint_cache.invalidate_worker(worker_id)
        # Worker cache doesn't need invalidation as it's read-only
        logging.debug(f"Invalidated caches for worker {worker_id}")
    
    def invalidate_date_caches(self, date: Union[datetime, date]):
        """
        Invalidate all caches related to a specific date.
        
        Args:
            date: Date to invalidate
        """
        self.constraint_cache.invalidate_date(date)
        # Date cache calculations are deterministic, no need to invalidate
        logging.debug(f"Invalidated caches for date {date}")
    
    def update_worker_data(self, workers_data: List[Dict[str, Any]]):
        """
        Update cached worker data.
        
        Args:
            workers_data: Updated worker data list
        """
        self.worker_cache.build_index(workers_data)
        logging.debug("Updated worker cache with new data")
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance statistics"""
        total_hits = (self.constraint_cache.cache.hits + 
                     self.worker_cache.cache.hits + 
                     self.date_cache.cache.hits)
        
        total_misses = (self.constraint_cache.cache.misses + 
                       self.worker_cache.cache.misses + 
                       self.date_cache.cache.misses)
        
        total_operations = total_hits + total_misses
        
        return {
            'constraint_cache': {
                'hits': self.constraint_cache.cache.hits,
                'misses': self.constraint_cache.cache.misses,
                'hit_rate': self.constraint_cache.hit_rate,
                'size': self.constraint_cache.cache.size
            },
            'worker_cache': {
                'hits': self.worker_cache.cache.hits,
                'misses': self.worker_cache.cache.misses,
                'hit_rate': self.worker_cache.cache.hit_rate,
                'size': self.worker_cache.cache.size
            },
            'date_cache': {
                'hits': self.date_cache.cache.hits,
                'misses': self.date_cache.cache.misses,
                'hit_rate': self.date_cache.cache.hit_rate,
                'size': self.date_cache.cache.size
            },
            'overall': {
                'total_hits': total_hits,
                'total_misses': total_misses,
                'total_operations': total_operations,
                'overall_hit_rate': total_hits / total_operations if total_operations > 0 else 0.0,
                'uptime_seconds': time.time() - self.performance_stats['start_time']
            }
        }
    
    def clear_all_caches(self):
        """Clear all caches"""
        self.constraint_cache.clear()
        self.worker_cache.clear()
        self.date_cache.clear()
        logging.info("All caches cleared")
    
    def log_performance_stats(self):
        """Log current performance statistics"""
        stats = self.get_performance_stats()
        logging.info(f"Cache Performance: Overall hit rate: {stats['overall']['overall_hit_rate']:.2%}, "
                    f"Total operations: {stats['overall']['total_operations']}")


def cached_method(cache_manager_attr: str, cache_type: str, key_func: Optional[Callable] = None):
    """
    Decorator for caching method results.
    
    Args:
        cache_manager_attr: Name of cache manager attribute on the class
        cache_type: Type of cache to use ('constraint', 'worker', 'date')  
        key_func: Optional function to generate cache key from method args
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # Get cache manager from instance
            cache_manager = getattr(self, cache_manager_attr, None)
            if not cache_manager:
                # No cache manager, call function directly
                return func(self, *args, **kwargs)
            
            # Get appropriate cache
            if cache_type == 'constraint':
                cache = cache_manager.constraint_cache.cache
            elif cache_type == 'worker':
                cache = cache_manager.worker_cache.cache
            elif cache_type == 'date':
                cache = cache_manager.date_cache.cache
            else:
                # Unknown cache type, call function directly
                return func(self, *args, **kwargs)
            
            # Generate cache key
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                cache_key = f"{func.__name__}:{hash((args, tuple(sorted(kwargs.items()))))}"
            
            # Try to get from cache
            found, result = cache.get(cache_key)
            if found:
                return result
            
            # Call function and cache result
            result = func(self, *args, **kwargs)
            cache.put(cache_key, result)
            return result
        
        return wrapper
    return decorator


# Performance monitoring decorator
def performance_monitor(operation_name: str):
    """
    Decorator to monitor performance of operations.
    
    Args:
        operation_name: Name of operation for logging
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                
                # Log slow operations
                if execution_time > PerformanceMetrics.SLOW_OPERATION_THRESHOLD:
                    if execution_time > PerformanceMetrics.VERY_SLOW_OPERATION_THRESHOLD:
                        logging.warning(f"Very slow operation {operation_name}: {execution_time:.3f}s")
                    else:
                        logging.info(f"Slow operation {operation_name}: {execution_time:.3f}s")
                
                return result
                
            except Exception as e:
                execution_time = time.time() - start_time
                logging.error(f"Operation {operation_name} failed after {execution_time:.3f}s: {str(e)}")
                raise
        
        return wrapper
    return decorator