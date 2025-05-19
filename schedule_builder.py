# Imports
from datetime import datetime, timedelta
import copy
import logging
import random
from typing import TYPE_CHECKING
from exceptions import SchedulerError
if TYPE_CHECKING:
    from scheduler import Scheduler

class ScheduleBuilder:
"""Handles schedule generation and improvement"""
     
# 1. Initialization
    def __init__(self, scheduler):
        """
        Initialize the schedule builder
        Args:
        scheduler: The main Scheduler object
        """
        self.scheduler = scheduler
        # IMPORTANT: Use direct references, not copies
        self.workers_data = scheduler.workers_data
        self.schedule = scheduler.schedule # self.schedule IS scheduler.schedule
        logging.debug(f"[ScheduleBuilder.__init__] self.schedule object ID: {id(self.schedule)}, Initial keys: {list(self.schedule.keys())}")
        self.config = scheduler.config
        self.worker_assignments = scheduler.worker_assignments  # Use the same reference
        self.num_shifts = scheduler.num_shifts
31|         self.holidays = scheduler.holidays
32|         self.constraint_checker = scheduler.constraint_checker
33|         self.best_schedule_data = None # Initialize the attribute to store the best state found
34|         self._locked_mandatory = set()
35|         # Keep track of which (worker_id, date) pairs are truly mandatory
36|         self.start_date = scheduler.start_date
37|         self.end_date = scheduler.end_date
38|         self.date_utils = scheduler.date_utils
39|         self.gap_between_shifts = scheduler.gap_between_shifts 
40|         self.max_shifts_per_worker = scheduler.max_shifts_per_worker
41|         self.data_manager = scheduler.data_manager
42|         self.worker_posts = scheduler.worker_posts
43|         self.worker_weekdays = scheduler.worker_weekdays
44|         self.worker_weekends = scheduler.worker_weekends
45|         self.constraint_skips = scheduler.constraint_skips
46|         self.last_assigned_date = scheduler.last_assigned_date # Used in calculate_score
47|         self.consecutive_shifts = scheduler.consecutive_shifts # Used in calculate_score
48| 
49|         logging.debug(f"[ScheduleBuilder.__init__] self.schedule object ID: {id(self.schedule)}, Initial keys: {list(self.schedule.keys())[:5]}")
50|         logging.info("ScheduleBuilder initialized")
51|         
52|     # 2. Utility Methods
53|     def _parse_dates(self, date_str):
54|         """
55|         Parse semicolon-separated dates using the date_utils
56|     
57|         Args:
58|             date_str: String with semicolon-separated dates in DD-MM-YYYY format
59|         Returns:
60|             list: List of datetime objects
61|         """
62|         if not date_str:
63|             return []
64|     
65|         # Delegate to the DateTimeUtils class
66|         return self.date_utils.parse_dates(date_str)
67| 
68|     def _ensure_data_integrity(self):
69|         """
70|         Ensure all data structures are consistent - delegates to scheduler
71|         """
72|         # Let the scheduler handle the data integrity check as it has the primary data
73|         return self.scheduler._ensure_data_integrity()    
74| 
75|     def _verify_assignment_consistency(self):
76|         """
77|         Verify and fix data consistency between schedule and tracking data
78|         """
79|         # Check schedule against worker_assignments and fix inconsistencies
80|         for date, shifts in self.schedule.items():
81|             for post, worker_id in enumerate(shifts):
82|                 if worker_id is None:
83|                     continue
84|                 
85|                 # Ensure worker is tracked for this date
86|                 if date not in self.worker_assignments.get(worker_id, set()):
87|                     self.worker_assignments[worker_id].add(date)
88|     
89|         # Check worker_assignments against schedule
90|         for worker_id, assignments in self.worker_assignments.items():
91|             for date in list(assignments):  # Make a copy to safely modify during iteration
92|                 # Check if this worker is actually in the schedule for this date
93|                 if date not in self.schedule or worker_id not in self.schedule[date]:
94|                     # Remove this inconsistent assignment
95|                     self.worker_assignments[worker_id].remove(date)
96|                     logging.warning(f"Fixed inconsistency: Worker {worker_id} was tracked for {date} but not in schedule")
97| 
98|     # 3. Worker Constraint Check Methods
99| 
100|     def _is_mandatory(self, worker_id, date):
101|         # This is a placeholder for your actual implementation
102|         worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
103|         if not worker: return False
104|         mandatory_days_str = worker.get('mandatory_days', '')
105|         if not mandatory_days_str: return False
106|         try:
107|             mandatory_dates = self.date_utils.parse_dates(mandatory_days_str)
108|             return date in mandatory_dates
109|         except:
110|             return False
111|             
112|     def _is_worker_unavailable(self, worker_id, date):
113|         """
114|         Check if a worker is unavailable on a specific date
115| 
116|         Args:
117|             worker_id: ID of the worker to check
118|             date: Date to check availability
119|     
120|         Returns:
121|             bool: True if worker is unavailable, False otherwise
122|         """
123|         # Get worker data
124|         worker_data = next((w for w in self.workers_data if w['id'] == worker_id), None) # Corrected to worker_data as per user
125|         if not worker_data:
126|             return True
127|     
128|         # Debug log
129|         logging.debug(f"Checking availability for worker {worker_id} on {date.strftime('%d-%m-%Y')}")
130| 
131|         # Check work periods - if work_periods is empty, worker is available for all dates
132|         work_periods_str = worker_data.get('work_periods', '')
133|         if work_periods_str:
134|             try:
135|                 work_ranges = self.date_utils.parse_date_ranges(work_periods_str)
136|                 if not any(start <= date <= end for start, end in work_ranges):
137|                     logging.debug(f"Worker {worker_id} not available - date outside work periods")
138|                     return True # Not within any defined work period
139|             except Exception as e:
140|                 logging.error(f"Error parsing work_periods for {worker_id}: {e}")
141|                 return True # Fail safe
142|             # If we reach here, it means work_periods_str was present, parsed, and date is within a period.
143|             # So, the worker IS NOT unavailable due to work_periods. We proceed to check days_off.
144| 
145|         # Check days off
146|         days_off_str = worker_data.get('days_off', '')
147|         if days_off_str:
148|             try:
149|                 off_ranges = self.date_utils.parse_date_ranges(days_off_str)
150|                 if any(start <= date <= end for start, end in off_ranges):
151|                     logging.debug(f"Worker {worker_id} not available - date is a day off")
152|                     return True
153|             except Exception as e:
154|                 logging.error(f"Error parsing days_off for {worker_id}: {e}")
155|                 return True # Fail safe
156| 
157|         logging.debug(f"Worker {worker_id} is available on {date.strftime('%d-%m-%Y')}")
158|         return False
159|     
160|     def _check_incompatibility_with_list(self, worker_id_to_check, assigned_workers_list):
161|         """Checks if worker_id_to_check is incompatible with anyone in the list."""
162|         worker_to_check_data = next((w for w in self.workers_data if w['id'] == worker_id_to_check), None)
163|         if not worker_to_check_data: return True # Should not happen, but fail safe
164| 
165|         incompatible_with_candidate = set(worker_to_check_data.get('incompatible_with', []))
166| 
167|         # *** ADD TYPE LOGGING HERE ***
168|         logging.debug(f"  CHECKING_INTERNAL: Worker={worker_id_to_check} (Type: {type(worker_id_to_check)}), AgainstList={assigned_workers_list}, IncompListForCheckWorker={incompatible_with_candidate}") # Corrected variable name
169| 
170|         for assigned_id in assigned_workers_list:
171|             if assigned_id is None or assigned_id == worker_id_to_check:
172|                 continue
173|             if str(assigned_id) in incompatible_with_candidate: # Ensure type consistency if IDs might be mixed
174|                 return False # Candidate is incompatible with an already assigned worker
175| 
176|             # Bidirectional check
177|             assigned_worker_data = next((w for w in self.workers_data if w['id'] == assigned_id), None)
178|             if assigned_worker_data:
179|                 if str(worker_id_to_check) in set(assigned_worker_data.get('incompatible_with', [])):
180|                     return False # Assigned worker is incompatible with the ca
181|         logging.debug(f"  CHECKING_INTERNAL: Worker={worker_id_to_check} vs List={assigned_workers_list} -> OK (No incompatibility detected)")
182|         return True # No incompatibilities found
183| 
184|     def _check_incompatibility(self, worker_id, date):
185|         # Placeholder using _check_incompatibility_with_list
186|         assigned_workers_on_date = [w for w in self.schedule.get(date, []) if w is not None]
187|         return self._check_incompatibility_with_list(worker_id, assigned_workers_on_date)
188| 
189|     def _are_workers_incompatible(self, worker1_id, worker2_id):
190|         """
191|         Check if two workers are incompatible with each other
192|     
193|         Args:
194|             worker1_id: ID of first worker
195|             worker2_id: ID of second worker
196|         
197|         Returns:
198|             bool: True if workers are incompatible, False otherwise
199|         """
200|         # Find the worker data for each worker
201|         worker1 = next((w for w in self.workers_data if w['id'] == worker1_id), None)
202|         worker2 = next((w for w in self.workers_data if w['id'] == worker2_id), None)
203|     
204|         if not worker1 or not worker2:
205|             return False
206|     
207|         # Check if either worker has the other in their incompatibility list
208|         incompatible_with_1 = worker1.get('incompatible_with', [])
209|         incompatible_with_2 = worker2.get('incompatible_with', [])
210|     
211|         return worker2_id in incompatible_with_1 or worker1_id in incompatible_with_2 
212| 
213|     def _would_exceed_weekend_limit(self, worker_id, date):
214|         """
215|         Check if adding this date would exceed the worker's weekend limit
216| 
217|         Args:
218|             worker_id: ID of the worker to check
219|             date: Date to potentially add
220|     
221|         Returns:
222|             bool: True if weekend limit would be exceeded, False otherwise
223|         """
224|         # Skip if not a weekend
225|         if not self.date_utils.is_weekend_day(date) and date not in self.holidays:
226|             return False
227| 
228|         # Get worker data
229|         worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
230|         if not worker:
231|             return True
232| 
233|         # Get weekend assignments for this worker
234|         weekend_dates = self.worker_weekends.get(worker_id, [])
235| 
236|         # Calculate the maximum allowed weekend shifts based on work percentage
237|         work_percentage = worker.get('work_percentage', 100)
238|         max_weekend_shifts = self.max_consecutive_weekends  # Use the configurable parameter
239|         if work_percentage < 100:
240|             # For part-time workers, adjust max consecutive weekends proportionally
241|             max_weekend_shifts = max(1, int(self.max_consecutive_weekends * work_percentage / 100))
242| 
243|         # Check if adding this date would exceed the limit for any 3-week period
244|         if date in weekend_dates:
245|             return False  # Already counted
246| 
247|         # Add the date temporarily
248|         test_dates = weekend_dates + [date]
249|         test_dates.sort()
250| 
251|         # Check for any 3-week period with too many weekend shifts
252|         three_weeks = timedelta(days=21)
253|         for i, start_date_val in enumerate(test_dates): # Renamed start_date to avoid conflict
254|             end_date_val = start_date_val + three_weeks # Renamed end_date to avoid conflict
255|             count = sum(1 for d in test_dates[i:] if d <= end_date_val)
256|             if count > max_weekend_shifts:
257|                 return True
258| 
259|         return False
260| 
261|     def _get_post_counts(self, worker_id):
262|         """
263|         Get the count of assignments for each post for a specific worker
264|     
265|         Args:
266|             worker_id: ID of the worker
267|         
268|         Returns:
269|             dict: Dictionary with post numbers as keys and counts as values
270|         """
271|         post_counts = {post: 0 for post in range(self.num_shifts)}
272|     
273|         for date_val, shifts in self.schedule.items(): # Renamed date to avoid conflict
274|             for post, assigned_worker in enumerate(shifts):
275|                 if assigned_worker == worker_id:
276|                     post_counts[post] = post_counts.get(post, 0) + 1
277|     
278|         return post_counts
279| 
280|     def _update_worker_stats(self, worker_id, date, removing=False):
281|         """
282|         Update worker statistics when adding or removing an assignment
283|     
284|         Args:
285|             worker_id: ID of the worker
286|             date: The date of the assignment
287|             removing: Whether we're removing (True) or adding (False) an assignment
288|         """
289|         # Update weekday counts
290|         weekday = date.weekday()
291|         if worker_id in self.worker_weekdays:
292|             if removing:
293|                 self.worker_weekdays[worker_id][weekday] = max(0, self.worker_weekdays[worker_id][weekday] - 1)
294|             else:
295|                 self.worker_weekdays[worker_id][weekday] += 1
296|     
297|         # Update weekend tracking
298|         is_weekend = date.weekday() >= 4 or date in self.holidays  # Friday, Saturday, Sunday or holiday
299|         if is_weekend and worker_id in self.worker_weekends:
300|             if removing:
301|                 if date in self.worker_weekends[worker_id]:
302|                     self.worker_weekends[worker_id].remove(date)
303|             else:
304|                 if date not in self.worker_weekends[worker_id]:
305|                     self.worker_weekends[worker_id].append(date)
306|                     self.worker_weekends[worker_id].sort()
307| 
308|     def _verify_no_incompatibilities(self):
309|         """
310|         Verify that the final schedule doesn't have any incompatibility violations
311|         and fix any found violations.
312|         """
313|         logging.info("Performing final incompatibility verification check")
314|     
315|         violations_found = 0
316|         violations_fixed = 0
317|     
318|         # Check each date for incompatible worker assignments
319|         for date_val in sorted(self.schedule.keys()): # Renamed date to avoid conflict
320|             workers_today = [w for w in self.schedule[date_val] if w is not None]
321|         
322|             # Process all pairs to find incompatibilities
323|             for i in range(len(workers_today)):
324|                 for j in range(i+1, len(workers_today)):
325|                     worker1_id = workers_today[i]
326|                     worker2_id = workers_today[j]
327|                 
328|                     # Check if they are incompatible
329|                     if self._are_workers_incompatible(worker1_id, worker2_id):
330|                         violations_found += 1
331|                         logging.warning(f"Final verification found incompatibility violation: {worker1_id} and {worker2_id} on {date_val.strftime('%d-%m-%Y')}")
332|                     
333|                         # Find their positions
334|                         post1 = self.schedule[date_val].index(worker1_id)
335|                         post2 = self.schedule[date_val].index(worker2_id)
336|                     
337|                         # Remove one of the workers (choose the one with more shifts assigned)
338|                         w1_shifts = len(self.worker_assignments.get(worker1_id, set()))
339|                         w2_shifts = len(self.worker_assignments.get(worker2_id, set()))
340|                     
341|                         # Remove the worker with more shifts or the second worker if equal
342|                         if w1_shifts > w2_shifts:
343|                             self.schedule[date_val][post1] = None
344|                             self.worker_assignments[worker1_id].remove(date_val)
345|                             self.scheduler._update_tracking_data(worker1_id, date_val, post1, removing=True)
346|                             violations_fixed += 1
347|                             logging.info(f"Removed worker {worker1_id} from {date_val.strftime('%d-%m-%Y')} to fix incompatibility")
348|                         else:
349|                             self.schedule[date_val][post2] = None
350|                             self.worker_assignments[worker2_id].remove(date_val)
351|                             self.scheduler._update_tracking_data(worker2_id, date_val, post2, removing=True)
352|                             violations_fixed += 1
353|                             logging.info(f"Removed worker {worker2_id} from {date_val.strftime('%d-%m-%Y')} to fix incompatibility")
354|     
355|         logging.info(f"Final verification: found {violations_found} violations, fixed {violations_fixed}")
356|         return violations_fixed > 0
357| 
358|     # 4. Worker Assignment Methods
359| 
360|     def _can_assign_worker(self, worker_id, date, post):
361|         try:
362|             # Log all constraint checks
363|             logging.debug(f"\nChecking worker {worker_id} for {date}, post {post}")
364|         
365|             # Skip if already assigned to this date
366|             if worker_id in self.schedule.get(date, []):
367|                 return False
368|             
369|             # Get worker data
370|             worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
371|             if not worker:
372|                 return False
373|             
374|             # Check worker availability (days off)
375|             if self._is_worker_unavailable(worker_id, date):
376|                 return False
377|             
378|             # Check for incompatibilities
379|             if not self._check_incompatibility(worker_id, date):
380|                 return False
381|             
382|             # Check minimum gap and 7-14 day pattern
383|             assignments = sorted(list(self.worker_assignments.get(worker_id, [])))
384|             if assignments:
385|                 for prev_date in assignments:
386|                     days_between = abs((date - prev_date).days)
387|                 
388|                     # Check minimum gap
389|                     if 0 < days_between < self.gap_between_shifts + 1:
390|                         logging.debug(f"- Failed: Insufficient gap ({days_between} days)")
391|                         return False
392|                 
393|                     # Check for 7-14 day pattern (same weekday in consecutive weeks)
394|                     if (days_between == 7 or days_between == 14) and date.weekday() == prev_date.weekday():
395|                         logging.debug(f"- Failed: Would create {days_between} day pattern")
396|                         return False
397|             
398|             # Special case: Friday-Monday check if gap is only 1 day
399|             if self.gap_between_shifts == 1:
400|                 for prev_date in assignments:
401|                     days_between = abs((date - prev_date).days)
402|                     if days_between == 3:
403|                         if ((prev_date.weekday() == 4 and date.weekday() == 0) or \
404|                             (date.weekday() == 4 and prev_date.weekday() == 0)):
405|                             return False
406| 
407|             # Check weekend limits
408|             if self._would_exceed_weekend_limit(worker_id, date):
409|                 return False
410| 
411|             # Part-time workers need more days between shifts
412|             work_percentage = worker.get('work_percentage', 100)
413|             if work_percentage < 70:
414|                 part_time_gap = max(3, self.gap_between_shifts + 2)
415|                 for prev_date in assignments:
416|                     days_between = abs((date - prev_date).days)
417|                     if days_between < part_time_gap:
418|                         return False
419| 
420|             # If we've made it this far, the worker can be assigned
421|             return True
422|     
423|         except Exception as e:
424|             logging.error(f"Error in _can_assign_worker for worker {worker_id}: {str(e)}", exc_info=True)
425|             return False
426| 
427|     def assign_worker_to_shift(self, worker_id, date, post):
428|         """Assign a worker to a shift with proper incompatibility checking"""
429|     
430|         # Check if the date already exists in the schedule
431|         if date not in self.schedule:
432|             self.schedule[date] = [None] * self.num_shifts
433|         
434|         # Check for incompatibility with already assigned workers
435|         already_assigned = [w for w in self.schedule[date] if w is not None]
436|         if not self._check_incompatibility_with_list(worker_id, already_assigned):
437|             logging.warning(f"Cannot assign worker {worker_id} due to incompatibility on {date}")
438|             return False
439|         
440|         # Proceed with assignment if no incompatibility
441|         self.schedule[date][post] = worker_id
442|         self._update_tracking_data(worker_id, date, post) # Corrected: self.scheduler._update_tracking_data
443|         return True
444|     
445|     def _can_swap_assignments(self, worker_id, date_from, post_from, date_to, post_to):
446|         """
447|         Checks if moving worker_id from (date_from, post_from) to (date_to, post_to) is valid.
448|         Uses deepcopy for safer simulation.
449|         """
450|         # --- Simulation Setup ---\
451|         # Create deep copies of the schedule and assignments
452|         try:
453|             # Use scheduler\'s references for deepcopy
454|             simulated_schedule = copy.deepcopy(self.scheduler.schedule)
455|             simulated_assignments = copy.deepcopy(self.scheduler.worker_assignments)
456| 
457|             # --- Simulate the Swap ---\
458|             # 1. Check if \'from\' state is valid before simulating removal
459|             if date_from not in simulated_schedule or \
460|                len(simulated_schedule[date_from]) <= post_from or \
461|                simulated_schedule[date_from][post_from] != worker_id or \
462|                worker_id not in simulated_assignments or \
463|                date_from not in simulated_assignments[worker_id]:
464|                     logging.warning(f"_can_swap_assignments: Initial state invalid for removing {worker_id} from {date_from}|P{post_from}. Aborting check.")
465|                     return False # Cannot simulate if initial state is wrong
466| 
467|             # 2. Simulate removing worker from \'from\' position
468|             simulated_schedule[date_from][post_from] = None
469|             simulated_assignments[worker_id].remove(date_from)
470|             # Clean up empty set for worker if needed
471|             if not simulated_assignments[worker_id]:
472|                  del simulated_assignments[worker_id]
473| 
474| 
475|             # 3. Simulate adding worker to \'to\' position
476|             # Ensure target list exists and is long enough in the simulation
477|             simulated_schedule.setdefault(date_to, [None] * self.num_shifts)
478|             while len(simulated_schedule[date_to]) <= post_to:
479|                 simulated_schedule[date_to].append(None)
480| 
481|             # Check if target slot is empty in simulation before placing
482|             if simulated_schedule[date_to][post_to] is not None:
483|                 logging.debug(f"_can_swap_assignments: Target slot {date_to}|P{post_to} is not empty in simulation. Aborting check.")
484|                 return False
485| 
486|             simulated_schedule[date_to][post_to] = worker_id
487|             simulated_assignments.setdefault(worker_id, set()).add(date_to)
488|             
489|             # --- Check Constraints on Simulated State ---\
490|             # Check if the worker can be assigned to the target slot considering the simulated state
491|             can_assign_to_target = self._check_constraints_on_simulated(\
492|                 worker_id, date_to, post_to, simulated_schedule, simulated_assignments\
493|             )
494| 
495|             # Also check if the source date is still valid *without* the worker
496|             # (e.g., maybe removing the worker caused an issue for others on date_from)
497|             source_date_still_valid = self._check_all_constraints_for_date_simulated(\
498|                 date_from, simulated_schedule, simulated_assignments\
499|             )
500| 
501|             # Also check if the target date remains valid *with* the worker added
502|             target_date_still_valid = self._check_all_constraints_for_date_simulated(\
503|                  date_to, simulated_schedule, simulated_assignments\
504|             )
505| 
506| 
507|             is_valid_swap = can_assign_to_target and source_date_still_valid and target_date_still_valid
508| 
509|             # --- End Simulation ---\
510|             # No rollback needed as we operated on copies.
511| 
512|             logging.debug(f"Swap Check: {worker_id} from {date_from}|P{post_from} to {date_to}|P{post_to}. Valid: {is_valid_swap} (Target OK: {can_assign_to_target}, Source OK: {source_date_still_valid}, Target Date OK: {target_date_still_valid})") # Corrected log string
513|             return is_valid_swap
514| 
515|         except Exception as e:
516|             logging.error(f"Error during _can_swap_assignments simulation for {worker_id}: {e}", exc_info=True)
517|             return False # Fail safe
518| 
519| 
520|     def _check_constraints_on_simulated(self, worker_id, date, post, simulated_schedule, simulated_assignments):
521|         """Checks constraints for a worker on a specific date using simulated data."""
522|         try:
523|             # Get worker data for percentage check if needed later
524|             worker_data = next((w for w in self.scheduler.workers_data if w['id'] == worker_id), None)
525|             work_percentage = worker_data.get('work_percentage', 100) if worker_data else 100
526| 
527|             # 1. Incompatibility (using simulated_schedule)
528|             if not self._check_incompatibility_simulated(worker_id, date, simulated_schedule):
529|                 logging.debug(f"Sim Check Fail: Incompatible {worker_id} on {date}")
530|                 return False
531| 
532|             # 2. Gap Constraint (using simulated_assignments)
533|             # This helper already includes basic gap logic
534|             if not self._check_gap_constraint_simulated(worker_id, date, simulated_assignments):
535|                 logging.debug(f"Sim Check Fail: Gap constraint {worker_id} on {date}")
536|                 return False
537| 
538|             # 3. Weekend Limit (using simulated_assignments)
539|             if self._would_exceed_weekend_limit_simulated(worker_id, date, simulated_assignments):
540|                  logging.debug(f"Sim Check Fail: Weekend limit {worker_id} on {date}")
541|                  return False
542| 
543|             # 4. Max Shifts (using simulated_assignments)
544|             # Use scheduler's max_shifts_per_worker config
545|             if len(simulated_assignments.get(worker_id, set())) > self.max_shifts_per_worker:
546|                  logging.debug(f"Sim Check Fail: Max shifts {worker_id}")
547|                  return False
548| 
549|             # 5. Basic Availability (Check if worker is unavailable fundamentally)
550|             if self._is_worker_unavailable(worker_id, date):
551|                  logging.debug(f"Sim Check Fail: Worker {worker_id} fundamentally unavailable on {date}")
552|                  return False
553| 
554|             # 6. Double Booking Check (using simulated_schedule)
555|             count = 0
556|             for assigned_post, assigned_worker in enumerate(simulated_schedule.get(date, [])):
557|                  if assigned_worker == worker_id:
558|                       if assigned_post != post: # Don't count the slot we are checking
559|                            count += 1
560|             if count > 0:
561|                  logging.debug(f"Sim Check Fail: Double booking {worker_id} on {date}")
562|                  return False
563| 
564|             sorted_sim_assignments = sorted(list(simulated_assignments.get(worker_id, [])))
565| 
566|             # 7. Friday-Monday Check (Only if gap constraint allows 3 days, i.e., gap_between_shifts == 1)
567|             # Apply strictly during simulation checks
568|             if self.scheduler.gap_between_shifts == 1: 
569|                  for prev_date in sorted_sim_assignments:
570|                       if prev_date == date: continue
571|                       days_between = abs((date - prev_date).days)
572|                       if days_between == 3:
573|                            # Check if one is Friday (4) and the other is Monday (0)
574|                            if ((prev_date.weekday() == 4 and date.weekday() == 0) or \
575|                                (date.weekday() == 4 and prev_date.weekday() == 0)):
576|                                logging.debug(f"Sim Check Fail: Friday-Monday conflict for {worker_id} between {prev_date} and {date}")
577|                                return False
578| 
579|             # 8. 7/14 Day Pattern Check (Same day of week in consecutive weeks)
580|             for prev_date in sorted_sim_assignments:
581|                 if prev_date == date: 
582|                     continue
583|                 days_between = abs((date - prev_date).days)
584|                 # Check for exactly 7 or 14 days pattern AND same weekday
585|                 if (days_between == 7 or days_between == 14) and date.weekday() == prev_date.weekday():
586|                     logging.debug(f"Sim Check Fail: {days_between} day pattern conflict for {worker_id} between {prev_date} and {date}")
587|                     return False
588|                 
589|             return True # All checks passed on simulated data        
590|         except Exception as e:
591|             logging.error(f"Error during _check_constraints_on_simulated for {worker_id} on {date}: {e}", exc_info=True)
592|             return False # Fail safe
593| 
594|     def _check_all_constraints_for_date_simulated(self, date, simulated_schedule, simulated_assignments):
595|          """ Checks all constraints for all workers assigned on a given date in the SIMULATED schedule. """
596|          if date not in simulated_schedule: return True # Date might not exist in sim if empty
597| 
598|          assignments_on_date = simulated_schedule[date]
599| 
600|          # Check pairwise incompatibility first for the whole date
601|          workers_present = [w for w in assignments_on_date if w is not None]
602|          for i in range(len(workers_present)):
603|               for j in range(i + 1, len(workers_present)):
604|                    worker1_id = workers_present[i]
605|                    worker2_id = workers_present[j]
606|                    if self._are_workers_incompatible(worker1_id, worker2_id):
607|                         logging.debug(f"Simulated state invalid: Incompatibility between {worker1_id} and {worker2_id} on {date}")
608|                         return False
609| 
610|          # Then check individual constraints for each worker
611|          for post, worker_id in enumerate(assignments_on_date):
612|               if worker_id is not None:
613|                    # Check this worker's assignment using the simulated state helper
614|                    if not self._check_constraints_on_simulated(worker_id, date, post, simulated_schedule, simulated_assignments):
615|                         # logging.debug(f"Simulated state invalid: Constraint fail for {worker_id} on {date} post {post}")
616|                         return False # Constraint failed for this worker in the simulated state
617|          return True
618|         
619|     def _check_incompatibility_simulated(self, worker_id, date, simulated_schedule):
620|         """Check incompatibility using the simulated schedule."""
621|         assigned_workers_list = simulated_schedule.get(date, [])
622|         # Use the existing helper, it only needs the list of workers on that day
623|         return self._check_incompatibility_with_list(worker_id, assigned_workers_list)
624| 
625|     def _check_gap_constraint_simulated(self, worker_id, date, simulated_assignments):
626|         """Check gap constraint using simulated assignments."""
627|         # Use scheduler's gap config
628|         min_days_between = self.scheduler.gap_between_shifts + 1
629|         # Add part-time adjustment if needed
630|         worker_data = next((w for w in self.scheduler.workers_data if w['id'] == worker_id), None)
631|         work_percentage = worker_data.get('work_percentage', 100) if worker_data else 100
632|         if work_percentage < 70: # Example threshold for part-time adjustment
633|             min_days_between = max(min_days_between, self.scheduler.gap_between_shifts + 2)
634| 
635|         assignments = sorted(list(simulated_assignments.get(worker_id, [])))
636| 
637|         for prev_date in assignments:
638|             if prev_date == date: continue # Don't compare date to itself
639|             days_between = abs((date - prev_date).days)
640|             if days_between < min_days_between:
641|                 return False
642|             # Add Friday-Monday / 7-14 day checks if needed here too, using relaxation_level=0 logic
643|             if self.scheduler.gap_between_shifts == 1 and work_percentage >= 20: # Corrected: work_percentage from worker_data
644|                 if days_between == 3:
645|                     if ((prev_date.weekday() == 4 and date.weekday() == 0) or \
646|                         (date.weekday() == 4 and prev_date.weekday() == 0)):
647|                         return False
648|             # Add check for weekly pattern (7/14 day)
649|             if (days_between == 7 or days_between == 14) and date.weekday() == prev_date.weekday():
650|                 return False
651|         return True
652| 
653|     def _would_exceed_weekend_limit_simulated(self, worker_id, date, simulated_assignments):
654|         """Check weekend limit using simulated assignments."""
655|         # Check if date is a weekend/holiday
656|         is_target_weekend = date.weekday() >= 4 or date in self.scheduler.holidays
657|         if not is_target_weekend:
658|             return False  # Not a weekend/holiday, so no limit applies
659|     
660|         # Get worker data to check work_percentage
661|         worker_data = next((w for w in self.scheduler.workers_data if w['id'] == worker_id), None)
662|         work_percentage = worker_data.get('work_percentage', 100) if worker_data else 100
663|     
664|         # Calculate max_weekend_count based on work_percentage
665|         max_weekend_count = self.scheduler.max_consecutive_weekends
666|         if work_percentage < 70:
667|             max_weekend_count = max(1, int(self.scheduler.max_consecutive_weekends * work_percentage / 100))
668|     
669|         # Get weekend assignments and add the current date
670|         weekend_dates = []
671|         for d_val in simulated_assignments.get(worker_id, set()): # Renamed d to d_val
672|             if d_val.weekday() >= 4 or d_val in self.scheduler.holidays:
673|                 weekend_dates.append(d_val)
674|     
675|         # Add the date if it's not already in the list
676|         if date not in weekend_dates:
677|             weekend_dates.append(date)
678|     
679|         # Sort dates to ensure chronological order
680|         weekend_dates.sort()
681|     
682|         # Check for consecutive weekends
683|         consecutive_groups = []
684|         current_group = []
685|     
686|         for i, d_val in enumerate(weekend_dates): # Renamed d to d_val
687|             # Start a new group or add to the current one
688|             if not current_group:
689|                 current_group = [d_val]
690|             else:
691|                 # Get the previous weekend's date
692|                 prev_weekend = current_group[-1]
693|                 # Calculate days between this weekend and the previous one
694|                 days_diff = (d_val - prev_weekend).days
695|             
696|                 # Checking if they are adjacent weekend dates (7-10 days apart)
697|                 # A weekend is consecutive to the previous if it's the next calendar weekend
698|                 # This is typically 7 days apart, but could be 6-8 days depending on which weekend days
699|                 if 5 <= days_diff <= 10:
700|                     current_group.append(d_val)
701|                 else:
702|                     # Not consecutive, save the current group and start a new one
703|                     if len(current_group) > 1:  # Only save groups with more than 1 weekend
704|                         consecutive_groups.append(current_group)
705|                     current_group = [d_val]
706|     
707|         # Add the last group if it has more than 1 weekend
708|         if len(current_group) > 1:
709|             consecutive_groups.append(current_group)
710|     
711|         # Find the longest consecutive sequence
712|         max_consecutive = 0
713|         if consecutive_groups:
714|             max_consecutive = max(len(group) for group in consecutive_groups)
715|         else:
716|             max_consecutive = 1  # No consecutive weekends found, or only single weekends
717|     
718|         # Check if maximum consecutive weekend count is exceeded
719|         if max_consecutive > max_weekend_count:
720|             logging.debug(f"Weekend limit exceeded: Worker {worker_id} would have {max_consecutive} consecutive weekend shifts (max allowed: {max_weekend_count})")
721|             return True
722|     
723|         return False
724|     
725|     def _calculate_worker_score(self, worker, date, post, relaxation_level=0):
726|         """
727|         Calculate score for a worker assignment with optional relaxation of constraints
728|     
729|         Args:
730|             worker: The worker to evaluate
731|             date: The date to assign
732|             post: The post number to assign
733|             relaxation_level: Level of constraint relaxation (0=strict, 1=moderate, 2=lenient)
734|     
735|         Returns:
736|             float: Score for this worker-date-post combination, higher is better
737|                   Returns float('-inf') if assignment is invalid
738|         """
739|         try:
740|             worker_id = worker['id']
741|             score = 0
742|         
743|             # --- Hard Constraints (never relaxed) ---\
744|         
745|             # Basic availability check
746|             if self._is_worker_unavailable(worker_id, date) or worker_id in self.schedule.get(date, []):
747|                 return float('-inf')
748|             
749|             # Check incompatibility against workers already assigned on this date (excluding the current post being considered)
750|             already_assigned_on_date = [w for idx, w in enumerate(self.schedule.get(date, [])) if w is not None and idx != post]
751|             if not self._check_incompatibility_with_list(worker_id, already_assigned_on_date):
752|                  logging.debug(f"Score check fail: Worker {worker_id} incompatible on {date}")
753|                  return float('-inf')
754|             
755|             # --- Check for mandatory shifts ---\
756|             worker_data = worker
757|             mandatory_days_str = worker_data.get('mandatory_days', '') # Corrected: mandatory_days_str
758|             mandatory_dates = self._parse_dates(mandatory_days_str) # Corrected: mandatory_days_str
759|         
760|             # If this is a mandatory date for this worker, give it maximum priority
761|             if date in mandatory_dates:
762|                 return float('inf')  # Highest possible score to ensure mandatory shifts are assigned
763|         
764|             # --- Target Shifts Check (excluding mandatory shifts) ---\
765|             current_shifts = len(self.worker_assignments[worker_id])
766|             target_shifts = worker.get('target_shifts', 0)
767|         
768|             # Count mandatory shifts that are already assigned
769|             mandatory_shifts_assigned = sum(\
770|                 1 for d in self.worker_assignments[worker_id] if d in mandatory_dates\
771|             )
772|         
773|             # Count mandatory shifts still to be assigned
774|             mandatory_shifts_remaining = sum(\
775|                 1 for d in mandatory_dates \
776|                 if d >= date and d not in self.worker_assignments[worker_id]\
777|             )
778|         
779|             # Calculate non-mandatory shifts target
780|             non_mandatory_target = target_shifts - len(mandatory_dates)
781|             non_mandatory_assigned = current_shifts - mandatory_shifts_assigned
782|         
783|             # Check if we've already met or exceeded non-mandatory target
784|             shift_difference = non_mandatory_target - non_mandatory_assigned
785|         
786|             # Reserve capacity for remaining mandatory shifts
787|             if non_mandatory_assigned + mandatory_shifts_remaining >= target_shifts and relaxation_level < 2:
788|                 return float('-inf')  # Need to reserve remaining slots for mandatory shifts
789|         
790|             # Stop if worker already met or exceeded non-mandatory target (except at higher relaxation)
791|             if shift_difference <= 0:
792|                 if relaxation_level < 2:
793|                     return float('-inf')  # Strict limit at relaxation levels 0-1
794|                 else:
795|                     score -= 10000  # Severe penalty but still possible at highest relaxation
796|             else:
797|                 # Higher priority for workers further from their non-mandatory target
798|                 score += shift_difference * 1000
799| 
800|             # --- MONTHLY TARGET CHECK ---\
801|             month_key = f"{date.year}-{date.month:02d}"
802|             monthly_targets = worker.get('monthly_targets', {})
803|             target_this_month = monthly_targets.get(month_key, 0)
804| 
805|             # Calculate current shifts assigned in this month
806|             shifts_this_month = 0
807|             if worker_id in self.scheduler.worker_assignments: # Use scheduler reference
808|                  for assigned_date in self.scheduler.worker_assignments[worker_id]:
809|                       if assigned_date.year == date.year and assigned_date.month == date.month:
810|                            shifts_this_month += 1
811| 
812|             # Define the acceptable range (+/- 1 from target)
813|             min_monthly = max(0, target_this_month - 1)
814|             max_monthly = target_this_month + 1
815| 
816|             monthly_diff = target_this_month - shifts_this_month
817| 
818|             # Penalize HARD if assignment goes over max_monthly + 1 (allow max+1 only)
819|             if shifts_this_month >= max_monthly + 1 and relaxation_level < 2:
820|                  logging.debug(f"Worker {worker_id} rejected for {date.strftime('%Y-%m-%d')}: Would exceed monthly max+1 ({shifts_this_month + 1} > {max_monthly})")
821|                  return float('-inf')
822|             elif shifts_this_month >= max_monthly and relaxation_level < 1:
823|                  logging.debug(f"Worker {worker_id} rejected for {date.strftime('%Y-%m-%d')}: Would exceed monthly max ({shifts_this_month + 1} > {max_monthly}) at relax level {relaxation_level}") # Corrected log string
824|                  return float('-inf')
825| 
826| 
827|             # Strong bonus if worker is below min_monthly for this month
828|             if shifts_this_month < min_monthly:
829|                  # Bonus increases the further below min they are
830|                  score += (min_monthly - shifts_this_month) * 2500 # High weight for monthly need
831|                  logging.debug(f"Worker {worker_id} gets monthly bonus: below min ({shifts_this_month} < {min_monthly})")
832|             # Moderate bonus if worker is within the target range but below target
833|             elif shifts_this_month < target_this_month:
834|                  score += 500 # Bonus for needing shifts this month
835|                  logging.debug(f"Worker {worker_id} gets monthly bonus: below target ({shifts_this_month} < {target_this_month})")
836|             # Penalty if worker is already at or above max_monthly
837|             elif shifts_this_month >= max_monthly:
838|                  score -= (shifts_this_month - max_monthly + 1) * 1500 # Penalty increases the further above max they go
839|                  logging.debug(f"Worker {worker_id} gets monthly penalty: at/above max ({shifts_this_month} >= {max_monthly})")
840|          
841| 
842|             # --- Gap Constraints ---\
843|             assignments = sorted(list(self.worker_assignments[worker_id]))
844|             if assignments:
845|                 work_percentage = worker.get('work_percentage', 100)
846|                 # Use configurable gap parameter (minimum gap is higher for part-time workers)
847|                 min_gap = self.gap_between_shifts + 2 if work_percentage < 70 else self.gap_between_shifts + 1
848|     
849|                 # Check if any previous assignment violates minimum gap
850|                 for prev_date in assignments:
851|                     days_between = abs((date - prev_date).days)
852|         
853|                     # Basic minimum gap check
854|                     if days_between < min_gap:
855|                         return float('-inf')
856|         
857|                     # Special rule No Friday + Monday (3-day gap)
858|                     if relaxation_level == 0 and self.gap_between_shifts == 1:
859|                         if ((prev_date.weekday() == 4 and date.weekday() == 0) or \
860|                             (date.weekday() == 4 and prev_date.weekday() == 0)):
861|                             if days_between == 3:
862|                                 return float('-inf')
863|                 
864|                     # Prevent same day of week in consecutive weeks (can be relaxed)
865|                     if relaxation_level < 2 and (days_between == 7 or days_between == 14) and date.weekday() == prev_date.weekday():
866|                         return float('-inf')
867|         
868|             # --- Weekend Limits ---\
869|             if relaxation_level < 2 and self._would_exceed_weekend_limit(worker_id, date):
870|                 return float('-inf')
871|         
872|             # --- Weekday Balance Check ---\
873|             weekday = date.weekday()
874|             weekday_counts = self.worker_weekdays[worker_id].copy()
875|             weekday_counts[weekday] += 1  # Simulate adding this assignment
876|         
877|             max_weekday = max(weekday_counts.values())
878|             min_weekday = min(weekday_counts.values())
879|         
880|             # If this assignment would create more than 1 day difference, reject it
881|             if (max_weekday - min_weekday) > 1 and relaxation_level < 1:
882|                 return float('-inf')
883|         
884|             # --- Scoring Components (softer constraints) ---\
885| 
886|             # 1. Overall Target Score (Reduced weight compared to monthly)
887|             if shift_difference > 0:
888|                  score += shift_difference * 500 # Reduced weight
889|             elif shift_difference <=0 and relaxation_level >= 2:
890|                  score -= 5000 # Keep penalty if over overall target at high relaxation
891|         
892|             # 2. Weekend Balance Score
893|             if date.weekday() >= 4:  # Friday, Saturday, Sunday
894|                 weekend_assignments = sum(\
895|                     1 for d in self.worker_assignments[worker_id]\
896|                     if d.weekday() >= 4\
897|                 )
898|                 # Lower score for workers with more weekend assignments
899|                 score -= weekend_assignments * 300
900| 
901|         
902|             # 4. Weekly Balance Score - avoid concentration in some weeks
903|             week_number = date.isocalendar()[1]
904|             week_counts = {}
905|             for d_val in self.worker_assignments[worker_id]: # Renamed d to d_val
906|                 w = d_val.isocalendar()[1]
907|                 week_counts[w] = week_counts.get(w, 0) + 1
908|         
909|             current_week_count = week_counts.get(week_number, 0)
910|             avg_week_count = len(assignments) / max(1, len(week_counts)) if week_counts else 0 # Added check for empty week_counts
911|         
912|             if current_week_count < avg_week_count:
913|                 score += 500  # Bonus for weeks with fewer assignments
914|         
915|             # 5. Schedule Progression Score - adjust priority as schedule fills up
916|             schedule_completion = sum(len(s) for s in self.schedule.values()) / (\
917|                 (self.end_date - self.start_date).days * self.num_shifts) if (self.end_date - self.start_date).days > 0 else 0 # Added check for zero days
918|         
919|             # Higher weight for target difference as schedule progresses
920|             score += shift_difference * 500 * schedule_completion
921|         
922|             # Log the score calculation
923|             logging.debug(f"Score for worker {worker_id}: {score} "\
924|                         f"(current: {current_shifts}, target: {target_shifts}, "\
925|                         f"relaxation: {relaxation_level})")
926|         
927|             return score
928|     
929|         except Exception as e:
930|             logging.error(f"Error calculating score for worker {worker['id']}: {str(e)}")
931|             return float('-inf')
932| 
933|     def _calculate_improvement_score(self, worker, date, post):
934|         """
935|         Calculate a score for a worker assignment during the improvement phase.
936|     
937|         This uses a more lenient scoring approach to encourage filling empty shifts.
938|         """
939|         worker_id = worker['id']
940|     
941|         # Base score from standard calculation
942|         base_score = self._calculate_worker_score(worker, date, post)
943|     
944|         # If base score is negative infinity, the assignment is invalid
945|         if base_score == float('-inf'):
946|             return float('-inf')
947|     
948|         # Bonus for balancing post rotation
949|         post_counts = self._get_post_counts(worker_id)
950|         total_assignments = sum(post_counts.values())
951|     
952|         # Skip post balance check for workers with few assignments
953|         if total_assignments >= self.num_shifts and self.num_shifts > 0: # Added check for num_shifts > 0
954|             expected_per_post = total_assignments / self.num_shifts
955|             current_count = post_counts.get(post, 0)
956|         
957|             # Give bonus if this post is underrepresented for this worker
958|             if current_count < expected_per_post:
959|                 base_score += 10 * (expected_per_post - current_count)
960|     
961|         # Bonus for balancing workload
962|         work_percentage = worker.get('work_percentage', 100)
963|         current_assignments = len(self.worker_assignments[worker_id])
964|     
965|         # Calculate average assignments per worker, adjusted for work percentage
966|         total_assignments_all = sum(len(self.worker_assignments[w_data['id']]) for w_data in self.workers_data) # Corrected: w_data
967|         total_work_percentage = sum(w_data.get('work_percentage', 100) for w_data in self.workers_data) # Corrected: w_data
968|     
969|         # Expected assignments based on work percentage
970|         expected_assignments = (total_assignments_all / (total_work_percentage / 100)) * (work_percentage / 100) if total_work_percentage > 0 else 0 # Added check for total_work_percentage
971|     
972|         # Bonus for underloaded workers
973|         if current_assignments < expected_assignments:
974|             base_score += 5 * (expected_assignments - current_assignments)
975|     
976|         return base_score
977| 
978| # 5. Schedule Generation Methods
979|             
980|     def _assign_mandatory_guards(self):
981|         logging.info("Starting mandatory guard assignment…")
982|         assigned_count = 0
983|         for worker in self.workers_data: # Use self.workers_data
984|             worker_id = worker['id']
985|             mandatory_str = worker.get('mandatory_days', '')
986|             try:
987|                 dates = self.date_utils.parse_dates(mandatory_str)
988|             except Exception as e:
989|                 logging.error(f"Error parsing mandatory_days for worker {worker_id}: {e}")
990|                 continue
991| 
992|             for date in dates:
993|                 if not (self.start_date <= date <= self.end_date): continue
994| 
995|                 if date not in self.schedule: # self.schedule is scheduler.schedule
996|                     self.schedule[date] = [None] * self.num_shifts
997|                 
998|                 # Try to place in any available post for that date
999|                 placed_mandatory = False
1000|                 for post in range(self.num_shifts):
1001|                     if len(self.schedule[date]) <= post: self.schedule[date].extend([None] * (post + 1 - len(self.schedule[date])))
1002| 
1003|                     if self.schedule[date][post] is None:
1004|                         # Check incompatibility before placing
1005|                         others_on_date = [w for i, w in enumerate(self.schedule.get(date, [])) if i != post and w is not None]
1006|                         if not self._check_incompatibility_with_list(worker_id, others_on_date):
1007|                             logging.debug(f"Mandatory shift for {worker_id} on {date.strftime('%Y-%m-%d')} post {post} incompatible. Trying next post.")
1008|                             continue
1009|                         
1010|                         self.schedule[date][post] = worker_id
1011|                         self.worker_assignments.setdefault(worker_id, set()).add(date) # Use self.worker_assignments
1012|                         self.scheduler._update_tracking_data(worker_id, date, post, removing=False) # Call scheduler's central update
1013|                         self._locked_mandatory.add((worker_id, date)) # Lock it
1014|                         logging.debug(f"Assigned worker {worker_id} to {date.strftime('%Y-%m-%d')} post {post} (mandatory) and locked.")
1015|                         assigned_count += 1
1016|                         placed_mandatory = True
1017|                         break 
1018|                 if not placed_mandatory:
1019|                      logging.warning(f"Could not place mandatory shift for {worker_id} on {date.strftime('%Y-%m-%d')}. All posts filled or incompatible.")
1020|         
1021|         logging.info(f"Finished mandatory guard assignment. Assigned {assigned_count} shifts.")
1022|         # No _save_current_as_best here; scheduler's generate_schedule will handle it after this.
1023|         # self._synchronize_tracking_data() # Ensure builder's view is also synced if it has separate copies (it shouldn't for core data)
1024|         return assigned_count > 0
1025|     
1026|     def _get_remaining_dates_to_process(self, forward):
1027|         """Get remaining dates that need to be processed"""
1028|         dates_to_process = []
1029|         current = self.start_date
1030|     
1031|         # Get all dates in period that are not weekends or holidays
1032|         # or that already have some assignments but need more
1033|         while current <= self.end_date:
1034|             # for each date, check if we need to generate more shifts
1035|             if current not in self.schedule:
1036|                 dates_to_process.append(current)
1037|             else:
1038|                 # compare actual slots vs configured for that date
1039|                 expected = self.scheduler._get_shifts_for_date(current)
1040|                 if len(self.schedule[current]) < expected:
1041|                     dates_to_process.append(current)
1042|             current += timedelta(days=1)
1043|     
1044|         # Sort based on direction
1045|         if forward:
1046|             dates_to_process.sort()
1047|         else:
1048|             dates_to_process.sort(reverse=True)
1049|     
1050|         return dates_to_process
1051|     
1052|     def _assign_day_shifts_with_relaxation(self, date, attempt_number=50, relaxation_level=0):
1053|         """Assign shifts for a given date with optional constraint relaxation"""
1054|         logging.debug(f"Assigning shifts for {date.strftime('%d-%m-%Y')} (attempt: {attempt_number}, initial relax: {relaxation_level})")
1055| 
1056|         # Ensure the date entry exists and is a list
1057|         if date not in self.schedule:
1058|             self.schedule[date] = []
1059|         # Ensure it's padded to current length if it exists but is shorter than previous post assignments
1060|         # (This shouldn't happen often but safeguards against potential inconsistencies)
1061|         current_len = len(self.schedule.get(date, []))
1062|         max_post_assigned_prev = -1
1063|         if current_len > 0:
1064|              max_post_assigned_prev = current_len -1
1065| 
1066| 
1067|         # Determine how many slots this date actually has (supports variable shifts)
1068|         start_post = len(self.schedule.get(date, []))
1069|         total_slots = self.scheduler._get_shifts_for_date(date) # Corrected: Use scheduler method
1070|         for post in range(start_post, total_slots):
1071|             # ← NEW: never overwrite a locked mandatory shift
1072|             # Check if self.schedule[date] is long enough before accessing by index
1073|             if len(self.schedule.get(date,[])) > post and (self.schedule[date][post] is not None and (self.schedule[date][post], date) in self._locked_mandatory) :
1074|                 continue
1075|             assigned_this_post = False
1076|             for relax_level in range(relaxation_level + 1): 
1077|                 candidates = self._get_candidates(date, post, relax_level)
1078| 
1079|                 logging.debug(f"Found {len(candidates)} candidates for {date.strftime('%d-%m-%Y')}, post {post}, relax level {relax_level}")
1080| 
1081|                 if candidates:
1082|                     # Log top candidates if needed
1083|                     # for i, (worker, score) in enumerate(candidates[:3]):
1084|                     #     logging.debug(f"  Candidate {i+1}: Worker {worker['id']} with score {score:.2f}")
1085| 
1086|                     # Sort candidates by score (descending)
1087|                     candidates.sort(key=lambda x: x[1], reverse=True)
1088| 
1089|                     # --- Try assigning the first compatible candidate ---\
1090|                     for candidate_worker, candidate_score in candidates:
1091|                         worker_id = candidate_worker['id']
1092| 
1093|                         # *** DEBUG LOGGING - START ***\
1094|                         current_assignments_on_date = [w for w in self.schedule.get(date, []) if w is not None]
1095|                         logging.debug(f"CHECKING: Date={date}, Post={post}, Candidate={worker_id}, CurrentlyAssigned={current_assignments_on_date}")
1096|                         # *** DEBUG LOGGING - END ***\
1097| 
1098|                         # *** EXPLICIT INCOMPATIBILITY CHECK ***\
1099|                         # Temporarily add logging INSIDE the check function call might also help, or log its result explicitly
1100|                         is_compatible = self._check_incompatibility_with_list(worker_id, current_assignments_on_date)
1101|                         logging.debug(f"  -> Incompatibility Check Result: {is_compatible}") # Log the result
1102| 
1103|                         # if not self._check_incompatibility_with_list(worker_id, current_assignments_on_date):
1104|                         if not is_compatible: # Use the variable to make logging easier
1105|                             logging.debug(f"  Skipping candidate {worker_id} for post {post} on {date}: Incompatible with current assignments on this date.")
1106|                             continue # Try next candidate
1107| 
1108|                         # *** If compatible, assign this worker ***\
1109|                         # Ensure list is long enough before assigning by index
1110|                         while len(self.schedule[date]) <= post:
1111|                              self.schedule[date].append(None)
1112| 
1113|                         # Double check slot is still None before assigning (paranoid check)
1114|                         if self.schedule[date][post] is None:
1115|                             self.schedule[date][post] = worker_id # Assign to the correct post index
1116|                             self.worker_assignments.setdefault(worker_id, set()).add(date)
1117|                             self.scheduler._update_tracking_data(worker_id, date, post)
1118| 
1119|                             logging.info(f"Assigned worker {worker_id} to {date.strftime('%d-%m-%Y')}, post {post} (Score: {candidate_score:.2f}, Relax: {relax_level})")
1120|                             assigned_this_post = True
1121|                             break # Found a compatible worker for this post, break candidate loop
1122|                         else:
1123|                             # This case should be rare if logic is correct, but log it
1124|                             logging.warning(f"  Slot {post} on {date} was unexpectedly filled before assigning candidate {worker_id}. Current value: {self.schedule[date][post]}")
1125|                             # Continue to the next candidate, as this one cannot be placed here anymore
1126| 
1127| 
1128|                     if assigned_this_post:
1129|                         break # Success at this relaxation level, break relaxation loop
1130|                     else:
1131|                         # If loop finishes without assigning (no compatible candidates found at this relax level)
1132|                         logging.debug(f"No compatible candidate found for post {post} at relax level {relax_level}")
1133|                 else:
1134|                      logging.debug(f"No candidates found for post {post} at relax level {relax_level}")
1135| 
1136| 
1137|             # --- Handle case where post remains unfilled after trying all relaxation levels ---\
1138|             if not assigned_this_post:
1139|                  # Ensure list is long enough before potentially assigning None
1140|                  while len(self.schedule[date]) <= post:
1141|                       self.schedule[date].append(None)
1142| 
1143|                  # Only log warning if the slot is genuinely still None
1144|                  if self.schedule[date][post] is None:
1145|                       logging.warning(f"No suitable worker found for {date.strftime('%d-%m-%Y')}, post {post} - shift unfilled after all checks.")
1146|                  # Else: it might have been filled by a mandatory assignment earlier, which is fine.
1147| 
1148|         # --- Ensure schedule[date] list has the correct final length ---\
1149|         # Pad with None if necessary, e.g., if initial assignment skipped posts
1150|         while len(self.schedule.get(date, [])) < self.num_shifts:
1151|              self.schedule.setdefault(date, []).append(None) # Use setdefault for safety if date somehow disappeared
1152| 
1153|     def _get_candidates(self, date, post, relaxation_level=0):
1154|         """
1155|         Get suitable candidates with their scores using the specified relaxation level
1156|     
1157|         Args:
1158|             date: The date to assign
1159|             post: The post number to assign
1160|             relaxation_level: Level of constraint relaxation (0=strict, 1=moderate, 2=lenient)
1161|         """
1162|         candidates = []
1163|         logging.debug(f"Looking for candidates for {date.strftime('%d-%m-%Y')}, post {post}")
1164| 
1165|         # Get workers already assigned to other posts on this date
1166|         already_assigned_on_date = [w for idx, w in enumerate(self.schedule.get(date, [])) if w is not None and idx != post]
1167| 
1168|         for worker in self.workers_data:
1169|             worker_id = worker['id']
1170|             logging.debug(f"Checking worker {worker_id} for {date.strftime('%d-%m-%Y')}")
1171| 
1172|             # --- PRE-FILTERING ---\
1173|             # Skip if already assigned to this date (redundant with score check, but safe)
1174|             if worker_id in self.schedule.get(date, []): # Check against all posts on this date
1175|                  logging.debug(f"  Worker {worker_id} skipped - already assigned to {date.strftime('%d-%m-%Y')}")
1176|                  continue
1177| 
1178|             # Skip if unavailable
1179|             if self._is_worker_unavailable(worker_id, date):
1180|                  logging.debug(f"  Worker {worker_id} skipped - unavailable on {date.strftime('%d-%m-%Y')}")
1181|                  continue
1182| 
1183|             # *** ADDED: Explicit Incompatibility Check BEFORE scoring ***\
1184|             # Never relax incompatibility constraint
1185|             if not self._check_incompatibility_with_list(worker_id, already_assigned_on_date):
1186|                  logging.debug(f"  Worker {worker_id} skipped - incompatible with already assigned workers on {date.strftime('%d-%m-%Y')}")
1187|                  continue
1188|             # Skip if max shifts reached
1189|             if len(self.worker_assignments[worker_id]) >= self.max_shifts_per_worker:
1190|                 logging.debug(f"Worker {worker_id} skipped - max shifts reached: {len(self.worker_assignments[worker_id])}/{self.max_shifts_per_worker}")
1191|                 continue
1192| 
1193|             # Calculate score using the main scoring function
1194|             score = self._calculate_worker_score(worker, date, post, relaxation_level)
1195|             
1196|             if score > float('-inf'): # Only add valid candidates
1197|                 logging.debug(f"Worker {worker_id} added as candidate with score {score}")
1198|                 candidates.append((worker, score))
1199| 
1200|         return candidates
1201| 
1202|     # 6. Schedule Improvement Methods
1203| 
1204|     def _try_fill_empty_shifts(self):
1205|         """
1206|         Try to fill empty shifts in the authoritative self.schedule.
1207|         Pass 1: Direct assignment.
1208|         Pass 2: Attempt swaps for remaining empty shifts.
1209|         """
1210|         logging.debug(f"ENTERED _try_fill_empty_shifts. self.schedule ID: {id(self.schedule)}. Keys count: {len(self.schedule.keys())}. Sample: {dict(list(self.schedule.items())[:2])}")
1211| 
1212|         initial_empty_slots = []
1213|         for date_val, workers_in_posts in self.schedule.items(): # Renamed date to date_val
1214|             for post_index, worker_in_post in enumerate(workers_in_posts):
1215|                 if worker_in_post is None:
1216|                     initial_empty_slots.append((date_val, post_index))
1217|         
1218|         logging.debug(f"[_try_fill_empty_shifts] Initial identified empty_slots count: {len(initial_empty_slots)}")
1219|         if not initial_empty_slots:
1220|             logging.info(f"--- No initial empty shifts to fill. ---")
1221|             return False
1222| 
1223|         logging.info(f"Attempting to fill {len(initial_empty_slots)} empty shifts...")
1224|         initial_empty_slots.sort(key=lambda x: (x[0], x[1])) # Process chronologically, then by post
1225| 
1226|         shifts_filled_this_pass_total = 0
1227|         made_change_overall = False
1228|         remaining_empty_shifts_after_pass1 = []
1229| 
1230|         logging.info("--- Starting Pass 1: Direct Fill ---")
1231|         for date_val, post_val in initial_empty_slots: # Renamed date, post
1232|             if self.schedule[date_val][post_val] is not None:
1233|                 logging.debug(f"[Pass 1] Slot ({date_val.strftime('%Y-%m-%d')}, {post_val}) already filled by {self.schedule[date_val][post_val]}. Skipping.")
1234|                 continue
1235|             
1236|             assigned_this_post_pass1 = False
1237|             pass1_candidates = []
1238| 
1239|             for worker_data_val in self.workers_data: # Renamed worker_data
1240|                 worker_id_val = worker_data_val['id'] # Renamed worker_id
1241|                 logging.debug(f"  [Pass 1 Candidate Check] Worker: {worker_id_val} for Date: {date_val.strftime('%Y-%m-%d')}, Post: {post_val}")
1242|                 
1243|                 # Use _calculate_worker_score with strict relaxation (0) for direct fill
1244|                 score = self._calculate_worker_score(worker_data_val, date_val, post_val, relaxation_level=0)
1245| 
1246|                 if score > float('-inf'):
1247|                     logging.debug(f"    -> Pass1 ACCEPTED as candidate: Worker {worker_id_val} for {date_val.strftime('%Y-%m-%d')} Post {post_val} with score {score}")
1248|                     pass1_candidates.append((worker_data_val, score))
1249|                 else:
1250|                     logging.debug(f"    -> Pass1 REJECTED (Score Check): Worker {worker_id_val} for {date_val.strftime('%Y-%m-%d')} Post {post_val}")
1251|             
1252|             if pass1_candidates:
1253|                 pass1_candidates.sort(key=lambda x: x[1], reverse=True)
1254|                 logging.debug(f"  [Pass 1] Candidates for {date_val.strftime('%Y-%m-%d')} Post {post_val}: {[(c[0]['id'], c[1]) for c in pass1_candidates]}")
1255|                 # In Pass 1, we only try the top candidate that is strictly valid
1256|                 candidate_worker_data, candidate_score = pass1_candidates[0]
1257|                 worker_id_to_assign = candidate_worker_data['id']
1258|                 
1259|                 # Final check, though _calculate_worker_score should have caught most issues
1260|                 if self.schedule[date_val][post_val] is None: 
1261|                     others_now = [w for i, w in enumerate(self.schedule.get(date_val, [])) if i != post_val and w is not None]
1262|                     if not self._check_incompatibility_with_list(worker_id_to_assign, others_now):
1263|                         logging.debug(f"    -> Pass1 Assignment REJECTED (Last Minute Incompat): W:{worker_id_to_assign} for {date_val.strftime('%Y-%m-%d')} P:{post_val}")
1264|                     else:
1265|                         self.schedule[date_val][post_val] = worker_id_to_assign
1266|                         self.worker_assignments.setdefault(worker_id_to_assign, set()).add(date_val)
1267|                         self.scheduler._update_tracking_data(worker_id_to_assign, date_val, post_val, removing=False)
1268|                         logging.info(f"[Pass 1 Direct Fill] Filled empty shift on {date_val.strftime('%Y-%m-%d')} Post {post_val} with W:{worker_id_to_assign} (Score: {candidate_score:.2f})")
1269|                         shifts_filled_this_pass_total += 1
1270|                         made_change_overall = True
1271|                         assigned_this_post_pass1 = True
1272|                 else: 
1273|                     assigned_this_post_pass1 = True 
1274|             
1275|             if not assigned_this_post_pass1 and self.schedule[date_val][post_val] is None:
1276|                 remaining_empty_shifts_after_pass1.append((date_val, post_val))
1277|                 logging.debug(f"Could not find compatible direct candidate in Pass 1 for {date_val.strftime('%Y-%m-%d')} Post {post_val}.")
1278| 
1279|         if not remaining_empty_shifts_after_pass1:
1280|             logging.info(f"--- Finished Pass 1. No remaining empty shifts for Pass 2. ---")
1281|         else:
1282|             logging.info(f"--- Finished Pass 1. Starting Pass 2: Attempting swaps for {len(remaining_empty_shifts_after_pass1)} empty shifts ---")
1283|             for date_empty, post_empty in remaining_empty_shifts_after_pass1:
1284|                 if self.schedule[date_empty][post_empty] is not None:
1285|                     logging.warning(f"[Pass 2 Swap] Slot ({date_empty.strftime('%Y-%m-%d')}, {post_empty}) no longer empty. Skipping.")
1286|                     continue
1287|                 swap_found = False
1288|                 potential_W_data = list(self.workers_data); random.shuffle(potential_W_data)
1289|                 for worker_W_data in potential_W_data:
1290|                     worker_W_id = worker_W_data['id']
1291|                     if not self.worker_assignments.get(worker_W_id): continue
1292|                     
1293|                     original_W_assignments = list(self.worker_assignments[worker_W_id]); random.shuffle(original_W_assignments)
1294|                     for date_conflict in original_W_assignments:
1295|                         if (worker_W_id, date_conflict) in self._locked_mandatory: continue
1296|                         try: post_conflict = self.schedule[date_conflict].index(worker_W_id)
1297|                         except (ValueError, KeyError, IndexError): continue
1298| 
1299|                         # Use _can_swap_assignments to check if W can move to the empty slot
1300|                         # AND if a suitable replacement X can be found for W's original slot
1301|                         
1302|                         # Simulate W moving out of (date_conflict, post_conflict)
1303|                         # This part is tricky because _can_swap_assignments does its own simulation
1304|                         # We need to find an X first.
1305| 
1306|                         # Can worker W take the empty slot (date_empty, post_empty)?
1307|                         # Simulate W removed from its original spot (date_conflict, post_conflict)
1308|                         # and check if it can be placed at (date_empty, post_empty)
1309|                         
1310|                         # Create a temporary state for checking W's move to empty
1311|                         temp_schedule_for_W_check = copy.deepcopy(self.schedule)
1312|                         temp_assignments_for_W_check = copy.deepcopy(self.worker_assignments)
1313|                         
1314|                         # Remove W from original conflict spot in temp
1315|                         temp_schedule_for_W_check[date_conflict][post_conflict] = None
1316|                         if worker_W_id in temp_assignments_for_W_check and date_conflict in temp_assignments_for_W_check[worker_W_id]:
1317|                             temp_assignments_for_W_check[worker_W_id].remove(date_conflict)
1318|                         
1319|                         # Check if W can be assigned to the empty slot in this temp state
1320|                         can_W_take_empty_simulated = self._check_constraints_on_simulated(
1321|                             worker_W_id, date_empty, post_empty, 
1322|                             temp_schedule_for_W_check, temp_assignments_for_W_check
1323|                         )
1324| 
1325|                         if not can_W_take_empty_simulated:
1326|                             continue # W cannot even take the empty slot
1327| 
1328|                         # Now, find a worker X who can take W's original spot (date_conflict, post_conflict)
1329|                         # The spot (date_conflict, post_conflict) is now considered empty for this check
1330|                         worker_X_id = self._find_swap_candidate(worker_W_id, date_conflict, post_conflict)
1331| 
1332|                         if worker_X_id:
1333|                             logging.info(f"[Pass 2 Swap Attempt] W:{worker_W_id} ({date_conflict.strftime('%Y-%m-%d')},P{post_conflict}) -> ({date_empty.strftime('%Y-%m-%d')},P{post_empty}); X:{worker_X_id} -> ({date_conflict.strftime('%Y-%m-%d')},P{post_conflict})")
1334|                             # Actual execution of swap (W takes empty, X takes W's old spot)
1335|                             
1336|                             # 1. Remove W from original spot
1337|                             self.schedule[date_conflict][post_conflict] = None
1338|                             self.worker_assignments[worker_W_id].remove(date_conflict)
1339|                             self.scheduler._update_tracking_data(worker_W_id, date_conflict, post_conflict, removing=True)
1340| 
1341|                             # 2. Assign X to W's original spot
1342|                             self.schedule[date_conflict][post_conflict] = worker_X_id
1343|                             self.worker_assignments.setdefault(worker_X_id, set()).add(date_conflict)
1344|                             self.scheduler._update_tracking_data(worker_X_id, date_conflict, post_conflict, removing=False)
1345|                             
1346|                             # 3. Assign W to the empty spot
1347|                             self.schedule[date_empty][post_empty] = worker_W_id
1348|                             self.worker_assignments[worker_W_id].add(date_empty)
1349|                             self.scheduler._update_tracking_data(worker_W_id, date_empty, post_empty, removing=False)
1350|                             
1351|                             shifts_filled_this_pass_total += 1; made_change_overall = True; swap_found = True; break
1352|                     if swap_found: break
1353|                 if not swap_found: logging.debug(f"No swap for empty {date_empty.strftime('%Y-%m-%d')} P{post_empty}")
1354|         
1355|         logging.info(f"--- Finished _try_fill_empty_shifts. Total filled/swapped: {shifts_filled_this_pass_total} ---")
1356|         if made_change_overall:
1357|             self._synchronize_tracking_data()
1358|             self._save_current_as_best()
1359|         return made_change_overall
1360| 
1361|     
1362|     def _find_swap_candidate(self, worker_W_id, conflict_date, conflict_post):
1363|         """
1364|         Finds a worker (X) who can take the shift at (conflict_date, conflict_post),
1365|         ensuring they are not worker_W_id and not already assigned on that date.
1366|         Uses strict constraints (_can_assign_worker via constraint_checker or _calculate_worker_score).
1367|         Assumes (conflict_date, conflict_post) is currently "empty" for the purpose of this check,
1368|         as worker_W is hypothetically moved out.
1369|         """
1370|         potential_X_workers = [
1371|             w_data for w_data in self.scheduler.workers_data 
1372|             if w_data['id'] != worker_W_id and \
1373|                w_data['id'] not in self.scheduler.schedule.get(conflict_date, []) 
1374|         ]
1375|         random.shuffle(potential_X_workers)
1376| 
1377|         for worker_X_data in potential_X_workers:
1378|             worker_X_id = worker_X_data['id']
1379|             
1380|             # Check if X can strictly take W's old slot (which is now considered notionally empty)
1381|             # We use _calculate_worker_score with relaxation_level=0 for a comprehensive check
1382|             # The schedule state for this check should reflect W being absent from conflict_date/post
1383|             
1384|             # Simulate W's absence for X's check
1385|             sim_schedule_for_X = copy.deepcopy(self.scheduler.schedule)
1386|             if conflict_date in sim_schedule_for_X and len(sim_schedule_for_X[conflict_date]) > conflict_post:
1387|                 # Only set to None if it was W, to be safe, though it should be.
1388|                 if sim_schedule_for_X[conflict_date][conflict_post] == worker_W_id:
1389|                      sim_schedule_for_X[conflict_date][conflict_post] = None
1390|             
1391|             # Temporarily use the simulated schedule for this specific score calculation for X
1392|             original_schedule_ref = self.schedule # Keep original ref
1393|             self.schedule = sim_schedule_for_X # Temporarily point to sim
1394|             
1395|             score_for_X = self._calculate_worker_score(worker_X_data, conflict_date, conflict_post, relaxation_level=0)
1396|             
1397|             self.schedule = original_schedule_ref # Restore original ref
1398| 
1399|             if score_for_X > float('-inf'): # If X can be assigned
1400|                  logging.debug(f"Found valid swap candidate X={worker_X_id} for W={worker_W_id}'s slot ({conflict_date.strftime('%Y-%m-%d')},{conflict_post}) with score {score_for_X}")
1401|                  return worker_X_id
1402| 
1403|         logging.debug(f"No suitable swap candidate X found for W={worker_W_id}'s slot ({conflict_date.strftime('%Y-%m-%d')},{conflict_post})")
1404|         return None
1405|     
1406|     def _balance_weekend_shifts(self):
1407|         """
1408|         Balance weekend/holiday shifts across workers based on their percentage of working days.
1409|         Each worker should have approximately:
1410|         (total_shifts_for_worker) * (total_weekend_days / total_days) shifts on weekends/holidays, ±1.
1411|         """
1412|         logging.info("Balancing weekend and holiday shifts among workers...")
1413|         fixes_made = 0
1414|     
1415|         # Calculate the total days and weekend/holiday days in the schedule period
1416|         total_days_in_period = (self.end_date - self.start_date).days + 1 # Renamed total_days
1417|         weekend_days_in_period = sum(1 for d_val in self.date_utils.generate_date_range(self.start_date, self.end_date) # Renamed d, use generate_date_range
1418|                       if self.date_utils.is_weekend_day(d_val) or d_val in self.holidays)
1419|     
1420|         # Calculate the target percentage
1421|         weekend_percentage = weekend_days_in_period / total_days_in_period if total_days_in_period > 0 else 0
1422|         logging.info(f"Schedule period has {weekend_days_in_period} weekend/holiday days out of {total_days_in_period} total days ({weekend_percentage:.1%})")
1423|     
1424|         # Check each worker's current weekend shift allocation
1425|         workers_to_check = self.workers_data.copy()
1426|         random.shuffle(workers_to_check)  # Process in random order
1427|     
1428|         for worker_val in workers_to_check: # Renamed worker
1429|             worker_id_val = worker_val['id'] # Renamed worker_id
1430|             assignments = self.worker_assignments.get(worker_id_val, set())
1431|             total_shifts = len(assignments)
1432|         
1433|             if total_shifts == 0:
1434|                 continue  # Skip workers with no assignments
1435|             
1436|             # Count weekend assignments for this worker
1437|             weekend_shifts = sum(1 for date_val in assignments # Renamed date
1438|                                 if self.date_utils.is_weekend_day(date_val) or date_val in self.holidays)
1439|         
1440|             # Calculate target weekend shifts for this worker
1441|             target_weekend_shifts = total_shifts * weekend_percentage
1442|             deviation = weekend_shifts - target_weekend_shifts
1443|             allowed_deviation = 1.0  # Allow ±1 shift from perfect distribution
1444|         
1445|             logging.debug(f"Worker {worker_id_val}: {weekend_shifts} weekend shifts, target {target_weekend_shifts:.2f}, deviation {deviation:.2f}")
1446|         
1447|             # Case 1: Worker has too many weekend shifts
1448|             if deviation > allowed_deviation:
1449|                 logging.info(f"Worker {worker_id_val} has too many weekend shifts ({weekend_shifts}, target {target_weekend_shifts:.2f})")
1450|                 swap_found = False
1451|             
1452|                 # Find workers with too few weekend shifts to swap with
1453|                 potential_swap_partners = []
1454|                 for other_worker_val in self.workers_data: # Renamed other_worker
1455|                     other_id = other_worker_val['id']
1456|                     if other_id == worker_id_val:
1457|                         continue
1458|                 
1459|                     other_total = len(self.worker_assignments.get(other_id, []))
1460|                     if other_total == 0:
1461|                         continue
1462|                     
1463|                     other_weekend = sum(1 for d_val in self.worker_assignments.get(other_id, []) # Renamed d
1464|                                        if self.date_utils.is_weekend_day(d_val) or d_val in self.holidays)
1465|                                     
1466|                     other_target = other_total * weekend_percentage
1467|                     other_deviation = other_weekend - other_target
1468|                 
1469|                     if other_deviation < -allowed_deviation:
1470|                         potential_swap_partners.append((other_id, other_deviation))
1471|             
1472|                 # Sort potential partners by how under-assigned they are
1473|                 potential_swap_partners.sort(key=lambda x: x[1])
1474|             
1475|                 # Try to swap a weekend shift from this worker to an under-assigned worker
1476|                 if potential_swap_partners:
1477|                     for swap_partner_id, _ in potential_swap_partners:
1478|                         # Find a weekend assignment from this worker to swap
1479|                         possible_from_dates = [d_val for d_val in assignments # Renamed d
1480|                                              if (self.date_utils.is_weekend_day(d_val) or d_val in self.holidays)\
1481|                                              and not self._is_mandatory(worker_id_val, d_val)]
1482|                     
1483|                         if not possible_from_dates:
1484|                             continue  # No swappable weekend shifts
1485|                         
1486|                         random.shuffle(possible_from_dates)
1487|                     
1488|                         for from_date in possible_from_dates:
1489|                             # Find the post this worker is assigned to
1490|                             from_post = self.schedule[from_date].index(worker_id_val)
1491|                         
1492|                             # Find a weekday assignment from the swap partner that could be exchanged
1493|                             partner_assignments = self.worker_assignments.get(swap_partner_id, set())
1494|                             possible_to_dates = [d_val for d_val in partner_assignments # Renamed d
1495|                                                if not (self.date_utils.is_weekend_day(d_val) or d_val in self.holidays)\
1496|                                                and not self._is_mandatory(swap_partner_id, d_val)]
1497|                         
1498|                             if not possible_to_dates:
1499|                                 continue  # No swappable weekday shifts for partner
1500|                             
1501|                             random.shuffle(possible_to_dates)
1502|                         
1503|                             for to_date in possible_to_dates:
1504|                                 # Find the post the partner is assigned to
1505|                                 to_post = self.schedule[to_date].index(swap_partner_id)
1506|                             
1507|                                 # Check if swap is valid (worker1 <-> worker2)
1508|                                 if self._can_worker_swap(worker_id_val, from_date, from_post, swap_partner_id, to_date, to_post): # Corrected: _can_worker_swap
1509|                                     # Execute worker-worker swap
1510|                                     self._execute_worker_swap(worker_id_val, from_date, from_post, swap_partner_id, to_date, to_post)
1511|                                     logging.info(f"Swapped weekend shift: Worker {worker_id_val} on {from_date.strftime('%Y-%m-%d')} with "\
1512|                                                f"Worker {swap_partner_id} on {to_date.strftime('%Y-%m-%d')}")
1513|                                     fixes_made += 1
1514|                                     swap_found = True
1515|                                     break
1516|                         
1517|                             if swap_found:
1518|                                 break
1519|                     
1520|                         if swap_found:
1521|                             break
1522|                         
1523|             # Case 2: Worker has too few weekend shifts
1524|             elif deviation < -allowed_deviation:
1525|                 logging.info(f"Worker {worker_id_val} has too few weekend shifts ({weekend_shifts}, target {target_weekend_shifts:.2f})")
1526|                 swap_found = False
1527|             
1528|                 # Find workers with too many weekend shifts to swap with
1529|                 potential_swap_partners = []
1530|                 for other_worker_val in self.workers_data: # Renamed other_worker
1531|                     other_id = other_worker_val['id']
1532|                     if other_id == worker_id_val:
1533|                         continue
1534|                 
1535|                     other_total = len(self.worker_assignments.get(other_id, []))
1536|                     if other_total == 0:
1537|                         continue
1538|                     
1539|                     other_weekend = sum(1 for d_val in self.worker_assignments.get(other_id, []) # Renamed d
1540|                                        if self.date_utils.is_weekend_day(d_val) or d_val in self.holidays)
1541|                                     
1542|                     other_target = other_total * weekend_percentage
1543|                     other_deviation = other_weekend - other_target
1544|                 
1545|                     if other_deviation > allowed_deviation:
1546|                         potential_swap_partners.append((other_id, other_deviation))
1547|             
1548|                 # Sort potential partners by how over-assigned they are
1549|                 potential_swap_partners.sort(key=lambda x: -x[1])
1550|             
1551|                 # Implementation similar to above but with roles reversed
1552|                 if potential_swap_partners:
1553|                     for swap_partner_id, _ in potential_swap_partners:
1554|                         # Find a weekend assignment from the partner to swap
1555|                         partner_assignments = self.worker_assignments.get(swap_partner_id, set())
1556|                         possible_from_dates = [d_val for d_val in partner_assignments # Renamed d
1557|                                              if (self.date_utils.is_weekend_day(d_val) or d_val in self.holidays)\
1558|                                              and not self._is_mandatory(swap_partner_id, d_val)]
1559|                     
1560|                         if not possible_from_dates:
1561|                             continue
1562|                         
1563|                         random.shuffle(possible_from_dates)
1564|                     
1565|                         for from_date in possible_from_dates:
1566|                             from_post = self.schedule[from_date].index(swap_partner_id)
1567|                         
1568|                             # Find a weekday assignment from this worker
1569|                             possible_to_dates = [d_val for d_val in assignments # Renamed d
1570|                                                if not (self.date_utils.is_weekend_day(d_val) or d_val in self.holidays)\
1571|                                                and not self._is_mandatory(worker_id_val, d_val)]
1572|                         
1573|                             if not possible_to_dates:
1574|                                 continue
1575|                             
1576|                             random.shuffle(possible_to_dates)
1577|                         
1578|                             for to_date in possible_to_dates:
1579|                                 to_post = self.schedule[to_date].index(worker_id_val)
1580|                             
1581|                                 # Check if swap is valid (partner <-> this worker)
1582|                                 if self._can_worker_swap(swap_partner_id, from_date, from_post, worker_id_val, to_date, to_post): # Corrected: _can_worker_swap
1583|                                     self._execute_worker_swap(swap_partner_id, from_date, from_post, worker_id_val, to_date, to_post)
1584|                                     logging.info(f"Swapped weekend shift: Worker {swap_partner_id} on {from_date.strftime('%Y-%m-%d')} with "\
1585|                                                f"Worker {worker_id_val} on {to_date.strftime('%Y-%m-%d')}")
1586|                                     fixes_made += 1
1587|                                     swap_found = True
1588|                                     break
1589|                         
1590|                             if swap_found:
1591|                                 break
1592|                     
1593|                         if swap_found:
1594|                             break
1595|     
1596|         logging.info(f"Weekend shift balancing: made {fixes_made} changes")
1597|         if fixes_made > 0:
1598|             self._save_current_as_best()
1599|         return fixes_made > 0
1600| 
1601|     def _execute_worker_swap(self, worker1_id, date1, post1, worker2_id, date2, post2):
1602|         """
1603|         Swap two workers' assignments between dates/posts.
1604|     
1605|         Args:
1606|             worker1_id: First worker's ID
1607|             date1: First worker's date
1608|             post1: First worker's post
1609|             worker2_id: Second worker's ID
1610|             date2: Second worker's date
1611|             post2: Second worker's post
1612|         """
1613|         # Ensure both workers are currently assigned as expected
1614|         if (self.schedule[date1][post1] != worker1_id or
1615|             self.schedule[date2][post2] != worker2_id):
1616|             logging.error(f"Worker swap failed: Workers not in expected positions")
1617|             return False
1618|     
1619|         # Swap the workers in the schedule
1620|         self.schedule[date1][post1] = worker2_id
1621|         self.schedule[date2][post2] = worker1_id
1622|     
1623|         # Update worker_assignments for both workers
1624|         self.worker_assignments[worker1_id].remove(date1)
1625|         self.worker_assignments[worker1_id].add(date2)
1626|         self.worker_assignments[worker2_id].remove(date2)
1627|         self.worker_assignments[worker2_id].add(date1)
1628|     
1629|         # Update tracking data for both workers
1630|         self.scheduler._update_tracking_data(worker1_id, date1, post1, removing=True)
1631|         self.scheduler._update_tracking_data(worker1_id, date2, post2)
1632|         self.scheduler._update_tracking_data(worker2_id, date2, post2, removing=True)
1633|         self.scheduler._update_tracking_data(worker2_id, date1, post1)
1634|     
1635|         return True
1636|         
1637|     def _identify_imbalanced_posts(self, deviation_threshold=1.5):
1638|         """
1639|         Identifies workers with an imbalanced distribution of assigned posts.
1640| 
1641|         Args:
1642|             deviation_threshold: How much the count for a single post can deviate
1643|                                  from the average before considering the worker imbalanced.
1644| 
1645|         Returns:
1646|             List of tuples: [(worker_id, post_counts, max_deviation), ...]
1647|                            Sorted by max_deviation descending.
1648|         """
1649|         imbalanced_workers = []
1650|         num_posts = self.num_shifts
1651|         if num_posts == 0: return [] # Avoid division by zero
1652| 
1653|         # Use scheduler's worker data and post tracking
1654|         for worker_val in self.scheduler.workers_data: # Renamed worker
1655|             worker_id_val = worker_val['id'] # Renamed worker_id
1656|             # Get post counts, defaulting to an empty dict if worker has no assignments yet
1657|             actual_post_counts = self.scheduler.worker_posts.get(worker_id_val, {})
1658|             total_assigned = sum(actual_post_counts.values())
1659| 
1660|             # If worker has no shifts or only one type of post, they can't be imbalanced yet
1661|             if total_assigned == 0 or num_posts <= 1:
1662|                 continue
1663| 
1664|             target_per_post = total_assigned / num_posts
1665|             max_deviation = 0
1666|             post_deviations = {} # Store deviation per post
1667| 
1668|             for post_val in range(num_posts): # Renamed post
1669|                 actual_count = actual_post_counts.get(post_val, 0)
1670|                 deviation = actual_count - target_per_post
1671|                 post_deviations[post_val] = deviation
1672|                 if abs(deviation) > max_deviation:
1673|                     max_deviation = abs(deviation)
1674| 
1675|             # Consider imbalanced if the count for any post is off by more than the threshold
1676|             if max_deviation > deviation_threshold:
1677|                 # Store the actual counts, not the deviations map for simplicity
1678|                 imbalanced_workers.append((worker_id_val, actual_post_counts.copy(), max_deviation))
1679|                 logging.debug(f"Worker {worker_id_val} identified as imbalanced for posts. Max Deviation: {max_deviation:.2f}, Target/Post: {target_per_post:.2f}, Counts: {actual_post_counts}")
1680| 
1681| 
1682|         # Sort by the magnitude of imbalance (highest deviation first)
1683|         imbalanced_workers.sort(key=lambda x: x[2], reverse=True)
1684|         return imbalanced_workers
1685| 
1686|     def _get_over_under_posts(self, post_counts, total_assigned, balance_threshold=1.0):
1687|         """
1688|         Given a worker's post counts, find which posts they have significantly
1689|         more or less than the average.
1690| 
1691|         Args:
1692|             post_counts (dict): {post_index: count} for the worker.
1693|             total_assigned (int): Total shifts assigned to the worker.
1694|             balance_threshold: How far from the average count triggers over/under.
1695| 
1696|         Returns:
1697|             tuple: (list_of_overassigned_posts, list_of_underassigned_posts)
1698|                    Each list contains tuples: [(post_index, count), ...]\
1699|                    Sorted by deviation magnitude.
1700|         """
1701|         overassigned = []
1702|         underassigned = []
1703|         num_posts = self.num_shifts
1704|         if num_posts <= 1 or total_assigned == 0:
1705|             return [], [] # Cannot be over/under assigned
1706| 
1707|         target_per_post = total_assigned / num_posts
1708| 
1709|         for post_val in range(num_posts): # Renamed post
1710|             actual_count = post_counts.get(post_val, 0)
1711|             deviation = actual_count - target_per_post
1712| 
1713|             # Use a threshold slightly > 0 to avoid minor float issues
1714|             # Consider overassigned if count is clearly higher than target
1715|             if deviation > balance_threshold:
1716|                 overassigned.append((post_val, actual_count, deviation)) # Include deviation for sorting
1717|             # Consider underassigned if count is clearly lower than target
1718|             elif deviation < -balance_threshold:
1719|                  underassigned.append((post_val, actual_count, deviation)) # Deviation is negative
1720| 
1721|         # Sort overassigned: highest count (most over) first
1722|         overassigned.sort(key=lambda x: x[2], reverse=True)
1723|         # Sort underassigned: lowest count (most under) first (most negative deviation)
1724|         underassigned.sort(key=lambda x: x[2])
1725| 
1726|         # Return only (post, count) tuples
1727|         overassigned_simple = [(p, c) for p, c, d_val in overassigned] # Renamed d to d_val
1728|         underassigned_simple = [(p, c) for p, c, d_val in underassigned] # Renamed d to d_val
1729| 
1730|         return overassigned_simple, underassigned_simple
1731|     
1732|     def _balance_workloads(self):
1733|         """
1734|         """
1735|         logging.info("Attempting to balance worker workloads")
1736|         # Ensure data consistency before proceeding
1737|         self._ensure_data_integrity()
1738| 
1739|         # First verify and fix data consistency
1740|         self._verify_assignment_consistency()
1741| 
1742|         # Count total assignments for each worker
1743|         assignment_counts = {}
1744|         for worker_val in self.workers_data: # Renamed worker
1745|             worker_id_val = worker_val['id'] # Renamed worker_id
1746|             work_percentage = worker_val.get('work_percentage', 100)
1747|     
1748|             # Count assignments
1749|             count = len(self.worker_assignments[worker_id_val])
1750|     
1751|             # Normalize by work percentage
1752|             normalized_count = count * 100 / work_percentage if work_percentage > 0 else 0
1753|     
1754|             assignment_counts[worker_id_val] = {\
1755|                 'worker_id': worker_id_val,\
1756|                 'count': count,\
1757|                 'work_percentage': work_percentage,\
1758|                 'normalized_count': normalized_count\
1759|             }    
1760| 
1761|         # Calculate average normalized count
1762|         total_normalized = sum(data['normalized_count'] for data in assignment_counts.values())
1763|         avg_normalized = total_normalized / len(assignment_counts) if assignment_counts else 0
1764| 
1765|         # Identify overloaded and underloaded workers
1766|         overloaded = []
1767|         underloaded = []
1768| 
1769|         for worker_id_val, data_val in assignment_counts.items(): # Renamed worker_id, data
1770|             # Allow 10% deviation from average
1771|             if data_val['normalized_count'] > avg_normalized * 1.1:
1772|                 overloaded.append((worker_id_val, data_val))
1773|             elif data_val['normalized_count'] < avg_normalized * 0.9:
1774|                 underloaded.append((worker_id_val, data_val))
1775| 
1776|         # Sort by most overloaded/underloaded
1777|         overloaded.sort(key=lambda x: x[1]['normalized_count'], reverse=True)
1778|         underloaded.sort(key=lambda x: x[1]['normalized_count'])
1779| 
1780|         changes_made = 0
1781|         max_changes = 30  # Limit number of changes to avoid disrupting the schedule too much
1782| 
1783|         # Try to redistribute shifts from overloaded to underloaded workers
1784|         for over_worker_id, over_data in overloaded:
1785|             if changes_made >= max_changes or not underloaded:
1786|                 break
1787|         
1788|             # Find shifts that can be reassigned from this overloaded worker
1789|             possible_shifts = []
1790|     
1791|             for date_val in sorted(self.scheduler.worker_assignments.get(over_worker_id, set())): # Renamed date
1792|                 # never touch a locked mandatory
1793|                 if (over_worker_id, date_val) in self._locked_mandatory:
1794|                     logging.debug(f"Skipping workload‐balance move for mandatory shift: {over_worker_id} on {date_val}")
1795|                     continue
1796| 
1797|                 # --- MANDATORY CHECK --- (you already had this, but now enforced globally)
1798|                 # skip if this date is mandatory for this worker
1799|                 if self._is_mandatory(over_worker_id, date_val):
1800|                     continue
1801| 
1802|             
1803|                 # Make sure the worker is actually in the schedule for this date
1804|                 if date_val not in self.schedule:
1805|                     # This date is in worker_assignments but not in schedule
1806|                     logging.warning(f"Worker {over_worker_id} has assignment for date {date_val} but date is not in schedule")
1807|                     continue
1808|                 
1809|                 try:
1810|                     # Find the post this worker is assigned to
1811|                     if over_worker_id not in self.schedule[date_val]:
1812|                         # Worker is supposed to be assigned to this date but isn't in the schedule
1813|                         logging.warning(f"Worker {over_worker_id} has assignment for date {date_val} but is not in schedule")
1814|                         continue
1815|                     
1816|                     post_val = self.schedule[date_val].index(over_worker_id) # Renamed post
1817|                     possible_shifts.append((date_val, post_val))
1818|                 except ValueError:
1819|                     # Worker not found in schedule for this date
1820|                     logging.warning(f"Worker {over_worker_id} has assignment for date {date_val} but is not in schedule")
1821|                     continue
1822|     
1823|             # Shuffle to introduce randomness
1824|             random.shuffle(possible_shifts)
1825|     
1826|             # Try each shift
1827|             for date_val, post_val in possible_shifts: # Renamed date, post
1828|                 reassigned = False
1829|                 for under_worker_id, _ in underloaded:
1830|                     # ... (check if under_worker already assigned) ...
1831|                     if self._can_assign_worker(under_worker_id, date_val, post_val):
1832|                         # remove only if it wasn't locked mandatory
1833|                         if (over_worker_id, date_val) in self._locked_mandatory:
1834|                             continue
1835|                         self.scheduler.schedule[date_val][post_val] = under_worker_id
1836|                         self.scheduler.worker_assignments[over_worker_id].remove(date_val)
1837|                         # Ensure under_worker tracking exists
1838|                         if under_worker_id not in self.scheduler.worker_assignments:
1839|                              self.scheduler.worker_assignments[under_worker_id] = set()
1840|                         self.scheduler.worker_assignments[under_worker_id].add(date_val)
1841| 
1842|                         # Update tracking data (Needs FIX: update for BOTH workers)
1843|                         self.scheduler._update_tracking_data(over_worker_id, date_val, post_val, removing=True) # Remove stats for over_worker
1844|                         self.scheduler._update_tracking_data(under_worker_id, date_val, post_val) # Add stats for under_worker
1845| 
1846|                         changes_made += 1
1847|                         logging.info(f"Balanced workload: Moved shift on {date_val.strftime('%Y-%m-%d')} post {post_val} from {over_worker_id} to {under_worker_id}")
1848|                         
1849|                         # Update counts
1850|                         assignment_counts[over_worker_id]['count'] -= 1
1851|                         assignment_counts[over_worker_id]['normalized_count'] = (\
1852|                             assignment_counts[over_worker_id]['count'] * 100 / \
1853|                             assignment_counts[over_worker_id]['work_percentage']\
1854|                         ) if assignment_counts[over_worker_id]['work_percentage'] > 0 else 0 # Added check for zero division
1855|                 
1856|                         assignment_counts[under_worker_id]['count'] += 1
1857|                         assignment_counts[under_worker_id]['normalized_count'] = (\
1858|                             assignment_counts[under_worker_id]['count'] * 100 / \
1859|                             assignment_counts[under_worker_id]['work_percentage']\
1860|                         ) if assignment_counts[under_worker_id]['work_percentage'] > 0 else 0 # Added check for zero division
1861|                 
1862|                         reassigned = True
1863|                 
1864|                         # Check if workers are still overloaded/underloaded
1865|                         if assignment_counts[over_worker_id]['normalized_count'] <= avg_normalized * 1.1:
1866|                             # No longer overloaded
1867|                             overloaded = [(w, d_val_loop) for w, d_val_loop in overloaded if w != over_worker_id] # Renamed d to d_val_loop
1868|                 
1869|                         if assignment_counts[under_worker_id]['normalized_count'] >= avg_normalized * 0.9:
1870|                             # No longer underloaded
1871|                             underloaded = [(w, d_val_loop) for w, d_val_loop in underloaded if w != under_worker_id] # Renamed d to d_val_loop
1872|                 
1873|                         break
1874|         
1875|                 if reassigned:
1876|                     break
1877|             
1878|                 if changes_made >= max_changes:
1879|                     break
1880| 
1881|         logging.info(f"Workload balancing: made {changes_made} changes")
1882|         if changes_made > 0:
1883|             self._save_current_as_best()
1884|         return changes_made > 0
1885| 
1886|     def _optimize_schedule(self, iterations=3):
1887|         """Main schedule optimization function"""
1888|         best_score = self._evaluate_schedule()
1889|         iterations_without_improvement = 0
1890|         max_iterations_without_improvement = iterations
1891| 
1892|         logging.info(f"Starting schedule optimization. Initial score: {best_score:.2f}")
1893| 
1894|         while iterations_without_improvement < max_iterations_without_improvement:
1895|             improved = False
1896|     
1897|             # 1. First perform all basic scheduling optimizations
1898|             if self._try_fill_empty_shifts():
1899|                 logging.info("Improved schedule by filling empty shifts")
1900|                 improved = True
1901|         
1902|             # 3. Try weekend/holiday balancing 
1903|             if self._balance_weekend_shifts():
1904|                 logging.info("Improved schedule through weekend shift balancing")
1905|                 improved = True
1906|         
1907|             # 4. Try shift target balancing
1908|             if self._balance_workloads():
1909|                 logging.info("Improved schedule through shift target balancing")
1910|                 improved = True
1911| 
1912|             # Check if we've improved with the regular optimization steps
1913|             current_score = self._evaluate_schedule()
1914|             if current_score > best_score:
1915|                 logging.info(f"Schedule improved. New score: {current_score:.2f} (was {best_score:.2f})")
1916|                 best_score = current_score
1917|                 iterations_without_improvement = 0
1918|             else:
1919|                 iterations_without_improvement += 1
1920|                 logging.info(f"No improvement in iteration. Score: {current_score:.2f}. "\
1921|                             f"{iterations_without_improvement}/{max_iterations_without_improvement}")
1922|         
1923|             # Break if we've reached maximum iterations without improvement
1924|             if iterations_without_improvement >= max_iterations_without_improvement:
1925|                 break
1926|     
1927|         # 5. AFTER all other swaps and optimizations are done,
1928|         # NOW perform the last post adjustments on SAME DAY only
1929|         self._synchronize_tracking_data()  # Add this line
1930|         if self._adjust_last_post_distribution():
1931|             logging.info("Improved schedule through last post adjustments")            # Check final score after last post adjustment
1932|             final_score = self._evaluate_schedule()
1933|             if final_score > best_score:
1934|                 best_score = final_score
1935|                 logging.info(f"Last post adjustment improved score to: {final_score:.2f}")
1936| 
1937|         logging.info(f"Optimization complete. Final score: {best_score:.2f}")
1938|         return best_score
1939| 
1940|     def _can_worker_swap(self, worker1_id, date1, post1, worker2_id, date2, post2):
1941|         """
1942|         Check if two workers can swap their assignments between dates/posts.
1943|         This method performs a comprehensive check of all constraints to ensure
1944|         that the swap would be valid according to the system's rules.
1945|     
1946|         Args:
1947|             worker1_id: First worker's ID
1948|             date1: First worker's date
1949|             post1: First worker's post
1950|             worker2_id: Second worker's ID
1951|             date2: Second worker's date
1952|             post2: Second worker's post
1953|     
1954|         Returns:
1955|             bool: True if the swap is valid, False otherwise
1956|         """
1957|         # First check: Make sure neither assignment is mandatory
1958|         if self._is_mandatory(worker1_id, date1) or self._is_mandatory(worker2_id, date2):
1959|             logging.debug(f"Swap rejected: Config-defined mandatory assignment detected by _is_mandatory. W1_mandatory: {self._is_mandatory(worker1_id, date1)}, W2_mandatory: {self._is_mandatory(worker2_id, date2)}") # Corrected log string
1960|             return False
1961|     
1962|         # Make a copy of the schedule and assignments to simulate the swap
1963|         schedule_copy = copy.deepcopy(self.schedule)
1964|         assignments_copy = {}
1965|         for worker_id_val, assignments_val in self.worker_assignments.items(): # Renamed worker_id, assignments
1966|             assignments_copy[worker_id_val] = set(assignments_val)
1967|     
1968|         # Simulate the swap
1969|         schedule_copy[date1][post1] = worker2_id
1970|         schedule_copy[date2][post2] = worker1_id
1971|     
1972|         # Update worker_assignments copies
1973|         assignments_copy[worker1_id].remove(date1)
1974|         assignments_copy[worker1_id].add(date2)
1975|         assignments_copy[worker2_id].remove(date2)
1976|         assignments_copy[worker2_id].add(date1)
1977|     
1978|         # Check all constraints for both workers in the simulated state
1979|     
1980|         # 1. Check incompatibility constraints for worker1 on date2
1981|         currently_assigned_date2 = [w for i, w in enumerate(schedule_copy[date2]) \
1982|                                    if w is not None and i != post2]
1983|         if not self._check_incompatibility_with_list(worker1_id, currently_assigned_date2):
1984|             logging.debug(f"Swap rejected: Worker {worker1_id} incompatible with workers on {date2}")
1985|             return False
1986|     
1987|         # 2. Check incompatibility constraints for worker2 on date1
1988|         currently_assigned_date1 = [w for i, w in enumerate(schedule_copy[date1]) \
1989|                                    if w is not None and i != post1]
1990|         if not self._check_incompatibility_with_list(worker2_id, currently_assigned_date1):
1991|             logging.debug(f"Swap rejected: Worker {worker2_id} incompatible with workers on {date1}")
1992|             return False
1993|     
1994|         # 3. Check minimum gap constraints for worker1
1995|         min_days_between = self.gap_between_shifts + 1
1996|         worker1_dates = sorted(list(assignments_copy[worker1_id]))
1997|     
1998|         for assigned_date_val in worker1_dates: # Renamed assigned_date
1999|             if assigned_date_val == date2:
2000|                 continue  # Skip the newly assigned date
2001|         
2002|             days_between = abs((date2 - assigned_date_val).days)
2003|             if days_between < min_days_between:
2004|                 logging.debug(f"Swap rejected: Worker {worker1_id} would have insufficient gap between {assigned_date_val} and {date2}")
2005|                 return False
2006|         
2007|             # Special case for Friday-Monday if gap is only 1 day
2008|             if self.gap_between_shifts == 1 and days_between == 3:
2009|                 if ((assigned_date_val.weekday() == 4 and date2.weekday() == 0) or \
2010|                     (date2.weekday() == 4 and assigned_date_val.weekday() == 0)):
2011|                     logging.debug(f"Swap rejected: Worker {worker1_id} would have Friday-Monday pattern")
2012|                     return False
2013|         
2014|             # NEW: Check for 7/14 day pattern (same day of week in consecutive weeks)
2015|             if (days_between == 7 or days_between == 14) and date2.weekday() == assigned_date_val.weekday():
2016|                 logging.debug(f"Swap rejected: Worker {worker1_id} would have {days_between} day pattern")
2017|                 return False
2018|     
2019|         # 4. Check minimum gap constraints for worker2
2020|         worker2_dates = sorted(list(assignments_copy[worker2_id]))
2021|     
2022|         for assigned_date_val in worker2_dates: # Renamed assigned_date
2023|             if assigned_date_val == date1:
2024|                 continue  # Skip the newly assigned date
2025|         
2026|             days_between = abs((date1 - assigned_date_val).days)
2027|             if days_between < min_days_between:
2028|                 logging.debug(f"Swap rejected: Worker {worker2_id} would have insufficient gap between {assigned_date_val} and {date1}")
2029|                 return False
2030|         
2031|             # Special case for Friday-Monday if gap is only 1 day
2032|             if self.gap_between_shifts == 1 and days_between == 3:
2033|                 if ((assigned_date_val.weekday() == 4 and date1.weekday() == 0) or \
2034|                     (date1.weekday() == 4 and assigned_date_val.weekday() == 0)):
2035|                     logging.debug(f"Swap rejected: Worker {worker2_id} would have Friday-Monday pattern")
2036|                     return False
2037|         
2038|             # NEW: Check for 7/14 day pattern (same day of week in consecutive weeks)
2039|             if (days_between == 7 or days_between == 14) and date1.weekday() == assigned_date_val.weekday():
2040|                 logging.debug(f"Swap rejected: Worker {worker2_id} would have {days_between} day pattern")
2041|                 return False
2042|         
2043|         # 5. Check weekend constraints for worker1
2044|         worker1_data_val = next((w for w in self.workers_data if w['id'] == worker1_id), None) # Renamed worker1 to worker1_data_val
2045|         if worker1_data_val:
2046|             worker1_weekend_dates = [d_val for d_val in worker1_dates # Renamed d to d_val
2047|                                     if self.date_utils.is_weekend_day(d_val) or d_val in self.holidays]
2048|         
2049|             # If the new date is a weekend/holiday, add it to the list
2050|             if self.date_utils.is_weekend_day(date2) or date2 in self.holidays:
2051|                 if date2 not in worker1_weekend_dates:
2052|                     worker1_weekend_dates.append(date2)
2053|                     worker1_weekend_dates.sort()
2054|         
2055|             # Check if this would violate max consecutive weekends
2056|             max_weekend_count = self.max_consecutive_weekends
2057|             work_percentage = worker1_data_val.get('work_percentage', 100)
2058|             if work_percentage < 100:
2059|                 max_weekend_count = max(1, int(self.max_consecutive_weekends * work_percentage / 100))
2060|         
2061|             for i, weekend_date_val in enumerate(worker1_weekend_dates): # Renamed weekend_date
2062|                 window_start = weekend_date_val - timedelta(days=10)
2063|                 window_end = weekend_date_val + timedelta(days=10)
2064|             
2065|                 # Count weekend/holiday dates in this window
2066|                 window_count = sum(1 for d_val in worker1_weekend_dates # Renamed d to d_val
2067|                                   if window_start <= d_val <= window_end)
2068|             
2069|                 if window_count > max_weekend_count:
2070|                     logging.debug(f"Swap rejected: Worker {worker1_id} would exceed weekend limit")
2071|                     return False
2072|     
2073|         # 6. Check weekend constraints for worker2
2074|         worker2_data_val = next((w for w in self.workers_data if w['id'] == worker2_id), None) # Renamed worker2 to worker2_data_val
2075|         if worker2_data_val:
2076|             worker2_weekend_dates = [d_val for d_val in worker2_dates # Renamed d to d_val
2077|                                     if self.date_utils.is_weekend_day(d_val) or d_val in self.holidays]
2078|         
2079|             # If the new date is a weekend/holiday, add it to the list
2080|             if self.date_utils.is_weekend_day(date1) or date1 in self.holidays:
2081|                 if date1 not in worker2_weekend_dates:
2082|                     worker2_weekend_dates.append(date1)
2083|                     worker2_weekend_dates.sort()
2084|         
2085|             # Check if this would violate max consecutive weekends
2086|             max_weekend_count = self.max_consecutive_weekends
2087|             work_percentage = worker2_data_val.get('work_percentage', 100)
2088|             if work_percentage < 100:
2089|                 max_weekend_count = max(1, int(self.max_consecutive_weekends * work_percentage / 100))
2090|         
2091|             for i, weekend_date_val in enumerate(worker2_weekend_dates): # Renamed weekend_date
2092|                 window_start = weekend_date_val - timedelta(days=10)
2093|                 window_end = weekend_date_val + timedelta(days=10)
2094|             
2095|                 # Count weekend/holiday dates in this window
2096|                 window_count = sum(1 for d_val in worker2_weekend_dates # Renamed d to d_val
2097|                                   if window_start <= d_val <= window_end)
2098|             
2099|                 if window_count > max_weekend_count:
2100|                     logging.debug(f"Swap rejected: Worker {worker2_id} would exceed weekend limit")
2101|                     return False
2102|     
2103|         # All constraints passed, the swap is valid
2104|         logging.debug(f"Swap between Worker {worker1_id} ({date1}/{post1}) and Worker {worker2_id} ({date2}/{post2}) is valid")
2105|         return True
2106| 
2107|     def _execute_swap(self, worker_id, date_from, post_from, worker_X_id, date_to, post_to):
2108|         """ Helper to perform the actual swap updates. Can handle either a single worker swap or a swap between two workers. """
2109|         # 1. Update schedule dictionary
2110|         self.scheduler.schedule[date_from][post_from] = None if worker_X_id is None else worker_X_id
2111|     
2112|         # Ensure target list is long enough before assignment
2113|         while len(self.scheduler.schedule[date_to]) <= post_to:
2114|             self.scheduler.schedule[date_to].append(None)
2115|         self.scheduler.schedule[date_to][post_to] = worker_id
2116| 
2117|         # 2. Update worker_assignments set for the first worker
2118|         # Check if the date exists in the worker's assignments before removing
2119|         if date_from in self.scheduler.worker_assignments.get(worker_id, set()):
2120|             self.scheduler.worker_assignments[worker_id].remove(date_from)
2121|     
2122|         # Add the new date to the worker's assignments
2123|         self.scheduler.worker_assignments.setdefault(worker_id, set()).add(date_to)
2124| 
2125|         # 3. Update worker_assignments for the second worker if present
2126|         if worker_X_id is not None:
2127|             # Check if the date exists in worker_X's assignments before removing
2128|             if date_to in self.scheduler.worker_assignments.get(worker_X_id, set()):
2129|                 self.scheduler.worker_assignments[worker_X_id].remove(date_to)
2130|         
2131|             # Add the from_date to worker_X's assignments
2132|             self.scheduler.worker_assignments.setdefault(worker_X_id, set()).add(date_from)
2133| 
2134|         # 4. Update detailed tracking stats for both workers
2135|         # Only update tracking data for removal if the worker was actually assigned to that date
2136|         if date_from in self.scheduler.worker_assignments.get(worker_id, set()) or (date_from in self.scheduler.schedule and self.scheduler.schedule[date_from].count(worker_id) > 0): # Corrected condition
2137|             self.scheduler._update_tracking_data(worker_id, date_from, post_from, removing=True)
2138|     
2139|         self.scheduler._update_tracking_data(worker_id, date_to, post_to)
2140|     
2141|         if worker_X_id is not None:
2142|             # Only update tracking data for removal if worker_X was actually assigned to that date
2143|             if date_to in self.scheduler.worker_assignments.get(worker_X_id, set()) or (date_to in self.scheduler.schedule and self.scheduler.schedule[date_to].count(worker_X_id) > 0): # Corrected condition
2144|                 self.scheduler._update_tracking_data(worker_X_id, date_to, post_to, removing=True)
2145|         
2146|             self.scheduler._update_tracking_data(worker_X_id, date_from, post_from)
2147|             
2148|     def _can_swap_assignments(self, worker_id, date_from, post_from, date_to, post_to): # This seems to be a duplicate definition, the one above at 445 is more complete
2149|         """ Checks if moving worker_id from (date_from, post_from) to (date_to, post_to) is valid """
2150|         # 1. Temporarily apply the swap to copies or directly (need rollback)
2151|         original_val_from = self.scheduler.schedule[date_from][post_from]
2152|         # Ensure target list is long enough for check
2153|         original_len_to = len(self.scheduler.schedule.get(date_to, []))
2154|         original_val_to = None
2155|         if original_len_to > post_to:
2156|             original_val_to = self.scheduler.schedule[date_to][post_to]
2157|         elif original_len_to == post_to: # Can append
2158|             pass
2159|         else: # Cannot place here if list isn't long enough and we aren't appending
2160|             return False
2161| 
2162|         self.scheduler.schedule[date_from][post_from] = None
2163|         # Ensure list exists and is long enough
2164|         self.scheduler.schedule.setdefault(date_to, [None] * self.num_shifts) # Ensure list exists
2165|         while len(self.scheduler.schedule[date_to]) <= post_to:
2166|              self.scheduler.schedule[date_to].append(None)
2167|         self.scheduler.schedule[date_to][post_to] = worker_id
2168|         if date_from in self.scheduler.worker_assignments.get(worker_id, set()):
2169|             self.scheduler.worker_assignments[worker_id].remove(date_from)
2170|         else:
2171|             logging.warning(\
2172|                 f"_can_swap_assignments: cannot remove {date_from} for worker {worker_id} — "\
2173|                 "date not tracked in worker_assignments"\
2174|             )
2175|         self.scheduler.worker_assignments[worker_id].add(date_to)
2176| 
2177|         # 2. Check constraints for BOTH dates with the new state
2178|         valid_from = self._check_all_constraints_for_date(date_from)
2179|         valid_to = self._check_all_constraints_for_date(date_to)
2180| 
2181|         # 3. Rollback the temporary changes
2182|         self.scheduler.schedule[date_from][post_from] = original_val_from # Should be worker_id
2183|         if original_len_to > post_to:
2184|             self.scheduler.schedule[date_to][post_to] = original_val_to # Should be None
2185|         elif original_len_to == post_to: # We appended, so remove
2186|             if len(self.scheduler.schedule.get(date_to,[])) > post_to : # Check if index exists before pop
2187|                 self.scheduler.schedule[date_to].pop(post_to) # Pop specific index if it was appended there
2188|         # If list was shorter and not appendable, we returned False earlier
2189| 
2190|         # Adjust list length if needed after pop
2191|         if date_to in self.scheduler.schedule and not self.scheduler.schedule[date_to]: # if list becomes empty
2192|             # Maybe don't delete empty dates? Or handle carefully.
2193|             # Let's assume empty lists are okay.
2194|             pass
2195| 
2196| 
2197|         self.scheduler.worker_assignments[worker_id].add(date_from)
2198|         self.scheduler.worker_assignments[worker_id].remove(date_to)
2199| 
2200| 
2201|         return valid_from and valid_to
2202| 
2203|     def _check_all_constraints_for_date(self, date):
2204|         """ Checks all constraints for all workers assigned on a given date. """
2205|         # Indent level 1
2206|         if date not in self.scheduler.schedule:
2207|             return True # No assignments, no violations
2208| 
2209|         assignments_on_date = self.scheduler.schedule[date]
2210|         workers_present = [w for w in assignments_on_date if w is not None]
2211| 
2212|         # Direct check for pairwise incompatibility on this date
2213|         for i in range(len(workers_present)):
2214|             # Indent level 2
2215|             for j in range(i + 1, len(workers_present)):
2216|                 # Indent level 3
2217|                 worker1_id = workers_present[i]
2218|                 worker2_id = workers_present[j]
2219|                 if self._are_workers_incompatible(worker1_id, worker2_id):
2220|                     # Indent level 4
2221|                     logging.debug(f"Constraint check failed (direct): Incompatibility between {worker1_id} and {worker2_id} on {date}")
2222|                     return False
2223| 
2224|         # Now check individual worker constraints (gap, weekend limits, etc.)
2225|         for post, worker_id in enumerate(assignments_on_date):
2226|             # Indent level 2
2227|             if worker_id is not None:
2228|                 # Indent level 3
2229|                 # Assuming _check_constraints uses live data from self.scheduler
2230|                 # Ensure the constraint checker method exists and is correctly referenced
2231|                 try:
2232|                     passed, reason = self.scheduler.constraint_checker._check_constraints(\
2233|                         worker_id,\
2234|                         date,\
2235|                         skip_constraints=False\
2236|                     )
2237|                     if not passed:
2238|                         logging.debug(f"Constraint violation for worker {worker_id} on {date}: {reason}")
2239|                         return False
2240|                 except AttributeError:
2241|                     logging.error("Constraint checker or _check_constraints method not found during swap validation.")
2242|                     return False
2243|                 except Exception as e_constr:
2244|                     logging.error(f"Error calling constraint checker for {worker_id} on {date}: {e_constr}", exc_info=True)
2245|                     return False
2246| 
2247|         # Indent level 1 (aligned with the initial 'if' and 'for' loops)
2248|         return True
2249| 
2250|     def _improve_weekend_distribution(self):
2251|         """
2252|         Improve weekend distribution by balancing weekend shifts more evenly among workers
2253|         and attempting to resolve weekend overloads
2254|         """
2255|         logging.info("Attempting to improve weekend distribution")
2256|     
2257|         # Ensure data consistency before proceeding
2258|         self._ensure_data_integrity()
2259| 
2260|         # Count weekend assignments for each worker by month
2261|         weekend_counts_by_month = {}
2262|         months = {}
2263|         current_date_iter = self.start_date # Renamed current_date
2264|         while current_date_iter <= self.end_date:
2265|             month_key = (current_date_iter.year, current_date_iter.month)
2266|             if month_key not in months: months[month_key] = []
2267|             months[month_key].append(current_date_iter)
2268|             current_date_iter += timedelta(days=1)
2269| 
2270|         for month_key, dates_in_month in months.items(): # Renamed dates
2271|             weekend_counts = {}
2272|             for worker_val in self.workers_data: # Renamed worker
2273|                 worker_id_val = worker_val['id'] # Renamed worker_id
2274|                 # Use scheduler references
2275|                 weekend_count = sum(1 for date_val in dates_in_month if date_val in self.scheduler.worker_assignments.get(worker_id_val, set()) and self.date_utils.is_weekend_day(date_val)) # Renamed date
2276|                 weekend_counts[worker_id_val] = weekend_count
2277|             weekend_counts_by_month[month_key] = weekend_counts
2278|     
2279|         changes_made = 0
2280|     
2281|          # Identify months with overloaded workers
2282|         for month_key, weekend_counts_val in weekend_counts_by_month.items(): # Renamed weekend_counts
2283|             overloaded_workers = []
2284|             underloaded_workers = []
2285| 
2286|             for worker_val in self.workers_data: # Renamed worker
2287|                 worker_id_val = worker_val['id'] # Renamed worker_id
2288|                 work_percentage = worker_val.get('work_percentage', 100)
2289|                 max_weekends = self.max_consecutive_weekends # Use configured param
2290|                 if work_percentage < 100:
2291|                     max_weekends = max(1, int(self.max_consecutive_weekends * work_percentage / 100))
2292| 
2293|                 weekend_count = weekend_counts_val.get(worker_id_val, 0)
2294| 
2295|                 if weekend_count > max_weekends:
2296|                     overloaded_workers.append((worker_id_val, weekend_count, max_weekends))
2297|                 elif weekend_count < max_weekends:
2298|                     available_slots = max_weekends - weekend_count
2299|                     underloaded_workers.append((worker_id_val, weekend_count, available_slots))
2300| 
2301|             overloaded_workers.sort(key=lambda x: x[1] - x[2], reverse=True)
2302|             underloaded_workers.sort(key=lambda x: x[2], reverse=True)
2303| 
2304|             month_dates = months[month_key]
2305|             # Use scheduler reference for holidays
2306|             weekend_dates_in_month = [date_val for date_val in month_dates if self.date_utils.is_weekend_day(date_val) or date_val in self.scheduler.holidays] # Renamed weekend_dates, date
2307| 
2308|             # Try to redistribute weekend shifts
2309|             for over_worker_id, over_count, over_limit in overloaded_workers:
2310|                 if not underloaded_workers: break # No one to give shifts to
2311| 
2312|                 # Iterate through potential dates to move FROM
2313|                 possible_dates_to_move = [wd for wd in weekend_dates_in_month if over_worker_id in self.scheduler.schedule.get(wd, [])]
2314|                 random.shuffle(possible_dates_to_move) # Randomize
2315| 
2316|                 for weekend_date_val in possible_dates_to_move: # Renamed weekend_date
2317| 
2318|                     # --- ADDED MANDATORY CHECK ---\
2319|                     if (over_worker_id, weekend_date_val) in self._locked_mandatory:
2320|                         logging.debug(f"Cannot move worker {over_worker_id} from locked mandatory weekend shift on {weekend_date_val.strftime('%Y-%m-%d')} for balancing.")
2321|                         continue
2322|                     if self._is_mandatory(over_worker_id, weekend_date_val): # Existing check
2323|                         logging.debug(f"Cannot move worker {over_worker_id} from config-mandatory weekend shift on {weekend_date_val.strftime('%Y-%m-%d')} for balancing.")
2324|                         continue
2325|                     # --- END ADDED CHECK ---\
2326| 
2327|                     # Find the post this worker is assigned to
2328|                     try:
2329|                         # Use scheduler reference
2330|                         post_val = self.scheduler.schedule[weekend_date_val].index(over_worker_id) # Renamed post
2331|                     except (ValueError, KeyError, IndexError):
2332|                          logging.warning(f"Inconsistency finding post for {over_worker_id} on {weekend_date_val} during weekend balance.")
2333|                          continue # Skip if schedule state is inconsistent
2334| 
2335|                     swap_done_for_date = False
2336|                     # Try to find a suitable replacement (underloaded worker X)
2337|                     for under_worker_id, _, _ in underloaded_workers:
2338|                         # Skip if X is already assigned on this date
2339|                         # Use scheduler reference
2340|                         if under_worker_id in self.scheduler.schedule[weekend_date_val]:
2341|                             continue
2342| 
2343|                         # Check if X can be assigned to this shift (using strict check)
2344|                         if self._can_assign_worker(under_worker_id, weekend_date_val, post_val):
2345|                             # Make the swap (directly modify scheduler's data)
2346|                             self.scheduler.schedule[weekend_date_val][post_val] = under_worker_id
2347|                             self.scheduler.worker_assignments[over_worker_id].remove(weekend_date_val)
2348|                             self.scheduler.worker_assignments.setdefault(under_worker_id, set()).add(weekend_date_val)
2349| 
2350|                             # Update tracking data for BOTH workers
2351|                             self.scheduler._update_tracking_data(over_worker_id, weekend_date_val, post_val, removing=True)
2352|                             self.scheduler._update_tracking_data(under_worker_id, weekend_date_val, post_val) # Corrected: removed removing=False
2353| 
2354|                             # Update local counts for this month
2355|                             weekend_counts_val[over_worker_id] -= 1
2356|                             weekend_counts_val[under_worker_id] = weekend_counts_val.get(under_worker_id, 0) + 1 # Ensure key exists
2357| 
2358|                             changes_made += 1
2359|                             logging.info(f"Improved weekend distribution: Moved weekend shift on {weekend_date_val.strftime('%Y-%m-%d')} "\
2360|                                          f"from worker {over_worker_id} to worker {under_worker_id}")
2361| 
2362|                             # Update overloaded/underloaded lists locally for this month
2363|                             if weekend_counts_val[over_worker_id] <= over_limit:
2364|                                 overloaded_workers = [(w, c, l) for w, c, l in overloaded_workers if w != over_worker_id]
2365| 
2366|                             for i, (w_id, count_val, slots_val) in enumerate(underloaded_workers): # Renamed count, slots
2367|                                 if w_id == under_worker_id:
2368|                                     # Check if worker X is now at their max for the month
2369|                                     worker_X_data = next((w for w in self.workers_data if w['id'] == under_worker_id), None)
2370|                                     worker_X_max = self.max_consecutive_weekends
2371|                                     if worker_X_data and worker_X_data.get('work_percentage', 100) < 100:
2372|                                         worker_X_max = max(1, int(self.max_consecutive_weekends * worker_X_data.get('work_percentage', 100) / 100))
2373| 
2374|                                     if weekend_counts_val[w_id] >= worker_X_max:
2375|                                         underloaded_workers.pop(i)
2376|                                     break # Found worker X in the list
2377| 
2378|                             swap_done_for_date = True
2379|                             break # Found a swap for this weekend_date, move to next overloaded worker
2380| 
2381|                 # If a swap was done for this overloaded worker, break to check the next (potentially new) most overloaded worker
2382|                 if swap_done_for_date:
2383|                      break # Break from the weekend_date loop for the current over_worker_id
2384| 
2385|         logging.info(f"Weekend distribution improvement: made {changes_made} changes")
2386|         if changes_made > 0:
2387|             self._save_current_as_best() # Save if changes were made
2388|         return changes_made > 0
2389| 
2390|     def _fix_incompatibility_violations(self):
2391|         """
2392|         Check the entire schedule for incompatibility violations and fix them
2393|         by reassigning incompatible workers to different days
2394|         """
2395|         logging.info("Checking and fixing incompatibility violations")
2396|     
2397|         violations_fixed = 0
2398|         violations_found = 0
2399|     
2400|         # Check each date for incompatible worker assignments
2401|         for date_val in sorted(self.schedule.keys()): # Renamed date
2402|             workers_today = [w for w in self.schedule[date_val] if w is not None]
2403|         
2404|             # Check each pair of workers
2405|             for i, worker1_id in enumerate(workers_today):
2406|                 for worker2_id in workers_today[i+1:]:
2407|                     # Check if these workers are incompatible
2408|                     if self._are_workers_incompatible(worker1_id, worker2_id):
2409|                         violations_found += 1
2410|                         logging.warning(f"Found incompatibility violation: {worker1_id} and {worker2_id} on {date_val}")
2411|                     
2412|                         # Try to fix the violation by moving one of the workers
2413|                         # Let's try to move the second worker first
2414|                         if self._try_reassign_worker(worker2_id, date_val):
2415|                             violations_fixed += 1
2416|                             logging.info(f"Fixed by reassigning {worker2_id} from {date_val}")
2417|                         # If that didn't work, try moving the first worker
2418|                         elif self._try_reassign_worker(worker1_id, date_val):
2419|                             violations_fixed += 1
2420|                             logging.info(f"Fixed by reassigning {worker1_id} from {date_val}")
2421|     
2422|         logging.info(f"Incompatibility check: found {violations_found} violations, fixed {violations_fixed}")
2423|         return violations_fixed > 0
2424|         
2425|     def _try_reassign_worker(self, worker_id, date):
2426|         """
2427|         Try to find a new date to assign this worker to fix an incompatibility
2428|         """
2429|         # --- ADD MANDATORY CHECK ---\
2430|         if (worker_id, date) in self._locked_mandatory:
2431|             logging.warning(f"Cannot reassign worker {worker_id} from locked mandatory shift on {date.strftime('%Y-%m-%d')} to fix incompatibility.")
2432|             return False
2433|         if self._is_mandatory(worker_id, date): # Existing check
2434|             logging.warning(f"Cannot reassign worker {worker_id} from config-mandatory shift on {date.strftime('%Y-%m-%d')} to fix incompatibility.")
2435|             return False
2436|         # --- END MANDATORY CHECK ---\
2437|         # Find the position this worker is assigned to
2438|         try:
2439|            post_val = self.schedule[date].index(worker_id) # Renamed post
2440|         except ValueError:
2441|             return False
2442|     
2443|         # First, try to find a date with an empty slot for the same post
2444|         current_date_iter = self.start_date # Renamed current_date
2445|         while current_date_iter <= self.end_date:
2446|             # Skip the current date
2447|             if current_date_iter == date:
2448|                 current_date_iter += timedelta(days=1)
2449|                 continue
2450|             
2451|             # Check if this date has an empty slot at the same post
2452|             if (current_date_iter in self.schedule and \
2453|                 len(self.schedule[current_date_iter]) > post_val and \
2454|                 self.schedule[current_date_iter][post_val] is None):
2455|             
2456|                 # Check if worker can be assigned to this date
2457|                 if self._can_assign_worker(worker_id, current_date_iter, post_val):
2458|                     # Remove from original date
2459|                     self.schedule[date][post_val] = None
2460|                     self.worker_assignments[worker_id].remove(date)
2461|                 
2462|                     # Assign to new date
2463|                     self.schedule[current_date_iter][post_val] = worker_id
2464|                     self.worker_assignments[worker_id].add(current_date_iter)
2465|                 
2466|                     # Update tracking data
2467|                     self._update_worker_stats(worker_id, date, removing=True)
2468|                     self.scheduler._update_tracking_data(worker_id, current_date_iter, post_val) # Corrected: was under_worker_id, weekend_date
2469|                 
2470|                     return True
2471|                 
2472|             current_date_iter += timedelta(days=1)
2473|     
2474|         # If we couldn't find a new assignment, just remove this worker
2475|         self.schedule[date][post_val] = None
2476|         self.worker_assignments[worker_id].remove(date)
2477|         self._update_worker_stats(worker_id, date, removing=True)
2478|     
2479|         return True
2480| 
2481|     def validate_mandatory_shifts(self):
2482|         """Validate that all mandatory shifts have been assigned"""
2483|         missing_mandatory = []
2484|     
2485|         for worker_val in self.workers_data: # Renamed worker
2486|             worker_id_val = worker_val['id'] # Renamed worker_id
2487|             mandatory_days_str = worker_val.get('mandatory_days', '') # Renamed mandatory_days
2488|         
2489|             if not mandatory_days_str:
2490|                 continue
2491|             
2492|             mandatory_dates_list = self.date_utils.parse_dates(mandatory_days_str) # Renamed mandatory_dates
2493|             for date_val in mandatory_dates_list: # Renamed date
2494|                 if date_val < self.start_date or date_val > self.end_date:
2495|                     continue  # Skip dates outside scheduling period
2496|                 
2497|                 # Check if worker is assigned on this date
2498|                 assigned = False
2499|                 if date_val in self.schedule:
2500|                     if worker_id_val in self.schedule[date_val]:
2501|                         assigned = True
2502|                     
2503|                 if not assigned:
2504|                     missing_mandatory.append((worker_id_val, date_val))
2505|     
2506|         return missing_mandatory
2507| 
2508|     def _apply_targeted_improvements(self, attempt_number):
2509|         """
2510|         Apply targeted improvements to the schedule. Runs multiple improvement steps.
2511|         Returns True if ANY improvement step made a change, False otherwise.
2512|         """
2513|         random.seed(1000 + attempt_number)
2514|         any_change_made = False
2515| 
2516|         logging.info(f"--- Starting Improvement Attempt {attempt_number} ---")
2517| 
2518|         # 1. Try to fill empty shifts (using direct fill and swaps)
2519|         if self._try_fill_empty_shifts():
2520|             logging.info(f"Attempt {attempt_number}: Filled some empty shifts.")
2521|             any_change_made = True
2522|             # Re-verify integrity after potentially complex swaps
2523|             self._verify_assignment_consistency()
2524| 
2525|         # 3. Try to improve weekend distribution
2526|         if self._improve_weekend_distribution():
2527|             logging.info(f"Attempt {attempt_number}: Improved weekend distribution.")
2528|             any_change_made = True
2529|             self._verify_assignment_consistency()
2530| 
2531| 
2532|         # 4. Try to balance workload distribution
2533|         if self._balance_workloads():
2534|             logging.info(f"Attempt {attempt_number}: Balanced workloads.")
2535|             any_change_made = True
2536|             self._verify_assignment_consistency()
2537| 
2538|         # 5. Final Incompatibility Check (Important after swaps/reassignments)
2539|         # It might be better to run this *last* to clean up any issues created by other steps.
2540|         if self._verify_no_incompatibilities(): # Assuming this tries to fix them
2541|              logging.info(f"Attempt {attempt_number}: Fixed incompatibility violations.")
2542|              any_change_made = True
2543|              # No need to verify consistency again, as this function should handle it
2544| 
2545| 
2546|         logging.info(f"--- Finished Improvement Attempt {attempt_number}. Changes made: {any_change_made} ---")
2547|         return any_change_made # Return True if any step made a change
2548| 
2549|     def _synchronize_tracking_data(self):
2550|         # Placeholder for your method in ScheduleBuilder if it exists, or call scheduler\'s
2551|         if hasattr(self.scheduler, \'_synchronize_tracking_data\'):
2552|             self.scheduler._synchronize_tracking_data()
2553|         else:
2554|             logging.warning("Scheduler\'s _synchronize_tracking_data not found by builder.")
2555|             # Fallback or simplified sync if necessary:
2556|             new_worker_assignments = {w['id']: set() for w in self.workers_data}
2557|             new_worker_posts = {w['id']: {p: 0 for p in range(self.num_shifts)} for w in self.workers_data}
2558|             for date_val, shifts_on_date in self.schedule.items(): # Renamed date
2559|                 for post_idx, worker_id_in_post in enumerate(shifts_on_date):
2560|                     if worker_id_in_post is not None:
2561|                         new_worker_assignments.setdefault(worker_id_in_post, set()).add(date_val)
2562|                         new_worker_posts.setdefault(worker_id_in_post, {p: 0 for p in range(self.num_shifts)})[post_idx] += 1
2563|             self.worker_assignments = new_worker_assignments # Update builder\'s reference
2564|             self.scheduler.worker_assignments = new_worker_assignments # Update scheduler\'s reference
2565|             self.worker_posts = new_worker_posts
2566|             self.scheduler.worker_posts = new_worker_posts
2567|             self.scheduler.worker_shift_counts = {wid: len(dates_val) for wid, dates_val in new_worker_assignments.items()} # Renamed worker_shift_counts, dates
2568|             # self.scheduler.worker_shift_counts = self.worker_shift_counts # This line is redundant
2569|             # Add other tracking data sync if needed (weekends, etc.)
2570|             
2571|     def _adjust_last_post_distribution(self, balance_tolerance=0.5, date=None):
2572|         """
2573|         Adjust the distribution of last-post slots (the highest-index shift each day)
2574|         among workers to ensure a fair spread.
2575| 
2576|         Args:
2577|             balance_tolerance (float): how many slots over the average triggers a swap
2578|             date (datetime.date, optional): if provided, only rebalance the last-post slot on this date
2579| 
2580|         Returns:
2581|             bool: True if a swap was made, False otherwise
2582|         """
2583|         # 1) Ensure our tracking data is up-to-date
2584|         self._synchronize_tracking_data()
2585| 
2586|         # 2) Count actual last-post assignments for each worker
2587|         last_post_counts = {w['id']: 0 for w in self.workers_data}
2588|         dates_with_last = []
2589| 
2590|         for d_val, shifts in self.schedule.items(): # Renamed d
2591|             if not shifts:
2592|                 continue
2593|             dates_with_last.append(d_val)
2594|             last_idx = len(shifts) - 1
2595|             wid = shifts[last_idx]
2596|             if wid is not None:
2597|                 last_post_counts[wid] += 1
2598| 
2599|         total_last_slots = len(dates_with_last)
2600|         # nothing to balance if no days have any shifts
2601|         if total_last_slots == 0:
2602|             return False
2603| 
2604|         # Filter the set of workers who actually have any last-post assignments
2605|         active_workers = {wid: cnt for wid, cnt in last_post_counts.items() if cnt > 0}
2606|         if not active_workers:
2607|             return False
2608| 
2609|         # 3) Compute the average last-post count across active workers
2610|         avg_last = total_last_slots / len(active_workers) if active_workers else 0 # Added check for empty active_workers
2611| 
2612|         # 4) Determine which dates to examine
2613|         dates_to_check = [date] if date is not None else sorted(self.schedule.keys())
2614| 
2615|         for d_val in dates_to_check: # Renamed d
2616|             shifts = self.schedule.get(d_val)
2617|             if not shifts:
2618|                 continue
2619| 
2620|             last_idx = len(shifts) - 1
2621|             current_wid = shifts[last_idx]
2622|             if current_wid is None:
2623|                 continue
2624| 
2625|             # If this worker exceeds the average by more than the tolerance, attempt a swap
2626|             if last_post_counts.get(current_wid, 0) - avg_last > balance_tolerance:
2627|                 # Find the worker with the minimum last-post count
2628|                 candidate_wid, _ = min(active_workers.items(), key=lambda x: x[1]) if active_workers else (None, 0) # Added check
2629|                 if candidate_wid is None: continue # No candidate to swap with
2630| 
2631|                 # Look for that candidate on a non-last-post slot today
2632|                 for idx in range(len(shifts) - 1):
2633|                     if shifts[idx] == candidate_wid:
2634|                         # Perform the swap
2635|                         shifts[last_idx], shifts[idx] = shifts[idx], shifts[last_idx]
2636|                         logging.info(\
2637|                             f"Swapped last post on {d_val.isoformat()}: "\
2638|                             f"{current_wid} (pos {last_idx}) ↔ {candidate_wid} (pos {idx})"\
2639|                         )
2640|                         return True
2641| 
2642|         # No swaps made
2643|         return False
2644| 
2645|     # 7. Backup and Restore Methods
2646| 
2647|     def _backup_best_schedule(self):
2648|         """Save a backup of the current best schedule by delegating to scheduler"""
2649|         return self.scheduler._backup_best_schedule()
2650|     
2651|     def _restore_best_schedule(self):
2652|         """Restore backup by delegating to scheduler"""
2653|         return self.scheduler._restore_best_schedule()
2654| 
2655|     def _save_current_as_best(self, initial=False):
2656|         # Placeholder for your method in ScheduleBuilder or call scheduler\'s
2657|         if hasattr(self.scheduler, \'_save_current_as_best\') and not initial : # initial is handled by scheduler itself
2658|              # This might be complex if the scheduler\'s method relies on its own context deeply
2659|              # For now, let\'s assume it\'s mostly about copying data.
2660|              # The builder should update its own best_schedule_data
2661|             current_score = self.calculate_score()
2662|             old_score = self.best_schedule_data['score'] if self.best_schedule_data is not None else float('-inf')
2663|             if initial or self.best_schedule_data is None or current_score > old_score:
2664|                 self.best_schedule_data = {\
2665|                     'schedule': copy.deepcopy(self.schedule),\
2666|                     'worker_assignments': copy.deepcopy(self.worker_assignments),\
2667|                     'worker_shift_counts': copy.deepcopy(self.scheduler.worker_shift_counts), # Use scheduler\'s as source of truth
2668|                     'worker_weekend_shifts': copy.deepcopy(self.scheduler.worker_weekend_shifts),\
2669|                     'worker_posts': copy.deepcopy(self.worker_posts),\
2670|                     'last_assigned_date': copy.deepcopy(self.scheduler.last_assigned_date),\
2671|                     'consecutive_shifts': copy.deepcopy(self.scheduler.consecutive_shifts),\
2672|                     'score': current_score\
2673|                 }
2674|                 logging.info(f"Builder saved new best schedule. Score: {current_score:.2f}")
2675| 
2676|     def get_best_schedule(self):
2677|         """ Returns the best schedule data dictionary found. """
2678|         if self.best_schedule_data is None:
2679|              logging.warning("get_best_schedule called but no best schedule was saved.")
2680|         return self.best_schedule_data
2681| 
2682|     def calculate_score(self, schedule_to_score=None, assignments_to_score=None):
2683|         # Placeholder - use scheduler\'s score calculation for consistency
2684|         return self.scheduler.calculate_score(schedule_to_score or self.schedule, assignments_to_score or self.worker_assignments)
2685| 
