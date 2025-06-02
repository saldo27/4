class AdaptiveIterationManager:
    """Manages iteration counts for scheduling optimization based on problem complexity"""
    
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.start_time = None
        self.convergence_threshold = 4  # Stop if no improvement for 4 iterations
        self.max_time_minutes = 5  # Maximum optimization time in minutes
        
    def calculate_base_iterations(self):
        """Calculate base iteration count based on problem complexity"""
        num_workers = len(self.scheduler.workers_data)
        shifts_per_day = self.scheduler.num_shifts
        total_days = (self.scheduler.end_date - self.scheduler.start_date).days + 1
        
        # Calculate complexity factors
        base_complexity = num_workers * shifts_per_day * total_days
        
        # Count constraints that add complexity
        constraint_complexity = 0
        
        # Variable shifts add complexity
        if hasattr(self.scheduler, 'variable_shifts') and self.scheduler.variable_shifts:
            constraint_complexity += len(self.scheduler.variable_shifts) * 0.1
        
        # Incompatible workers add complexity
        incompatible_count = sum(1 for w in self.scheduler.workers_data 
                               if w.get('is_incompatible', False))
        constraint_complexity += incompatible_count * 0.2
        
        # Part-time workers add complexity
        part_time_count = sum(1 for w in self.scheduler.workers_data 
                            if w.get('work_percentage', 100) < 70)
        constraint_complexity += part_time_count * 0.15
        
        # Days off and mandatory days add complexity
        complex_schedules = sum(1 for w in self.scheduler.workers_data 
                              if w.get('days_off', '') or w.get('mandatory_days', ''))
        constraint_complexity += complex_schedules * 0.1
        
        # Calculate final complexity score
        total_complexity = base_complexity * (1 + constraint_complexity)
        
        logging.info(f"Complexity calculation: base={base_complexity}, "
                    f"constraint_factor={constraint_complexity:.2f}, "
                    f"total={total_complexity:.0f}")
        
        return total_complexity
    
    def calculate_adaptive_iterations(self):
        """Calculate adaptive iteration counts for different optimization phases"""
        complexity = self.calculate_base_iterations()
        
        # Base iteration calculations
        if complexity < 1000:
            main_loops = 5
            fill_attempts = 3
            balance_iterations = 2
            weekend_passes = 2
            post_adjustment_iterations = 3
        elif complexity < 5000:
            main_loops = 8
            fill_attempts = 5
            balance_iterations = 3
            weekend_passes = 3
            post_adjustment_iterations = 5
        elif complexity < 15000:
            main_loops = 20
            fill_attempts = 10
            balance_iterations = 12
            weekend_passes = 8
            post_adjustment_iterations = 5
        else:
            # MODIFIED: Increased iterations for typical complex schedules
            main_loops = 40                    # Increased from 20 to 30
            fill_attempts = 12                 # Keeping at 12 (was already good)
            balance_iterations = 18            # Increased from 8 to 15
            weekend_passes = 12                # Increased from 6 to 10
            post_adjustment_iterations = 5     # Decreased from 12 to 5 as requested
        
        # Adjust based on worker count
        num_workers = len(self.scheduler.workers_data)
        if num_workers > 40:
            main_loops = int(main_loops * 1.3)
            balance_iterations = int(balance_iterations * 1.2)
        elif num_workers < 15:
            main_loops = max(3, int(main_loops * 0.8))
            balance_iterations = max(2, int(balance_iterations * 0.8))
        
        return {
            'max_optimization_loops': main_loops,
            'max_fill_attempts': fill_attempts,
            'max_balance_iterations': balance_iterations,
            'max_weekend_passes': weekend_passes,
            'last_post_max_iterations': post_adjustment_iterations,
            'convergence_threshold': self.convergence_threshold
        }
    
    def should_continue_optimization(self, current_iteration, iterations_without_improvement, 
                                   current_score, best_score):
        """Determine if optimization should continue based on various criteria"""
        
        # Check time limit
        if self.start_time:
            elapsed_minutes = (datetime.now() - self.start_time).total_seconds() / 60
            if elapsed_minutes > self.max_time_minutes:
                logging.info(f"Stopping optimization: Time limit reached ({elapsed_minutes:.1f} min)")
                return False
        
        # Check convergence
        if iterations_without_improvement >= self.convergence_threshold:
            logging.info(f"Stopping optimization: No improvement for {iterations_without_improvement} iterations")
            return False
        
        # Check if we have a good enough solution
        if current_score >= 95.0:  # Adjust this threshold as needed
            logging.info(f"Stopping optimization: Excellent score achieved ({current_score:.2f})")
            return False
        
        return True
    
    def start_optimization_timer(self):
        """Start the optimization timer"""
        self.start_time = datetime.now()
        logging.info(f"Starting optimization timer at {self.start_time}")
    
    def get_optimization_config(self):
        """Get complete optimization configuration"""
        adaptive_config = self.calculate_adaptive_iterations()
        
        # Add additional configuration
        adaptive_config.update({
            'max_time_minutes': self.max_time_minutes,
            'early_stop_score': 95.0,
            'last_post_balance_tolerance': 1.0,
            'improvement_threshold': 0.1  # Minimum improvement to count as progress
        })
        
        logging.info("Adaptive iteration configuration:")
        for key, value in adaptive_config.items():
            logging.info(f"  {key}: {value}")
        
        return adaptive_config
