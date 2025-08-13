#!/usr/bin/env python3
"""
Test script for proportional weekend and holiday distribution
"""

import logging
from datetime import datetime, timedelta
from scheduler import Scheduler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_test_config():
    """Create a test configuration with workers having different work percentages"""
    
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 1, 31)  # One month for testing
    
    # Create workers with different work percentages
    workers_data = [
        {
            'id': 'worker_100',
            'name': 'Trabajador Tiempo Completo',
            'work_percentage': 100,
            'target_shifts': 0,  # Will be calculated automatically
            'incompatible_with': [],
            'mandatory_days': '',
            'work_periods': '',
            'days_off': ''
        },
        {
            'id': 'worker_75', 
            'name': 'Trabajador 75%',
            'work_percentage': 75,
            'target_shifts': 0,
            'incompatible_with': [],
            'mandatory_days': '',
            'work_periods': '',
            'days_off': ''
        },
        {
            'id': 'worker_50',
            'name': 'Trabajador 50%',
            'work_percentage': 50,
            'target_shifts': 0,
            'incompatible_with': [],
            'mandatory_days': '',
            'work_periods': '',
            'days_off': ''
        },
        {
            'id': 'worker_25',
            'name': 'Trabajador 25%',
            'work_percentage': 25,
            'target_shifts': 0,
            'incompatible_with': [],
            'mandatory_days': '',
            'work_periods': '',
            'days_off': ''
        }
    ]
    
    # Add some holidays for testing
    holidays = [
        datetime(2025, 1, 6),   # Reyes
        datetime(2025, 1, 20),  # Holiday example
    ]
    
    config = {
        'start_date': start_date,
        'end_date': end_date,
        'num_shifts': 2,  # 2 shifts per day
        'workers_data': workers_data,
        'holidays': holidays,
        'enable_proportional_weekends': True,
        'weekend_tolerance': 1,  # +/- 1 tolerance
        'gap_between_shifts': 1,
        'max_consecutive_weekends': 3,
        'cache_enabled': True
    }
    
    return config

def analyze_weekend_distribution(scheduler):
    """Analyze and report weekend/holiday distribution"""
    
    print("\n" + "="*60)
    print("ANÁLISIS DE DISTRIBUCIÓN DE FINES DE SEMANA Y FESTIVOS")
    print("="*60)
    
    # Count weekend and holiday shifts for each worker
    worker_weekend_counts = {}
    worker_holiday_counts = {}
    worker_total_counts = {}
    
    for worker in scheduler.workers_data:
        worker_id = worker['id']
        work_percentage = worker.get('work_percentage', 100)
        
        # Count assignments
        assignments = scheduler.worker_assignments.get(worker_id, set())
        weekend_count = 0
        holiday_count = 0
        total_count = len(assignments)
        
        for date in assignments:
            # Weekend (Friday, Saturday, Sunday)
            if date.weekday() >= 4:
                weekend_count += 1
            
            # Holiday or pre-holiday
            if (date in scheduler.holidays or 
                (date + timedelta(days=1)) in scheduler.holidays):
                holiday_count += 1
        
        worker_weekend_counts[worker_id] = weekend_count
        worker_holiday_counts[worker_id] = holiday_count
        worker_total_counts[worker_id] = total_count
        
        print(f"\nTrabajador: {worker_id}")
        print(f"  Porcentaje de trabajo: {work_percentage}%")
        print(f"  Total turnos asignados: {total_count}")
        print(f"  Turnos fin de semana: {weekend_count}")
        print(f"  Turnos festivos/pre-festivos: {holiday_count}")
        print(f"  Turnos especiales total: {weekend_count + holiday_count}")
        
        if total_count > 0:
            weekend_percentage = (weekend_count / total_count) * 100
            print(f"  % de turnos en fin de semana: {weekend_percentage:.1f}%")
    
    # Calculate totals and averages
    total_weekend = sum(worker_weekend_counts.values())
    total_holiday = sum(worker_holiday_counts.values())
    total_all = sum(worker_total_counts.values())
    
    print(f"\nRESUMEN GENERAL:")
    print(f"  Total turnos fin de semana: {total_weekend}")
    print(f"  Total turnos festivos: {total_holiday}")
    print(f"  Total turnos especiales: {total_weekend + total_holiday}")
    print(f"  Total turnos asignados: {total_all}")
    
    if total_all > 0:
        special_percentage = ((total_weekend + total_holiday) / total_all) * 100
        print(f"  % turnos especiales: {special_percentage:.1f}%")
    
    # Check tolerance compliance
    print(f"\nVERIFICACIÓN DE TOLERANCIA (+/- {scheduler.weekend_tolerance}):")
    weekend_counts = list(worker_weekend_counts.values())
    holiday_counts = list(worker_holiday_counts.values())
    
    if weekend_counts:
        min_weekend = min(weekend_counts)
        max_weekend = max(weekend_counts)
        weekend_diff = max_weekend - min_weekend
        print(f"  Diferencia fin de semana: {weekend_diff} (min: {min_weekend}, max: {max_weekend})")
        print(f"  ✓ Cumple tolerancia fin de semana: {weekend_diff <= scheduler.weekend_tolerance * 2}")
    
    if holiday_counts:
        min_holiday = min(holiday_counts)
        max_holiday = max(holiday_counts)
        holiday_diff = max_holiday - min_holiday
        print(f"  Diferencia festivos: {holiday_diff} (min: {min_holiday}, max: {max_holiday})")
        print(f"  ✓ Cumple tolerancia festivos: {holiday_diff <= scheduler.weekend_tolerance * 2}")

def test_proportional_distribution():
    """Test proportional weekend and holiday distribution"""
    
    print("Iniciando prueba de distribución proporcional de fines de semana y festivos...")
    
    # Create configuration
    config = create_test_config()
    
    # Create scheduler
    scheduler = Scheduler(config)
    
    # Generate schedule
    print("Generando horario...")
    success = scheduler.generate_schedule(max_improvement_loops=30)
    
    if not success:
        print("❌ Error: No se pudo generar el horario")
        return False
    
    print("✓ Horario generado exitosamente")
    
    # Analyze distribution
    analyze_weekend_distribution(scheduler)
    
    # Export schedule for review
    try:
        scheduler.export_schedule('txt')
        print(f"\n✓ Horario exportado a archivo")
    except Exception as e:
        print(f"⚠️ Error exportando horario: {e}")
    
    return True

if __name__ == "__main__":
    try:
        success = test_proportional_distribution()
        if success:
            print("\n✅ Prueba completada exitosamente")
        else:
            print("\n❌ Prueba falló")
    except Exception as e:
        print(f"\n💥 Error durante la prueba: {e}")
        logging.error(f"Test failed with error: {e}", exc_info=True)
