#!/usr/bin/env python3
"""
Análisis específico de proporcionalidad en la distribución de turnos especiales
"""

from datetime import datetime, timedelta
from scheduler import Scheduler
from scheduler_config import SchedulerConfig

def analyze_proportional_distribution():
    """Analiza la distribución proporcional con diferentes configuraciones"""
    
    print("=" * 80)
    print("ANÁLISIS DE PROPORCIONALIDAD EN DISTRIBUCIÓN DE TURNOS ESPECIALES")
    print("=" * 80)
    
    # Configuración de 8 trabajadores con diferentes porcentajes
    workers = [
        {'id': 'worker_100_A', 'name': 'Worker 100% A', 'work_percentage': 100},
        {'id': 'worker_100_B', 'name': 'Worker 100% B', 'work_percentage': 100},
        {'id': 'worker_75_A', 'name': 'Worker 75% A', 'work_percentage': 75},
        {'id': 'worker_75_B', 'name': 'Worker 75% B', 'work_percentage': 75},
        {'id': 'worker_50_A', 'name': 'Worker 50% A', 'work_percentage': 50},
        {'id': 'worker_50_B', 'name': 'Worker 50% B', 'work_percentage': 50},
        {'id': 'worker_25_A', 'name': 'Worker 25% A', 'work_percentage': 25},
        {'id': 'worker_25_B', 'name': 'Worker 25% B', 'work_percentage': 25},
    ]
    
    # Configuración como diccionario
    config = {
        'start_date': datetime(2025, 1, 1),
        'end_date': datetime(2025, 3, 31),
        'workers_data': workers,
        'holidays': [
            datetime(2025, 1, 1),   # Año Nuevo
            datetime(2025, 1, 6),   # Reyes  
            datetime(2025, 4, 18),  # Viernes Santo
            datetime(2025, 5, 1),   # Día del Trabajo
            datetime(2025, 8, 15),  # Asunción
            datetime(2025, 12, 25), # Navidad
        ],
        'num_shifts': 2,
        'max_consecutive_weekends': 3,
        'gap_between_shifts': 24,
        'max_daily_hours': 12,
        'min_rest_between_shifts': 12,
        'max_weekly_hours': 40,
        'overtime_threshold': 8,
        'night_shift_start': "22:00",
        'night_shift_end': "06:00"
    }
    
    # Crear scheduler y generar horario
    scheduler = Scheduler(config)
    
    print("\n🔄 Generando horario con 8 trabajadores (3 meses)...")
    success = scheduler.generate_schedule()
    
    if not success:
        print("❌ Error al generar el horario")
        return
        
    print("✓ Horario generado exitosamente")
    
    # Análisis detallado de la distribución
    analyze_special_days_distribution(scheduler, workers)

def analyze_special_days_distribution(scheduler, workers):
    """Analiza en detalle la distribución de días especiales"""
    
    # Contar turnos especiales por trabajador
    special_days_count = {worker['id']: 0 for worker in workers}
    total_special_days = 0
    
    # Obtener fechas especiales
    special_dates = set()
    
    # Añadir fines de semana y festivos
    current_date = scheduler.start_date
    while current_date <= scheduler.end_date:
        weekday = current_date.weekday()
        
        # Viernes (4), Sábado (5), Domingo (6)
        if weekday >= 4:
            special_dates.add(current_date)
            
        # Festivos (tratados como domingo)
        if current_date in scheduler.holidays:
            special_dates.add(current_date)
            
        # Pre-festivos (día anterior a festivo, tratado como viernes)
        tomorrow = current_date + timedelta(days=1)
        if tomorrow in scheduler.holidays and weekday < 4:
            special_dates.add(current_date)
            
        current_date += timedelta(days=1)
    
    print(f"\n📊 Total días especiales en el período: {len(special_dates)}")
    
    # Analizar asignaciones
    for date in special_dates:
        date_key = date.strftime('%Y-%m-%d')
        if date_key in scheduler.schedule:
            day_schedule = scheduler.schedule[date_key]
            for shift_id, worker_id in day_schedule.items():
                if worker_id and worker_id != 'EMPTY':
                    special_days_count[worker_id] += 1
                    total_special_days += 1
    
    print(f"📊 Total turnos especiales asignados: {total_special_days}")
    
    # Calcular proporciones teóricas
    total_work_percentage = sum(w['work_percentage'] for w in workers)
    
    print("\n" + "="*60)
    print("ANÁLISIS DE PROPORCIONALIDAD")
    print("="*60)
    
    deviations = []
    
    for worker in workers:
        worker_id = worker['id']
        work_pct = worker['work_percentage']
        assigned = special_days_count[worker_id]
        
        # Proporción teórica ideal
        theoretical_proportion = (work_pct / total_work_percentage) * total_special_days
        deviation = abs(assigned - theoretical_proportion)
        deviation_pct = (deviation / theoretical_proportion * 100) if theoretical_proportion > 0 else 0
        
        deviations.append(deviation_pct)
        
        print(f"\n{worker_id}:")
        print(f"  Trabajo: {work_pct}%")
        print(f"  Asignados: {assigned}")
        print(f"  Teórico ideal: {theoretical_proportion:.2f}")
        print(f"  Desviación: {deviation:.2f} ({deviation_pct:.1f}%)")
    
    # Estadísticas generales
    print(f"\n📈 ESTADÍSTICAS GENERALES:")
    print(f"  Desviación promedio: {sum(deviations)/len(deviations):.1f}%")
    print(f"  Desviación máxima: {max(deviations):.1f}%")
    print(f"  Desviación mínima: {min(deviations):.1f}%")
    
    # Verificar tolerancia +/-1
    assigned_counts = list(special_days_count.values())
    min_assigned = min(assigned_counts)
    max_assigned = max(assigned_counts)
    tolerance_range = max_assigned - min_assigned
    
    print(f"\n🎯 TOLERANCIA +/-1:")
    print(f"  Rango actual: {min_assigned} - {max_assigned} (diferencia: {tolerance_range})")
    print(f"  Cumple +/-1: {'✓' if tolerance_range <= 1 else '❌'}")
    
    # Análisis de distribución por porcentaje de trabajo
    print(f"\n📊 DISTRIBUCIÓN POR GRUPO DE TRABAJO:")
    
    groups = {}
    for worker in workers:
        pct = worker['work_percentage']
        if pct not in groups:
            groups[pct] = []
        groups[pct].append(special_days_count[worker['id']])
    
    for pct in sorted(groups.keys(), reverse=True):
        counts = groups[pct]
        avg = sum(counts) / len(counts)
        print(f"  Trabajadores {pct}%: {counts} (promedio: {avg:.1f})")

if __name__ == "__main__":
    analyze_proportional_distribution()
