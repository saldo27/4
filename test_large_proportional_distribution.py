#!/usr/bin/env python3
"""
Prueba de distribución proporcional con más trabajadores y turnos
para verificar si se mantiene la proporcionalidad con mayor flexibilidad numérica.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, timedelta
from scheduler import Scheduler
from scheduler_config import SchedulerConfig
import logging

# Configurar logging para ver detalles
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def test_large_proportional_distribution():
    """
    Prueba con 8 trabajadores y 3 meses para ver si se mantiene la proporcionalidad
    """
    
    # Período de 3 meses (enero - marzo 2025)
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 3, 31)
    
    # Configuración extendida con más trabajadores
    workers_data = [
        # Trabajadores tiempo completo (100%)
        {
            'id': 'worker_100_A',
            'name': 'Trabajador 100% A',
            'work_percentage': 100,
            'target_shifts': 0,
            'incompatible_with': [],
            'mandatory_days': '',
            'work_periods': '',
            'days_off': ''
        },
        {
            'id': 'worker_100_B',
            'name': 'Trabajador 100% B',
            'work_percentage': 100,
            'target_shifts': 0,
            'incompatible_with': [],
            'mandatory_days': '',
            'work_periods': '',
            'days_off': ''
        },
        
        # Trabajadores 75%
        {
            'id': 'worker_75_A',
            'name': 'Trabajador 75% A',
            'work_percentage': 75,
            'target_shifts': 0,
            'incompatible_with': [],
            'mandatory_days': '',
            'work_periods': '',
            'days_off': ''
        },
        {
            'id': 'worker_75_B',
            'name': 'Trabajador 75% B',
            'work_percentage': 75,
            'target_shifts': 0,
            'incompatible_with': [],
            'mandatory_days': '',
            'work_periods': '',
            'days_off': ''
        },
        
        # Trabajadores 50%
        {
            'id': 'worker_50_A',
            'name': 'Trabajador 50% A',
            'work_percentage': 50,
            'target_shifts': 0,
            'incompatible_with': [],
            'mandatory_days': '',
            'work_periods': '',
            'days_off': ''
        },
        {
            'id': 'worker_50_B',
            'name': 'Trabajador 50% B',
            'work_percentage': 50,
            'target_shifts': 0,
            'incompatible_with': [],
            'mandatory_days': '',
            'work_periods': '',
            'days_off': ''
        },
        
        # Trabajadores 25%
        {
            'id': 'worker_25_A',
            'name': 'Trabajador 25% A',
            'work_percentage': 25,
            'target_shifts': 0,
            'incompatible_with': [],
            'mandatory_days': '',
            'work_periods': '',
            'days_off': ''
        },
        {
            'id': 'worker_25_B',
            'name': 'Trabajador 25% B',
            'work_percentage': 25,
            'target_shifts': 0,
            'incompatible_with': [],
            'mandatory_days': '',
            'work_periods': '',
            'days_off': ''
        },
    ]
    
    # Festivos para el período
    holidays = [
        datetime(2025, 1, 1),   # Año Nuevo
        datetime(2025, 1, 6),   # Epifanía
        datetime(2025, 2, 14),  # San Valentín (ejemplo)
        datetime(2025, 3, 17),  # San Patricio (ejemplo)
        datetime(2025, 3, 25),  # Anunciación (ejemplo)
    ]
    
    # Pre-festivos (día anterior a festivos)
    pre_holidays = [holiday - timedelta(days=1) for holiday in holidays]
    
    # Configurar el scheduler
    config = {
        'start_date': start_date,
        'end_date': end_date,
        'workers_data': workers_data,
        'holidays': holidays,
        'pre_holidays': pre_holidays,
        'incompatible_workers': [],
        'num_shifts': 2,
        'shift_names': ['P0', 'P1'],
        'enable_proportional_weekends': True,
        'variable_shifts': [],
        'gap_between_shifts': 24,
        'max_consecutive_weekends': 3,
        'weekend_penalty_factor': 1.0,
        'cache_enabled': False
    }
    
    scheduler = Scheduler(config)
    
    print("🧪 PRUEBA DE DISTRIBUCIÓN PROPORCIONAL CON ESCALA MAYOR")
    print("=" * 60)
    print(f"📅 Período: {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")
    print(f"👥 Trabajadores: {len(workers_data)} con diferentes porcentajes de trabajo")
    print(f"🎯 Turnos por día: {config['num_shifts']}")
    print(f"🎉 Festivos: {len(holidays)} días")
    print(f"📊 Expectativa: Mayor flexibilidad numérica debería preservar mejor la proporcionalidad")
    print()
    
    # Calcular estadísticas teóricas
    total_days = (end_date - start_date).days + 1
    weekend_days = sum(1 for i in range(total_days) 
                      if (start_date + timedelta(days=i)).weekday() in [4, 5, 6])  # Vie, Sáb, Dom
    holiday_days = len(holidays)
    pre_holiday_days = len(pre_holidays)
    special_days = weekend_days + holiday_days + pre_holiday_days
    
    print(f"📈 ESTADÍSTICAS TEÓRICAS:")
    print(f"  Total de días: {total_days}")
    print(f"  Días de fin de semana: {weekend_days}")
    print(f"  Días festivos: {holiday_days}")
    print(f"  Días pre-festivos: {pre_holiday_days}")
    print(f"  Total días especiales: {special_days}")
    print(f"  Total turnos especiales: {special_days * config['num_shifts']}")
    print()
    
    # Calcular distribución proporcional teórica
    total_work_percentage = sum(w['work_percentage'] for w in workers_data)
    total_special_shifts = special_days * config['num_shifts']
    
    print(f"💡 DISTRIBUCIÓN PROPORCIONAL TEÓRICA:")
    for worker in workers_data:
        worker_id = worker['id']
        work_pct = worker['work_percentage']
        proportion = work_pct / total_work_percentage
        expected_shifts = proportion * total_special_shifts
        print(f"  {worker_id}: {work_pct}% → {expected_shifts:.1f} turnos especiales")
    print()
    
    # Generar horario
    schedule_success = scheduler.generate_schedule()
    
    if not schedule_success:
        print("❌ Error: No se pudo generar el horario")
        return False
    
    print("✓ Horario generado exitosamente")
    print()
    
    # Analizar resultados
    print("=" * 60)
    print("ANÁLISIS DE DISTRIBUCIÓN DE FINES DE SEMANA Y FESTIVOS")
    print("=" * 60)
    print()
    
    # Contar turnos por trabajador
    worker_stats = {}
    for worker in workers_data:
        worker_id = worker['id']
        worker_stats[worker_id] = {
            'work_percentage': worker['work_percentage'],
            'total_shifts': 0,
            'weekend_shifts': 0,
            'holiday_shifts': 0,
            'pre_holiday_shifts': 0,
            'special_shifts_total': 0
        }
    
    # Analizar el horario generado
    for date, shifts in scheduler.schedule.items():
        is_weekend = date.weekday() in [4, 5, 6]  # Viernes, Sábado, Domingo
        is_holiday = date in holidays
        is_pre_holiday = date in pre_holidays
        is_special = is_weekend or is_holiday or is_pre_holiday
        
        for shift_idx, worker_id in enumerate(shifts):
            if worker_id and worker_id in worker_stats:
                worker_stats[worker_id]['total_shifts'] += 1
                
                if is_special:
                    worker_stats[worker_id]['special_shifts_total'] += 1
                    
                    if is_weekend:
                        worker_stats[worker_id]['weekend_shifts'] += 1
                    if is_holiday:
                        worker_stats[worker_id]['holiday_shifts'] += 1
                    if is_pre_holiday:
                        worker_stats[worker_id]['pre_holiday_shifts'] += 1
    
    # Mostrar resultados y verificar proporcionalidad
    special_shift_counts = []
    proportionality_errors = []
    
    for worker_id in sorted(worker_stats.keys()):
        stats = worker_stats[worker_id]
        work_pct = stats['work_percentage']
        special_shifts = stats['special_shifts_total']
        total_shifts = stats['total_shifts']
        
        # Calcular porcentaje de turnos especiales
        special_percentage = (special_shifts / total_shifts * 100) if total_shifts > 0 else 0
        
        # Calcular error de proporcionalidad
        expected_proportion = work_pct / total_work_percentage
        actual_proportion = special_shifts / total_special_shifts if total_special_shifts > 0 else 0
        proportionality_error = abs(actual_proportion - expected_proportion) * 100
        
        print(f"Trabajador: {worker_id}")
        print(f"  Porcentaje de trabajo: {work_pct}%")
        print(f"  Total turnos asignados: {total_shifts}")
        print(f"  Turnos fin de semana: {stats['weekend_shifts']}")
        print(f"  Turnos festivos: {stats['holiday_shifts']}")
        print(f"  Turnos pre-festivos: {stats['pre_holiday_shifts']}")
        print(f"  Turnos especiales total: {special_shifts}")
        print(f"  % de turnos especiales: {special_percentage:.1f}%")
        print(f"  Error de proporcionalidad: {proportionality_error:.2f}%")
        print()
        
        special_shift_counts.append(special_shifts)
        proportionality_errors.append(proportionality_error)
    
    # Resumen y verificación de tolerancia
    total_special_assigned = sum(special_shift_counts)
    min_special = min(special_shift_counts)
    max_special = max(special_shift_counts)
    special_range = max_special - min_special
    avg_proportionality_error = sum(proportionality_errors) / len(proportionality_errors)
    
    print("RESUMEN GENERAL:")
    print(f"  Total turnos especiales asignados: {total_special_assigned}")
    print(f"  Total turnos especiales teóricos: {total_special_shifts}")
    print(f"  Rango de turnos especiales: {min_special} - {max_special}")
    print(f"  Diferencia máxima: {special_range}")
    print(f"  Error promedio de proporcionalidad: {avg_proportionality_error:.2f}%")
    print()
    
    print("VERIFICACIÓN DE TOLERANCIA (+/- 1):")
    tolerance_met = special_range <= 1
    print(f"  ✓ Cumple tolerancia +/-1: {tolerance_met}")
    
    print("VERIFICACIÓN DE PROPORCIONALIDAD:")
    good_proportionality = avg_proportionality_error < 5.0  # Menos de 5% de error promedio
    print(f"  ✓ Mantiene buena proporcionalidad (<5% error): {good_proportionality}")
    print()
    
    # Exportar horario
    output_file = f"schedule_large_test_{datetime.now().strftime('%d%m%Y_%H%M%S')}.txt"
    scheduler.export_schedule(output_file)
    print(f"INFO: Schedule exported to {output_file}")
    print()
    
    # Conclusión
    if tolerance_met and good_proportionality:
        print("✅ ÉXITO: El algoritmo mantiene tanto la tolerancia como la proporcionalidad")
    elif tolerance_met:
        print("⚠️ PARCIAL: Cumple tolerancia pero pierde proporcionalidad")
    elif good_proportionality:
        print("⚠️ PARCIAL: Mantiene proporcionalidad pero viola tolerancia")
    else:
        print("❌ FALLO: No cumple ni tolerancia ni proporcionalidad")
    
    return schedule_success

if __name__ == "__main__":
    test_large_proportional_distribution()
