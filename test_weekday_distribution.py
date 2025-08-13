#!/usr/bin/env python3
"""
Test de validación para la distribución equilibrada de turnos por días de la semana.
Verifica que cada trabajador tenga aproximadamente los mismos turnos asignados 
en cada día de la semana con tolerancia ±2.
"""

import sys
from datetime import datetime, timedelta
from scheduler import Scheduler

def analyze_weekday_distribution(scheduler):
    """Analiza la distribución de turnos por días de la semana para cada trabajador."""
    
    print("\n🔍 ANÁLISIS DE DISTRIBUCIÓN POR DÍAS DE LA SEMANA:")
    print("=" * 70)
    
    weekday_names = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
    
    # Inicializar contadores por trabajador y día de la semana
    worker_weekday_counts = {}
    total_weekday_counts = {day: 0 for day in range(7)}
    
    # Obtener lista de trabajadores del scheduler
    if hasattr(scheduler, 'workers_data') and isinstance(scheduler.workers_data, dict):
        workers = list(scheduler.workers_data.keys())
    elif hasattr(scheduler, 'workers_data') and isinstance(scheduler.workers_data, list):
        workers = [w['id'] for w in scheduler.workers_data]
    else:
        # Fallback: extraer de la configuración original
        workers = ['T001', 'T002', 'T003', 'T004', 'T005', 'T006']
    
    for worker_id in workers:
        worker_weekday_counts[worker_id] = {day: 0 for day in range(7)}
    
    # Contar turnos por día de la semana
    total_shifts = 0
    for date_str, posts in scheduler.schedule.items():
        try:
            # Manejar diferentes tipos de fechas (datetime o string)
            if isinstance(date_str, datetime):
                date_obj = date_str
            else:
                date_obj = datetime.strptime(str(date_str), '%Y-%m-%d')
            weekday = date_obj.weekday()  # 0=Monday, 6=Sunday
            
            # Verificar estructura de posts (dict o list)
            if isinstance(posts, dict):
                for post_idx, worker_id in posts.items():
                    if worker_id and worker_id != "None" and str(worker_id) in worker_weekday_counts:
                        worker_weekday_counts[str(worker_id)][weekday] += 1
                        total_weekday_counts[weekday] += 1
                        total_shifts += 1
            elif isinstance(posts, list):
                for worker_id in posts:
                    if worker_id and worker_id != "None" and str(worker_id) in worker_weekday_counts:
                        worker_weekday_counts[str(worker_id)][weekday] += 1
                        total_weekday_counts[weekday] += 1
                        total_shifts += 1
        except (ValueError, TypeError) as e:
            print(f"⚠️ Error procesando fecha {date_str}: {e}")
            continue
    
    print(f"📊 Total de turnos analizados: {total_shifts}")
    
    if total_shifts == 0:
        print("⚠️ No hay turnos asignados en el horario")
        return {
            'all_balanced': False,
            'worker_distributions': worker_weekday_counts,
            'total_shifts': 0,
            'tolerance': tolerance,
            'error': 'No shifts assigned'
        }
    
    print(f"📅 Distribución general por día de la semana:")
    for day in range(7):
        print(f"   {weekday_names[day]}: {total_weekday_counts[day]} turnos")
    
    print(f"\n👥 ANÁLISIS POR TRABAJADOR:")
    print("-" * 70)
    
    all_balanced = True
    tolerance = 2
    
    for worker_id in workers:
        weekday_counts = worker_weekday_counts[worker_id]
        total_worker_shifts = sum(weekday_counts.values())
        
        if total_worker_shifts == 0:
            print(f"\n🔹 {worker_id}: Sin turnos asignados")
            continue
        
        expected_per_weekday = total_worker_shifts / 7.0
        max_count = max(weekday_counts.values())
        min_count = min(weekday_counts.values())
        range_diff = max_count - min_count
        
        print(f"\n🔹 {worker_id}:")
        print(f"   Total turnos: {total_worker_shifts}")
        print(f"   Promedio esperado por día: {expected_per_weekday:.2f}")
        print(f"   Distribución por día:")
        
        imbalances = []
        for day in range(7):
            count = weekday_counts[day]
            deviation = count - expected_per_weekday
            status = "✅" if abs(deviation) <= tolerance else "❌"
            
            print(f"     {weekday_names[day]}: {count} turnos (desviación: {deviation:+.1f}) {status}")
            
            if abs(deviation) > tolerance:
                imbalances.append((weekday_names[day], deviation))
        
        print(f"   Rango (max-min): {range_diff} turnos")
        print(f"   Dentro de tolerancia ±{tolerance}: {'✅ SÍ' if range_diff <= 2*tolerance else '❌ NO'}")
        
        if imbalances:
            all_balanced = False
            print(f"   ⚠️  Desequilibrios detectados:")
            for day_name, dev in imbalances:
                print(f"      - {day_name}: {dev:+.1f} turnos")
    
    return {
        'all_balanced': all_balanced,
        'worker_distributions': worker_weekday_counts,
        'total_shifts': total_shifts,
        'tolerance': tolerance
    }

def test_weekday_distribution():
    """Ejecuta un test completo de distribución de días de la semana."""
    
    print("🧪 TEST DE DISTRIBUCIÓN POR DÍAS DE LA SEMANA")
    print("=" * 70)
    
    # Configurar período de prueba (1 mes)
    start_date = datetime(2025, 2, 1)
    end_date = datetime(2025, 2, 28)
    
    # Configurar trabajadores con diferentes disponibilidades
    workers = [
        {
            'id': 'T001',
            'name': 'Trabajador 100% A',
            'work_percentage': 100,
            'target_shifts': 0,
            'incompatible_with': [],
            'mandatory_days': '',
            'work_periods': '',
            'days_off': ''
        },
        {
            'id': 'T002',
            'name': 'Trabajador 100% B',
            'work_percentage': 100,
            'target_shifts': 0,
            'incompatible_with': [],
            'mandatory_days': '',
            'work_periods': '',
            'days_off': ''
        },
        {
            'id': 'T003',
            'name': 'Trabajador 75% A',
            'work_percentage': 75,
            'target_shifts': 0,
            'incompatible_with': [],
            'mandatory_days': '',
            'work_periods': '',
            'days_off': ''
        },
        {
            'id': 'T004',
            'name': 'Trabajador 75% B',
            'work_percentage': 75,
            'target_shifts': 0,
            'incompatible_with': [],
            'mandatory_days': '',
            'work_periods': '',
            'days_off': ''
        },
        {
            'id': 'T005',
            'name': 'Trabajador 50%',
            'work_percentage': 50,
            'target_shifts': 0,
            'incompatible_with': [],
            'mandatory_days': '',
            'work_periods': '',
            'days_off': ''
        },
        {
            'id': 'T006',
            'name': 'Trabajador 25%',
            'work_percentage': 25,
            'target_shifts': 0,
            'incompatible_with': [],
            'mandatory_days': '',
            'work_periods': '',
            'days_off': ''
        }
    ]
    
    print(f"📅 Período: {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")
    print(f"👥 Trabajadores: {len(workers)}")
    print(f"📊 Turnos por día: 2")
    print(f"🎯 Tolerancia: ±2 turnos por día de la semana")
    
    for worker in workers:
        print(f"   {worker['name']}: {worker['work_percentage']}%")
    
    # Crear scheduler con configuración completa
    config = {
        'start_date': start_date,
        'end_date': end_date,
        'num_shifts': 2,  # 2 turnos por día
        'workers_data': workers,
        'holidays': [
            datetime(2025, 2, 14),  # San Valentín como ejemplo
        ],
        'unavailable_dates': [],
        'fixed_assignments': [],
        'variable_shifts': [],  # Debe ser una lista, no un dict
        'gap_between_shifts': 16,
        'max_shifts_per_worker': 100,
        'max_consecutive_weekends': 3,
        'weekend_limit': 2,
        'constraint_mode': 'strict'
    }
    
    scheduler_config = {
        'cache_enabled': True,
        'improvement_iterations': 50,
        'optimization_level': 'high',
        'weekday_balance_tolerance': 2,
        'weekday_balance_max_iterations': 5
    }
    scheduler = Scheduler(config)
    
    try:
        # Generar horario inicial
        print("\n🚀 Generando horario inicial...")
        success = scheduler.generate_schedule()
        
        if not success:
            print("❌ ERROR: No se pudo generar el horario")
            return False
            
        print("✅ Horario inicial generado exitosamente")
        
        # Analizar distribución inicial
        print("\n📈 ANÁLISIS INICIAL:")
        initial_analysis = analyze_weekday_distribution(scheduler)
        
        # Aplicar mejora de distribución de días de la semana
        print("\n🔧 Aplicando mejora de distribución por días de la semana...")
        
        # Acceder al schedule_builder para aplicar la mejora directamente
        improvements_made = scheduler.schedule_builder._balance_weekday_distribution(tolerance=2, max_iterations=5)
        
        if improvements_made:
            print("✅ Se realizaron mejoras en la distribución")
        else:
            print("ℹ️ No se necesitaron mejoras (distribución ya equilibrada)")
        
        # Analizar distribución final
        print("\n📈 ANÁLISIS FINAL:")
        final_analysis = analyze_weekday_distribution(scheduler)
        
        # Resumen de validación
        print(f"\n🎯 RESUMEN DE VALIDACIÓN:")
        print(f"   Distribución inicial equilibrada: {'✅ SÍ' if initial_analysis['all_balanced'] else '❌ NO'}")
        print(f"   Distribución final equilibrada: {'✅ SÍ' if final_analysis['all_balanced'] else '❌ NO'}")
        
        if improvements_made:
            print(f"   Se realizaron mejoras: ✅ SÍ")
        else:
            print(f"   Se realizaron mejoras: ℹ️ No necesarias")
        
        # Exportar horario para revisión
        timestamp = datetime.now().strftime("%d%m%Y_%H%M%S")
        export_filename = f"schedule_weekday_balanced_{timestamp}.txt"
        
        if hasattr(scheduler, 'export_schedule'):
            scheduler.export_schedule(export_filename)
            print(f"\n📄 Horario exportado a: {export_filename}")
        
        return final_analysis['all_balanced']
        
    except Exception as e:
        print(f"❌ ERROR durante el test: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_weekday_distribution()
    
    if success:
        print("\n✅ TEST COMPLETADO EXITOSAMENTE")
        print("🎯 La distribución por días de la semana cumple con la tolerancia ±2")
    else:
        print("\n❌ TEST FALLÓ")
        print("⚠️ La distribución por días de la semana necesita ajustes")
        sys.exit(1)
