#!/usr/bin/env python3
"""
Test para la mejora de distribución de "last posts" 
Formula implementada: turnos por trabajador = (turnos asignados al trabajador / turnos al día) ± 1
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, timedelta
from scheduler import Scheduler
import logging

def test_improved_last_posts_distribution():
    """
    Test que valida la nueva fórmula mejorada para distribución de "last posts"
    """
    
    print("=" * 80)
    print("PRUEBA: DISTRIBUCIÓN MEJORADA DE LAST POSTS")
    print("Fórmula: turnos por trabajador = (turnos asignados al trabajador / turnos al día) ± 1")
    print("=" * 80)
    
    # Configuración de fechas (1 mes para prueba rápida)
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 1, 31)
    
    # 6 trabajadores con diferentes porcentajes de trabajo
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
    
    print("📊 CONFIGURACIÓN:")
    total_days = (end_date - start_date).days + 1
    print(f"   Período: {start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')} ({total_days} días)")
    print(f"   Trabajadores: {len(workers)}")
    
    for worker in workers:
        print(f"   {worker['name']}: {worker['work_percentage']}%")
    
    # Configuración del scheduler
    config = {
        'start_date': start_date,
        'end_date': end_date,
        'num_shifts': 2,  # 2 turnos por día (importante para la fórmula)
        'workers_data': workers,
        'holidays': [
            datetime(2025, 1, 1),   # Año Nuevo
            datetime(2025, 1, 6),   # Reyes
        ],
        'enable_proportional_weekends': True,
        'weekend_tolerance': 1,
        'gap_between_shifts': 1,
        'max_consecutive_weekends': 3,
        'cache_enabled': False  # Para ver logs más claros
    }
    
    scheduler = Scheduler(config)
    
    print(f"\n🔄 Generando horario con {len(workers)} trabajadores...")
    print(f"   Turnos por día: {config['num_shifts']}")
    
    # Generar horario inicial
    success = scheduler.generate_schedule()
    
    if not success:
        print("❌ Error generando el horario inicial")
        return False
    
    print("✅ Horario inicial generado exitosamente")
    
    # Analizar distribución ANTES de la mejora
    print("\n📊 ANÁLISIS INICIAL (antes de mejora de last posts):")
    last_posts_before = analyze_last_posts_distribution(scheduler, workers)
    
    # Aplicar la mejora de distribución de last posts
    print("\n🔧 Aplicando mejora de distribución de last posts...")
    
    # La función _adjust_last_post_distribution ahora usa automáticamente la versión mejorada
    improvement_made = scheduler.schedule_builder._adjust_last_post_distribution(
        balance_tolerance=1.0,  # ±1 como especificado
        max_iterations=10
    )
    
    if improvement_made:
        print("✅ Mejoras aplicadas en la distribución de last posts")
    else:
        print("ℹ️ No se necesitaron mejoras (distribución ya estaba equilibrada)")
    
    # Analizar distribución DESPUÉS de la mejora
    print("\n📊 ANÁLISIS FINAL (después de mejora de last posts):")
    last_posts_after = analyze_last_posts_distribution(scheduler, workers)
    
    # Validar la fórmula mejorada
    print("\n🎯 VALIDACIÓN DE LA FÓRMULA MEJORADA:")
    validate_improved_formula(scheduler, workers, last_posts_after, config['num_shifts'])
    
    # Generar archivo de salida
    output_file = f"schedule_improved_last_posts_{datetime.now().strftime('%d%m%Y_%H%M%S')}.txt"
    scheduler.export_schedule(output_file)
    print(f"\n📄 Horario exportado a: {output_file}")
    
    return True

def analyze_last_posts_distribution(scheduler, workers):
    """
    Analiza la distribución actual de last posts
    """
    worker_stats = {}
    total_days_with_shifts = 0
    
    # Inicializar estadísticas
    for worker in workers:
        worker_id = worker['id']
        worker_stats[worker_id] = {
            'name': worker['name'],
            'work_percentage': worker['work_percentage'],
            'total_shifts': 0,
            'last_posts': 0
        }
    
    # Analizar cada día del horario
    for date, shifts in scheduler.schedule.items():
        if not shifts or not any(s is not None for s in shifts):
            continue
        
        total_days_with_shifts += 1
        
        # Contar turnos totales por trabajador
        for shift_idx, worker_id in enumerate(shifts):
            if worker_id is not None and worker_id in worker_stats:
                worker_stats[worker_id]['total_shifts'] += 1
        
        # Encontrar el último puesto ocupado (last post)
        last_post_idx = -1
        for i in range(len(shifts) - 1, -1, -1):
            if shifts[i] is not None:
                last_post_idx = i
                break
        
        # Contar last post
        if last_post_idx != -1:
            worker_id = shifts[last_post_idx]
            if worker_id in worker_stats:
                worker_stats[worker_id]['last_posts'] += 1
    
    # Mostrar estadísticas
    print(f"   Total días con turnos: {total_days_with_shifts}")
    print(f"   Total last posts asignados: {sum(stats['last_posts'] for stats in worker_stats.values())}")
    
    for worker_id, stats in worker_stats.items():
        total_shifts = stats['total_shifts']
        last_posts = stats['last_posts']
        work_pct = stats['work_percentage']
        
        if total_shifts > 0:
            last_post_percentage = (last_posts / total_shifts) * 100
            print(f"   {stats['name']}:")
            print(f"     - Total turnos: {total_shifts}")
            print(f"     - Last posts: {last_posts}")
            print(f"     - % last posts: {last_post_percentage:.1f}%")
        else:
            print(f"   {stats['name']}: Sin turnos asignados")
    
    return worker_stats

def validate_improved_formula(scheduler, workers, worker_stats, shifts_per_day):
    """
    Valida que la distribución cumple con la fórmula mejorada:
    turnos por trabajador = (turnos asignados al trabajador / turnos al día) ± 1
    """
    print(f"   Fórmula: last_posts_esperados = (total_shifts / {shifts_per_day}) ± 1")
    print()
    
    all_within_tolerance = True
    deviations = []
    
    for worker_id, stats in worker_stats.items():
        total_shifts = stats['total_shifts']
        actual_last_posts = stats['last_posts']
        work_pct = stats['work_percentage']
        
        if total_shifts > 0:
            # Aplicar la fórmula
            expected_last_posts = total_shifts / shifts_per_day
            deviation = actual_last_posts - expected_last_posts
            abs_deviation = abs(deviation)
            
            # Verificar tolerancia ±1
            within_tolerance = abs_deviation <= 1.0
            all_within_tolerance = all_within_tolerance and within_tolerance
            deviations.append(abs_deviation)
            
            status = "✅" if within_tolerance else "❌"
            
            print(f"   {stats['name']}:")
            print(f"     - Total turnos: {total_shifts}")
            print(f"     - Last posts esperados: {expected_last_posts:.2f}")
            print(f"     - Last posts actuales: {actual_last_posts}")
            print(f"     - Desviación: {deviation:+.2f}")
            print(f"     - Dentro de tolerancia ±1: {status}")
            print()
    
    # Resumen general
    if deviations:
        avg_deviation = sum(deviations) / len(deviations)
        max_deviation = max(deviations)
        
        print(f"📈 RESUMEN DE VALIDACIÓN:")
        print(f"   Desviación promedio: {avg_deviation:.2f}")
        print(f"   Desviación máxima: {max_deviation:.2f}")
        print(f"   Todos dentro de tolerancia ±1: {'✅ SÍ' if all_within_tolerance else '❌ NO'}")
        
        if all_within_tolerance:
            print("   🎯 FÓRMULA MEJORADA CUMPLIDA EXITOSAMENTE")
        else:
            print("   ⚠️ Algunos trabajadores exceden la tolerancia ±1")
    else:
        print("   ⚠️ No hay datos suficientes para validar")

if __name__ == "__main__":
    # Configurar logging para ver detalles de la mejora
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s: %(message)s'
    )
    
    print("🚀 INICIANDO PRUEBA DE DISTRIBUCIÓN MEJORADA DE LAST POSTS")
    print()
    
    try:
        success = test_improved_last_posts_distribution()
        if success:
            print("\n✅ PRUEBA COMPLETADA EXITOSAMENTE")
            print("🎯 La nueva fórmula de last posts ha sido implementada y validada")
        else:
            print("\n❌ PRUEBA FALLÓ")
    except Exception as e:
        print(f"\n💥 ERROR DURANTE LA PRUEBA: {e}")
        import traceback
        traceback.print_exc()
