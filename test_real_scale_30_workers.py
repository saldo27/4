#!/usr/bin/env python3
"""
Prueba de escalabilidad real: 30 trabajadores para validar distribución proporcional
"""

from datetime import datetime, timedelta
from scheduler import Scheduler

def test_real_scale_proportional_distribution():
    """Prueba con 30 trabajadores (escenario real de producción)"""
    
    print("=" * 80)
    print("PRUEBA REAL: DISTRIBUCIÓN PROPORCIONAL CON 30 TRABAJADORES")
    print("=" * 80)
    
    # Configuración realista de 30 trabajadores con distribución variada de porcentajes
    workers = []
    
    # 15 trabajadores al 100% (jornada completa)
    for i in range(1, 16):
        workers.append({
            'id': f'worker_100_{i:02d}',
            'name': f'Trabajador Completo {i:02d}',
            'work_percentage': 100
        })
    
    # 8 trabajadores al 75% (jornada de 3/4)
    for i in range(1, 9):
        workers.append({
            'id': f'worker_75_{i:02d}',
            'name': f'Trabajador 75% {i:02d}',
            'work_percentage': 75
        })
    
    # 5 trabajadores al 50% (media jornada)
    for i in range(1, 6):
        workers.append({
            'id': f'worker_50_{i:02d}',
            'name': f'Trabajador 50% {i:02d}',
            'work_percentage': 50
        })
    
    # 2 trabajadores al 25% (jornada mínima)
    for i in range(1, 3):
        workers.append({
            'id': f'worker_25_{i:02d}',
            'name': f'Trabajador 25% {i:02d}',
            'work_percentage': 25
        })
    
    print(f"📊 Total trabajadores configurados: {len(workers)}")
    print(f"   - 100%: {len([w for w in workers if w['work_percentage'] == 100])} trabajadores")
    print(f"   - 75%:  {len([w for w in workers if w['work_percentage'] == 75])} trabajadores")
    print(f"   - 50%:  {len([w for w in workers if w['work_percentage'] == 50])} trabajadores")
    print(f"   - 25%:  {len([w for w in workers if w['work_percentage'] == 25])} trabajadores")
    
    # Configuración para 3 meses (período de prueba realista)
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
        'gap_between_shifts': 24
    }
    
    # Crear scheduler y generar horario
    scheduler = Scheduler(config)
    
    print("\n🔄 Generando horario con 30 trabajadores (3 meses)...")
    print("   Esto puede tardar unos momentos debido al tamaño...")
    
    success = scheduler.generate_schedule(max_improvement_loops=50)  # Reducir iteraciones para acelerar
    
    if not success:
        print("❌ Error al generar el horario")
        return
        
    print("✓ Horario generado exitosamente")
    
    # Análisis detallado de la distribución con 30 trabajadores
    analyze_real_scale_distribution(scheduler, workers)

def analyze_real_scale_distribution(scheduler, workers):
    """Análisis específico para escala real de 30 trabajadores"""
    
    # Contar días especiales y turnos asignados
    special_dates = get_special_dates(scheduler)
    special_days_count = count_special_assignments(scheduler, workers, special_dates)
    
    total_special_days = sum(special_days_count.values())
    
    print(f"\n📊 ANÁLISIS DE ESCALA REAL (30 trabajadores)")
    print(f"📊 Total días especiales en el período: {len(special_dates)}")
    print(f"📊 Total turnos especiales asignados: {total_special_days}")
    
    if total_special_days == 0:
        print("⚠️  No se asignaron turnos especiales - verificar lógica de acceso al horario")
        return
    
    # Calcular distribución teórica ideal
    total_work_percentage = sum(w['work_percentage'] for w in workers)
    
    # Análisis por grupos de trabajo
    groups = {100: [], 75: [], 50: [], 25: []}
    group_ideals = {100: [], 75: [], 50: [], 25: []}
    
    for worker in workers:
        work_pct = worker['work_percentage']
        worker_id = worker['id']
        assigned = special_days_count.get(worker_id, 0)
        
        # Cálculo ideal proporcional
        ideal = (work_pct / total_work_percentage) * total_special_days
        
        groups[work_pct].append(assigned)
        group_ideals[work_pct].append(ideal)
    
    print(f"\n🎯 DISTRIBUCIÓN POR GRUPOS (Escala Real):")
    
    overall_deviations = []
    
    for pct in [100, 75, 50, 25]:
        assigned_list = groups[pct]
        ideal_list = group_ideals[pct]
        
        if not assigned_list:
            continue
            
        avg_assigned = sum(assigned_list) / len(assigned_list)
        avg_ideal = sum(ideal_list) / len(ideal_list)
        
        min_assigned = min(assigned_list)
        max_assigned = max(assigned_list)
        range_assigned = max_assigned - min_assigned
        
        # Desviación promedio del grupo respecto al ideal
        group_deviations = [abs(assigned - ideal) for assigned, ideal in zip(assigned_list, ideal_list)]
        avg_deviation = sum(group_deviations) / len(group_deviations)
        avg_deviation_pct = (avg_deviation / avg_ideal * 100) if avg_ideal > 0 else 0
        
        overall_deviations.extend([abs(a-i)/i*100 for a,i in zip(assigned_list, ideal_list) if i > 0])
        
        print(f"  Trabajadores {pct}% ({len(assigned_list)} personas):")
        print(f"    Asignados: {assigned_list}")
        print(f"    Rango: {min_assigned}-{max_assigned} (diferencia: {range_assigned})")
        print(f"    Promedio asignado: {avg_assigned:.1f}")
        print(f"    Promedio ideal: {avg_ideal:.1f}")
        print(f"    Desviación promedio: {avg_deviation:.1f} ({avg_deviation_pct:.1f}%)")
    
    # Verificación de tolerancia +/-1 global
    all_assigned = list(special_days_count.values())
    min_global = min(all_assigned)
    max_global = max(all_assigned)
    global_range = max_global - min_global
    
    print(f"\n🎯 VALIDACIÓN DE TOLERANCIA +/-1 (Global):")
    print(f"  Rango global: {min_global} - {max_global} (diferencia: {global_range})")
    print(f"  Cumple tolerancia +/-1: {'✓' if global_range <= 1 else '❌'}")
    
    # Estadísticas de proporcionalidad
    print(f"\n📈 ESTADÍSTICAS DE PROPORCIONALIDAD:")
    if overall_deviations:
        print(f"  Desviación promedio global: {sum(overall_deviations)/len(overall_deviations):.1f}%")
        print(f"  Desviación máxima: {max(overall_deviations):.1f}%")
        print(f"  Desviación mínima: {min(overall_deviations):.1f}%")
        
        # Calificación del algoritmo
        avg_error = sum(overall_deviations)/len(overall_deviations)
        if avg_error < 5:
            rating = "🌟 EXCELENTE"
        elif avg_error < 10:
            rating = "✅ BUENO"
        elif avg_error < 20:
            rating = "⚠️ ACEPTABLE"
        else:
            rating = "❌ NECESITA MEJORA"
            
        print(f"  Calificación del algoritmo: {rating}")
    
    # Predicción de escalabilidad
    print(f"\n🔮 PREDICCIÓN PARA 25-35 TRABAJADORES:")
    print(f"  Con {len(workers)} trabajadores: flexibilidad numérica {'ALTA' if global_range <= 1 else 'MEDIA'}")
    print(f"  Escalabilidad a 35 trabajadores: {'✓ EXCELENTE' if global_range <= 1 and sum(overall_deviations)/len(overall_deviations) < 10 else '⚠️ REQUIERE MONITOREO'}")

def get_special_dates(scheduler):
    """Obtiene todas las fechas especiales del período"""
    special_dates = set()
    current_date = scheduler.start_date
    
    while current_date <= scheduler.end_date:
        weekday = current_date.weekday()
        
        # Viernes (4), Sábado (5), Domingo (6)
        if weekday >= 4:
            special_dates.add(current_date)
            
        # Festivos
        if current_date in scheduler.holidays:
            special_dates.add(current_date)
            
        # Pre-festivos
        tomorrow = current_date + timedelta(days=1)
        if tomorrow in scheduler.holidays and weekday < 4:
            special_dates.add(current_date)
            
        current_date += timedelta(days=1)
    
    return special_dates

def count_special_assignments(scheduler, workers, special_dates):
    """Cuenta asignaciones de turnos especiales por trabajador"""
    special_days_count = {worker['id']: 0 for worker in workers}
    
    for date in special_dates:
        date_key = date.strftime('%Y-%m-%d')
        if date_key in scheduler.schedule:
            day_schedule = scheduler.schedule[date_key]
            for shift_id, worker_id in day_schedule.items():
                if worker_id and worker_id != 'EMPTY' and worker_id in special_days_count:
                    special_days_count[worker_id] += 1
                    
    return special_days_count

if __name__ == "__main__":
    test_real_scale_proportional_distribution()
