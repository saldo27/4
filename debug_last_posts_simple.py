#!/usr/bin/env python3
"""
Debug simplificado de la distribución de last posts.
Prueba con un caso más controlado para validar la fórmula.
"""

import sys
from datetime import datetime, timedelta
from scheduler import Scheduler

def setup_simple_test():
    """Configura un test simplificado de 7 días con 3 trabajadores."""
    
    # Configurar fechas (una semana)
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 1, 7)
    
    # 3 trabajadores con disponibilidades diferentes
    workers = [
        {"id": "W1", "name": "Worker100", "work_percentage": 100},  # Disponible 100%
        {"id": "W2", "name": "Worker75", "work_percentage": 75},   # Disponible 75%
        {"id": "W3", "name": "Worker50", "work_percentage": 50},   # Disponible 50%
    ]
    
    # Sin restricciones especiales
    unavailable_dates = []
    holidays = []
    fixed_assignments = []
    
    # Variable shifts (2 turnos por día)
    variable_shifts = {}
    
    return {
        'start_date': start_date,
        'end_date': end_date,
        'workers': workers,
        'unavailable_dates': unavailable_dates,
        'holidays': holidays,
        'fixed_assignments': fixed_assignments,
        'variable_shifts': variable_shifts
    }

def analyze_last_posts_simple(scheduler):
    """Analiza la distribución de last posts de forma simplificada."""
    
    print("\n🔍 ANÁLISIS DETALLADO DE LAST POSTS:")
    print("=" * 60)
    
    # Calcular estadísticas por trabajador
    for worker_id in ["W1", "W2", "W3"]:
        total_shifts = 0
        last_post_count = 0
        post_distribution = {"P0": 0, "P1": 0}
        
        # Contar turnos y last posts
        for date_key, posts in scheduler.schedule.items():
            for post_idx, assigned_worker in posts.items():
                if assigned_worker == worker_id:
                    total_shifts += 1
                    post_distribution[f"P{post_idx}"] += 1
                    
                    # Determinar si es last post
                    date_posts = list(posts.keys())
                    if date_posts and post_idx == max(date_posts):
                        last_post_count += 1
        
        # Calcular fórmula
        shifts_per_day = 2
        expected_last_posts = total_shifts / shifts_per_day
        deviation = last_post_count - expected_last_posts
        
        print(f"\n📊 {worker_id}:")
        print(f"   Total turnos: {total_shifts}")
        print(f"   Distribución posts: {post_distribution}")
        print(f"   Last posts actuales: {last_post_count}")
        print(f"   Last posts esperados (fórmula): {expected_last_posts:.2f}")
        print(f"   Desviación: {deviation:+.2f}")
        print(f"   Dentro de tolerancia ±1: {'✅' if abs(deviation) <= 1.0 else '❌'}")

def test_simple_last_posts():
    """Ejecuta un test simplificado de distribución de last posts."""
    
    print("🧪 TEST SIMPLIFICADO DE DISTRIBUCIÓN DE LAST POSTS")
    print("=" * 70)
    
    # Configurar test
    config = setup_simple_test()
    
    print(f"📅 Período: {config['start_date'].strftime('%Y-%m-%d')} a {config['end_date'].strftime('%Y-%m-%d')}")
    print(f"👥 Trabajadores: {len(config['workers'])}")
    print(f"📊 Turnos por día: 2")
    print(f"🎯 Fórmula: last_posts_esperados = total_turnos / 2")
    
    # Crear scheduler con configuración básica
    scheduler_config = {
        'cache_enabled': True,
        'improvement_iterations': 10,
        'optimization_level': 'medium'
    }
    scheduler = Scheduler(scheduler_config)
    
    try:
        # Generar horario
        print("\n🚀 Generando horario...")
        success = scheduler.generate_schedule(**config)
        
        if not success:
            print("❌ ERROR: No se pudo generar el horario")
            return False
            
        print("✅ Horario generado exitosamente")
        
        # Analizar distribución inicial
        print("\n📈 ANÁLISIS INICIAL:")
        analyze_last_posts_simple(scheduler)
        
        # Aplicar mejora de last posts
        print("\n🔧 Aplicando mejora de distribución de last posts...")
        scheduler.schedule_builder._adjust_last_post_distribution_improved()
        
        # Analizar distribución final
        print("\n📈 ANÁLISIS DESPUÉS DE MEJORA:")
        analyze_last_posts_simple(scheduler)
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR durante el test: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_simple_last_posts()
    
    if success:
        print("\n✅ TEST COMPLETADO EXITOSAMENTE")
    else:
        print("\n❌ TEST FALLÓ")
        sys.exit(1)
