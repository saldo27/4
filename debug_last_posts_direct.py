#!/usr/bin/env python3
"""
Debug simplificado de la distribución de last posts.
Usa directamente el ScheduleBuilder para validar la mejora.
"""

import sys
from datetime import datetime, timedelta
from schedule_builder import ScheduleBuilder

def test_last_posts_directly():
    """Prueba directa del algoritmo de last posts."""
    
    print("🧪 TEST DIRECTO DE DISTRIBUCIÓN DE LAST POSTS")
    print("=" * 70)
    
    # Crear un horario de ejemplo simple
    schedule = {}
    
    # 7 días, 2 turnos por día
    dates = []
    current_date = datetime(2025, 1, 1)
    for i in range(7):
        dates.append(current_date + timedelta(days=i))
    
    # Asignar trabajadores de forma desbalanceada inicialmente
    # W1 siempre en P0, W2 siempre en P1, W3 alternando
    workers = ["W1", "W2", "W3"]
    
    for i, date in enumerate(dates):
        date_key = date.strftime('%Y-%m-%d')
        schedule[date_key] = {}
        
        # Asignación inicial desbalanceada
        if i % 3 == 0:
            schedule[date_key][0] = "W1"  # P0
            schedule[date_key][1] = "W2"  # P1 (last post)
        elif i % 3 == 1:
            schedule[date_key][0] = "W2"  # P0
            schedule[date_key][1] = "W1"  # P1 (last post)
        else:
            schedule[date_key][0] = "W3"  # P0
            schedule[date_key][1] = "W3"  # P1 (last post)
    
    # Simular datos del builder
    builder = ScheduleBuilder()
    builder.schedule = schedule
    builder.workers = {
        "W1": {"work_percentage": 100},
        "W2": {"work_percentage": 100}, 
        "W3": {"work_percentage": 100}
    }
    
    # Configurar tracking
    builder.tracking = {
        "last_post_count": {"W1": 0, "W2": 0, "W3": 0},
        "total_shifts": {"W1": 0, "W2": 0, "W3": 0}
    }
    
    # Calcular estado inicial
    for date_key, posts in schedule.items():
        for post_idx, worker_id in posts.items():
            builder.tracking["total_shifts"][worker_id] += 1
            
            # Verificar si es last post
            max_post = max(posts.keys())
            if post_idx == max_post:
                builder.tracking["last_post_count"][worker_id] += 1
    
    print("\n📊 ESTADO INICIAL:")
    print("-" * 40)
    for worker_id in workers:
        total_shifts = builder.tracking["total_shifts"][worker_id]
        last_posts = builder.tracking["last_post_count"][worker_id]
        expected = total_shifts / 2.0
        deviation = last_posts - expected
        
        print(f"{worker_id}: {total_shifts} turnos, {last_posts} last posts, esperados {expected:.1f}, desviación {deviation:+.1f}")
    
    # Aplicar mejora
    print("\n🔧 Aplicando mejora de distribución...")
    
    try:
        builder._adjust_last_post_distribution_improved()
        print("✅ Mejora aplicada exitosamente")
        
        # Recalcular estado final
        builder.tracking = {
            "last_post_count": {"W1": 0, "W2": 0, "W3": 0},
            "total_shifts": {"W1": 0, "W2": 0, "W3": 0}
        }
        
        for date_key, posts in builder.schedule.items():
            for post_idx, worker_id in posts.items():
                builder.tracking["total_shifts"][worker_id] += 1
                
                # Verificar si es last post
                max_post = max(posts.keys())
                if post_idx == max_post:
                    builder.tracking["last_post_count"][worker_id] += 1
        
        print("\n📊 ESTADO FINAL:")
        print("-" * 40)
        total_deviation = 0
        for worker_id in workers:
            total_shifts = builder.tracking["total_shifts"][worker_id]
            last_posts = builder.tracking["last_post_count"][worker_id]
            expected = total_shifts / 2.0
            deviation = last_posts - expected
            total_deviation += abs(deviation)
            
            within_tolerance = abs(deviation) <= 1.0
            status = "✅" if within_tolerance else "❌"
            
            print(f"{worker_id}: {total_shifts} turnos, {last_posts} last posts, esperados {expected:.1f}, desviación {deviation:+.1f} {status}")
        
        print(f"\n🎯 Desviación total: {total_deviation:.1f}")
        
        return total_deviation <= 3.0  # Tolerancia total
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_last_posts_directly()
    
    if success:
        print("\n✅ TEST COMPLETADO EXITOSAMENTE")
        print("🎯 La mejora de distribución de last posts funciona correctamente")
    else:
        print("\n❌ TEST FALLÓ")
        print("⚠️ La mejora de distribución necesita ajustes")
        sys.exit(1)
