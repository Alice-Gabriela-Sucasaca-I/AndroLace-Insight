from main import MainOrchestrator
from mysql_database import MySQLManager
import time

def main():
    print("INICIANDO PROCESAMIENTO COMPLETO DE DATOS NASA")
    print("=" * 60)
    
    orchestrator = MainOrchestrator()
    
    print("1. verificando conexión a la base de datos...")
    if not orchestrator.db.test_connection():
        print("    Error de conexión")
        return
    
    # Paso 2: Generar efectos
    print(" Generando efectos para papers...")
    effects_count = orchestrator.step4_generate_effects()
    print(f" {effects_count} efectos generados")
    
    time.sleep(1)
    
    # Paso 3: Asignar temas
    print("3, Asignando temas a papers...")
    themes_count = orchestrator.step5_assign_themes()
    print(f"    {themes_count} temas asignados")
    
    time.sleep(1)
    
    # Paso 4: Generar comparaciones
    print("4.  Generando comparaciones...")
    comparisons_count = orchestrator.step6_generate_comparisons()
    print(f"    {comparisons_count} comparaciones generadas")
    
    print("=" * 60)
    print("PROCESAMIENTO COMPLETADO")
    print(f" Resumen:")
    print(f"   • Efectos: {effects_count}")
    print(f"   • Temas: {themes_count}") 
    print(f"   • Comparaciones: {comparisons_count}")

if __name__ == "__main__":
    main()