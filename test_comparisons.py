from main import MainOrchestrator

o = MainOrchestrator()

stats = o.get_system_status()
print(f"Papers: {stats['total_papers']}")
print(f"Papers con IA: {stats['papers_with_ai']}")
print(f"Efectos: {stats['total_effects']}")
print(f"Temas: {stats['themes']}")

if stats['papers_with_ai'] == 0:
    print("\nProcesando papers con IA...")
    o.step3_process_with_ai(limit=5)

if stats['total_effects'] == 0:
    print("\nGenerando efectos...")
    o.step6_generate_effects()  # <-- CORREGIDO

print("\nAsignando temas...")
o.step5_assign_themes()

print("\nGenerando comparaciones...")
comparisons = o.step8_comparisons()
print(f"Comparaciones creadas: {comparisons}")

from sqlalchemy import text
session = o.db.get_session()
result = session.execute(text("SELECT * FROM COMPARISON")).fetchall()
print(f"\nTotal comparaciones en DB: {len(result)}")
for comp in result:
    print(f"  - {comp.topic}: {comp.consensus_level}")
session.close()