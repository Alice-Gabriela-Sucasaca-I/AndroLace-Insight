import sys
import argparse
from pathlib import Path
from visual_extractor import VisualElementsExtractor

def main():
    parser = argparse.ArgumentParser(
        description='Extraer elementos visuales de cualquier paper de investigación'
    )
    
    parser.add_argument(
        'url', 
        help='URL del paper a extraer (ej: https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4136787/)'
    )
    
    parser.add_argument(
        '-o', '--output', 
        default='extracted_visuals',
        help='Directorio de salida (por defecto: extracted_visuals)'
    )
    
    args = parser.parse_args()
    
    print("="*60)
    print("EXTRACTOR DE ELEMENTOS VISUALES - PAPER PERSONALIZADO")
    print("="*60)
    print(f"📄 URL del paper: {args.url}")
    print(f"📁 Directorio de salida: {args.output}")
    
    extractor = VisualElementsExtractor(args.url, args.output)
    
    print("\nIniciando extracción...")
    results = extractor.run_extraction()
    
    if not results:
        print("Error en la extracción")
        return False
    
    # Mostrar resultados
    print("\n Extracción completada!")
    print(f" Figuras: {len(results['figures'])}")
    print(f" Tablas: {len(results['tables'])}")
    print(f"  Imágenes: {len(results['images'])}")
    print(f"Guardado en: {Path(args.output).absolute()}")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
