import sys
from pathlib import Path
from visual_extractor import VisualElementsExtractor

def main():
    """Run the complete extraction process."""
    print("="*60)
    print("VISUAL ELEMENTS EXTRACTION FOR RESEARCH PAPERS")
    print("="*60)
    
    # Configuration
    paper_url = "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4136787/"
    output_dir = "extracted_visuals"
    
    # Step 1: Extract visual elements
    print("\n1. Extracting visual elements from paper...")
    print(f"URL: {paper_url}")
    
    extractor = VisualElementsExtractor(paper_url, output_dir)
    results = extractor.run_extraction()
    
    if not results:
        print(" Extraction failed!")
        return False
    
    print("✅ Extraction completed successfully!")
    
    # Step 2: Display results summary
    print("\n2. Extraction Results Summary:")
    print("-" * 40)
    print(f"📊 Total Figures: {len(results['figures'])}")
    print(f"📋 Total Tables: {len(results['tables'])}")
    print(f"🖼️  Total Images: {len(results['images'])}")
    print(f"📁 Output Directory: {output_dir}")
    
    # Step 3: Show extracted elements details
    print("\n3. Detailed Results:")
    print("-" * 40)
    
    if results['figures']:
        print("\n📊 FIGURES:")
        for i, fig in enumerate(results['figures'], 1):
            print(f"  {i}. Caption: {fig.get('caption', 'No caption')[:100]}...")
            print(f"     URL: {fig.get('image_url', 'No URL')}")
            print(f"     Local: {fig.get('local_path', 'Not saved')}")
    
    if results['tables']:
        print("\n📋 TABLES:")
        for i, table in enumerate(results['tables'], 1):
            print(f"  {i}. Caption: {table.get('caption', 'No caption')[:100]}...")
            print(f"     Local: {table.get('local_path', 'Not saved')}")
    
    if results['images']:
        print("\n🖼️  IMAGES:")
        for i, img in enumerate(results['images'], 1):
            print(f"  {i}. Alt text: {img.get('alt_text', 'No alt text')[:100]}...")
            print(f"     URL: {img.get('image_url', 'No URL')}")
            print(f"     Local: {img.get('local_path', 'Not saved')}")
    
    # Step 4: Final summary
    print("\n" + "="*60)
    print("EXTRACTION COMPLETE")
    print("="*60)
    print(f"📁 All files saved in: {Path(output_dir).absolute()}")
    print(f"📄 Metadata JSON: {Path(output_dir) / 'visual_elements_metadata.json'}")
    print("\nNext steps:")
    print("1. Review extracted files in the output directory")
    print("2. Use extract_custom_paper.py for other papers")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
