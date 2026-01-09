import pandas as pd
from pathlib import Path
from src import config, utils

logger = utils.setup_logging("excel_exporter")

def export_to_excel(csv_path: Path):
    """
    Converts the signals CSV to a formatted Excel file.
    """
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_path}")
        return

    excel_path = csv_path.with_suffix('.xlsx')
    
    try:
        logger.info(f"Reading {csv_path}...")
        df = pd.read_csv(csv_path)
        
        # Sort by timestamp descending (newest first)
        if 'timestamp' in df.columns:
            df = df.sort_values('timestamp', ascending=False)

        logger.info(f"Writing to {excel_path}...")
        
        # Write to Excel
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Binance Signals')
            
            # Access the workbook and worksheet to adjust column widths
            worksheet = writer.sheets['Binance Signals']
            for column in worksheet.columns:
                max_length = 0
                column = [cell for cell in column]
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = (max_length + 2)
                worksheet.column_dimensions[column[0].column_letter].width = adjusted_width

        logger.info(f"Excel export complete: {excel_path}")
        print(f"\n[+] Created Excel Report: {excel_path}")
        
    except Exception as e:
        logger.error(f"Failed to export to Excel: {e}")

if __name__ == "__main__":
    csv_file = config.DATA_DIR / "signals_report.csv"
    export_to_excel(csv_file)
