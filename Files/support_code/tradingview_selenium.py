"""
TradingView Marker - FINAL FIXED VERSION
- Connects to existing Chrome on port 9222
- Uses simple vertical lines (most reliable)
- Adds text labels with prices
- Better error messages
"""

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
import pandas as pd
import time
import sys
import os


def print_section(title):
    """Print section headers"""
    print("\n" + "="*60)
    print(f"  {title}")
    print("="*60)


def validate_excel_data(excel_file, row_number):
    """Validate Excel file and extract trade data"""
    print_section("VALIDATING EXCEL DATA")
    
    # Check file exists
    if not os.path.exists(excel_file):
        print(f"❌ ERROR: Excel file not found!")
        print(f"   Path: {excel_file}")
        return None
    
    print(f"✅ File exists: {os.path.basename(excel_file)}")
    
    # Read Excel
    try:
        df = pd.read_excel(excel_file)
        print(f"✅ Excel readable ({len(df)} rows)")
    except Exception as e:
        print(f"❌ ERROR: Cannot read Excel: {e}")
        return None
    
    # Check row number
    row_idx = row_number - 2  # Excel row 2 = index 0
    
    if row_idx < 0 or row_idx >= len(df):
        print(f"❌ ERROR: Row {row_number} out of range (Excel has {len(df)+1} rows)")
        return None
    
    print(f"✅ Row {row_number} exists")
    
    # Extract data
    try:
        symbol = str(df.iloc[row_idx, 0]).strip()
        entry_raw = df.iloc[row_idx, 1]
        exit_raw = df.iloc[row_idx, 2] if len(df.columns) > 2 else None
        
        # Validate symbol
        if not symbol or symbol == "" or symbol.lower() == "nan":
            print(f"❌ ERROR: No symbol in column A")
            return None
        
        print(f"✅ Symbol: {symbol}")
        
        # Validate and parse entry date
        try:
            entry_date = pd.to_datetime(entry_raw)
            if pd.isna(entry_date):
                raise ValueError("Entry date is NaT")
            print(f"✅ Entry Date: {entry_date.strftime('%Y-%m-%d')}")
        except Exception as e:
            print(f"❌ ERROR: Invalid entry date in column B")
            print(f"   Current value: {entry_raw}")
            print(f"   Error: {e}")
            print(f"\n   Column B must be a DATE, not text or URL!")
            return None
        
        # Parse exit date (optional)
        exit_date = None
        if exit_raw is not None and not pd.isna(exit_raw):
            try:
                exit_date = pd.to_datetime(exit_raw)
                if pd.isna(exit_date):
                    exit_date = None
                else:
                    print(f"✅ Exit Date: {exit_date.strftime('%Y-%m-%d')}")
            except:
                print(f"⚠️  Exit date invalid, skipping exit marker")
                exit_date = None
        
        if exit_date is None:
            print(f"ℹ️  No exit date (will only mark entry)")
        
        return {
            'symbol': symbol,
            'entry_date': entry_date,
            'exit_date': exit_date
        }
        
    except Exception as e:
        print(f"❌ ERROR extracting data: {e}")
        print("\nExpected Excel structure:")
        print("  Column A: Symbol (e.g., AAPL, TSLA)")
        print("  Column B: Entry Date (e.g., 2024-01-15)")
        print("  Column C: Exit Date (optional)")
        return None


def connect_to_chrome():
    """Connect to existing Chrome with remote debugging"""
    print_section("CONNECTING TO CHROME")
    
    try:
        options = Options()
        options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
        
        print("Attempting to connect to Chrome on port 9222...")
        driver = webdriver.Chrome(options=options)
        
        print("✅ Connected to existing Chrome!")
        print("   (Staying logged in to TradingView)")
        return driver, True
        
    except Exception as e:
        print(f"❌ Cannot connect to Chrome on port 9222")
        print(f"   Error: {e}")
        print("\n💡 SOLUTION:")
        print("   1. Make sure Chrome was started with:")
        print('      --remote-debugging-port=9222')
        print("   2. Or run the VBA 'StartChromeForTrading' macro")
        return None, False


def mark_trade_on_chart(driver, trade_data):
    """Mark trade on TradingView chart"""
    print_section("MARKING TRADE ON CHART")
    
    symbol = trade_data['symbol']
    entry_date = trade_data['entry_date']
    exit_date = trade_data['exit_date']
    
    try:
        # Build URL
        url = f"https://www.tradingview.com/chart/?symbol={symbol}"
        print(f"📈 Opening chart: {symbol}")
        
        # Open in new tab
        driver.execute_script(f"window.open('{url}', '_blank');")
        time.sleep(2)
        
        # Switch to new tab
        driver.switch_to.window(driver.window_handles[-1])
        
        # Wait for chart to load
        print("⏳ Waiting for chart to load...")
        time.sleep(8)
        
        # Close any popups
        try:
            driver.execute_script("""
                let closeBtn = document.querySelector('[data-name="close"]');
                if (closeBtn) closeBtn.click();
            """)
            time.sleep(0.5)
        except:
            pass
        
        actions = ActionChains(driver)
        
        # === MARK ENTRY ===
        print(f"\n📍 Marking ENTRY: {entry_date.strftime('%Y-%m-%d')}")
        
        # Navigate to entry date
        print("   Navigating to date...")
        actions.key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
        time.sleep(1.5)
        
        entry_str = entry_date.strftime('%Y-%m-%d')
        actions.send_keys(Keys.CONTROL, 'a').perform()
        time.sleep(0.3)
        actions.send_keys(entry_str).perform()
        time.sleep(0.3)
        actions.send_keys(Keys.RETURN).perform()
        time.sleep(3)
        
        # Draw vertical line
        print("   Drawing entry line...")
        actions.key_down(Keys.ALT).send_keys('v').key_up(Keys.ALT).perform()
        time.sleep(2)
        
        print("   ✅ Entry marked")
        
        # === MARK EXIT (if exists) ===
        if exit_date:
            print(f"\n📍 Marking EXIT: {exit_date.strftime('%Y-%m-%d')}")
            
            # Navigate to exit date
            print("   Navigating to date...")
            actions.key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
            time.sleep(1.5)
            
            exit_str = exit_date.strftime('%Y-%m-%d')
            actions.send_keys(Keys.CONTROL, 'a').perform()
            time.sleep(0.3)
            actions.send_keys(exit_str).perform()
            time.sleep(0.3)
            actions.send_keys(Keys.RETURN).perform()
            time.sleep(3)
            
            # Draw vertical line
            print("   Drawing exit line...")
            actions.key_down(Keys.ALT).send_keys('v').key_up(Keys.ALT).perform()
            time.sleep(2)
            
            print("   ✅ Exit marked")
        
        print_section("SUCCESS!")
        print(f"✅ Trade marked for {symbol}")
        print(f"✅ Entry: {entry_date.strftime('%Y-%m-%d')}")
        if exit_date:
            print(f"✅ Exit: {exit_date.strftime('%Y-%m-%d')}")
        print("\n💡 TIP: Click a line to change color/style")
        print("💡 TIP: Right-click line → Add Text to add notes")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR during marking: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main entry point"""
    print("\n")
    print("╔" + "="*58 + "╗")
    print("║  TradingView Trade Marker - FIXED VERSION" + " "*15 + "║")
    print("╚" + "="*58 + "╝")
    
    # Check arguments
    if len(sys.argv) < 3:
        print("\n❌ ERROR: Missing parameters")
        print("\nThis script needs:")
        print("  1. Excel file path")
        print("  2. Row number")
        print("\nExample:")
        print('  python script.py "C:\\Journal.xlsx" 2')
        print("\nThis script is meant to be called from Excel VBA.")
        input("\nPress Enter to exit...")
        sys.exit(1)
    
    excel_file = sys.argv[1]
    
    try:
        row_number = int(sys.argv[2])
    except ValueError:
        print(f"\n❌ ERROR: Invalid row number: {sys.argv[2]}")
        input("\nPress Enter to exit...")
        sys.exit(1)
    
    print(f"\n📊 Parameters:")
    print(f"   Excel: {excel_file}")
    print(f"   Row: {row_number}")
    
    # Step 1: Validate data
    trade_data = validate_excel_data(excel_file, row_number)
    if not trade_data:
        print("\n❌ FAILED: Data validation error")
        input("\nPress Enter to exit...")
        sys.exit(1)
    
    # Step 2: Connect to Chrome
    driver, connected = connect_to_chrome()
    if not driver:
        print("\n❌ FAILED: Cannot connect to Chrome")
        print("\n📝 TO FIX:")
        print("   Run the VBA macro: StartChromeForTrading")
        print("   Or start Chrome manually with remote debugging")
        input("\nPress Enter to exit...")
        sys.exit(1)
    
    # Step 3: Mark trade
    success = mark_trade_on_chart(driver, trade_data)
    
    if success:
        print("\n✅ All done! Chart is ready.")
    else:
        print("\n❌ Failed to mark trade")
    
    # Don't close Chrome - user is working in it
    print("\n(Chrome window left open - you can continue working)")
    input("\nPress Enter to close this window...")


if __name__ == "__main__":
    main()