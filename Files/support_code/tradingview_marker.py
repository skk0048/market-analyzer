"""
TradingView Marker - FIXED DATE SELECTION
- Properly clears old date before pasting new one
- Better Chrome connection detection
- Works with both Chrome and Edge
"""

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.common.action_chains import ActionChains
import pandas as pd
import time
import sys
import os
import requests


def print_section(title):
    """Print section headers"""
    print("\n" + "="*60)
    print(f"  {title}")
    print("="*60)


def check_debug_port():
    """Check what's actually running on port 9222"""
    try:
        response = requests.get("http://127.0.0.1:9222/json/version", timeout=2)
        if response.status_code == 200:
            data = response.json()
            browser_info = data.get('Browser', '')
            user_agent = data.get('User-Agent', '')
            
            print(f"✅ Port 9222 is active")
            print(f"   Browser: {browser_info}")
            
            if 'Edg' in browser_info or 'Edge' in user_agent:
                return "Edge"
            elif 'Chrome' in browser_info or 'Chrome' in user_agent:
                return "Chrome"
            else:
                return "Unknown"
        else:
            return None
    except:
        return None


def connect_to_browser():
    """
    Connect to existing browser on port 9222
    Detects which browser is running and uses appropriate driver
    """
    print_section("CONNECTING TO BROWSER")
    
    # First check what's on port 9222
    browser_type = check_debug_port()
    
    if browser_type is None:
        print("❌ No browser running on port 9222")
        print("\n💡 SOLUTION:")
        print("\nFor Chrome:")
        print('   "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe" --remote-debugging-port=9222 --user-data-dir="C:\\chrome_debug"')
        print("\nFor Edge:")
        print('   "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe" --remote-debugging-port=9222 --user-data-dir="C:\\edge_debug"')
        return None, None
    
    print(f"Detected browser: {browser_type}")
    
    # Try to connect with appropriate driver
    if browser_type == "Chrome":
        try:
            print("Connecting to Chrome...")
            chrome_options = ChromeOptions()
            chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
            driver = webdriver.Chrome(options=chrome_options)
            print("✅ Connected to Chrome!")
            return driver, "Chrome"
        except Exception as e:
            print(f"❌ Chrome driver error: {str(e)[:150]}")
            print("\n⚠️  Chrome is running but ChromeDriver failed")
            print("   Trying Edge driver as fallback...")
    
    # Try Edge (either detected as Edge, or Chrome connection failed)
    try:
        print("Connecting to Edge...")
        edge_options = EdgeOptions()
        edge_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
        driver = webdriver.Edge(options=edge_options)
        print("✅ Connected to Edge!")
        return driver, "Edge"
    except Exception as e:
        print(f"❌ Edge driver error: {str(e)[:150]}")
    
    print("\n❌ Could not connect with any driver")
    return None, None


def validate_excel_data(excel_file, row_number):
    """Validate Excel file and extract trade data"""
    print_section("VALIDATING EXCEL DATA")
    
    if not os.path.exists(excel_file):
        print(f"❌ ERROR: Excel file not found: {excel_file}")
        return None
    
    print(f"✅ File exists: {os.path.basename(excel_file)}")
    
    try:
        df = pd.read_excel(excel_file)
        print(f"✅ Excel readable ({len(df)} rows)")
    except Exception as e:
        print(f"❌ ERROR: Cannot read Excel: {e}")
        return None
    
    row_idx = row_number - 2
    
    if row_idx < 0 or row_idx >= len(df):
        print(f"❌ ERROR: Row {row_number} out of range")
        return None
    
    print(f"✅ Row {row_number} exists")
    
    try:
        symbol = str(df.iloc[row_idx, 0]).strip()
        entry_raw = df.iloc[row_idx, 1]
        exit_raw = df.iloc[row_idx, 2] if len(df.columns) > 2 else None
        
        if not symbol or symbol == "" or symbol.lower() == "nan":
            print(f"❌ ERROR: No symbol in column A")
            return None
        
        print(f"✅ Symbol: {symbol}")
        
        try:
            entry_date = pd.to_datetime(entry_raw)
            if pd.isna(entry_date):
                raise ValueError("Entry date is NaT")
            print(f"✅ Entry Date: {entry_date.strftime('%Y-%m-%d')}")
        except Exception as e:
            print(f"❌ ERROR: Invalid entry date")
            print(f"   Value: {entry_raw}")
            return None
        
        exit_date = None
        if exit_raw is not None and not pd.isna(exit_raw):
            try:
                exit_date = pd.to_datetime(exit_raw)
                if not pd.isna(exit_date):
                    print(f"✅ Exit Date: {exit_date.strftime('%Y-%m-%d')}")
            except:
                print(f"⚠️  Exit date invalid, skipping")
                exit_date = None
        
        if exit_date is None:
            print(f"ℹ️  No exit date (only marking entry)")
        
        return {
            'symbol': symbol,
            'entry_date': entry_date,
            'exit_date': exit_date
        }
        
    except Exception as e:
        print(f"❌ ERROR extracting data: {e}")
        return None


def go_to_date_properly(actions, target_date):
    """
    Navigate to date with PROPER selection and clearing
    This fixes the date paste issue!
    """
    date_str = target_date.strftime('%Y-%m-%d')
    
    # Step 1: Open Go To Date dialog (Alt+G)
    actions.key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
    time.sleep(1.5)
    #backspace to delete
    actions.send_keys(Keys.BACKSPACE * 10).perform()
    time.sleep(0.3)
    
    # Step 4: Type new date
    actions.send_keys(date_str).perform()
    time.sleep(0.5)
    
    # Step 5: Press Enter
    actions.send_keys(Keys.RETURN).perform()
    time.sleep(3)
    
    print(f"   ✓ Navigated to {date_str}")


def mark_trade_on_chart(driver, trade_data, browser_name):
    """Mark trade on TradingView chart with FIXED date navigation"""
    print_section(f"MARKING TRADE (using {browser_name})")
    
    symbol = trade_data['symbol']
    entry_date = trade_data['entry_date']
    exit_date = trade_data['exit_date']
    
    try:
        # Build URL
        url = f"https://www.tradingview.com/chart/?symbol={symbol}"
        print(f"📈 Opening chart: {symbol}")
        
        # Check current windows
        initial_windows = len(driver.window_handles)
        print(f"   Current tabs: {initial_windows}")
        
        # Open in new tab
        driver.execute_script(f"window.open('{url}', '_blank');")
        time.sleep(2)
        
        # Switch to new tab
        new_windows = len(driver.window_handles)
        print(f"   Tabs after opening: {new_windows}")
        
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
        print(f"\n📍 MARKING ENTRY: {entry_date.strftime('%Y-%m-%d')}")
        
        # Navigate to entry date (FIXED METHOD)
        go_to_date_properly(actions, entry_date)
        
        # Draw vertical line (Alt+V)
        print("   Drawing entry line...")
        actions.key_down(Keys.ALT).send_keys('v').key_up(Keys.ALT).perform()
        time.sleep(2)
        
        print("   ✅ Entry marked successfully")
        
        # === MARK EXIT ===
        if exit_date:
            print(f"\n📍 MARKING EXIT: {exit_date.strftime('%Y-%m-%d')}")
            
            # Navigate to exit date (FIXED METHOD)
            go_to_date_properly(actions, exit_date)
            
            # Draw vertical line (Alt+V)
            print("   Drawing exit line...")
            actions.key_down(Keys.ALT).send_keys('v').key_up(Keys.ALT).perform()
            time.sleep(2)
            
            print("   ✅ Exit marked successfully")
        
        print_section("SUCCESS!")
        print(f"✅ Trade marked for {symbol}")
        print(f"✅ Browser: {browser_name}")
        print(f"✅ Entry: {entry_date.strftime('%Y-%m-%d')}")
        if exit_date:
            print(f"✅ Exit: {exit_date.strftime('%Y-%m-%d')}")
        
        print("\n💡 TIPS:")
        print("   - Click a line to change color/style")
        print("   - Right-click line → Add text label")
        print("   - Lines are SAVED (you're logged in!)")
        
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
    print("║  TradingView Marker - FIXED DATE SELECTION" + " "*13 + "║")
    print("╚" + "="*58 + "╝")
    
    # Check arguments
    if len(sys.argv) < 3:
        print("\n❌ ERROR: Missing parameters")
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
    
    # Validate data
    trade_data = validate_excel_data(excel_file, row_number)
    if not trade_data:
        print("\n❌ FAILED: Data validation error")
        input("\nPress Enter to exit...")
        sys.exit(1)
    
    # Connect to browser
    driver, browser_name = connect_to_browser()
    if not driver:
        print("\n❌ FAILED: Cannot connect to browser")
        print("\n📝 TROUBLESHOOTING:")
        print("\n1. Check if browser is running with remote debugging:")
        print("   Open: http://127.0.0.1:9222/json/version")
        print("   Should show browser info")
        print("\n2. Make sure you started browser with:")
        print("   --remote-debugging-port=9222")
        print("\n3. Try closing ALL browser windows and start fresh")
        input("\nPress Enter to exit...")
        sys.exit(1)
    
    # Mark trade
    success = mark_trade_on_chart(driver, trade_data, browser_name)
    
    if success:
        print(f"\n✅ All done! Check {browser_name}.")
    else:
        print("\n❌ Failed to mark trade")
    
    print(f"\n({browser_name} window left open)")
    input("\nPress Enter to close this window...")


if __name__ == "__main__":
    main()
