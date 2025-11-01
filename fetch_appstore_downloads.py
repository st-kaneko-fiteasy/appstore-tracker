import jwt
import requests
import gzip
from datetime import datetime, timedelta
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import io

class AppStoreDownloadTracker:
    def __init__(self, key_id, issuer_id, private_key_path, 
                 google_creds_path, sheet_name):
        self.key_id = key_id
        self.issuer_id = issuer_id
        self.private_key_path = private_key_path
        self.google_creds_path = google_creds_path
        self.sheet_name = sheet_name
        
    def generate_token(self):
        """App Store Connect APIのJWTトークンを生成"""
        with open(self.private_key_path, 'r') as f:
            private_key = f.read()
        
        token = jwt.encode(
            {
                'iss': self.issuer_id,
                'exp': datetime.utcnow() + timedelta(minutes=20),
                'aud': 'appstoreconnect-v1'
            },
            private_key,
            algorithm='ES256',
            headers={'kid': self.key_id}
        )
        return token
    
    def get_sales_report(self, vendor_number, report_date):
        """App Store Connectからダウンロード数を取得"""
        token = self.generate_token()
        
        url = 'https://api.appstoreconnect.apple.com/v1/salesReports'
        headers = {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/a-gzip'
        }
        
        params = {
            'filter[frequency]': 'DAILY',
            'filter[reportSubType]': 'SUMMARY',
            'filter[reportType]': 'SALES',
            'filter[vendorNumber]': vendor_number,
            'filter[reportDate]': report_date
        }
        
        print(f"レポート取得中: {report_date}")
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = gzip.decompress(response.content).decode('utf-8')
            return data
        elif response.status_code == 404:
            print(f"レポートが見つかりません（データがない可能性があります）")
            return None
        else:
            print(f"エラー: {response.status_code}")
            print(response.text)
            return None
    
    def parse_report(self, report_data, report_date):
        """レポートをパースしてダウンロード数を抽出"""
        if not report_data:
            return pd.DataFrame()
        
        lines = report_data.strip().split('\n')
        header = lines[0].split('\t')
        data = [line.split('\t') for line in lines[1:]]
        
        df = pd.DataFrame(data, columns=header)
        
        # デバッグ: 利用可能な列名を表示
        print(f"利用可能な列: {df.columns.tolist()}")
        
        # ダウンロードのみを抽出
        downloads = df[df['Product Type Identifier'].isin(['1', '1F', '7'])].copy()
        
        if downloads.empty:
            print("ダウンロードデータがありません")
            return pd.DataFrame()
        
        # 基本のデータフレーム（Installation Typeを除外）
        result = pd.DataFrame({
            'Date': report_date,
            'App Name': downloads['Title'],
            'SKU': downloads['SKU'],
            'Country': downloads['Country Code'],
            'Device': downloads['Device'],
            'Units': downloads['Units'].astype(int),
            'Proceeds': downloads['Developer Proceeds'].astype(float),
            'Customer Price': downloads['Customer Price'],
            'Currency': downloads['Customer Currency'],
            'Product Type': downloads['Product Type Identifier'],
            'Promo Code': downloads['Promo Code']
        })
        
        # Installation Typeが存在する場合のみ追加
        if 'Installation Type' in downloads.columns:
            result['Install Type'] = downloads['Installation Type']
        elif 'Install Event' in downloads.columns:
            result['Install Type'] = downloads['Install Event']
        else:
            result['Install Type'] = 'N/A'
            print("注意: Installation Type列が見つかりません")
        
        # 追加の計算項目
        result['Year'] = pd.to_datetime(result['Date']).dt.year
        result['Month'] = pd.to_datetime(result['Date']).dt.month
        result['Week'] = pd.to_datetime(result['Date']).dt.isocalendar().week
        result['Weekday'] = pd.to_datetime(result['Date']).dt.day_name()
        
        # 地域マッピング
        region_map = {
            'JP': 'Asia', 'CN': 'Asia', 'KR': 'Asia', 'TW': 'Asia', 'HK': 'Asia',
            'US': 'North America', 'CA': 'North America', 'MX': 'North America',
            'GB': 'Europe', 'DE': 'Europe', 'FR': 'Europe', 'IT': 'Europe', 'ES': 'Europe',
            'BR': 'South America', 'AR': 'South America',
            'AU': 'Oceania', 'NZ': 'Oceania'
        }
        result['Region'] = result['Country'].map(region_map).fillna('Other')
        
        # カラムの順序を明示的に指定
        result = result[[
            'Date', 'Year', 'Month', 'Week', 'Weekday',
            'App Name', 'SKU', 'Country', 'Region', 'Device',
            'Install Type', 'Units', 'Proceeds', 'Customer Price',
            'Currency', 'Product Type', 'Promo Code'
        ]]
        
        # デバッグ情報を表示
        print(f"データフレームの形状: {result.shape}")
        print(f"データフレームのカラム: {result.columns.tolist()}")
        print(f"最初の行サンプル:")
        print(result.iloc[0].tolist())
        
        return result
    
    def connect_to_sheets(self):
        """Google Sheetsに接続"""
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds = Credentials.from_service_account_file(
            self.google_creds_path, scopes=scope)
        client = gspread.authorize(creds)
        
        return client.open(self.sheet_name)
    
    def save_to_sheets(self, df):
        """Google Sheetsにデータを保存"""
        if df.empty:
            print("保存するデータがありません")
            return
        
        print(f"\nGoogle Sheetsに保存中: {len(df)}行")
        
        try:
            spreadsheet = self.connect_to_sheets()
            worksheet = spreadsheet.sheet1
            
            # 既存データを取得
            existing_data = worksheet.get_all_values()
            
            expected_header = [
                'Date', 'Year', 'Month', 'Week', 'Weekday',
                'App Name', 'SKU', 'Country', 'Region', 'Device',
                'Install Type', 'Units', 'Proceeds', 'Customer Price',
                'Currency', 'Product Type', 'Promo Code'
            ]
            
            # ヘッダーがない場合は追加（修正箇所）
            if not existing_data or len(existing_data) == 0 or len(existing_data[0]) == 0 or existing_data[0][0] != 'Date':
                print(f"ヘッダーを追加: {len(expected_header)}列")
                worksheet.insert_row(expected_header, 1)
            else:
                print(f"既存のヘッダーが見つかりました")
            
            # データを変換
            data_to_append = df.values.tolist()
            
            print(f"\n追加するデータ:")
            print(f"  行数: {len(data_to_append)}")
            print(f"  各行のカラム数: {len(data_to_append[0]) if data_to_append else 0}")
            
            # データを追記
            worksheet.append_rows(data_to_append)
            
            print(f"\n✅ 保存完了: {len(df)}行")
            print(f"合計ダウンロード数: {df['Units'].sum()}")
            
        except Exception as e:
            print(f"\n❌ エラーが発生しました: {str(e)}")
            import traceback
            traceback.print_exc()
    
    def run(self, vendor_number, days_back=1):
        """メイン処理: データ取得とシート保存"""
        # 指定日数前の日付を取得
        target_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        print(f"=== App Store ダウンロード数取得開始 ===")
        print(f"対象日: {target_date}")
        
        # レポート取得
        report = self.get_sales_report(vendor_number, target_date)
        
        if report:
            # パース
            df = self.parse_report(report, target_date)
            
            if not df.empty:
                # Google Sheetsに保存
                self.save_to_sheets(df)
                
                # サマリー表示
                print("\n=== サマリー ===")
                print(df.groupby('App Name')['Units'].sum())
            else:
                print("データがありませんでした")
        else:
            print("レポートを取得できませんでした")

# メイン実行
def main():
    # 環境変数から取得（GitHub Actions用）
    KEY_ID = os.environ.get('KEY_ID')
    ISSUER_ID = os.environ.get('ISSUER_ID')
    PRIVATE_KEY_PATH = os.environ.get('PRIVATE_KEY_PATH', 'AuthKey.p8')
    VENDOR_NUMBER = os.environ.get('VENDOR_NUMBER')
    
    GOOGLE_CREDS_PATH = os.environ.get('GOOGLE_CREDS_PATH', 'credentials.json')
    SHEET_NAME = os.environ.get('SHEET_NAME', 'Daily Downloads')
    
    # デバッグ用（値が設定されているか確認）
    if not KEY_ID:
        print("エラー: KEY_ID環境変数が設定されていません")
        return
    if not ISSUER_ID:
        print("エラー: ISSUER_ID環境変数が設定されていません")
        return
    if not VENDOR_NUMBER:
        print("エラー: VENDOR_NUMBER環境変数が設定されていません")
        return
    
    print(f"KEY_ID: {KEY_ID[:5]}... (設定済み)")
    print(f"ISSUER_ID: {ISSUER_ID[:10]}... (設定済み)")
    print(f"VENDOR_NUMBER: {VENDOR_NUMBER} (設定済み)")
    
    tracker = AppStoreDownloadTracker(
        key_id=KEY_ID,
        issuer_id=ISSUER_ID,
        private_key_path=PRIVATE_KEY_PATH,
        google_creds_path=GOOGLE_CREDS_PATH,
        sheet_name=SHEET_NAME
    )
    
    # 昨日のデータを取得（days_back=1）
    tracker.run(vendor_number=VENDOR_NUMBER, days_back=3)

if __name__ == '__main__':
    main()
