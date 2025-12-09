import requests
import json

# ⚠️ DÁN API KEY CỦA BẠN VÀO ĐÂY
GOOGLE_API_KEY = "AIzaSyBfqLCa4_4t8nnsa7sUFke_9fpTWl_ZnwU"

def list_available_models():
    url = f"https://generativelanguage.googleapis.com/v1beta/models?key={GOOGLE_API_KEY}"
    
    try:
        response = requests.get(url)
        
        if response.status_code != 200:
            print(f"❌ Lỗi kết nối ({response.status_code}):")
            print(response.text)
            return

        data = response.json()
        models = data.get('models', [])
        
        print(f"\n✅ TÌM THẤY {len(models)} MODELS KHẢ DỤNG:")
        print("="*60)
        print(f"{'TÊN MODEL':<30} | {'CHỨC NĂNG HỖ TRỢ'}")
        print("-" * 60)
        
        valid_chat_models = []
        
        for m in models:
            name = m['name'].replace('models/', '')
            methods = m.get('supportedGenerationMethods', [])
            
            print(f"{name:<30} | {methods}")
            
            # Lưu lại các model có thể chat/sinh văn bản
            if 'generateContent' in methods:
                valid_chat_models.append(name)

        print("="*60)
        print("\n💡 GỢI Ý CÁC MODEL BẠN NÊN DÙNG CHO SCRIPT:")
        for vm in valid_chat_models:
             print(f"   - {vm}")

    except Exception as e:
        print(f"❌ Lỗi: {e}")

if __name__ == "__main__":
    list_available_models()